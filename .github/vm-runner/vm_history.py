"""Durable history of every command the agent was asked to run (SQLite, on the HOST).

Why it lives here and not in the session container:
  * The only host path the session container can see is the jobs directory (agent.JOBS_DIR). This database sits
    outside it (default ~/.vm-agent/history.db, directory mode 0700), so a command running in the container cannot
    read, edit or delete it.
  * A snapshot carries the whole database as one extra tar member; the container's own files can never supply that
    member (the exporter drops any such name), so a restored session gets back exactly what the host recorded.

What is recorded: every submission to POST /jobs, accepted or rejected (busy, restoring, capturing, ...), with the
command text, so that any attempt can be replayed by id. Relay transfer jobs are not recorded (they carry tokens and
are not user commands).

Replays are ordinary submissions: they pass the same validation, get their own job id and their own row
(kind='replay', replay_of=<original id>).
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

SCHEMA_VERSION = 1
OUTPUT_TAIL_CHARS = 2_000
MAX_IMPORT_BYTES = 256 * 1024 * 1024
COLUMNS = (
    "seq", "job_id", "request_id", "kind", "replay_of", "command", "timeout_seconds", "outcome", "reject_reason",
    "status", "exit_code", "cwd", "submitted_at", "ended_at", "output_tail",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS jobs (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL UNIQUE,
    request_id      TEXT,
    kind            TEXT NOT NULL,
    replay_of       TEXT,
    command         TEXT NOT NULL,
    timeout_seconds INTEGER,
    outcome         TEXT NOT NULL,
    reject_reason   TEXT,
    status          TEXT NOT NULL,
    exit_code       INTEGER,
    cwd             TEXT,
    submitted_at    REAL NOT NULL,
    ended_at        REAL,
    output_tail     TEXT
);
CREATE INDEX IF NOT EXISTS jobs_request ON jobs(request_id);
"""


class HistoryError(Exception):
    pass


def default_path() -> Path:
    env = os.environ.get("VM_HISTORY_DB", "").strip()
    return Path(env) if env else Path.home() / ".vm-agent" / "history.db"


class HistoryStore:
    """Thread-safe; one short-lived connection per operation (the agent serves requests on worker threads)."""

    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path) if path is not None else default_path()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        self._lock = threading.Lock()
        db = self._connect()
        try:
            db.executescript(_SCHEMA)
            db.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),))
        finally:
            db.close()
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA trusted_schema=OFF")
        return db

    def _run(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            db = self._connect()
            try:
                return db.execute(sql, params)
            finally:
                db.close()

    def _all(self, sql: str, params: tuple = ()) -> list:
        with self._lock:
            db = self._connect()
            try:
                return db.execute(sql, params).fetchall()
            finally:
                db.close()

    # ---- recording ----
    def record_attempt(self, job_id: str, command: str, timeout_seconds: Optional[int], request_id: Optional[str] = None,
                       kind: str = "run", replay_of: Optional[str] = None, rejected: Optional[str] = None) -> None:
        """One row per submission. ``rejected`` is the reason when the agent refused it (the command did NOT run)."""
        now = time.time()
        self._run(
            "INSERT INTO jobs(job_id, request_id, kind, replay_of, command, timeout_seconds, outcome, reject_reason,"
            " status, submitted_at, ended_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (job_id, request_id, kind, replay_of, command, timeout_seconds,
             "rejected" if rejected else "accepted", rejected, "rejected" if rejected else "running", now,
             now if rejected else None),
        )

    def finish(self, job_id: str, status: str, exit_code: Optional[int], cwd: Optional[str], output_tail: str) -> None:
        self._run(
            "UPDATE jobs SET status=?, exit_code=?, cwd=?, ended_at=?, output_tail=? WHERE job_id=?",
            (status, exit_code, cwd, time.time(), output_tail[-OUTPUT_TAIL_CHARS:], job_id),
        )

    def interrupt_running(self, why: str = "interrupted") -> int:
        """Mark rows still 'running' (the agent or VM went away under them) so they are never shown as live."""
        return self._run("UPDATE jobs SET status=?, ended_at=? WHERE status='running'", (why, time.time())).rowcount

    # ---- reading ----
    @staticmethod
    def _row(r: sqlite3.Row) -> dict:
        return {k: r[k] for k in COLUMNS}

    def get(self, job_id: str) -> Optional[dict]:
        rows = self._all("SELECT * FROM jobs WHERE job_id=?", (job_id,))
        return self._row(rows[0]) if rows else None

    def recent(self, limit: int = 10) -> list:
        """Newest first."""
        limit = max(1, min(int(limit), 500))
        return [self._row(r) for r in self._all("SELECT * FROM jobs ORDER BY seq DESC LIMIT ?", (limit,))]

    def count(self) -> int:
        return int(self._all("SELECT COUNT(*) FROM jobs")[0][0])

    # ---- snapshots ----
    def export_bytes(self) -> bytes:
        """A consistent copy of the whole database (SQLite online backup), as bytes."""
        fd, tmp = tempfile.mkstemp(prefix="history-export-", suffix=".db", dir=str(self.path.parent))
        os.close(fd)
        try:
            with self._lock:
                src = self._connect()
                dst = sqlite3.connect(tmp)
                try:
                    src.backup(dst)
                finally:
                    dst.close()
                    src.close()
            return Path(tmp).read_bytes()
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def import_bytes(self, data: bytes) -> int:
        """Load an exported copy into this database; returns the number of rows imported from it.

        The bytes come out of a snapshot, so they are validated like outside input: size cap, SQLite header, schema
        version, and only the known columns of the known table are read. Rows that were 'running' when the snapshot
        was taken are marked 'interrupted': that process is gone."""
        if len(data) > MAX_IMPORT_BYTES:
            raise HistoryError("history database in the snapshot is too large")
        if not data.startswith(b"SQLite format 3\x00"):
            raise HistoryError("history in the snapshot is not a SQLite database")
        fd, tmp = tempfile.mkstemp(prefix="history-import-", suffix=".db", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            probe = sqlite3.connect(f"file:{tmp}?mode=ro", uri=True)
            try:
                probe.execute("PRAGMA trusted_schema=OFF")
                ver = probe.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
                if not ver or int(ver[0]) > SCHEMA_VERSION:
                    raise HistoryError("history in the snapshot has an unsupported schema version")
                cols = ", ".join(COLUMNS)
                rows = probe.execute(f"SELECT {cols} FROM jobs ORDER BY seq").fetchall()
            except (sqlite3.Error, ValueError) as err:
                raise HistoryError(f"history in the snapshot could not be read: {err}") from err
            finally:
                probe.close()
            with self._lock:
                db = self._connect()
                try:
                    db.execute("BEGIN IMMEDIATE")
                    try:
                        # Attempts recorded here before the import (e.g. commands refused while the session was
                        # still restoring) are kept and ordered AFTER the restored history.
                        local = [list(r) for r in db.execute(f"SELECT {', '.join(COLUMNS)} FROM jobs ORDER BY seq")]
                        db.execute("DELETE FROM jobs")
                        db.execute("DELETE FROM sqlite_sequence WHERE name='jobs'")
                        marks = ",".join("?" for _ in COLUMNS)
                        si, ei = COLUMNS.index("status"), COLUMNS.index("ended_at")
                        for r in rows:
                            values = list(r)
                            if values[si] == "running":
                                values[si] = "interrupted"
                                values[ei] = values[ei] or time.time()
                            db.execute(f"INSERT OR IGNORE INTO jobs({cols}) VALUES ({marks})", values)
                        rest = ", ".join(COLUMNS[1:])           # without seq: they get fresh numbers after the restored rows
                        rest_marks = ",".join("?" for _ in COLUMNS[1:])
                        for values in local:
                            db.execute(f"INSERT OR IGNORE INTO jobs({rest}) VALUES ({rest_marks})", values[1:])
                        db.execute("COMMIT")
                    except BaseException:
                        db.execute("ROLLBACK")
                        raise
                finally:
                    db.close()
            return len(rows)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def public_view(row: dict, with_command: bool = True) -> dict:
    """What the API shows for a history row."""
    out = {
        "job_id": row["job_id"], "kind": row["kind"], "replay_of": row["replay_of"], "outcome": row["outcome"],
        "reject_reason": row["reject_reason"], "status": row["status"], "exit_code": row["exit_code"],
        "cwd": row["cwd"], "submitted_at": row["submitted_at"], "ended_at": row["ended_at"],
        "timeout_seconds": row["timeout_seconds"],
    }
    if with_command:
        out["command"] = row["command"]
    return out
