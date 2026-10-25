"""Deep snapshots: manifests, image VOLUME data and runtime config are captured on request, applied only when asked, and
a restored container is compared with what was saved. fake_docker + a canned manifest (the fake runs `docker exec` on the
host, so the real package databases are never consulted)."""
import json
import os

import pytest

os.environ["VM_SNAPSHOT_TEST"] = "1"
import vm_deep  # noqa: E402
import vm_container as vc  # noqa: E402
from test_container import env, make, rootfs, IMG  # noqa: E402,F401
from test_snapshot import gh  # noqa: E402,F401
from test_session import manager, mw, bundle, boot_sync, write_file  # noqa: E402,F401

MANIFEST = "### dpkg\ncurl\t8.5\tamd64\ngit\t2.43\tamd64\n### apt-manual\ncurl\ngit\n### pip\nrequests==2.32.0\n### cargo\n"


def canned(c, text=MANIFEST):
    c.exec_text = lambda script, timeout=0: text
    return c


def set_inspect(env, c, **info):
    d = env["state"] / "containers" / c.name
    (d / "inspect.json").write_text(json.dumps(info))


VOLUME_INFO = {
    "Config": {"Image": IMG, "Env": ["PATH=/usr/bin", "GH_TOKEN=ghs_secret", "MY_PASSWORD=pw", "LANG=C.UTF-8"], "User": "dev"},
    "Mounts": [{"Type": "volume", "Destination": "/var/lib/data"}, {"Type": "bind", "Destination": "/host"}],
}


def test_collect_deep_records_manifests_env_without_secrets_and_volume_data(gh, env):
    c = canned(make(env)); c.create()
    set_inspect(env, c, **VOLUME_INFO)
    (rootfs(env, c) / "var/lib/data").mkdir(parents=True)
    (rootfs(env, c) / "var/lib/data/rows.db").write_bytes(b"volume payload")
    extras = c.collect_deep()
    deep = vm_deep.parse(extras["deep.json"])
    assert deep["manifests"]["dpkg"] == ["curl\t8.5\tamd64", "git\t2.43\tamd64"] and deep["manifests"]["cargo"] == []
    assert deep["env"] == {"PATH": "/usr/bin", "LANG": "C.UTF-8"}
    assert deep["env_dropped"] == ["GH_TOKEN", "MY_PASSWORD"]
    assert b"ghs_secret" not in extras["deep.json"] and b"pw" not in extras["deep.json"].replace(b"password", b"")
    assert deep["user"] == "dev" and deep["image_id"] == "sha256:fakeimage"
    assert [v["dest"] for v in deep["volumes"]] == ["/var/lib/data"]       # the bind mount is not a volume
    assert b"volume payload" in extras["volume-0.tar"]


def test_apply_deep_restores_volume_data_and_reports_clean_when_packages_match(gh, env):
    c = canned(make(env)); c.create()
    set_inspect(env, c, **VOLUME_INFO)
    (rootfs(env, c) / "var/lib/data").mkdir(parents=True)
    (rootfs(env, c) / "var/lib/data/rows.db").write_bytes(b"volume payload")
    extras = c.collect_deep()
    c2 = canned(make(env, sid="other")); c2.create()
    res = c2.apply_deep(extras)
    assert res["volumes"] == 1 and res["report"]["clean"] is True
    assert (rootfs(env, c2) / "var/lib/data/rows.db").read_bytes() == b"volume payload"
    assert res["summary"] == "installed packages match the snapshot"


def test_apply_deep_reports_drift_but_changes_nothing_else(gh, env):
    c = canned(make(env)); c.create()
    extras = c.collect_deep()
    c2 = canned(make(env, sid="other"), "### dpkg\ncurl\t8.5\tamd64\nvim\t9.0\tamd64\n### pip\n### cargo\n"); c2.create()
    res = c2.apply_deep(extras)
    rep = res["report"]
    assert rep["clean"] is False
    assert rep["sections"]["dpkg"]["missing_examples"] == ["git\t2.43\tamd64"]
    assert rep["sections"]["dpkg"]["unexpected_examples"] == ["vim\t9.0\tamd64"]
    assert rep["sections"]["pip"]["missing"] == 1
    assert "drift" in res["summary"]


def test_oversized_volume_is_skipped_and_recorded(gh, env, monkeypatch):
    monkeypatch.setattr(vm_deep, "MAX_VOLUME_BYTES", 2000)
    c = canned(make(env)); c.create()
    set_inspect(env, c, **VOLUME_INFO)
    (rootfs(env, c) / "var/lib/data").mkdir(parents=True)
    (rootfs(env, c) / "var/lib/data/big").write_bytes(os.urandom(20000))
    extras = c.collect_deep()
    deep = vm_deep.parse(extras["deep.json"])
    assert deep["volumes"] == [] and deep["volumes_skipped"][0]["dest"] == "/var/lib/data"
    assert "volume-0.tar" not in extras


def test_bad_deep_json_is_refused():
    for raw in (b"not json", b'{"schema": 99, "manifests": {}}', b'{"schema": 1}'):
        with pytest.raises(ValueError):
            vm_deep.parse(raw)


def test_unsafe_volume_paths_never_reach_the_container(gh, env):
    raw = json.dumps({"schema": 1, "manifests": {}, "volumes": [
        {"dest": "/../etc", "extra": "volume-0.tar", "bytes": 1}, {"dest": "relative", "extra": "x", "bytes": 1},
        {"dest": "/ok/path", "extra": "volume-1.tar", "bytes": 1}]}).encode()
    assert [v["dest"] for v in vm_deep.parse(raw)["volumes"]] == ["/ok/path"]


# --------------------------------------------------------------------------- through the session manager
def test_deep_save_then_deep_start_applies_and_reports(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    canned(c)
    boot_sync(m)
    write_file(env, c, "a.txt", "work\n")
    m.save("manual", deep=True)
    assert m.status()["deep"]["last_save_deep"] is True
    m2, c2, _ = manager(env, mw, tmp_path / "second", b=bundle(epoch=2, deep=True))
    canned(c2)
    assert boot_sync(m2) == "ready"
    d = m2.status()["deep"]
    assert "deep apply" in d["note"] and "match" in d["note"] and d["drift"]["clean"] is True
    assert (rootfs(env, c2) / "workspace" / "a.txt").read_text() == "work\n"
    assert not (rootfs(env, c2) / ".vm-snapshot").exists()


def test_deep_snapshot_started_without_the_flag_is_a_normal_restore(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    canned(c)
    boot_sync(m)
    write_file(env, c)
    m.save("manual", deep=True)
    m2, c2, _ = manager(env, mw, tmp_path / "second", b=bundle(epoch=2))
    called = []
    c2.apply_deep = lambda extras: called.append(1)
    assert boot_sync(m2) == "ready"
    assert not called and "not requested" in m2.status()["deep"]["note"]


def test_deep_start_of_a_normal_snapshot_says_so_and_still_restores(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c, "a.txt", "plain\n")
    m.save("manual")
    m2, c2, _ = manager(env, mw, tmp_path / "second", b=bundle(epoch=2, deep=True))
    assert boot_sync(m2) == "ready"
    assert "normal one" in m2.status()["deep"]["note"]
    assert (rootfs(env, c2) / "workspace" / "a.txt").read_text() == "plain\n"


def test_a_failing_deep_apply_never_fails_the_session(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    canned(c)
    boot_sync(m)
    write_file(env, c)
    m.save("manual", deep=True)
    m2, c2, _ = manager(env, mw, tmp_path / "second", b=bundle(epoch=2, deep=True))
    def boom(extras): raise RuntimeError("manifest tool exploded")
    c2.apply_deep = boom
    assert boot_sync(m2) == "ready"
    assert "failed after the filesystem was restored" in m2.status()["deep"]["note"]


def test_deep_flag_in_the_claim_bundle_is_parsed():
    import vm_session as vsn
    raw = json.dumps({"token": "t", "session": {"id": "s_ab12cd34", "epoch": 2, "repo": "o/vmsession-s_ab12cd34",
                                                "image": "ubuntu", "repo_token": "ghs_x", "deep": True,
                                                "repo_token_expires_at": "2026-10-22T10:00:00Z"}})
    assert vsn.parse_bundle(raw).deep is True
