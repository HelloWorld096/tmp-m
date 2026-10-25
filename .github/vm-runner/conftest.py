"""Test isolation. The suite runs real process sweeps (agent.sweep_marked_processes) and, through fake_docker, container
sweeps. On an agent-run VM the shell that runs pytest itself carries VM_AGENT_JOB, so an unscoped sweep kills the job
running the tests (exit 137). Give this run a unique scope: jobs started by the tests inherit it, nothing else has it.
fake_docker confines its own sweep the same way (FAKE_DOCKER_SCOPE)."""
import os
import uuid

os.environ["VM_AGENT_SWEEP_SCOPE"] = uuid.uuid4().hex

# History database of the agent: never the real ~/.vm-agent during tests. One scratch directory per run; tests that
# need a clean history pass their own vm_history.HistoryStore to create_app.
import tempfile  # noqa: E402

os.environ.setdefault("VM_HISTORY_DB", os.path.join(tempfile.mkdtemp(prefix="vm-history-test-"), "history.db"))
