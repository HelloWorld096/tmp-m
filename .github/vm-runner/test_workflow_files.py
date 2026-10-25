"""The workflow must copy every runner module next to the agent: a module that is imported but not shipped crashes the
agent on the VM (the tests never see that, they run from the source directory)."""
import re
from pathlib import Path

HERE = Path(__file__).parent
WORKFLOW = HERE.parent / "workflows" / "vm-worker.yml"


def shipped_modules() -> set:
    m = re.search(r"for m in ([a-z_ ]+); do\s*\n\s*install -m 0644 \".github/vm-runner/\$\{m\}\.py\"", WORKFLOW.read_text())
    assert m, "could not find the module copy loop in vm-worker.yml"
    return set(m.group(1).split())


def runner_modules() -> set:
    return {p.stem for p in HERE.glob("vm_*.py")}


def test_every_vm_module_is_shipped_with_the_agent():
    assert shipped_modules() == runner_modules()


def test_agent_and_keepalive_are_installed_too():
    text = WORKFLOW.read_text()
    assert ".github/vm-runner/agent.py /tmp/vm-agent.py" in text
    assert ".github/vm-runner/keepalive.sh /tmp/vm-keepalive.sh" in text


def test_every_module_the_agent_and_modules_import_by_name_is_shipped():
    imported = set()
    for f in [HERE / "agent.py", *HERE.glob("vm_*.py")]:
        imported |= set(re.findall(r"^\s*(?:import|from) (vm_[a-z_]+)", f.read_text(), flags=re.M))
    assert imported <= shipped_modules(), imported - shipped_modules()
