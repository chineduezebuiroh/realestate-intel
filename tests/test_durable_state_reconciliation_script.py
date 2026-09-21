from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts/audit/durable_state_reconciliation.py"
SPEC = importlib.util.spec_from_file_location("durable_state_reconciliation", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_audit_uses_fixed_safe_output_and_expected_authority() -> None:
    assert MODULE.AUDIT_ROOT == Path("/tmp/realestate-intel-durable-audit")
    assert MODULE.EXPECTED_BRANCH == "monthly-refresh-orchestration"
    assert MODULE.EXPECTED_HEAD == "860b077cb3c4c637f5b9404e3b7cb843cf9688f2"


def test_read_only_command_allowlist_accepts_audit_reads() -> None:
    assert MODULE.command_is_read_only(
        ["git", "status", "--porcelain", "--untracked-files=no"]
    )
    assert MODULE.command_is_read_only(
        ["git", "fetch", "origin", "--prune", "--tags"]
    )
    assert MODULE.command_is_read_only(["gh", "api", "repos/example/project"])
    assert MODULE.command_is_read_only(["gh", "auth", "status"])


def test_read_only_command_allowlist_rejects_mutations() -> None:
    forbidden = [
        ["git", "checkout", "main"],
        ["git", "commit", "-m", "no"],
        ["git", "push", "origin", "main"],
        ["gh", "api", "--method", "POST", "repos/example/project/actions/workflows/x/dispatches"],
        ["gh", "api", "-X", "DELETE", "repos/example/project/releases/1"],
        ["gh", "api", "repos/example/project", "-f", "name=value"],
    ]
    assert all(not MODULE.command_is_read_only(command) for command in forbidden)


def test_tracked_status_explicitly_ignores_untracked(monkeypatch) -> None:
    observed: list[tuple[str, ...]] = []

    def fake_git(root: Path, *args: str) -> str:
        observed.append(args)
        return ""

    monkeypatch.setattr(MODULE, "_git", fake_git)
    assert MODULE._tracked_status(Path("/repo")) == ""
    assert observed == [("status", "--porcelain", "--untracked-files=no")]
