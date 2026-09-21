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
    assert MODULE.EXPECTED_BASE_BRANCH == "monthly-refresh-orchestration"
    assert MODULE.EXPECTED_BASE_SHA == "860b077cb3c4c637f5b9404e3b7cb843cf9688f2"


def test_read_only_command_allowlist_accepts_audit_reads() -> None:
    assert MODULE.command_is_read_only(
        ["git", "status", "--porcelain", "--untracked-files=no"]
    )
    assert MODULE.command_is_read_only(
        ["git", "fetch", "origin", "--prune", "--tags"]
    )
    assert MODULE.command_is_read_only(
        ["git", "merge-base", "--is-ancestor", MODULE.EXPECTED_BASE_SHA, "HEAD"]
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


def _mock_checkout(monkeypatch, *, head: str, ancestor: bool, status: str = "") -> None:
    values = {
        ("branch", "--show-current"): "feature/audit",
        ("rev-parse", "HEAD"): head,
        ("rev-parse", "--verify", f"{MODULE.EXPECTED_BASE_SHA}^{{commit}}"): MODULE.EXPECTED_BASE_SHA,
        ("status", "--porcelain", "--untracked-files=no"): status,
    }
    monkeypatch.setattr(MODULE, "_git", lambda root, *args: values[args])

    class Result:
        returncode = 0 if ancestor else 1

    monkeypatch.setattr(MODULE, "_run", lambda *args, **kwargs: Result())


def test_exact_base_checkout_is_allowed(monkeypatch) -> None:
    _mock_checkout(monkeypatch, head=MODULE.EXPECTED_BASE_SHA, ancestor=True)
    context = MODULE._validate_checkout(Path("/repo"))
    assert context["execution_head"] == MODULE.EXPECTED_BASE_SHA


def test_pr_head_descending_from_base_is_allowed(monkeypatch) -> None:
    pr_head = "e" * 40
    _mock_checkout(monkeypatch, head=pr_head, ancestor=True)
    context = MODULE._validate_checkout(Path("/repo"))
    assert context["execution_branch"] == "feature/audit"
    assert context["execution_head"] == pr_head
    assert context["expected_base_sha"] == MODULE.EXPECTED_BASE_SHA


def test_unrelated_head_is_rejected(monkeypatch) -> None:
    _mock_checkout(monkeypatch, head="f" * 40, ancestor=False)
    try:
        MODULE._validate_checkout(Path("/repo"))
    except RuntimeError as exc:
        assert "does not descend" in str(exc)
    else:
        raise AssertionError("unrelated HEAD was accepted")


def test_tracked_modifications_are_rejected(monkeypatch) -> None:
    _mock_checkout(monkeypatch, head="e" * 40, ancestor=True, status=" M tracked.py")
    try:
        MODULE._validate_checkout(Path("/repo"))
    except RuntimeError as exc:
        assert "tracked working tree is not clean" in str(exc)
    else:
        raise AssertionError("tracked modification was accepted")


def test_untracked_files_are_ignored_by_checkout_guard(monkeypatch) -> None:
    _mock_checkout(monkeypatch, head="e" * 40, ancestor=True, status="")
    MODULE._validate_checkout(Path("/repo"))
