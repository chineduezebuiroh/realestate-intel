"""Guard the repository's cross-version monthly timestamp convention."""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd

from regime.pandas_compat import MONTH_END


def _literal_month_end_aliases(path: Path) -> list[int]:
    """Return date_range lines that use pandas' version-sensitive ``ME`` alias."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        name = function.attr if isinstance(function, ast.Attribute) else (
            function.id if isinstance(function, ast.Name) else None
        )
        if name != "date_range":
            continue
        for keyword in node.keywords:
            if (
                keyword.arg == "freq"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value == "ME"
            ):
                violations.append(node.lineno)
    return violations


def main() -> None:
    dates = pd.date_range("2015-01-31", periods=132, freq=MONTH_END)
    assert len(dates) == 132
    assert dates.is_monotonic_increasing
    assert dates.is_month_end.all()

    months = dates.to_period("M")
    assert len(months) == len(dates)
    assert months.is_monotonic_increasing
    assert (months[6:] == months[:-6] + 6).all()

    violations: list[str] = []
    for root in (Path("regime"), Path("scripts")):
        for path in root.rglob("*.py"):
            violations.extend(
                f"{path}:{line}" for line in _literal_month_end_aliases(path)
            )
    assert not violations, "unsupported date_range freq='ME': " + ", ".join(violations)
    print("pandas monthly frequency compatibility smoke: PASS")


if __name__ == "__main__":
    main()
