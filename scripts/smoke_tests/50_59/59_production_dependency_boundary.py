from __future__ import annotations

import ast
from pathlib import Path


def main() -> int:
    violations: list[str] = []
    for path in sorted(Path("regime").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            module = ""
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("regime.experiments"):
                        violations.append(f"{path}:{node.lineno}: {alias.name}")
            if module.startswith("regime.experiments"):
                violations.append(f"{path}:{node.lineno}: {module}")

    if violations:
        raise AssertionError(
            "Production modules import experiment implementations:\n"
            + "\n".join(violations)
        )

    print("[production_dependency_boundary] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
