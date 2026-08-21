"""Deployment smoke for an authoritative Macro Regime static-site build."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.build_macro_regime_site import DC_GEO_ID, build
from visualization.regime_snapshot import SCHEMA_VERSION, VISUALIZATION_VERSION


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test deployment using an explicit authoritative run.")
    parser.add_argument("--run", type=Path, required=True)
    args = parser.parse_args()
    with TemporaryDirectory(prefix="macro-regime-deploy-smoke-") as temporary:
        first, second = Path(temporary) / "first", Path(temporary) / "second"
        build(args.run, first)
        build(args.run, second)
        expected = ["index.html", "manifest.json", f"counties/{DC_GEO_ID}.html",
                    f"counties/{DC_GEO_ID}_snapshot.json"]
        assert all((first / path).is_file() for path in expected)
        assert all(digest(first / path) == digest(second / path) for path in expected)

        manifest = json.loads((first / "manifest.json").read_text(encoding="utf-8"))
        snapshot = json.loads((first / expected[-1]).read_text(encoding="utf-8"))
        assert manifest["source_run_id"] == args.run.resolve().name == snapshot["run_id"]
        assert manifest["schema_version"] == SCHEMA_VERSION == snapshot["schema_version"]
        assert manifest["visualization_version"] == VISUALIZATION_VERSION == snapshot["visualization_version"]
        assert manifest["generated_counties"] == [{"geo_id": DC_GEO_ID, "market_name": "Washington DC"}]
        assert snapshot["geo_id"] == DC_GEO_ID and snapshot["as_of_date"]
        assert {row["path"] for row in manifest["outputs"]} == set(expected) - {"manifest.json"}
        for row in manifest["outputs"]:
            output = first / row["path"]
            assert digest(output) == row["sha256"] and output.stat().st_size == row["size_bytes"]

        index = (first / "index.html").read_text(encoding="utf-8")
        county = (first / expected[2]).read_text(encoding="utf-8")
        published = "\n".join((first / path).read_text(encoding="utf-8") for path in expected)
        assert f'counties/{DC_GEO_ID}.html' in index
        assert 'href="../index.html"' in county
        assert "plotly" in county.lower()
        # Embedded Plotly legitimately contains the literal string "file://"
        # in origin/security checks. Reject actual published file-protocol
        # references and machine-local paths rather than the library literal.
        assert "/Users/" not in published
        assert not re.search(
            r'(?i)(?:href|src)\s*=\s*["\']file://',
            published,
        )
        assert not re.search(
            r'(?<![\w-])/(?:home|workspace|tmp|var)/',
            published,
        )
    print("[macro_regime_static_deployment] authoritative static publication: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
