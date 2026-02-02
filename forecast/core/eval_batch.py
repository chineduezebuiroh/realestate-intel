from __future__ import annotations
# forecast/core/eval_batch.py

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def new_eval_batch_id(prefix: str = "eval") -> str:
    # UTC timestamp so it's stable across machines/timezones
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}"


def eval_out_dir(artifact_root: str, eval_batch_id: str) -> Path:
    return Path(artifact_root) / eval_batch_id


def _safe_slug(s: str) -> str:
    s = str(s)
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s[:120]


def make_run_batch_id(
    eval_batch_id: str,
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    anchor_date: str,
    horizon: int,
    seed: int,
) -> str:
    # Keep it readable but safe; selector runner will refuse overwrite anyway.
    return _safe_slug(
        f"{eval_batch_id}__sel__{metric_id}__{geo_id}__pt{property_type_id}__a{anchor_date}__h{horizon}__s{seed}"
    )


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True, default=str) + "\n")
