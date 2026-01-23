from __future__ import annotations
# forecast/contracts/serialize.py

import json
from typing import Any


def json_dumps_canonical(obj: Any) -> str:
    """
    Canonical JSON for hashing / equality checks:
    - sorted keys
    - compact separators
    - stable floats handled by json module default repr
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
