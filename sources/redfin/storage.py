from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .governance import RAW_ROOT, GovernanceError, assert_safe_delete, bootstrap

RAW_SUFFIXES = (".csv", ".tsv", ".tsv000", ".gz")


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def raw_files(folder: Path) -> list[Path]:
    return sorted(p for p in folder.iterdir() if p.is_file() and any(p.name.endswith(s) for s in RAW_SUFFIXES))


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise GovernanceError(f"invalid or missing metadata: {path}") from exc


def current(root: Path = RAW_ROOT) -> dict | None:
    path = root / "current" / "current.json"
    return read_json(path) if path.exists() else None


def promote(drop_id: str, root: Path = RAW_ROOT) -> None:
    metadata = read_json(root / "drops" / drop_id / "metadata.json")
    pointer = current(root)
    if pointer and pointer.get("promoted_drop") == drop_id and metadata.get("status") == "promoted":
        return
    if metadata.get("status") != "published":
        raise GovernanceError("only a successfully published drop may be promoted")
    metadata["status"] = metadata["promotion_status"] = "promoted"
    atomic_json(root / "drops" / drop_id / "metadata.json", metadata)
    promoted_at = datetime.now(timezone.utc).isoformat()
    atomic_json(root / "current" / "current.json", {
        "baseline": "2026-07", "promoted_drop": drop_id,
        "status": "validated_and_published", "promoted_at": promoted_at,
    })
    history_path = root / "current" / "history.json"
    history = read_json(history_path) if history_path.exists() else {"promotions": []}
    if not any(item.get("drop_id") == drop_id for item in history["promotions"]):
        history["promotions"].append({"drop_id":drop_id,"baseline_id":"2026-07","promoted_at":promoted_at,"candidate_rows":metadata.get("candidate_rows"),"governed_geographies":metadata.get("governed_geographies",[]),"latest_month":metadata.get("latest_month",drop_id),"source_hashes":{item["filename"]:item["sha256"] for item in metadata.get("files",[])},"publication_status":metadata.get("publication_status")})
        atomic_json(history_path, history)


def retain(root: Path = RAW_ROOT, keep: int = 3, quarantine_days: int = 90, dry_run: bool = True) -> list[str]:
    bootstrap(root)
    eligible = []
    drops = []
    all_drops = sorted((folder for folder in (root / "drops").iterdir() if folder.is_dir()), reverse=True)
    newest_complete = False
    for folder in all_drops:
        if folder.is_dir() and (folder / "metadata.json").exists():
            meta = read_json(folder / "metadata.json")
            if meta.get("status") in {"published", "promoted"}:
                drops.append(folder)
    if all_drops and (all_drops[0] / "metadata.json").exists():
        newest = read_json(all_drops[0] / "metadata.json")
        newest_complete = newest.get("status") == "promoted" and newest.get("publication_status") == "published" and newest.get("promotion_status") == "promoted"
    if newest_complete:
        eligible.extend(drops[keep:])
    cutoff = datetime.now(timezone.utc) - timedelta(days=quarantine_days)
    for folder in (root / "quarantine").iterdir():
        if folder.is_dir() and datetime.fromtimestamp(folder.stat().st_mtime, timezone.utc) < cutoff:
            eligible.append(folder)
    for path in eligible:
        assert_safe_delete(path, root)
        if not dry_run:
            if path.parent == root / "drops":
                if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", path.name):
                    raise GovernanceError(f"invalid drop retention target: {path}")
                archive_path = root / "current" / "drop_history.json"
                archive = read_json(archive_path) if archive_path.exists() else {"drops": []}
                metadata = read_json(path / "metadata.json")
                if not any(item.get("drop_id") == metadata.get("drop_id") for item in archive["drops"]):
                    archive["drops"].append(metadata); archive["drops"] = archive["drops"][-12:]; atomic_json(archive_path, archive)
            shutil.rmtree(path)
    return [str(p) for p in eligible]


def quarantine(drop_id: str, reason: str, root: Path = RAW_ROOT) -> Path:
    """Move one failed drop into quarantine without ever touching protected roots."""
    source = root / "drops" / drop_id
    if not source.is_dir():
        raise GovernanceError(f"drop does not exist: {drop_id}")
    destination = root / "quarantine" / drop_id
    if destination.exists():
        raise GovernanceError(f"quarantine destination already exists: {drop_id}")
    meta_path = source / "metadata.json"
    metadata = read_json(meta_path) if meta_path.exists() else {"drop_id": drop_id}
    metadata.update(status="quarantine", quarantine_reason=reason, quarantined_at=datetime.now(timezone.utc).isoformat())
    atomic_json(meta_path, metadata)
    source.replace(destination)
    return destination
