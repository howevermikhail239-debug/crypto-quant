"""Safe external data-root handling; project repository is never a data store by default."""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_data_root(value: str | Path, *, allow_inside_repository: bool) -> Path:
    root = Path(value).expanduser().resolve(strict=False)
    repo = repository_root().resolve()
    if not allow_inside_repository:
        try:
            root.relative_to(repo)
        except ValueError:
            pass
        else:
            raise ValueError("data_root must be outside the repository unless explicitly allowed")
    return root


RUNTIME_TREE = (
    "raw",
    "normalized",
    "quarantine",
    "spool",
    "logs",
    "control/manifests",
    "control/checkpoints",
    "control/gap_registry",
    "control/deletion_ledger",
    "control/schema_registry",
)


def initialize_data_root(root: Path) -> list[Path]:
    """Create the approved empty runtime tree; never creates market dataset files."""
    created: list[Path] = []
    for relative in RUNTIME_TREE:
        target = root / relative
        target.mkdir(parents=True, exist_ok=True)
        created.append(target)
    return created


def disk_free_bytes(root: Path) -> int:
    if os.name == "nt":
        return shutil.disk_usage(root).free
    stats = os.statvfs(root)
    return stats.f_bavail * stats.f_frsize


def disk_free_percent(root: Path) -> float:
    usage = shutil.disk_usage(root)
    return round(usage.free / usage.total * 100, 2)


def tree_size_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def atomic_rename_probe(root: Path) -> bool:
    """Create/remove a zero-data temporary probe only under an already-existing root."""
    if not root.is_dir():
        raise ValueError("atomic rename probe requires an existing data root")
    source = root / f".atomic-probe-{uuid.uuid4().hex}.partial"
    target = source.with_suffix(".ok")
    try:
        source.write_bytes(b"probe")
        source.replace(target)
        return target.read_bytes() == b"probe"
    finally:
        source.unlink(missing_ok=True)
        target.unlink(missing_ok=True)
