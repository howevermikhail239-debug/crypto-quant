"""Read-only analytical catalog over authoritative external datasets."""

from .catalog import ActiveArtifact, CatalogBuildResult, build_catalog, resolve_active_artifacts

__all__ = [
    "ActiveArtifact",
    "CatalogBuildResult",
    "build_catalog",
    "resolve_active_artifacts",
]
