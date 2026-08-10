"""Version compatibility primitives for immutable dataset generations."""

from __future__ import annotations

from dataclasses import dataclass

PACKAGE_VERSION = "0.1.0"


@dataclass(frozen=True, order=True)
class SemanticVersion:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: str) -> SemanticVersion:
        parts = value.split(".")
        if len(parts) != 3 or not all(part.isdigit() for part in parts):
            raise ValueError(f"Invalid semantic version: {value!r}")
        return cls(*(int(part) for part in parts))

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


def require_compatible_major(left: str, right: str) -> None:
    """Reject an implicit union of incompatible dataset schema generations."""
    if SemanticVersion.parse(left).major != SemanticVersion.parse(right).major:
        raise ValueError(f"Incompatible major versions: {left} and {right}")
