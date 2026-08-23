from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path

class ArtifactResolver(ABC):
    @abstractmethod
    def resolve(self, uri: str) -> Path: ...

class LocalArtifactResolver(ArtifactResolver):
    """Maps exact artifact:// identities to configured immutable local directories."""
    def __init__(self, records: dict[str, Path] | None = None): self.records = records or {}
    def resolve(self, uri: str) -> Path:
        if uri.startswith("file://"): return Path(uri[7:])
        if uri not in self.records: raise FileNotFoundError(f"unresolvable exact artifact URI: {uri}")
        return self.records[uri]

class ArtifactPublisher(ABC):
    @abstractmethod
    def publish(self, artifact_dir: Path) -> str: ...
