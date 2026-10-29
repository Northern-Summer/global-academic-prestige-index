"""Storage boundaries for acquisition-only raw source material."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def require_external_cache(cache_dir: Path) -> Path:
    """Reject persistent source caches inside the repository tree."""
    resolved = cache_dir.expanduser().resolve()
    try:
        resolved.relative_to(project_root())
    except ValueError:
        return resolved
    raise ValueError(
        "raw-source caches must be outside the project directory; "
        "omit the cache option for temporary processing or choose an external scratch directory"
    )


def temporary_raw_cache(prefix: str) -> tempfile.TemporaryDirectory[str]:
    """Create an automatically removed raw cache outside the project tree."""
    configured_root = os.environ.get("GAPI_SCRATCH_DIR")
    temporary_root = require_external_cache(Path(configured_root) if configured_root else Path(tempfile.gettempdir()))
    temporary_root.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix=prefix, dir=temporary_root)
