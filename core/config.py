from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

# Project root = folder that contains /core, /infra, /processing, etc.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_PIPELINE_CONFIG_PATH = _PROJECT_ROOT / "infra" / "config" / "pipeline.yaml"

def load_config() -> Dict[str, Any]:
    if not _PIPELINE_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Pipeline config not found: {_PIPELINE_CONFIG_PATH}")
    with _PIPELINE_CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid pipeline config format in {_PIPELINE_CONFIG_PATH} (expected YAML mapping).")
    return data  # type: ignore[return-value]

# Central config object (loaded once at import time)
CONFIG: Dict[str, Any] = load_config()
