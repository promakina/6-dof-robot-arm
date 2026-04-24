from __future__ import annotations

import json
from pathlib import Path

from app_paths import get_settings_path


def get_robot_joints_path() -> Path:
    return get_settings_path("robot_joints.json")


def load_robot_joints(settings_path: Path | None = None) -> list[dict]:
    path = settings_path or get_robot_joints_path()
    data = json.loads(path.read_text(encoding="utf-8"))
    joints = data.get("joints", [])
    if not isinstance(joints, list):
        raise ValueError(f"Invalid joints payload in {path}: expected a list.")
    return sorted(joints, key=lambda joint: int(joint.get("id", 0)))
