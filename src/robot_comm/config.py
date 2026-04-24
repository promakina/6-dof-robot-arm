from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from app_paths import get_settings_path
from joint_settings import load_robot_joints


@dataclass(frozen=True)
class JointConfig:
    id: int
    name: str
    min_angle: float
    max_angle: float
    home_pos: float
    speed_rpm: int
    max_speed_rpm: int
    acc: int
    work_current_mA: int
    sign_map: int
    gear_ratio: float
    invert_dir: bool


@dataclass(frozen=True)
class CommConfig:
    robot_name: str
    can_interface: str
    can_port: str
    can_bitrate: int
    can_tty_baudrate: int
    can_recv_timeout: float
    motor_microstep: int
    motor_steps_per_rev: int
    motor_ticks_per_rev: int
    encoder_ticks_per_rev: int
    motor_enable_on_start: bool
    motor_disable_on_exit: bool
    joints: List[JointConfig]
    settings_path: Path


_CONFIG: Optional[CommConfig] = None


def _load_settings(path: Path) -> CommConfig:
    data = json.loads(path.read_text(encoding="utf-8"))

    can_settings = data.get("can_settings", {})
    motor_settings = data.get("motor_settings", {})

    stepper_steps = int(motor_settings.get("stepper_steps_per_rev", 200))
    microstep = int(motor_settings.get("microstep", 64))
    ticks_per_rev = stepper_steps * microstep
    encoder_ticks_per_rev = int(
        motor_settings.get(
            "encoder_ticks_per_rev",
            motor_settings.get("axis_ticks_per_rev", 16384),
        )
    )
    if encoder_ticks_per_rev <= 0:
        encoder_ticks_per_rev = 16384

    # NOTE: encoder_ticks_per_rev (16384) differs from motor_ticks_per_rev (12800).
    # This is EXPECTED for Servo42D: 14-bit magnetic encoder ≠ 200×microstep counter.
    # F5 commands and encoder reads (0x30) use encoder_ticks_per_rev (hardware protocol).
    # FD relative pulse commands use motor_ticks_per_rev (stepper microsteps).
    if encoder_ticks_per_rev != ticks_per_rev:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(
            "encoder_ticks_per_rev (%d) != motor_ticks_per_rev (%d). "
            "Expected for Servo42D: encoder=14-bit magnetic (16384), motor=200step×%dmicrostep (%d).",
            encoder_ticks_per_rev,
            ticks_per_rev,
            microstep,
            ticks_per_rev,
        )

    joints = []
    for j in load_robot_joints():
        joints.append(
            JointConfig(
                id=int(j.get("id", 0)),
                name=str(j.get("name", "")),
                min_angle=float(j.get("min_angle", -180.0)),
                max_angle=float(j.get("max_angle", 180.0)),
                home_pos=float(j.get("home_pos", 0.0)),
                speed_rpm=int(j.get("speed_rpm", 4)),
                max_speed_rpm=int(j.get("max_speed_rpm", 6)),
                acc=int(j.get("acc", 6)),
                work_current_mA=int(j.get("work_current_mA", 1200)),
                sign_map=int(j.get("sign_map", 1)),
                gear_ratio=float(j.get("gear_ratio", 1.0)),
                invert_dir=bool(j.get("invert_dir", False)),
            )
        )

    return CommConfig(
        robot_name=str(data.get("robot_name", "Robot")),
        can_interface=str(can_settings.get("interface", "slcan")),
        can_port=str(can_settings.get("port", "COM6")),
        can_bitrate=int(can_settings.get("bitrate", 500000)),
        can_tty_baudrate=int(can_settings.get("tty_baudrate", 2_000_000)),
        can_recv_timeout=float(can_settings.get("recv_timeout_s", 0.35)),
        motor_microstep=microstep,
        motor_steps_per_rev=stepper_steps,
        motor_ticks_per_rev=ticks_per_rev,
        encoder_ticks_per_rev=encoder_ticks_per_rev,
        motor_enable_on_start=bool(motor_settings.get("enable_on_start", True)),
        motor_disable_on_exit=bool(motor_settings.get("disable_on_exit", True)),
        joints=joints,
        settings_path=path,
    )


def get_comm_config(settings_path: Optional[Path] = None, force_reload: bool = False) -> CommConfig:
    global _CONFIG
    if _CONFIG is not None and settings_path is None and not force_reload:
        return _CONFIG

    if settings_path is None:
        settings_path = get_settings_path("robot_comm_settings.json")

    _CONFIG = _load_settings(settings_path)
    return _CONFIG
