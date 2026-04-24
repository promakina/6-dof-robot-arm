from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from app_paths import get_settings_path
from joint_settings import load_robot_joints


@dataclass(frozen=True)
class DHRow:
    joint: int
    alpha_deg: float
    d: float
    a: float
    theta_offset_deg: float


@dataclass(frozen=True)
class TcpConfig:
    xyz_mm: List[float]
    rpy_deg: List[float]


@dataclass(frozen=True)
class IKSeedParamsConfig:
    n_random: int
    jitter_deg: float
    rng_seed: int


@dataclass(frozen=True)
class IKSingleSeedConfig:
    max_iters: int
    tol_pos_mm: float
    tol_ori_rad: float
    damping: float
    step_scale: float
    max_dq_deg: float | None


@dataclass(frozen=True)
class IKMultiSeedConfig:
    max_attempts: int
    prefer_closest_to_q0: bool
    seed_params: IKSeedParamsConfig


@dataclass(frozen=True)
class IKSolverConfig:
    single_seed: IKSingleSeedConfig
    multi_seed: IKMultiSeedConfig


@dataclass(frozen=True)
class RobotConfig:
    dh_table: List[DHRow]
    joint_limits_deg: List[Tuple[float, float]]
    tcp: TcpConfig
    solver: IKSolverConfig
    settings_path: Path


_CONFIG: Optional[RobotConfig] = None


def _load_settings(path: Path) -> RobotConfig:
    data = json.loads(path.read_text(encoding="utf-8"))

    dh_table = []
    for row in data.get("dh_parameters", {}).get("table", []):
        dh_table.append(
            DHRow(
                joint=int(row.get("joint", 0)),
                alpha_deg=float(row.get("alpha_deg", 0.0)),
                d=float(row.get("d", 0.0)),
                a=float(row.get("a", 0.0)),
                theta_offset_deg=float(row.get("theta_offset_deg", 0.0)),
            )
        )

    joints = load_robot_joints()
    joint_limits = [
        (float(j.get("min_angle", 0.0)), float(j.get("max_angle", 0.0)))
        for j in joints
    ]

    tcp_obj = data.get("tcp", {})
    tcp = TcpConfig(
        xyz_mm=list(tcp_obj.get("xyz_mm", [0.0, 0.0, 0.0])),
        rpy_deg=list(tcp_obj.get("rpy_deg", [0.0, 0.0, 0.0])),
    )

    solver_obj = data.get("solver", {})
    single_seed_obj = solver_obj.get("single_seed", {})
    multi_seed_obj = solver_obj.get("multi_seed", {})
    seed_params_obj = multi_seed_obj.get("seed_params", {})
    solver = IKSolverConfig(
        single_seed=IKSingleSeedConfig(
            max_iters=int(single_seed_obj.get("max_iters", 200)),
            tol_pos_mm=float(single_seed_obj.get("tol_pos_mm", 0.5)),
            tol_ori_rad=float(single_seed_obj.get("tol_ori_rad", 1e-3)),
            damping=float(single_seed_obj.get("damping", 0.05)),
            step_scale=float(single_seed_obj.get("step_scale", 1.0)),
            max_dq_deg=(
                None
                if single_seed_obj.get("max_dq_deg", None) is None
                else float(single_seed_obj.get("max_dq_deg"))
            ),
        ),
        multi_seed=IKMultiSeedConfig(
            max_attempts=int(multi_seed_obj.get("max_attempts", 25)),
            prefer_closest_to_q0=bool(multi_seed_obj.get("prefer_closest_to_q0", True)),
            seed_params=IKSeedParamsConfig(
                n_random=int(seed_params_obj.get("n_random", 12)),
                jitter_deg=float(seed_params_obj.get("jitter_deg", 25.0)),
                rng_seed=int(seed_params_obj.get("rng_seed", 0)),
            ),
        ),
    )

    return RobotConfig(
        dh_table=dh_table,
        joint_limits_deg=joint_limits,
        tcp=tcp,
        solver=solver,
        settings_path=path,
    )


def get_config(settings_path: Optional[Path] = None) -> RobotConfig:
    global _CONFIG
    if _CONFIG is not None and settings_path is None:
        return _CONFIG

    if settings_path is None:
        settings_path = get_settings_path("ik_settings.json")

    config = _load_settings(settings_path)
    if settings_path is None:
        _CONFIG = config
    else:
        _CONFIG = config
    return config
