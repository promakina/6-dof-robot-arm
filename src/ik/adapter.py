from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .config import get_config
from .kinematics import build_pose_target, ik_solve, ik_solve_multiseed


@dataclass
class IKSolution:
    ok: bool
    q_deg: List[float]
    iters: int
    pos_err_mm: float
    ori_err_rad: float
    seed_used: List[float]
    attempts: int


def _pad_joints(q_deg: List[float]) -> List[float]:
    if len(q_deg) >= 6:
        return list(q_deg[:6])
    return list(q_deg) + [0.0] * (6 - len(q_deg))


def solve_ik_for_pose(
    x_mm: float,
    y_mm: float,
    z_mm: float,
    rx_deg: float,
    ry_deg: float,
    rz_deg: float,
    q0_deg: List[float],
    position_only: bool = False,
) -> IKSolution:
    config = get_config()
    q0 = _pad_joints(q0_deg)
    T_target = build_pose_target(x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg)
    single_seed = config.solver.single_seed
    multi_seed = config.solver.multi_seed

    ok, q_sol, iters, pos_err, ori_err, seed_used, attempts = ik_solve_multiseed(
        T_target=T_target,
        q0_deg=q0,
        position_only=position_only,
        joint_limits_deg=config.joint_limits_deg,
        dh_table=config.dh_table,
        max_attempts=multi_seed.max_attempts,
        prefer_closest_to_q0=multi_seed.prefer_closest_to_q0,
        seed_params={
            "n_random": multi_seed.seed_params.n_random,
            "jitter_deg": multi_seed.seed_params.jitter_deg,
            "rng_seed": multi_seed.seed_params.rng_seed,
        },
        max_iters=single_seed.max_iters,
        tol_pos_mm=single_seed.tol_pos_mm,
        tol_ori_rad=single_seed.tol_ori_rad,
        damping=single_seed.damping,
        step_scale=single_seed.step_scale,
        max_dq_deg=single_seed.max_dq_deg,
    )

    return IKSolution(
        ok=ok,
        q_deg=[float(v) for v in q_sol],
        iters=int(iters),
        pos_err_mm=float(pos_err),
        ori_err_rad=float(ori_err),
        seed_used=[float(v) for v in seed_used],
        attempts=int(attempts),
    )


def solve_ik_for_pose_single_seed(
    x_mm: float,
    y_mm: float,
    z_mm: float,
    rx_deg: float,
    ry_deg: float,
    rz_deg: float,
    q0_deg: List[float],
    position_only: bool = False,
    *,
    max_iters: int = 80,
    tol_pos_mm: float = 0.5,
    tol_ori_rad: float = 1e-3,
    damping: float = 0.05,
    step_scale: float = 1.0,
    max_dq_deg: float | None = None,
) -> IKSolution:
    config = get_config()
    q0 = _pad_joints(q0_deg)
    T_target = build_pose_target(x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg)
    single_seed = config.solver.single_seed

    ok, q_sol, iters, pos_err, ori_err = ik_solve(
        T_target=T_target,
        q0_deg=q0,
        position_only=position_only,
        joint_limits_deg=config.joint_limits_deg,
        dh_table=config.dh_table,
        max_iters=single_seed.max_iters if max_iters == 80 else max_iters,
        tol_pos_mm=single_seed.tol_pos_mm if tol_pos_mm == 0.5 else tol_pos_mm,
        tol_ori_rad=single_seed.tol_ori_rad if tol_ori_rad == 1e-3 else tol_ori_rad,
        damping=single_seed.damping if damping == 0.05 else damping,
        step_scale=single_seed.step_scale if step_scale == 1.0 else step_scale,
        max_dq_deg=single_seed.max_dq_deg if max_dq_deg is None else max_dq_deg,
    )

    return IKSolution(
        ok=ok,
        q_deg=[float(v) for v in q_sol],
        iters=int(iters),
        pos_err_mm=float(pos_err),
        ori_err_rad=float(ori_err),
        seed_used=[float(v) for v in q0],
        attempts=1,
    )
