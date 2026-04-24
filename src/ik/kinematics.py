"""
Forward Kinematics Module for 6-Axis Robot Arm.

Computes end-effector pose from joint angles using DH parameters.
Based on reference implementation with added Euler angle extraction.
"""

from __future__ import annotations

from typing import List, Tuple, Optional

import numpy as np

from .config import DHRow, get_config
from .logging import get_logger


log = get_logger("kinematics")

_LIMIT_EPS_DEG = 0.1  # Max overshoot (degrees) that can be clamped back to limits


def dh_transform(theta: float, alpha: float, d: float, a: float) -> np.ndarray:
    """
    Compute DH transformation matrix.

    Standard DH convention: RotZ(theta) * TransZ(d) * TransX(a) * RotX(alpha)

    Args:
        theta: Joint angle in radians.
        alpha: Link twist in radians.
        d: Link offset in mm.
        a: Link length in mm.

    Returns:
        4x4 homogeneous transformation matrix.
    """
    ct, st = np.cos(theta), np.sin(theta)
    ca, sa = np.cos(alpha), np.sin(alpha)

    return np.array(
        [
            [ct, -st * ca, st * sa, a * ct],
            [st, ct * ca, -ct * sa, a * st],
            [0.0, sa, ca, d],
            [0.0, 0.0, 0.0, 1.0],
        ],
        dtype=float,
    )


def fk_T06(q_deg: List[float], dh_table: Optional[List[DHRow]] = None) -> np.ndarray:
    """
    Compute forward kinematics - end effector pose from joint angles.

    Args:
        q_deg: Joint angles in degrees [q1, q2, q3, q4, q5, q6].
        dh_table: Optional DH parameter table. If None, loads from config.

    Returns:
        4x4 homogeneous transformation matrix (pose of end effector in base frame).
    """
    q = np.radians(np.asarray(q_deg, dtype=float).reshape(6,))

    if dh_table is None:
        config = get_config()
        dh_table = config.dh_table

    T = np.eye(4)

    for i, dh in enumerate(dh_table):
        theta = q[i] + np.radians(dh.theta_offset_deg)
        alpha = np.radians(dh.alpha_deg)
        d = dh.d
        a = dh.a

        Ai = dh_transform(theta, alpha, d, a)
        T = T @ Ai

    return T


def get_T6_tcp(
    tcp_xyz_mm: Optional[List[float]] = None,
    tcp_rpy_deg: Optional[List[float]] = None,
    config: Optional[object] = None,
) -> np.ndarray:
    """
    Build the fixed transform from frame 6 to TCP (tool center point).

    Translation is expressed in *frame 6 coordinates* (X6,Y6,Z6), in mm.
    Rotation uses the same ZYX Euler convention used elsewhere in this module:
        R = Rz(rz) * Ry(ry) * Rx(rx)
    If no TCP is configured, returns identity.
    """
    xyz = None
    rpy = None

    if tcp_xyz_mm is not None:
        xyz = np.asarray(tcp_xyz_mm, dtype=float).reshape(3,)
    if tcp_rpy_deg is not None:
        rpy = np.asarray(tcp_rpy_deg, dtype=float).reshape(3,)

    if (xyz is None or rpy is None) and config is not None:
        tcp_obj = None
        for key in ("tcp", "tool", "tool_frame", "tcp_frame"):
            if isinstance(config, dict) and key in config:
                tcp_obj = config[key]
                break
            if hasattr(config, key):
                tcp_obj = getattr(config, key)
                break

        if tcp_obj is not None:
            if xyz is None:
                if isinstance(tcp_obj, dict) and "xyz_mm" in tcp_obj:
                    xyz = np.asarray(tcp_obj["xyz_mm"], dtype=float).reshape(3,)
                elif hasattr(tcp_obj, "xyz_mm"):
                    xyz = np.asarray(getattr(tcp_obj, "xyz_mm"), dtype=float).reshape(3,)
            if rpy is None:
                if isinstance(tcp_obj, dict) and "rpy_deg" in tcp_obj:
                    rpy = np.asarray(tcp_obj["rpy_deg"], dtype=float).reshape(3,)
                elif hasattr(tcp_obj, "rpy_deg"):
                    rpy = np.asarray(getattr(tcp_obj, "rpy_deg"), dtype=float).reshape(3,)

    if xyz is None:
        xyz = np.zeros(3, dtype=float)
    if rpy is None:
        rpy = np.zeros(3, dtype=float)

    T = np.eye(4, dtype=float)
    T[:3, :3] = euler_zyx_to_rotation_matrix(rpy[0], rpy[1], rpy[2])
    T[:3, 3] = xyz
    return T


def fk_T0tcp(
    q_deg: List[float],
    dh_table: Optional[List[DHRow]] = None,
    tcp_xyz_mm: Optional[List[float]] = None,
    tcp_rpy_deg: Optional[List[float]] = None,
    config: Optional[object] = None,
) -> np.ndarray:
    """
    Compute forward kinematics - TCP pose from joint angles.

    Returns:
        T_0_TCP = T_0_6 * T_6_TCP
    """
    if config is None:
        config = get_config()
    if dh_table is None:
        dh_table = config.dh_table

    T_06 = fk_T06(q_deg, dh_table)
    T_6_tcp = get_T6_tcp(tcp_xyz_mm=tcp_xyz_mm, tcp_rpy_deg=tcp_rpy_deg, config=config)
    return T_06 @ T_6_tcp


def extract_position(T: np.ndarray) -> Tuple[float, float, float]:
    """Extract XYZ position from transformation matrix (mm)."""
    return float(T[0, 3]), float(T[1, 3]), float(T[2, 3])


def extract_euler_angles_zyx(T: np.ndarray) -> Tuple[float, float, float]:
    """
    Extract ZYX Euler angles (yaw-pitch-roll) from rotation matrix.

    Convention: R = Rz(rz) * Ry(ry) * Rx(rx)

    Returns:
        Tuple of (rx, ry, rz) in degrees.
    """
    R = T[:3, :3]

    sy = np.sqrt(R[0, 0] ** 2 + R[1, 0] ** 2)

    if sy > 1e-6:
        rx = np.arctan2(R[2, 1], R[2, 2])
        ry = np.arctan2(-R[2, 0], sy)
        rz = np.arctan2(R[1, 0], R[0, 0])
    else:
        log.debug("Gimbal lock detected in Euler angle extraction")
        rx = np.arctan2(-R[1, 2], R[1, 1])
        ry = np.arctan2(-R[2, 0], sy)
        rz = 0.0

    return (
        float(np.degrees(rx)),
        float(np.degrees(ry)),
        float(np.degrees(rz)),
    )


def compute_cartesian_pose(
    q_deg: List[float],
) -> Tuple[float, float, float, float, float, float]:
    """
    Compute complete cartesian pose from joint angles.

    Returns:
        Tuple of (x, y, z, rx, ry, rz) where position is in mm and angles in degrees.
    """
    T = fk_T0tcp(q_deg)
    x, y, z = extract_position(T)
    rx, ry, rz = extract_euler_angles_zyx(T)
    return (x, y, z, rx, ry, rz)


def rotation_matrix_to_axis_angle(R: np.ndarray) -> Tuple[np.ndarray, float]:
    """Convert rotation matrix to axis-angle representation."""
    tr = np.trace(R)
    cos_angle = (tr - 1.0) * 0.5
    cos_angle = np.clip(cos_angle, -1.0, 1.0)
    angle = np.arccos(cos_angle)

    if angle < 1e-9:
        return np.array([0.0, 0.0, 1.0]), 0.0

    if angle > np.pi - 1e-9:
        diag = np.diag(R)
        idx = np.argmax(diag)
        axis = np.zeros(3)
        axis[idx] = 1.0
        return axis, angle

    denom = 2.0 * np.sin(angle)
    axis = np.array(
        [
            (R[2, 1] - R[1, 2]) / denom,
            (R[0, 2] - R[2, 0]) / denom,
            (R[1, 0] - R[0, 1]) / denom,
        ]
    )

    return axis, angle


def rotvec_from_R(R: np.ndarray) -> np.ndarray:
    """Convert rotation matrix to rotation vector (axis * angle)."""
    axis, angle = rotation_matrix_to_axis_angle(R)
    return axis * angle


def euler_zyx_to_rotation_matrix(rx_deg: float, ry_deg: float, rz_deg: float) -> np.ndarray:
    """
    Convert ZYX Euler angles (degrees) to rotation matrix.

    Convention: R = Rz(rz) * Ry(ry) * Rx(rx)
    """
    rx, ry, rz = np.radians([rx_deg, ry_deg, rz_deg])
    cx, sx = np.cos(rx), np.sin(rx)
    cy, sy = np.cos(ry), np.sin(ry)
    cz, sz = np.cos(rz), np.sin(rz)

    Rz = np.array(
        [[cz, -sz, 0.0], [sz, cz, 0.0], [0.0, 0.0, 1.0]],
        dtype=float,
    )
    Ry = np.array(
        [[cy, 0.0, sy], [0.0, 1.0, 0.0], [-sy, 0.0, cy]],
        dtype=float,
    )
    Rx = np.array(
        [[1.0, 0.0, 0.0], [0.0, cx, -sx], [0.0, sx, cx]],
        dtype=float,
    )

    return Rz @ Ry @ Rx


def build_pose_target(
    x: float, y: float, z: float, rx_deg: float, ry_deg: float, rz_deg: float
) -> np.ndarray:
    """Build a 4x4 target pose matrix from XYZ (mm) and RxRyRz (deg)."""
    T = np.eye(4, dtype=float)
    T[:3, :3] = euler_zyx_to_rotation_matrix(rx_deg, ry_deg, rz_deg)
    T[:3, 3] = np.array([x, y, z], dtype=float)
    return T


def wrap_to_pi(x: np.ndarray) -> np.ndarray:
    """Wrap radians to [-pi, pi]."""
    return (x + np.pi) % (2.0 * np.pi) - np.pi


def pose_error(T_curr: np.ndarray, T_tgt: np.ndarray) -> np.ndarray:
    """6x1 error: [position_error; orientation_error(rotvec)], in base frame."""
    p_c = T_curr[:3, 3]
    p_t = T_tgt[:3, 3]
    dp = p_t - p_c

    R_c = T_curr[:3, :3]
    R_t = T_tgt[:3, :3]
    R_err = R_t @ R_c.T
    dr = rotvec_from_R(R_err)

    return np.hstack([dp, dr])


def numerical_jacobian(
    q_deg: List[float], dh_table: Optional[List[DHRow]] = None, eps: float = 1e-4
) -> np.ndarray:
    """
    Numerical Jacobian of [p; rotvec] wrt joints.
    p in mm, rotvec in rad.
    """
    q_deg = np.asarray(q_deg, dtype=float).reshape(6,)
    T0 = fk_T0tcp(q_deg, dh_table)
    p0 = T0[:3, 3]
    R0 = T0[:3, :3]

    J = np.zeros((6, 6), dtype=float)

    for i in range(6):
        dq = np.zeros(6)
        dq[i] = eps
        T1 = fk_T0tcp(q_deg + dq, dh_table)

        p1 = T1[:3, 3]
        R1 = T1[:3, :3]

        dp = (p1 - p0) / np.radians(eps)
        R_delta = R1 @ R0.T
        domega = rotvec_from_R(R_delta) / np.radians(eps)

        J[:, i] = np.hstack([dp, domega])

    return J


def within_limits(
    q_deg: List[float], joint_limits_deg: List[Tuple[float, float]], eps: float = 1e-9
) -> bool:
    """Return True if q_deg is within [min,max] for each joint."""
    lim = np.asarray(joint_limits_deg, dtype=float).reshape(6, 2)
    q = np.asarray(q_deg, dtype=float).reshape(6,)
    return np.all(q >= (lim[:, 0] - eps)) and np.all(q <= (lim[:, 1] + eps))


def clamp_to_limits(
    q_deg: List[float], joint_limits_deg: List[Tuple[float, float]]
) -> np.ndarray:
    """Clamp q_deg to joint limits."""
    lim = np.asarray(joint_limits_deg, dtype=float).reshape(6, 2)
    q = np.asarray(q_deg, dtype=float).reshape(6,)
    return np.minimum(np.maximum(q, lim[:, 0]), lim[:, 1])


def angular_distance_deg(q_a_deg: List[float], q_b_deg: List[float]) -> float:
    """L2 distance in joint space, respecting angle wrap."""
    qa = np.radians(np.asarray(q_a_deg, dtype=float).reshape(6,))
    qb = np.radians(np.asarray(q_b_deg, dtype=float).reshape(6,))
    d = wrap_to_pi(qa - qb)
    return float(np.linalg.norm(np.degrees(d)))


def build_seed_list(
    q0_deg: List[float],
    joint_limits_deg: Optional[List[Tuple[float, float]]] = None,
    n_random: int = 12,
    jitter_deg: float = 25.0,
    rng_seed: int = 0,
) -> List[np.ndarray]:
    """
    Build a list of initial guesses to try.
    - Starts with q0
    - Adds a few deterministic seeds
    - Adds random seeds uniformly within limits (if provided)
    """
    q0 = np.asarray(q0_deg, dtype=float).reshape(6,)
    seeds: List[np.ndarray] = [q0.copy()]

    rng = np.random.default_rng(rng_seed)

    if joint_limits_deg is not None:
        lim = np.asarray(joint_limits_deg, dtype=float).reshape(6, 2)
        mid = (lim[:, 0] + lim[:, 1]) * 0.5

        seeds.append(mid)
        seeds.append(clamp_to_limits(np.zeros(6), joint_limits_deg))
        seeds.append(clamp_to_limits(q0, joint_limits_deg))

        s = mid.copy()
        s[3] = lim[3, 0]
        seeds.append(s)

        s = mid.copy()
        s[3] = lim[3, 1]
        seeds.append(s)

        for _ in range(int(n_random)):
            seeds.append(rng.uniform(lim[:, 0], lim[:, 1]))

        for _ in range(4):
            j = q0 + rng.uniform(-jitter_deg, jitter_deg, size=6)
            seeds.append(clamp_to_limits(j, joint_limits_deg))
    else:
        seeds.append(np.zeros(6))
        for _ in range(int(n_random)):
            seeds.append(q0 + rng.uniform(-jitter_deg, jitter_deg, size=6))

    uniq: List[np.ndarray] = []
    for s in seeds:
        if all(angular_distance_deg(s, u) > 1e-3 for u in uniq):
            uniq.append(np.degrees(wrap_to_pi(np.radians(s))))
    return uniq


def ik_solve(
    T_target: np.ndarray,
    q0_deg: Optional[List[float]] = None,
    position_only: bool = False,
    max_iters: int = 200,
    tol_pos_mm: float = 0.5,
    tol_ori_rad: float = 1e-3,
    damping: float = 0.05,
    step_scale: float = 1.0,
    joint_limits_deg: Optional[List[Tuple[float, float]]] = None,
    dh_table: Optional[List[DHRow]] = None,
    verbose: bool = False,
    max_dq_deg: Optional[float] = None,
) -> Tuple[bool, np.ndarray, int, float, float]:
    """
    Numerical IK using damped least squares.

    - T_target: 4x4 desired pose in base frame (mm, radians).
    - q0_deg: initial guess (6,) in degrees. Typically current robot joint angles.
    - position_only: if True, ignores orientation (solves XYZ only).
    - joint_limits_deg: optional list/array shape (6,2): [(min,max), ...]
    - max_dq_deg: optional per-iteration joint step clamp (degrees). If the largest
      joint change in an iteration exceeds this, the entire dq vector is scaled down
      proportionally. None = no clamp (default).

    Joint limits are checked only after convergence.
    """
    if q0_deg is None:
        q_deg = np.zeros(6, dtype=float)
    else:
        q_deg = np.asarray(q0_deg, dtype=float).reshape(6,)

    lam = float(damping)

    for it in range(max_iters):
        T_curr = fk_T0tcp(q_deg, dh_table)
        e6 = pose_error(T_curr, T_target)

        pos_err = np.linalg.norm(e6[:3])
        ori_err = np.linalg.norm(e6[3:])

        if position_only:
            converged = pos_err <= tol_pos_mm
            e = e6[:3]
            J = numerical_jacobian(q_deg, dh_table)[:3, :]
        else:
            converged = (pos_err <= tol_pos_mm) and (ori_err <= tol_ori_rad)
            e = e6
            J = numerical_jacobian(q_deg, dh_table)

        if converged:
            if joint_limits_deg is not None and not within_limits(q_deg, joint_limits_deg):
                # Try clamping marginal violations and recheck TCP error
                q_clamped = clamp_to_limits(q_deg, joint_limits_deg)
                overshoot = np.max(np.abs(q_clamped - q_deg))
                if overshoot <= _LIMIT_EPS_DEG:
                    T_clamped = fk_T0tcp(q_clamped, dh_table)
                    e_clamped = pose_error(T_clamped, T_target)
                    pos_err_clamped = np.linalg.norm(e_clamped[:3])
                    if pos_err_clamped <= tol_pos_mm:
                        log.info(
                            "IK clamped marginal overshoot (%.4f°) — pos_err after clamp: %.3f mm",
                            overshoot, pos_err_clamped,
                        )
                        return True, q_clamped, it, pos_err_clamped, np.linalg.norm(e_clamped[3:])
                # Genuine violation — log details
                lim = np.asarray(joint_limits_deg, dtype=float).reshape(6, 2)
                for j in range(6):
                    if q_deg[j] < lim[j, 0] or q_deg[j] > lim[j, 1]:
                        log.warning(
                            "IK converged but J%d=%.4f° violates limits [%.1f, %.1f] (over by %.4f°)",
                            j + 1, q_deg[j], lim[j, 0], lim[j, 1],
                            max(lim[j, 0] - q_deg[j], q_deg[j] - lim[j, 1]),
                        )
                return False, q_deg, it, pos_err, ori_err
            return True, q_deg, it, pos_err, ori_err

        JT = J.T
        H = JT @ J + (lam * lam) * np.eye(6)
        g = JT @ e

        try:
            dq_rad = np.linalg.solve(H, g)
        except np.linalg.LinAlgError:
            dq_rad = np.linalg.lstsq(H, g, rcond=None)[0]

        dq_rad = step_scale * dq_rad
        dq_deg = np.degrees(dq_rad)

        if max_dq_deg is not None:
            max_abs = np.max(np.abs(dq_deg))
            if max_abs > max_dq_deg:
                dq_deg = dq_deg * (max_dq_deg / max_abs)

        q_deg = q_deg + dq_deg
        q_deg = np.degrees(wrap_to_pi(np.radians(q_deg)))

        if verbose and (it % 10 == 0 or it == max_iters - 1):
            log.info(
                "IK iter %3d | pos_err=%8.3f mm | ori_err=%10.6f rad | q=%s",
                it,
                pos_err,
                ori_err,
                q_deg,
            )

    return False, q_deg, max_iters, pos_err, ori_err


def ik_solve_multiseed(
    T_target: np.ndarray,
    q0_deg: List[float],
    position_only: bool = False,
    joint_limits_deg: Optional[List[Tuple[float, float]]] = None,
    max_attempts: int = 25,
    seed_params: Optional[dict] = None,
    prefer_closest_to_q0: bool = True,
    dh_table: Optional[List[DHRow]] = None,
    **ik_kwargs,
) -> Tuple[bool, np.ndarray, int, float, float, np.ndarray, int]:
    """
    Try IK from multiple seeds. Returns the best valid solution (within limits).
    - prefer_closest_to_q0=True picks the valid solution closest to q0_deg.
    - ik_kwargs are forwarded to ik_solve (max_iters, tol_pos_mm, damping, etc.)
    """
    if seed_params is None:
        seed_params = dict(n_random=12, jitter_deg=25.0, rng_seed=0)

    seeds = build_seed_list(
        q0_deg=q0_deg, joint_limits_deg=joint_limits_deg, **seed_params
    )

    seeds = seeds[:max_attempts]

    best_valid = None
    best_any = None

    for seed in seeds:
        ok, q_sol, iters, pe, oe = ik_solve(
            T_target=T_target,
            q0_deg=seed,
            position_only=position_only,
            joint_limits_deg=joint_limits_deg,
            dh_table=dh_table,
            **ik_kwargs,
        )

        score_any = pe if position_only else (pe + 1000.0 * oe)
        if best_any is None or score_any < best_any[0]:
            best_any = (score_any, ok, q_sol, iters, pe, oe, seed)

        if not ok:
            continue

        if prefer_closest_to_q0:
            dist = angular_distance_deg(q_sol, q0_deg)
            score_valid = dist
        else:
            score_valid = pe if position_only else (pe + 1000.0 * oe)

        if best_valid is None or score_valid < best_valid[0]:
            best_valid = (score_valid, q_sol, iters, pe, oe, seed)

    if best_valid is not None:
        _, q_sol, iters, pe, oe, seed_used = best_valid
        return True, q_sol, iters, pe, oe, seed_used, len(seeds)

    _, ok_last, q_last, iters, pe, oe, seed_used = best_any
    return False, q_last, iters, pe, oe, seed_used, len(seeds)


class ForwardKinematics:
    """
    Forward kinematics calculator with caching.

    Provides convenient interface for FK calculations with
    automatic configuration loading.
    """

    def __init__(self):
        self._config = get_config()
        self._dh_table = self._config.dh_table
        self._last_q = None
        self._last_T = None
        log.info(
            "ForwardKinematics initialized with %d DH parameter rows",
            len(self._dh_table),
        )

    def compute(self, q_deg: List[float]) -> np.ndarray:
        """Compute FK transformation matrix."""
        q_array = np.asarray(q_deg)

        if self._last_q is not None and np.allclose(q_array, self._last_q):
            return self._last_T

        T = fk_T0tcp(q_deg, self._dh_table)
        self._last_q = q_array.copy()
        self._last_T = T

        return T

    def get_position(self, q_deg: List[float]) -> Tuple[float, float, float]:
        """Get XYZ position."""
        T = self.compute(q_deg)
        return extract_position(T)

    def get_euler_angles(self, q_deg: List[float]) -> Tuple[float, float, float]:
        """Get Euler angles in degrees."""
        T = self.compute(q_deg)
        return extract_euler_angles_zyx(T)

    def get_full_pose(
        self, q_deg: List[float]
    ) -> Tuple[float, float, float, float, float, float]:
        """Get complete pose (x, y, z, rx, ry, rz)."""
        T = self.compute(q_deg)
        x, y, z = extract_position(T)
        rx, ry, rz = extract_euler_angles_zyx(T)
        return (x, y, z, rx, ry, rz)


_fk: Optional[ForwardKinematics] = None


def get_fk() -> ForwardKinematics:
    """Get or create ForwardKinematics instance."""
    global _fk
    if _fk is None:
        _fk = ForwardKinematics()
    return _fk


if __name__ == "__main__":
    log.info("Forward Kinematics Test")
    log.info("%s", "=" * 50)

    q_home = [0, 0, 0, 0, 0, 0]
    pose = compute_cartesian_pose(q_home)
    log.info("Home position (all zeros):")
    log.info("  X: %.2f mm", pose[0])
    log.info("  Y: %.2f mm", pose[1])
    log.info("  Z: %.2f mm", pose[2])
    log.info("  Rx: %.2f deg", pose[3])
    log.info("  Ry: %.2f deg", pose[4])
    log.info("  Rz: %.2f deg", pose[5])

    q_test = [-45, 40, -40, 30, 90, 0]
    pose = compute_cartesian_pose(q_test)
    log.info("Test position %s:", q_test)
    log.info("  X: %.2f mm", pose[0])
    log.info("  Y: %.2f mm", pose[1])
    log.info("  Z: %.2f mm", pose[2])
    log.info("  Rx: %.2f deg", pose[3])
    log.info("  Ry: %.2f deg", pose[4])
    log.info("  Rz: %.2f deg", pose[5])
