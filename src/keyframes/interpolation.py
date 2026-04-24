from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable

import numpy as np

from app.logging_utils import get_keyframe_logger
from ik.adapter import solve_ik_for_pose
from ik.kinematics import compute_cartesian_pose, extract_euler_angles_zyx, euler_zyx_to_rotation_matrix


ROTATION_DURATION_MM_PER_DEG = 1.0
DEFAULT_MAX_JOINT_STEP_DEG = 10.0
_LOG = get_keyframe_logger("keyframes")


class InterpolationPlanningCancelled(RuntimeError):
    pass


@dataclass
class KeyframePose:
    q_deg: list[float]
    xyz_mm: list[float]
    quat_xyzw: list[float]
    rpy_deg: list[float]


@dataclass
class InterpolatedSegment:
    segment_index: int
    start_keyframe_index: int
    end_keyframe_index: int
    joint_trajectory_deg: list[list[float]]
    sample_count: int
    duration_s: float
    max_joint_step_deg: float


@dataclass
class InterpolationPlannerStats:
    ik_call_count: int = 0
    ik_success_count: int = 0
    ik_failure_count: int = 0
    ik_total_time_s: float = 0.0
    ik_max_time_s: float = 0.0
    ik_total_attempts: int = 0
    ik_max_attempts: int = 0
    ik_total_iters: int = 0
    ik_max_iters: int = 0
    failed_sample_count: int = 0
    rejected_joint_jump_count: int = 0
    recovered_endpoint_jump_count: int = 0
    fallback_terminal_joint_blend_count: int = 0


@dataclass
class KeyframeInterpolationPlan:
    segments: list[InterpolatedSegment]
    full_joint_trajectory_deg: list[list[float]]
    update_rate_hz: float
    estimated_duration_s: float
    planner_stats: InterpolationPlannerStats

def extend_plan_terminal_hold(
    plan: KeyframeInterpolationPlan,
    hold_s: float,
) -> KeyframeInterpolationPlan:
    hold_s = max(0.0, float(hold_s or 0.0))
    update_rate_hz = float(getattr(plan, "update_rate_hz", 0.0) or 0.0)
    if hold_s <= 0.0 or update_rate_hz <= 0.0:
        return plan

    base_trajectory = [list(sample[:6]) for sample in getattr(plan, "full_joint_trajectory_deg", [])]
    tail_samples = max(0, int(round(hold_s * update_rate_hz)))
    if tail_samples < 1 or not base_trajectory:
        return plan

    final_sample = list(base_trajectory[-1])
    hold_trajectory = [list(final_sample)]
    for _ in range(tail_samples):
        hold_trajectory.append(list(final_sample))

    segment_count = len(getattr(plan, "segments", []) or [])
    last_keyframe_index = 1
    if segment_count > 0:
        last_keyframe_index = max(
            1,
            int(getattr(plan.segments[-1], "end_keyframe_index", segment_count + 1) or (segment_count + 1)),
        )
    extended_segments = list(plan.segments)
    extended_segments.append(
        InterpolatedSegment(
            segment_index=segment_count,
            start_keyframe_index=last_keyframe_index,
            end_keyframe_index=last_keyframe_index,
            joint_trajectory_deg=hold_trajectory,
            sample_count=len(hold_trajectory),
            duration_s=float(tail_samples / update_rate_hz),
            max_joint_step_deg=0.0,
        )
    )
    extended_trajectory = list(base_trajectory)
    extended_trajectory.extend([list(final_sample) for _ in range(tail_samples)])

    planner_stats = getattr(plan, "planner_stats", None)
    if planner_stats is not None:
        setattr(planner_stats, "evolution_terminal_hold_s", float(tail_samples / update_rate_hz))
        setattr(planner_stats, "evolution_terminal_hold_samples", int(tail_samples))

    _LOG.info(
        "Extended interpolation plan with terminal hold hold_s=%.3f update_rate_hz=%.3f tail_samples=%d "
        "total_samples=%d total_duration_s=%.3f",
        float(tail_samples / update_rate_hz),
        update_rate_hz,
        tail_samples,
        len(extended_trajectory),
        float(getattr(plan, "estimated_duration_s", 0.0) or 0.0) + (tail_samples / update_rate_hz),
    )
    return KeyframeInterpolationPlan(
        segments=extended_segments,
        full_joint_trajectory_deg=extended_trajectory,
        update_rate_hz=update_rate_hz,
        estimated_duration_s=float(getattr(plan, "estimated_duration_s", 0.0) or 0.0) + (tail_samples / update_rate_hz),
        planner_stats=plan.planner_stats,
    )


def _fmt_joints(values) -> str:
    return "[" + ", ".join(f"{float(value):+.2f}" for value in list(values)[:6]) + "]"


def _fmt_xyz(values) -> str:
    xyz = list(values)[:3]
    return "(" + ", ".join(f"{float(value):+.2f}" for value in xyz) + ")"


def _fmt_rpy(values) -> str:
    rpy = list(values)[:3]
    return "(" + ", ".join(f"{float(value):+.2f}" for value in rpy) + ")"


def _max_joint_delta(lhs, rhs) -> float:
    return max(abs(float(cur) - float(prev)) for cur, prev in zip(list(lhs)[:6], list(rhs)[:6]))


def _as_joint_list(values) -> list[float]:
    if values is None:
        raise ValueError("Missing joint angles.")
    items = list(values)
    if len(items) < 6:
        raise ValueError("Each keyframe must include 6 joint angles.")
    return [float(value) for value in items[:6]]


def _normalize_quaternion(quat_xyzw) -> np.ndarray:
    quat = np.asarray(quat_xyzw, dtype=float).reshape(4,)
    norm = float(np.linalg.norm(quat))
    if norm <= 1e-12:
        raise ValueError("Quaternion norm is zero.")
    return quat / norm


def _rotation_matrix_to_quaternion_xyzw(rotation: np.ndarray) -> np.ndarray:
    matrix = np.asarray(rotation, dtype=float).reshape(3, 3)
    trace = float(np.trace(matrix))
    if trace > 0.0:
        scale = math.sqrt(trace + 1.0) * 2.0
        qw = 0.25 * scale
        qx = (matrix[2, 1] - matrix[1, 2]) / scale
        qy = (matrix[0, 2] - matrix[2, 0]) / scale
        qz = (matrix[1, 0] - matrix[0, 1]) / scale
    elif matrix[0, 0] > matrix[1, 1] and matrix[0, 0] > matrix[2, 2]:
        scale = math.sqrt(1.0 + matrix[0, 0] - matrix[1, 1] - matrix[2, 2]) * 2.0
        qw = (matrix[2, 1] - matrix[1, 2]) / scale
        qx = 0.25 * scale
        qy = (matrix[0, 1] + matrix[1, 0]) / scale
        qz = (matrix[0, 2] + matrix[2, 0]) / scale
    elif matrix[1, 1] > matrix[2, 2]:
        scale = math.sqrt(1.0 + matrix[1, 1] - matrix[0, 0] - matrix[2, 2]) * 2.0
        qw = (matrix[0, 2] - matrix[2, 0]) / scale
        qx = (matrix[0, 1] + matrix[1, 0]) / scale
        qy = 0.25 * scale
        qz = (matrix[1, 2] + matrix[2, 1]) / scale
    else:
        scale = math.sqrt(1.0 + matrix[2, 2] - matrix[0, 0] - matrix[1, 1]) * 2.0
        qw = (matrix[1, 0] - matrix[0, 1]) / scale
        qx = (matrix[0, 2] + matrix[2, 0]) / scale
        qy = (matrix[1, 2] + matrix[2, 1]) / scale
        qz = 0.25 * scale
    return _normalize_quaternion([qx, qy, qz, qw])


def _quaternion_xyzw_to_rotation_matrix(quat_xyzw) -> np.ndarray:
    qx, qy, qz, qw = _normalize_quaternion(quat_xyzw)
    xx = qx * qx
    yy = qy * qy
    zz = qz * qz
    xy = qx * qy
    xz = qx * qz
    yz = qy * qz
    wx = qw * qx
    wy = qw * qy
    wz = qw * qz
    return np.array(
        [
            [1.0 - 2.0 * (yy + zz), 2.0 * (xy - wz), 2.0 * (xz + wy)],
            [2.0 * (xy + wz), 1.0 - 2.0 * (xx + zz), 2.0 * (yz - wx)],
            [2.0 * (xz - wy), 2.0 * (yz + wx), 1.0 - 2.0 * (xx + yy)],
        ],
        dtype=float,
    )


def _quaternion_xyzw_to_rpy_deg(quat_xyzw) -> list[float]:
    transform = np.eye(4, dtype=float)
    transform[:3, :3] = _quaternion_xyzw_to_rotation_matrix(quat_xyzw)
    return [float(value) for value in extract_euler_angles_zyx(transform)]


def _quaternion_angle_deg(quat_a_xyzw, quat_b_xyzw) -> float:
    quat_a = _normalize_quaternion(quat_a_xyzw)
    quat_b = _normalize_quaternion(quat_b_xyzw)
    dot = float(np.clip(abs(np.dot(quat_a, quat_b)), -1.0, 1.0))
    return float(math.degrees(2.0 * math.acos(dot)))


def build_keyframe_poses(
    keyframes,
    fk_fn: Callable[[list[float]], tuple[float, float, float, float, float, float]] = compute_cartesian_pose,
) -> list[KeyframePose]:
    poses: list[KeyframePose] = []
    for index, keyframe in enumerate(list(keyframes)):
        q_deg = _as_joint_list(keyframe)
        pose = fk_fn(q_deg)
        if pose is None or len(pose) < 6:
            _LOG.error("FK failed for keyframe %d q=%s", index + 1, _fmt_joints(q_deg))
            raise ValueError(f"FK failed for keyframe {index + 1}.")
        x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg = [float(value) for value in list(pose)[:6]]
        quat_xyzw = _rotation_matrix_to_quaternion_xyzw(
            euler_zyx_to_rotation_matrix(rx_deg, ry_deg, rz_deg)
        )
        poses.append(
            KeyframePose(
                q_deg=q_deg,
                xyz_mm=[x_mm, y_mm, z_mm],
                quat_xyzw=[float(value) for value in quat_xyzw],
                rpy_deg=[rx_deg, ry_deg, rz_deg],
            )
        )
        _LOG.info(
            "Keyframe pose %d q=%s xyz_mm=%s rpy_deg=%s quat_xyzw=(%.5f, %.5f, %.5f, %.5f)",
            index + 1,
            _fmt_joints(q_deg),
            _fmt_xyz([x_mm, y_mm, z_mm]),
            _fmt_rpy([rx_deg, ry_deg, rz_deg]),
            quat_xyzw[0],
            quat_xyzw[1],
            quat_xyzw[2],
            quat_xyzw[3],
        )
    return poses


def slerp_quaternion(quat_a_xyzw, quat_b_xyzw, progress: float) -> list[float]:
    q0 = _normalize_quaternion(quat_a_xyzw)
    q1 = _normalize_quaternion(quat_b_xyzw)
    s = max(0.0, min(1.0, float(progress)))
    dot = float(np.dot(q0, q1))
    if dot < 0.0:
        q1 = -q1
        dot = -dot
    dot = float(np.clip(dot, -1.0, 1.0))
    if dot > 0.9995:
        blended = q0 + s * (q1 - q0)
        return [float(value) for value in _normalize_quaternion(blended)]
    theta_0 = math.acos(dot)
    sin_theta_0 = math.sin(theta_0)
    theta = theta_0 * s
    sin_theta = math.sin(theta)
    scale_0 = math.sin(theta_0 - theta) / sin_theta_0
    scale_1 = sin_theta / sin_theta_0
    quat = (scale_0 * q0) + (scale_1 * q1)
    return [float(value) for value in _normalize_quaternion(quat)]


def interpolate_segment_pose(start_pose: KeyframePose, end_pose: KeyframePose, progress: float) -> tuple[list[float], list[float], list[float]]:
    s = max(0.0, min(1.0, float(progress)))
    xyz = (
        np.asarray(start_pose.xyz_mm, dtype=float) * (1.0 - s)
        + np.asarray(end_pose.xyz_mm, dtype=float) * s
    )
    quat_xyzw = slerp_quaternion(start_pose.quat_xyzw, end_pose.quat_xyzw, s)
    rpy_deg = _quaternion_xyzw_to_rpy_deg(quat_xyzw)
    return (
        [float(value) for value in xyz],
        [float(value) for value in rpy_deg],
        [float(value) for value in quat_xyzw],
    )


def sample_trapezoidal_progress(
    translation_distance_mm: float,
    speed_mm_s: float,
    accel_mm_s2: float,
    update_rate_hz: float,
    orientation_delta_deg: float = 0.0,
) -> tuple[list[float], float]:
    speed = float(speed_mm_s)
    accel = float(accel_mm_s2)
    rate = float(update_rate_hz)
    distance = max(0.0, float(translation_distance_mm))
    rotation_floor = max(0.0, float(orientation_delta_deg)) * ROTATION_DURATION_MM_PER_DEG
    driver_distance = max(distance, rotation_floor)
    if speed <= 0.0:
        raise ValueError("Interpolation speed must be > 0.")
    if accel <= 0.0:
        raise ValueError("Interpolation accel must be > 0.")
    if rate <= 0.0:
        raise ValueError("Interpolation update rate must be > 0.")
    if driver_distance <= 1e-9:
        _LOG.info(
            "Progress profile degenerate: distance_mm=%.4f orientation_delta_deg=%.4f -> using endpoints only",
            distance,
            orientation_delta_deg,
        )
        return [0.0, 1.0], 0.0

    dt_s = 1.0 / rate
    progress = [0.0]
    traveled = 0.0
    velocity_prev = 0.0
    max_steps = max(4, int(math.ceil((driver_distance / max(speed, 1e-6)) * rate * 8.0)) + 8)

    for _ in range(max_steps):
        remaining = max(driver_distance - traveled, 0.0)
        if remaining <= 1e-9:
            break
        velocity_cap = min(speed, math.sqrt(max(0.0, 2.0 * accel * remaining)))
        delta_v = accel * dt_s
        if velocity_prev < velocity_cap:
            velocity_next = min(velocity_prev + delta_v, velocity_cap)
        else:
            velocity_next = max(velocity_prev - delta_v, velocity_cap)
        step = 0.5 * (velocity_prev + velocity_next) * dt_s
        if step <= 1e-9:
            step = min(driver_distance - traveled, max(speed * dt_s * 0.25, 1e-6))
        traveled = min(driver_distance, traveled + step)
        progress.append(float(min(1.0, traveled / driver_distance)))
        velocity_prev = velocity_next
        if traveled >= driver_distance - 1e-9:
            break

    if progress[-1] < 1.0:
        progress.append(1.0)
    deduped = [progress[0]]
    for value in progress[1:]:
        if value > deduped[-1] + 1e-9:
            deduped.append(float(value))
    if deduped[-1] < 1.0:
        deduped.append(1.0)
    duration_s = max(0.0, (len(deduped) - 1) * dt_s)
    _LOG.info(
        "Progress profile distance_mm=%.3f orientation_delta_deg=%.3f driver_distance=%.3f "
        "speed_mm_s=%.3f accel_mm_s2=%.3f rate_hz=%.3f samples=%d duration_s=%.3f",
        distance,
        orientation_delta_deg,
        driver_distance,
        speed,
        accel,
        rate,
        len(deduped),
        duration_s,
    )
    return deduped, duration_s


def sample_tolerance_progress(
    translation_distance_mm: float,
    orientation_delta_deg: float,
    position_tolerance_mm: float = 0.0,
    orientation_tolerance_deg: float = 0.0,
) -> list[float]:
    if float(position_tolerance_mm) <= 0.0 and float(orientation_tolerance_deg) <= 0.0:
        raise ValueError(
            "Tolerance sampling requires position_tolerance_mm or orientation_tolerance_deg to be > 0."
        )
    constraints = [1.0]
    if float(position_tolerance_mm) > 0.0:
        constraints.append(float(translation_distance_mm) / float(position_tolerance_mm))
    if float(orientation_tolerance_deg) > 0.0:
        constraints.append(float(orientation_delta_deg) / float(orientation_tolerance_deg))
    interval_count = max(1, int(math.ceil(max(constraints))))
    progress = [float(index) / float(interval_count) for index in range(interval_count + 1)]
    _LOG.info(
        "Tolerance profile distance_mm=%.3f orientation_delta_deg=%.3f position_tol_mm=%.3f "
        "orientation_tol_deg=%.3f intervals=%d samples=%d",
        float(translation_distance_mm),
        float(orientation_delta_deg),
        float(position_tolerance_mm),
        float(orientation_tolerance_deg),
        interval_count,
        len(progress),
    )
    return progress


def _derive_execution_constrained_speed_mm_s(
    requested_speed_mm_s: float,
    update_rate_hz: float,
    position_tolerance_mm: float = 0.0,
    orientation_tolerance_deg: float = 0.0,
) -> tuple[float, float | None, float | None]:
    requested_speed = float(requested_speed_mm_s)
    rate = float(update_rate_hz)
    translation_speed_limit_mm_s = None
    orientation_speed_limit_mm_s = None

    if float(position_tolerance_mm) > 0.0:
        translation_speed_limit_mm_s = float(position_tolerance_mm) * rate
    if float(orientation_tolerance_deg) > 0.0:
        orientation_speed_limit_mm_s = (
            float(orientation_tolerance_deg) * ROTATION_DURATION_MM_PER_DEG * rate
        )

    speed_limits = [requested_speed]
    if translation_speed_limit_mm_s is not None:
        speed_limits.append(float(translation_speed_limit_mm_s))
    if orientation_speed_limit_mm_s is not None:
        speed_limits.append(float(orientation_speed_limit_mm_s))

    actual_speed_mm_s = min(speed_limits)
    return float(actual_speed_mm_s), translation_speed_limit_mm_s, orientation_speed_limit_mm_s


def _solve_segment_pose_candidates(
    *,
    segment_index: int,
    sample_index: int,
    progress: float,
    xyz_mm,
    rpy_deg,
    seeds,
    solve_ik_fn,
    planner_stats: InterpolationPlannerStats,
):
    successful_candidates: list[tuple[str, list[float], float, float, float, object]] = []
    candidate_error = None

    for seed_name, seed in seeds:
        ik_started_mono = time.monotonic()
        solution = solve_ik_fn(
            xyz_mm[0],
            xyz_mm[1],
            xyz_mm[2],
            rpy_deg[0],
            rpy_deg[1],
            rpy_deg[2],
            q0_deg=list(seed[:6]),
            position_only=False,
        )
        ik_elapsed_s = time.monotonic() - ik_started_mono
        planner_stats.ik_call_count += 1
        planner_stats.ik_total_time_s += ik_elapsed_s
        planner_stats.ik_max_time_s = max(planner_stats.ik_max_time_s, ik_elapsed_s)
        attempts_used = int(getattr(solution, "attempts", 0) or 0)
        iters_used = int(getattr(solution, "iters", 0) or 0)
        planner_stats.ik_total_attempts += attempts_used
        planner_stats.ik_max_attempts = max(planner_stats.ik_max_attempts, attempts_used)
        planner_stats.ik_total_iters += iters_used
        planner_stats.ik_max_iters = max(planner_stats.ik_max_iters, iters_used)

        if getattr(solution, "ok", False):
            planner_stats.ik_success_count += 1
            successful_candidates.append(
                (
                    str(seed_name),
                    _as_joint_list(getattr(solution, "q_deg", None)),
                    ik_elapsed_s,
                    float(getattr(solution, "pos_err_mm", 0.0) or 0.0),
                    float(getattr(solution, "ori_err_rad", 0.0) or 0.0),
                    solution,
                )
            )
            continue

        planner_stats.ik_failure_count += 1
        candidate_error = solution
        _LOG.warning(
            "Segment %d sample %d progress=%.4f seed=%s seed_q=%s IK failed pos_err_mm=%s ori_err_rad=%s",
            segment_index + 1,
            sample_index + 1,
            progress,
            seed_name,
            _fmt_joints(seed),
            "?" if getattr(solution, "pos_err_mm", None) is None else f"{float(solution.pos_err_mm):.6f}",
            "?" if getattr(solution, "ori_err_rad", None) is None else f"{float(solution.ori_err_rad):.6f}",
        )

    return successful_candidates, candidate_error


def _recover_endpoint_branch_suffix(
    *,
    segment_index: int,
    progress_samples: list[float],
    joint_trajectory: list[list[float]],
    start_pose: KeyframePose,
    end_pose: KeyframePose,
    solve_ik_fn,
    planner_stats: InterpolationPlannerStats,
    step_limit: float,
) -> tuple[list[list[float]] | None, dict]:
    internal_progress = list(progress_samples[1:-1])
    if not internal_progress or len(joint_trajectory) < 2:
        return None, {"reason": "no_internal_samples"}

    forward_internal = [list(sample[:6]) for sample in joint_trajectory[1:]]
    reverse_internal: list[list[float] | None] = [None] * len(internal_progress)
    reverse_jump_to_next: list[float] = [math.inf] * len(internal_progress)
    next_solution = list(end_pose.q_deg)

    _LOG.info(
        "Attempting endpoint branch recovery segment=%d internal_samples=%d step_limit_deg=%.3f "
        "forward_last_q=%s end_q=%s",
        segment_index + 1,
        len(internal_progress),
        step_limit,
        _fmt_joints(forward_internal[-1]),
        _fmt_joints(end_pose.q_deg),
    )

    for reverse_offset, progress in enumerate(reversed(internal_progress), start=1):
        internal_index = len(internal_progress) - reverse_offset
        xyz_mm, rpy_deg, _ = interpolate_segment_pose(start_pose, end_pose, progress)
        candidates, candidate_error = _solve_segment_pose_candidates(
            segment_index=segment_index,
            sample_index=internal_index,
            progress=progress,
            xyz_mm=xyz_mm,
            rpy_deg=rpy_deg,
            seeds=(
                ("reverse_next", next_solution),
                ("end", end_pose.q_deg),
                ("forward_same_progress", forward_internal[internal_index]),
            ),
            solve_ik_fn=solve_ik_fn,
            planner_stats=planner_stats,
        )
        if not candidates:
            _LOG.error(
                "Endpoint recovery failed segment=%d sample=%d progress=%.4f next_q=%s end_q=%s pos_err_mm=%s ori_err_rad=%s",
                segment_index + 1,
                internal_index + 1,
                progress,
                _fmt_joints(next_solution),
                _fmt_joints(end_pose.q_deg),
                "?" if getattr(candidate_error, "pos_err_mm", None) is None else f"{float(candidate_error.pos_err_mm):.6f}",
                "?" if getattr(candidate_error, "ori_err_rad", None) is None else f"{float(candidate_error.ori_err_rad):.6f}",
            )
            return None, {"reason": "reverse_ik_failed", "sample_index": internal_index, "progress": progress}

        ranked_candidates = []
        for seed_name, q_deg, ik_elapsed_s, _, _, _ in candidates:
            jump_to_next = _max_joint_delta(q_deg, next_solution)
            gap_to_forward = _max_joint_delta(q_deg, forward_internal[internal_index])
            ranked_candidates.append((jump_to_next, gap_to_forward, seed_name, q_deg, ik_elapsed_s))
        ranked_candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        jump_to_next, gap_to_forward, seed_name, chosen_q, ik_elapsed_s = ranked_candidates[0]
        reverse_internal[internal_index] = list(chosen_q)
        reverse_jump_to_next[internal_index] = float(jump_to_next)
        _LOG.info(
            "Endpoint recovery segment=%d sample=%d progress=%.4f chosen_seed=%s reverse_q=%s "
            "jump_to_next_deg=%.3f gap_to_forward_deg=%.3f ik_elapsed_s=%.3f",
            segment_index + 1,
            internal_index + 1,
            progress,
            seed_name,
            _fmt_joints(chosen_q),
            jump_to_next,
            gap_to_forward,
            ik_elapsed_s,
        )
        next_solution = list(chosen_q)

    best_splice = None
    for splice_index in range(len(internal_progress)):
        suffix_jump_max = max(reverse_jump_to_next[splice_index:]) if splice_index < len(reverse_jump_to_next) else 0.0
        if suffix_jump_max > step_limit:
            continue
        previous_forward = start_pose.q_deg if splice_index == 0 else forward_internal[splice_index - 1]
        reverse_start = reverse_internal[splice_index]
        if reverse_start is None:
            continue
        connect_jump = _max_joint_delta(previous_forward, reverse_start)
        if best_splice is None or connect_jump < best_splice["connect_jump"]:
            best_splice = {
                "splice_index": splice_index,
                "connect_jump": connect_jump,
                "suffix_jump_max": suffix_jump_max,
            }
        if connect_jump <= step_limit:
            recovered_internal = forward_internal[:splice_index] + [list(sample) for sample in reverse_internal[splice_index:] if sample is not None]
            _LOG.info(
                "Endpoint recovery succeeded segment=%d splice_sample=%d connect_jump_deg=%.3f suffix_jump_max_deg=%.3f "
                "forward_before_q=%s reverse_after_q=%s",
                segment_index + 1,
                splice_index + 1,
                connect_jump,
                suffix_jump_max,
                _fmt_joints(previous_forward),
                _fmt_joints(reverse_start),
            )
            return recovered_internal, {
                "reason": "recovered",
                "splice_index": splice_index,
                "connect_jump": connect_jump,
                "suffix_jump_max": suffix_jump_max,
            }

    if best_splice is None:
        best_splice = {
            "splice_index": len(internal_progress) - 1,
            "connect_jump": math.inf,
            "suffix_jump_max": math.inf,
        }
    _LOG.error(
        "Endpoint recovery failed to find splice segment=%d best_splice_sample=%d best_connect_jump_deg=%s suffix_jump_max_deg=%s",
        segment_index + 1,
        int(best_splice["splice_index"]) + 1,
        "inf" if not math.isfinite(float(best_splice["connect_jump"])) else f"{float(best_splice['connect_jump']):.3f}",
        "inf" if not math.isfinite(float(best_splice["suffix_jump_max"])) else f"{float(best_splice['suffix_jump_max']):.3f}",
    )
    return None, best_splice


def _build_terminal_joint_blend_suffix(
    *,
    segment_index: int,
    joint_trajectory: list[list[float]],
    end_joint: list[float],
    step_limit: float,
) -> tuple[list[list[float]] | None, dict]:
    if len(joint_trajectory) < 2:
        return None, {"reason": "insufficient_samples"}

    latest_feasible_index = None
    latest_feasible_steps = None
    previous_q = None
    for bridge_start_index in range(len(joint_trajectory) - 1, 0, -1):
        previous_q = list(joint_trajectory[bridge_start_index - 1][:6])
        steps = len(joint_trajectory) - bridge_start_index + 1
        total_delta = _max_joint_delta(previous_q, end_joint)
        per_step_delta = total_delta / max(1, steps)
        if per_step_delta <= step_limit:
            latest_feasible_index = bridge_start_index
            latest_feasible_steps = steps
            break

    if latest_feasible_index is None or latest_feasible_steps is None or previous_q is None:
        _LOG.error(
            "Terminal joint blend fallback unavailable segment=%d trajectory_samples=%d step_limit_deg=%.3f "
            "start_q=%s end_q=%s",
            segment_index + 1,
            len(joint_trajectory),
            step_limit,
            _fmt_joints(joint_trajectory[0]),
            _fmt_joints(end_joint),
        )
        return None, {"reason": "no_feasible_bridge"}

    blended_internal = []
    for step_number in range(1, latest_feasible_steps):
        ratio = float(step_number) / float(latest_feasible_steps)
        blended_internal.append(
            [
                float(prev) + (float(dst) - float(prev)) * ratio
                for prev, dst in zip(previous_q[:6], end_joint[:6])
            ]
        )

    blended_trajectory = [list(sample[:6]) for sample in joint_trajectory[:latest_feasible_index]]
    blended_trajectory.extend(blended_internal)

    _LOG.warning(
        "Terminal joint blend fallback segment=%d bridge_start_sample=%d bridge_steps=%d "
        "bridge_start_q=%s end_q=%s per_step_delta_deg=%.3f",
        segment_index + 1,
        latest_feasible_index,
        latest_feasible_steps,
        _fmt_joints(previous_q),
        _fmt_joints(end_joint),
        _max_joint_delta(previous_q, end_joint) / float(latest_feasible_steps),
    )
    return blended_trajectory, {
        "reason": "joint_blend",
        "bridge_start_index": latest_feasible_index,
        "bridge_steps": latest_feasible_steps,
    }


def plan_interpolated_keyframe_path(
    keyframes,
    speed_mm_s: float,
    accel_mm_s2: float,
    update_rate_hz: float,
    solve_ik_fn=solve_ik_for_pose,
    fk_fn: Callable[[list[float]], tuple[float, float, float, float, float, float]] = compute_cartesian_pose,
    max_joint_step_deg: float = DEFAULT_MAX_JOINT_STEP_DEG,
    sampling_mode: str = "update_rate",
    position_tolerance_mm: float = 0.0,
    orientation_tolerance_deg: float = 0.0,
    planner_kind: str = "baseline",
    cancel_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict], None] | None = None,
) -> KeyframeInterpolationPlan:
    keyframe_list = list(keyframes)
    if len(keyframe_list) < 2:
        raise ValueError("Need at least 2 keyframes for interpolation.")
    if float(speed_mm_s) <= 0.0:
        raise ValueError("Interpolation speed must be > 0.")
    if float(accel_mm_s2) <= 0.0:
        raise ValueError("Interpolation accel must be > 0.")
    if float(update_rate_hz) <= 0.0:
        raise ValueError("Interpolation update rate must be > 0.")

    planner_mode = str(planner_kind or "baseline").strip().lower()
    poses = build_keyframe_poses(keyframe_list, fk_fn=fk_fn)
    plan_started_mono = time.monotonic()
    actual_speed_mm_s = float(speed_mm_s)
    translation_speed_limit_mm_s = None
    orientation_speed_limit_mm_s = None
    if planner_mode == "experimental_tolerance_timing":
        actual_speed_mm_s, translation_speed_limit_mm_s, orientation_speed_limit_mm_s = (
            _derive_execution_constrained_speed_mm_s(
                requested_speed_mm_s=speed_mm_s,
                update_rate_hz=update_rate_hz,
                position_tolerance_mm=position_tolerance_mm,
                orientation_tolerance_deg=orientation_tolerance_deg,
            )
        )
    _LOG.info(
        "Planning interpolated keyframe path keyframes=%d planner_kind=%s speed_mm_s=%.3f "
        "actual_speed_mm_s=%.3f accel_mm_s2=%.3f update_rate_hz=%.3f max_joint_step_deg=%.3f",
        len(keyframe_list),
        planner_mode,
        float(speed_mm_s),
        float(actual_speed_mm_s),
        float(accel_mm_s2),
        float(update_rate_hz),
        float(max_joint_step_deg),
    )
    segments: list[InterpolatedSegment] = []
    full_trajectory: list[list[float]] = []
    total_duration_s = 0.0
    step_limit = max(0.1, float(max_joint_step_deg))
    planner_stats = InterpolationPlannerStats()

    for segment_index in range(len(poses) - 1):
        if cancel_check is not None and cancel_check():
            _LOG.info(
                "Interpolation planning cancelled before segment %d after %.3f s",
                segment_index + 1,
                time.monotonic() - plan_started_mono,
            )
            raise InterpolationPlanningCancelled("Interpolation planning cancelled.")
        start_pose = poses[segment_index]
        end_pose = poses[segment_index + 1]
        distance_mm = float(
            np.linalg.norm(np.asarray(end_pose.xyz_mm, dtype=float) - np.asarray(start_pose.xyz_mm, dtype=float))
        )
        orientation_delta_deg = _quaternion_angle_deg(start_pose.quat_xyzw, end_pose.quat_xyzw)
        if planner_mode == "experimental_tolerance_timing":
            progress_samples, duration_s = sample_trapezoidal_progress(
                distance_mm,
                actual_speed_mm_s,
                accel_mm_s2,
                update_rate_hz,
                orientation_delta_deg=orientation_delta_deg,
            )
        else:
            progress_samples, duration_s = sample_trapezoidal_progress(
                distance_mm,
                speed_mm_s,
                accel_mm_s2,
                update_rate_hz,
                orientation_delta_deg=orientation_delta_deg,
            )
        if planner_mode != "experimental_tolerance_timing" and str(sampling_mode).lower() == "tolerance":
            progress_samples = sample_tolerance_progress(
                translation_distance_mm=distance_mm,
                orientation_delta_deg=orientation_delta_deg,
                position_tolerance_mm=position_tolerance_mm,
                orientation_tolerance_deg=orientation_tolerance_deg,
            )
        _LOG.info(
            "Segment %d start_keyframe=%d end_keyframe=%d start_q=%s end_q=%s start_xyz_mm=%s end_xyz_mm=%s "
            "start_rpy_deg=%s end_rpy_deg=%s distance_mm=%.3f orientation_delta_deg=%.3f progress_samples=%d duration_s=%.3f",
            segment_index + 1,
            segment_index + 1,
            segment_index + 2,
            _fmt_joints(start_pose.q_deg),
            _fmt_joints(end_pose.q_deg),
            _fmt_xyz(start_pose.xyz_mm),
            _fmt_xyz(end_pose.xyz_mm),
            _fmt_rpy(start_pose.rpy_deg),
            _fmt_rpy(end_pose.rpy_deg),
            distance_mm,
            orientation_delta_deg,
            len(progress_samples),
            duration_s,
        )

        joint_trajectory = [list(start_pose.q_deg)]
        previous_solution = list(start_pose.q_deg)
        segment_started_mono = time.monotonic()

        for sample_offset, progress in enumerate(progress_samples[1:-1], start=1):
            if cancel_check is not None and cancel_check():
                _LOG.info(
                    "Interpolation planning cancelled at segment %d sample %d after %.3f s",
                    segment_index + 1,
                    sample_offset + 1,
                    time.monotonic() - plan_started_mono,
                )
                raise InterpolationPlanningCancelled("Interpolation planning cancelled.")
            if progress_cb is not None:
                progress_cb(
                    {
                        "phase": "planning",
                        "segment_index": segment_index,
                        "segment_count": len(poses) - 1,
                        "sample_index": sample_offset + 1,
                        "sample_count": len(progress_samples),
                        "elapsed_s": time.monotonic() - plan_started_mono,
                    }
                )
            xyz_mm, rpy_deg, _ = interpolate_segment_pose(start_pose, end_pose, progress)
            sample_started_mono = time.monotonic()
            seed_candidates = [
                ("previous", previous_solution),
                ("start", start_pose.q_deg),
            ]
            if progress >= 0.85:
                seed_candidates.append(("end", end_pose.q_deg))
            candidates, candidate_error = _solve_segment_pose_candidates(
                segment_index=segment_index,
                sample_index=sample_offset,
                progress=progress,
                xyz_mm=xyz_mm,
                rpy_deg=rpy_deg,
                seeds=seed_candidates,
                solve_ik_fn=solve_ik_fn,
                planner_stats=planner_stats,
            )
            candidate_solution = None
            chosen_seed_name = None
            chosen_seed_q = None
            ranked_candidates = []
            for seed_name, q_deg, ik_elapsed_s, _, _, _ in candidates:
                jump_deg = _max_joint_delta(q_deg, previous_solution)
                end_gap_deg = _max_joint_delta(q_deg, end_pose.q_deg)
                ranked_candidates.append((jump_deg, end_gap_deg, seed_name, q_deg, ik_elapsed_s))
            if ranked_candidates:
                feasible_candidates = [item for item in ranked_candidates if item[0] <= step_limit]
                if progress >= 0.9 and feasible_candidates:
                    preferred_candidates = feasible_candidates
                    if any(item[2] == "end" for item in feasible_candidates):
                        preferred_candidates = [item for item in feasible_candidates if item[2] == "end"]
                    preferred_candidates.sort(key=lambda item: (item[1], item[0], item[2]))
                    jump_deg, _, chosen_seed_name, candidate_solution, _ = preferred_candidates[0]
                else:
                    ranked_candidates.sort(key=lambda item: (item[0], item[1], item[2]))
                    jump_deg, _, chosen_seed_name, candidate_solution, _ = ranked_candidates[0]
                if chosen_seed_name == "previous":
                    chosen_seed_q = list(previous_solution)
                elif chosen_seed_name == "start":
                    chosen_seed_q = list(start_pose.q_deg)
                elif chosen_seed_name == "end":
                    chosen_seed_q = list(end_pose.q_deg)
            if candidate_solution is None:
                planner_stats.failed_sample_count += 1
                pos_err = getattr(candidate_error, "pos_err_mm", None)
                ori_err = getattr(candidate_error, "ori_err_rad", None)
                _LOG.error(
                    "Segment %d sample %d progress=%.4f xyz_mm=%s rpy_deg=%s previous_q=%s start_q=%s "
                    "IK failed pos_err_mm=%s ori_err_rad=%s",
                    segment_index + 1,
                    sample_offset + 1,
                    progress,
                    _fmt_xyz(xyz_mm),
                    _fmt_rpy(rpy_deg),
                    _fmt_joints(previous_solution),
                    _fmt_joints(start_pose.q_deg),
                    "?" if pos_err is None else f"{float(pos_err):.6f}",
                    "?" if ori_err is None else f"{float(ori_err):.6f}",
                )
                raise ValueError(
                    "IK failed at segment %d sample %d (pos_err=%s, ori_err=%s)."
                    % (
                        segment_index + 1,
                        sample_offset + 1,
                        "?" if pos_err is None else f"{float(pos_err):.3f} mm",
                        "?" if ori_err is None else f"{float(ori_err):.6f} rad",
                    )
                )
            jump_deg = _max_joint_delta(candidate_solution, previous_solution)
            _LOG.info(
                "Segment %d sample %d progress=%.4f xyz_mm=%s rpy_deg=%s seed=%s seed_q=%s solved_q=%s jump_deg=%.3f "
                "sample_elapsed_s=%.3f segment_elapsed_s=%.3f total_elapsed_s=%.3f",
                segment_index + 1,
                sample_offset + 1,
                progress,
                _fmt_xyz(xyz_mm),
                _fmt_rpy(rpy_deg),
                chosen_seed_name or "unknown",
                _fmt_joints(chosen_seed_q if chosen_seed_q is not None else previous_solution),
                _fmt_joints(candidate_solution),
                jump_deg,
                time.monotonic() - sample_started_mono,
                time.monotonic() - segment_started_mono,
                time.monotonic() - plan_started_mono,
            )
            if jump_deg > step_limit:
                planner_stats.rejected_joint_jump_count += 1
                _LOG.error(
                    "Excessive joint jump segment=%d sample=%d progress=%.4f jump_deg=%.3f step_limit_deg=%.3f "
                    "previous_q=%s solved_q=%s start_q=%s end_q=%s xyz_mm=%s rpy_deg=%s",
                    segment_index + 1,
                    sample_offset + 1,
                    progress,
                    jump_deg,
                    step_limit,
                    _fmt_joints(previous_solution),
                    _fmt_joints(candidate_solution),
                    _fmt_joints(start_pose.q_deg),
                    _fmt_joints(end_pose.q_deg),
                    _fmt_xyz(xyz_mm),
                    _fmt_rpy(rpy_deg),
                )
                raise ValueError(
                    "Excessive joint jump at segment %d sample %d (%.2f deg > %.2f deg)."
                    % (segment_index + 1, sample_offset + 1, jump_deg, step_limit)
                )
            joint_trajectory.append(candidate_solution)
            previous_solution = candidate_solution

        end_joint = list(end_pose.q_deg)
        end_jump_deg = _max_joint_delta(end_joint, previous_solution)
        _LOG.info(
            "Segment %d endpoint previous_q=%s end_q=%s end_jump_deg=%.3f",
            segment_index + 1,
            _fmt_joints(previous_solution),
            _fmt_joints(end_joint),
            end_jump_deg,
        )
        if end_jump_deg > step_limit and len(progress_samples) > 2:
            blend_details = None
            recovered_internal, recovery_details = _recover_endpoint_branch_suffix(
                segment_index=segment_index,
                progress_samples=progress_samples,
                joint_trajectory=joint_trajectory,
                start_pose=start_pose,
                end_pose=end_pose,
                solve_ik_fn=solve_ik_fn,
                planner_stats=planner_stats,
                step_limit=step_limit,
            )
            if recovered_internal is not None:
                joint_trajectory = [list(start_pose.q_deg)] + recovered_internal
                previous_solution = list(joint_trajectory[-1])
                end_jump_deg = _max_joint_delta(end_joint, previous_solution)
                planner_stats.recovered_endpoint_jump_count += 1
                _LOG.info(
                    "Endpoint recovery applied segment=%d splice_sample=%s previous_q=%s end_q=%s end_jump_deg=%.3f",
                    segment_index + 1,
                    "?" if recovery_details.get("splice_index") is None else int(recovery_details["splice_index"]) + 1,
                    _fmt_joints(previous_solution),
                    _fmt_joints(end_joint),
                    end_jump_deg,
                )
            if end_jump_deg > step_limit:
                blended_trajectory, blend_details = _build_terminal_joint_blend_suffix(
                    segment_index=segment_index,
                    joint_trajectory=joint_trajectory,
                    end_joint=end_joint,
                    step_limit=step_limit,
                )
                if blended_trajectory is not None:
                    joint_trajectory = blended_trajectory
                    previous_solution = list(joint_trajectory[-1])
                    end_jump_deg = _max_joint_delta(end_joint, previous_solution)
                    planner_stats.fallback_terminal_joint_blend_count += 1
                    _LOG.info(
                        "Terminal joint blend applied segment=%d bridge_start_sample=%s previous_q=%s end_q=%s end_jump_deg=%.3f",
                        segment_index + 1,
                        blend_details.get("bridge_start_index"),
                        _fmt_joints(previous_solution),
                        _fmt_joints(end_joint),
                        end_jump_deg,
                    )
                if end_jump_deg > step_limit:
                    _LOG.error(
                        "Excessive endpoint jump segment=%d end_jump_deg=%.3f step_limit_deg=%.3f previous_q=%s end_q=%s recovery=%s blend=%s",
                        segment_index + 1,
                        end_jump_deg,
                        step_limit,
                        _fmt_joints(previous_solution),
                        _fmt_joints(end_joint),
                        recovery_details,
                        blend_details,
                    )
                    raise ValueError(
                        "Excessive joint jump at segment %d endpoint (%.2f deg > %.2f deg)."
                        % (segment_index + 1, end_jump_deg, step_limit)
                    )
        joint_trajectory.append(end_joint)

        segment_step_deg = 0.0
        for index in range(1, len(joint_trajectory)):
            step_deg = _max_joint_delta(joint_trajectory[index], joint_trajectory[index - 1])
            segment_step_deg = max(segment_step_deg, step_deg)

        segments.append(
            InterpolatedSegment(
                segment_index=segment_index,
                start_keyframe_index=segment_index,
                end_keyframe_index=segment_index + 1,
                joint_trajectory_deg=joint_trajectory,
                sample_count=len(joint_trajectory),
                duration_s=float(duration_s),
                max_joint_step_deg=float(segment_step_deg),
            )
        )
        _LOG.info(
            "Segment %d planned sample_count=%d duration_s=%.3f max_joint_step_deg=%.3f segment_elapsed_s=%.3f total_elapsed_s=%.3f",
            segment_index + 1,
            len(joint_trajectory),
            duration_s,
            segment_step_deg,
            time.monotonic() - segment_started_mono,
            time.monotonic() - plan_started_mono,
        )
        total_duration_s += float(duration_s)
        if not full_trajectory:
            full_trajectory.extend([list(sample) for sample in joint_trajectory])
        else:
            full_trajectory.extend([list(sample) for sample in joint_trajectory[1:]])

    plan = KeyframeInterpolationPlan(
        segments=segments,
        full_joint_trajectory_deg=full_trajectory,
        update_rate_hz=float(update_rate_hz),
        estimated_duration_s=float(total_duration_s),
        planner_stats=planner_stats,
    )
    setattr(plan.planner_stats, "planner_kind", planner_mode)
    setattr(plan.planner_stats, "requested_speed_mm_s", float(speed_mm_s))
    setattr(plan.planner_stats, "actual_speed_mm_s", float(actual_speed_mm_s))
    setattr(plan.planner_stats, "translation_speed_limit_mm_s", translation_speed_limit_mm_s)
    setattr(plan.planner_stats, "orientation_speed_limit_mm_s", orientation_speed_limit_mm_s)
    _LOG.info(
        "Interpolation plan complete segments=%d total_samples=%d estimated_duration_s=%.3f update_rate_hz=%.3f "
        "planner_kind=%s actual_speed_mm_s=%.3f total_elapsed_s=%.3f",
        len(plan.segments),
        len(plan.full_joint_trajectory_deg),
        plan.estimated_duration_s,
        plan.update_rate_hz,
        planner_mode,
        float(actual_speed_mm_s),
        time.monotonic() - plan_started_mono,
    )
    return plan
