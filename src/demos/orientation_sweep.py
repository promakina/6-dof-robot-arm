from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class SweepConfig:
    anchor_xyz_mm: list[float]
    center_rpy_deg: list[float]
    pattern: str
    amplitude_deg: float
    speed_deg_s: float
    cycles: int
    update_rate_hz: float


@dataclass
class SweepPlan:
    joint_trajectory_deg: list[list[float]]
    orientation_trajectory_deg: list[list[float]]
    update_rate_hz: float
    estimated_duration_s: float
    feasible_samples: int
    total_samples: int
    skipped_samples: int
    full_joint_trajectory_deg: list[list[float]] = field(default_factory=list)


@dataclass
class ReachabilityReport:
    total_probes: int
    feasible_probes: int
    max_feasible_amplitude_deg: float
    failure_map: list[tuple[float, float, float, str]]


def _normalized_pattern(pattern: str) -> str:
    return str(pattern or "").strip().lower().replace(" ", "_")


def _sample_count(config: SweepConfig) -> int:
    cycles = max(1, int(config.cycles))
    rate_hz = max(1.0, float(config.update_rate_hz))
    speed_deg_s = max(1e-6, float(config.speed_deg_s))
    amplitude_deg = max(0.1, float(config.amplitude_deg))
    cycle_distance_deg = max(8.0, 2.0 * math.pi * amplitude_deg)
    duration_s = cycles * cycle_distance_deg / speed_deg_s
    return max(2, int(round(duration_s * rate_hz)) + 1)


def _ramp_weight(index: int, total: int, rate_hz: float) -> float:
    if total <= 2:
        return 0.0 if index in {0, total - 1} else 1.0
    ramp_len = min(max(1, int(round(0.5 * max(1.0, float(rate_hz))))), max(1, total // 2))
    if index == 0 or index == total - 1:
        return 0.0
    if index < ramp_len:
        phase = index / float(ramp_len)
        return 0.5 * (1.0 - math.cos(math.pi * phase))
    trailing = (total - 1) - index
    if trailing < ramp_len:
        phase = trailing / float(ramp_len)
        return 0.5 * (1.0 - math.cos(math.pi * phase))
    return 1.0


def generate_orientation_path(config: SweepConfig) -> list[list[float]]:
    pattern = _normalized_pattern(config.pattern)
    center_rx, center_ry, center_rz = [float(v) for v in config.center_rpy_deg[:3]]
    amplitude_deg = float(config.amplitude_deg)
    total = _sample_count(config)
    samples: list[list[float]] = []

    if pattern not in {"cone", "axis_wobble", "raster"}:
        raise ValueError(f"Unsupported orientation sweep pattern: {config.pattern}")

    for index in range(total):
        progress = index / float(max(1, total - 1))
        weight = _ramp_weight(index, total, config.update_rate_hz)

        if pattern == "cone":
            theta = progress * float(max(1, int(config.cycles))) * 2.0 * math.pi
            rx = center_rx + amplitude_deg * weight * math.cos(theta)
            ry = center_ry + amplitude_deg * weight * math.sin(theta)
            rz = center_rz
        elif pattern == "axis_wobble":
            total_phase = progress * float(max(1, int(config.cycles))) * 3.0
            phase_index = int(total_phase) % 3
            local_phase = total_phase - math.floor(total_phase)
            swing = amplitude_deg * weight * math.sin(local_phase * 2.0 * math.pi)
            rx = center_rx + (swing if phase_index == 0 else 0.0)
            ry = center_ry + (swing if phase_index == 1 else 0.0)
            rz = center_rz + (swing if phase_index == 2 else 0.0)
        else:
            side = max(2, int(round(max(2.0, (2.0 * amplitude_deg) / max(0.5, float(config.speed_deg_s) / max(1.0, float(config.update_rate_hz)))))))
            row = int(round(progress * max(1, side - 1)))
            col_progress = (progress * max(1, side - 1)) - math.floor(progress * max(1, side - 1))
            col = col_progress if row % 2 == 0 else (1.0 - col_progress)
            rx = center_rx + ((col * 2.0) - 1.0) * amplitude_deg * weight
            row_frac = (row / float(max(1, side - 1))) if side > 1 else 0.0
            ry = center_ry + ((row_frac * 2.0) - 1.0) * amplitude_deg * weight
            rz = center_rz

        samples.append([float(rx), float(ry), float(rz)])

    if samples:
        samples[0] = [center_rx, center_ry, center_rz]
        samples[-1] = [center_rx, center_ry, center_rz]
    return samples


def probe_reachability(
    anchor_xyz_mm,
    center_rpy_deg,
    amplitude_deg,
    probe_grid_size=8,
    solve_ik_fn=None,
    q_seed_deg=None,
    cancel_check=None,
) -> ReachabilityReport:
    if solve_ik_fn is None:
        raise ValueError("solve_ik_fn is required")
    cancel_check = cancel_check or (lambda: False)
    q_seed = list(q_seed_deg or [0.0] * 6)[:6]
    x_mm, y_mm, z_mm = [float(v) for v in list(anchor_xyz_mm)[:3]]
    center_rx, center_ry, center_rz = [float(v) for v in list(center_rpy_deg)[:3]]
    grid_size = max(2, int(probe_grid_size))
    failure_map: list[tuple[float, float, float, str]] = []
    feasible = 0

    for row in range(grid_size):
        for col in range(grid_size):
            if cancel_check():
                raise RuntimeError("Orientation reachability probe cancelled")
            rx = center_rx + (((col / float(grid_size - 1)) * 2.0) - 1.0) * float(amplitude_deg)
            ry = center_ry + (((row / float(grid_size - 1)) * 2.0) - 1.0) * float(amplitude_deg)
            rz = center_rz
            solution = solve_ik_fn(x_mm, y_mm, z_mm, rx, ry, rz, q0_deg=q_seed, position_only=False)
            if bool(getattr(solution, "ok", False)):
                feasible += 1
            else:
                failure_map.append((float(rx), float(ry), float(rz), "ik_failed"))

    total = grid_size * grid_size
    max_amp = float(amplitude_deg) if feasible == total else 0.0
    return ReachabilityReport(
        total_probes=total,
        feasible_probes=feasible,
        max_feasible_amplitude_deg=max_amp,
        failure_map=failure_map,
    )


def plan_orientation_sweep(
    config: SweepConfig,
    q_seed_deg,
    solve_ik_fn,
    cancel_check=None,
    progress_cb=None,
) -> SweepPlan:
    cancel_check = cancel_check or (lambda: False)
    progress_cb = progress_cb or (lambda _current, _total: None)
    if float(config.amplitude_deg) <= 0.0 or float(config.amplitude_deg) > 60.0:
        raise ValueError("amplitude_deg must be in (0, 60]")
    if float(config.speed_deg_s) <= 0.0:
        raise ValueError("speed_deg_s must be > 0")
    if int(config.cycles) <= 0:
        raise ValueError("cycles must be > 0")
    if float(config.update_rate_hz) <= 0.0:
        raise ValueError("update_rate_hz must be > 0")

    x_mm, y_mm, z_mm = [float(v) for v in config.anchor_xyz_mm[:3]]
    center_rx, center_ry, center_rz = [float(v) for v in config.center_rpy_deg[:3]]
    seed = list(q_seed_deg or [0.0] * 6)[:6]
    center_solution = solve_ik_fn(x_mm, y_mm, z_mm, center_rx, center_ry, center_rz, q0_deg=seed, position_only=False)
    if not bool(getattr(center_solution, "ok", False)):
        raise ValueError("Center orientation is not IK reachable")

    orientation_samples = generate_orientation_path(config)
    joint_trajectory: list[list[float]] = []
    skipped_samples = 0
    prev_solution = list(getattr(center_solution, "q_deg", seed)[:6])

    for index, (rx_deg, ry_deg, rz_deg) in enumerate(orientation_samples):
        if cancel_check():
            raise RuntimeError("Orientation sweep planning cancelled")
        solution = solve_ik_fn(x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg, q0_deg=prev_solution, position_only=False)
        if bool(getattr(solution, "ok", False)):
            q_deg = [float(v) for v in list(getattr(solution, "q_deg", prev_solution))[:6]]
            if joint_trajectory:
                jump = max(abs(cur - prev) for cur, prev in zip(q_deg, joint_trajectory[-1]))
                if jump > 10.0:
                    raise ValueError(f"Adjacent joint jump exceeds limit: {jump:.3f} deg")
            joint_trajectory.append(q_deg)
            prev_solution = q_deg
        else:
            skipped_samples += 1
        progress_cb(index + 1, len(orientation_samples))

    total_samples = len(orientation_samples)
    if total_samples == 0 or not joint_trajectory:
        raise ValueError("No feasible orientation sweep samples were planned")
    if skipped_samples / float(total_samples) > 0.2:
        raise ValueError("Too many IK failures while planning orientation sweep")

    return SweepPlan(
        joint_trajectory_deg=joint_trajectory,
        orientation_trajectory_deg=orientation_samples,
        update_rate_hz=float(config.update_rate_hz),
        estimated_duration_s=total_samples / float(config.update_rate_hz),
        feasible_samples=len(joint_trajectory),
        total_samples=total_samples,
        skipped_samples=skipped_samples,
        full_joint_trajectory_deg=list(joint_trajectory),
    )
