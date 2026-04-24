from __future__ import annotations

import math
import threading
import time
from typing import Callable, Optional

from app.logging_utils import get_stream_logger
from ik.kinematics import build_pose_target, ik_solve, ik_solve_multiseed


def _parse_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_joint_angles(values):
    if not isinstance(values, (list, tuple)):
        return None
    angles = []
    for value in list(values)[:6]:
        try:
            angles.append(float(value))
        except (TypeError, ValueError):
            return None
    return angles if len(angles) >= 6 else None


def _max_abs_joint_error(a, b):
    aa = _coerce_joint_angles(a)
    bb = _coerce_joint_angles(b)
    if aa is None or bb is None:
        return None
    return max(abs(float(aa[idx]) - float(bb[idx])) for idx in range(min(len(aa), len(bb))))


DEFAULT_POS_TOL_MM = 1.0
DEFAULT_IK_MAX_ITERS = 30  # Reduced from 120 for streaming performance (was causing 2s IK solves)
USE_POSITION_ONLY_IK = True
MULTISEED_MAX_ATTEMPTS = 25
PRECOMPUTE_MAX_DQ_DEG = 5.0  # Per-iteration joint step clamp for trajectory pre-computation


class StreamingInterpolator:
    def __init__(
        self,
        shared_state,
        settings: Optional[dict] = None,
        status_cb: Optional[Callable[[str], None]] = None,
        progress_cb: Optional[Callable[[float], None]] = None,
        joint_command_cb: Optional[Callable[[int, list[float]], bool]] = None,
        segment_meta_cb: Optional[Callable[[dict], None]] = None,
        runtime_snapshot_cb: Optional[Callable[[], Optional[dict]]] = None,
        ik_fallback_fn: Optional[Callable] = None,
        update_shared_state: bool = True,
        logger_name: str = "streaming",
    ):
        self._shared_state = shared_state
        self._settings = settings or {}
        self._motion = self._settings.get("streaming_motion", {})
        self._status_cb = status_cb or (lambda _: None)
        self._progress_cb = progress_cb or (lambda _: None)
        self._joint_command_cb = joint_command_cb
        self._segment_meta_cb = segment_meta_cb
        self._runtime_snapshot_cb = runtime_snapshot_cb
        self._ik_fallback_fn = ik_fallback_fn
        self._update_shared_state = update_shared_state
        self._log = get_stream_logger(logger_name)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._trajectory_cache = {
            "point_a": None,
            "point_b": None,
            "speed": None,
            "accel": None,
            "rate_hz": None,
            "trajectory": None,
            "direction": None,
        }

    def start(self, point_a_mm, point_b_mm, loop_enabled, overrides=None) -> bool:
        if self._thread and self._thread.is_alive():
            return False
        overrides = overrides or {}
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(point_a_mm, point_b_mm, loop_enabled, overrides),
            daemon=True,
        )
        self._thread.start()
        return True

    def start_precomputed(
        self,
        trajectory,
        update_rate_hz,
        overrides=None,
        segment_meta: Optional[dict] = None,
        send_stop_burst: bool = True,
        step_cb: Optional[Callable[[int, int, list[float]], None]] = None,
    ) -> bool:
        if self._thread and self._thread.is_alive():
            return False
        overrides = overrides or {}
        samples = [list(sample[:6]) for sample in list(trajectory or []) if sample is not None]
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_precomputed,
            args=(samples, float(update_rate_hz), overrides, dict(segment_meta or {}), bool(send_stop_burst), step_cb),
            daemon=True,
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def update_settings(self, settings: Optional[dict]) -> None:
        self._settings = settings or {}
        self._motion = self._settings.get("streaming_motion", {})

    def _status(self, text: str) -> None:
        self._status_cb(text)

    def _progress(self, pct: float) -> None:
        self._progress_cb(pct)

    def _set_streaming_active(self, active: bool) -> None:
        with self._shared_state.lock:
            self._shared_state.streaming_active = active

    def _get_runtime_snapshot(self) -> Optional[dict]:
        if self._runtime_snapshot_cb is None:
            return None
        try:
            snapshot = self._runtime_snapshot_cb()
        except Exception as exc:
            self._log.warning("Runtime snapshot callback failed: %s", exc)
            return None
        return snapshot if isinstance(snapshot, dict) else None

    def _should_extend_terminal_capture(self, snapshot: Optional[dict]) -> bool:
        if not isinstance(snapshot, dict):
            return False
        if not bool(snapshot.get("active", False)):
            return False
        if bool(snapshot.get("manual_stop", False)):
            return False
        if snapshot.get("fault_reason"):
            return False
        if bool(snapshot.get("handoff_requested", False)):
            return False
        phase = str(snapshot.get("phase") or "").upper()
        return bool(snapshot.get("in_capture", False)) or phase == "CAPTURE"

    def _run_terminal_capture_hold(
        self,
        final_target,
        dt_s: float,
        planned_step_count: int,
        segment_index: int,
        segment_direction: str,
        step_cb: Optional[Callable[[int, int, list[float]], None]] = None,
    ):
        snapshot = self._get_runtime_snapshot()
        if not self._should_extend_terminal_capture(snapshot):
            return list(final_target), None

        self._log.info(
            "Planned trajectory exhausted in CAPTURE; holding final target for measured convergence."
        )

        hold_started_mono = time.monotonic()
        next_time = hold_started_mono
        tail_step_index = int(max(0, planned_step_count))
        configured_window_s = None
        if isinstance(snapshot, dict):
            try:
                configured_window_s = float(
                    snapshot.get("measured_handoff_recovery_window_s")
                )
            except (TypeError, ValueError):
                configured_window_s = None
        if configured_window_s is not None and configured_window_s > 0.0:
            safe_hold_limit_s = max(1.0, configured_window_s + 5.0)
        else:
            safe_hold_limit_s = max(1.0, float(dt_s) * 4.0 + 5.0)

        while True:
            if self._stop.is_set():
                return list(final_target), "paused"

            snapshot = self._get_runtime_snapshot()
            if isinstance(snapshot, dict) and bool(snapshot.get("handoff_requested", False)):
                return list(final_target), "paused"
            if not self._should_extend_terminal_capture(snapshot):
                return list(final_target), None
            if (time.monotonic() - hold_started_mono) >= safe_hold_limit_s:
                self._log.warning(
                    "CAPTURE hold safety timeout after %.2f s without handoff.",
                    safe_hold_limit_s,
                )
                return list(final_target), "command failed"

            if self._joint_command_cb is not None:
                try:
                    ok = self._joint_command_cb(tail_step_index, list(final_target))
                except Exception as exc:
                    self._log.warning("Terminal CAPTURE hold callback failed: %s", exc)
                    return list(final_target), "command failed"
                if ok is False:
                    self._log.warning("Terminal CAPTURE hold callback returned failure.")
                    return list(final_target), "command failed"

            if self._update_shared_state:
                with self._shared_state.lock:
                    self._shared_state.manual_joint_target_deg = list(final_target)
                    self._shared_state.manual_joint_pending = True
                    self._shared_state.manual_joint_motion_overrides = {}
                    self._shared_state.ik_enabled = False

            if step_cb is not None:
                try:
                    terminal_idx = max(0, planned_step_count - 1)
                    step_cb(terminal_idx, terminal_idx, list(final_target))
                except Exception as exc:
                    self._log.warning("Terminal CAPTURE hold step callback failed: %s", exc)

            self._progress(100.0)

            next_time += dt_s
            now = time.monotonic()
            if next_time > now + 0.001:
                time.sleep(next_time - now)
            else:
                next_time = now
            tail_step_index += 1

    def _log_segment_boundary(
        self,
        label: str,
        segment_index: int,
        segment_direction: str,
        planned_angles,
        seed_angles=None,
    ) -> None:
        snapshot = self._get_runtime_snapshot()
        measured_angles = _coerce_joint_angles(
            None if snapshot is None else snapshot.get("measured_angles")
        )
        current_angles = _coerce_joint_angles(
            None if snapshot is None else snapshot.get("current_angles")
        )
        planned = _coerce_joint_angles(planned_angles)
        seed = _coerce_joint_angles(seed_angles)
        parts = [
            f"segment={int(segment_index)}",
            f"direction={str(segment_direction)}",
        ]
        if snapshot is not None and snapshot.get("phase") is not None:
            parts.append(f"phase={snapshot.get('phase')}")
        if planned is not None and seed is not None:
            seed_err = _max_abs_joint_error(seed, planned)
            if seed_err is not None:
                parts.append(f"seed_to_plan={seed_err:.2f} deg")
        if planned is not None and current_angles is not None:
            current_err = _max_abs_joint_error(current_angles, planned)
            if current_err is not None:
                parts.append(f"current_to_plan={current_err:.2f} deg")
        if planned is not None and measured_angles is not None:
            measured_err = _max_abs_joint_error(measured_angles, planned)
            if measured_err is not None:
                parts.append(f"measured_to_plan={measured_err:.2f} deg")
        if snapshot is not None:
            send_ms = snapshot.get("last_command_send_ms")
            fb_age_ms = snapshot.get("last_feedback_age_ms")
            fb_coherence_ms = snapshot.get("last_feedback_coherence_ms")
            fb_valid_count = snapshot.get("last_feedback_valid_count")
            fb_required = snapshot.get("last_feedback_required_valid")
            if send_ms is not None:
                parts.append(f"send={float(send_ms):.1f} ms")
            if fb_age_ms is not None:
                parts.append(f"fb_age={float(fb_age_ms):.1f} ms")
            if fb_coherence_ms is not None:
                parts.append(f"fb_coherence={float(fb_coherence_ms):.1f} ms")
            if fb_valid_count is not None and fb_required is not None:
                parts.append(f"fb_valid={int(fb_valid_count)}/{int(fb_required)}")
        self._log.info("%s | %s", label, " ".join(parts))

    def _log_runtime_diag(self, label: str, segment_index: int, segment_direction: str) -> None:
        snapshot = self._get_runtime_snapshot()
        if snapshot is None:
            return
        parts = [
            f"segment={int(segment_index)}",
            f"direction={str(segment_direction)}",
        ]
        if snapshot.get("phase") is not None:
            parts.append(f"phase={snapshot.get('phase')}")
        send_ms = snapshot.get("last_command_send_ms")
        fb_age_ms = snapshot.get("last_feedback_age_ms")
        fb_coherence_ms = snapshot.get("last_feedback_coherence_ms")
        fb_valid_count = snapshot.get("last_feedback_valid_count")
        fb_sample_count = snapshot.get("last_feedback_sample_count")
        fb_required = snapshot.get("last_feedback_required_valid")
        if send_ms is not None:
            parts.append(f"send={float(send_ms):.1f} ms")
        if fb_age_ms is not None:
            parts.append(f"fb_age={float(fb_age_ms):.1f} ms")
        if fb_coherence_ms is not None:
            parts.append(f"fb_coherence={float(fb_coherence_ms):.1f} ms")
        if fb_valid_count is not None and fb_required is not None:
            sample_txt = ""
            if fb_sample_count is not None:
                sample_txt = f" sample={int(fb_sample_count)}"
            parts.append(
                f"fb_valid={int(fb_valid_count)}/{int(fb_required)}{sample_txt}"
            )
        self._log.info("%s | %s", label, " ".join(parts))

    def _publish_trajectory_meta(
        self,
        trajectory,
        dt_s: float,
        segment_index: int,
        segment_direction: str,
        overrides: Optional[dict] = None,
    ) -> list[float]:
        final_target = list(trajectory[-1]) if trajectory else []
        if final_target:
            with self._shared_state.lock:
                self._shared_state.trajectory_final_target = list(final_target)

        run_id_raw = overrides.get("run_id") if isinstance(overrides, dict) else None
        run_id = None
        if run_id_raw is not None:
            try:
                run_id = int(run_id_raw)
            except (TypeError, ValueError):
                run_id = None

        if self._segment_meta_cb is not None and final_target:
            try:
                self._segment_meta_cb(
                    {
                        "run_id": run_id,
                        "segment_index": int(segment_index),
                        "segment_direction": str(segment_direction),
                        "waypoint_count": int(len(trajectory)),
                        "dt_s": float(dt_s),
                        "final_target_angles": [float(v) for v in final_target[:6]],
                        "created_mono": float(time.monotonic()),
                    }
                )
            except Exception as exc:
                self._log.warning("Segment metadata callback failed: %s", exc)

        return [float(v) for v in final_target[:6]]

    def _execute_trajectory(
        self,
        trajectory,
        dt_s: float,
        q_seed,
        segment_index: int = 0,
        segment_direction: str = "precomputed",
        send_stop_burst: bool = True,
        step_cb: Optional[Callable[[int, int, list[float]], None]] = None,
    ):
        if not trajectory:
            self._log.error("Trajectory execution requested with empty trajectory")
            return q_seed, "empty_trajectory"

        final_target = [float(v) for v in list(trajectory[-1])[:6]]
        start_target = [float(v) for v in list(trajectory[0])[:6]]

        self._progress(0.0)
        last_report_time = time.monotonic()
        segment_start_mono = time.monotonic()

        self._log_segment_boundary(
            "Segment start diag",
            segment_index,
            segment_direction,
            start_target,
            seed_angles=q_seed,
        )

        step_index = 0
        total_skipped = 0

        while step_index < len(trajectory) and not self._stop.is_set():
            now = time.monotonic()
            elapsed = now - segment_start_mono
            expected_step = int(elapsed / dt_s)

            if expected_step > step_index + 1:
                expected_step = min(expected_step, len(trajectory) - 1)
                skipped_actual = expected_step - step_index - 1
                total_skipped += max(0, skipped_actual)

                if skipped_actual > 0:
                    self._log.warning(
                        "[SKIP] Skipping %d steps (%d->%d): %.0f ms behind schedule",
                        skipped_actual,
                        step_index,
                        expected_step,
                        (elapsed - step_index * dt_s) * 1000,
                    )
                    self._log_runtime_diag(
                        "[SKIP_DIAG]",
                        segment_index,
                        segment_direction,
                    )
                    step_index = expected_step

            if self._stop.is_set():
                return q_seed, "paused"

            q_target = [float(v) for v in list(trajectory[step_index])[:6]]

            if self._joint_command_cb is not None:
                try:
                    ok = self._joint_command_cb(step_index, list(q_target))
                except Exception as exc:
                    self._log.warning("Joint command callback failed: %s", exc)
                    return q_seed, "command failed"
                if ok is False:
                    self._log.warning("Joint command callback returned failure.")
                    return q_seed, "command failed"

            if self._update_shared_state:
                with self._shared_state.lock:
                    self._shared_state.manual_joint_target_deg = list(q_target)
                    self._shared_state.manual_joint_pending = True
                    self._shared_state.manual_joint_motion_overrides = {}
                    self._shared_state.ik_enabled = False

            if step_cb is not None:
                try:
                    step_cb(step_index, max(0, len(trajectory) - 1), list(q_target))
                except Exception as exc:
                    self._log.warning("Trajectory step callback failed: %s", exc)

            progress_pct = (step_index / max(1, len(trajectory) - 1)) * 100.0
            self._progress(progress_pct)

            now = time.monotonic()
            if now - last_report_time >= 1.0:
                self._log.info(
                    "Trajectory execution progress: step=%d/%d (%.1f%%) [skipped %d total]",
                    step_index + 1,
                    len(trajectory),
                    progress_pct,
                    total_skipped,
                )
                last_report_time = now

            next_time = segment_start_mono + (step_index + 1) * dt_s
            now = time.monotonic()
            slip_s = now - next_time
            if slip_s > 0:
                if slip_s > 0.002:
                    self._log.warning(
                        "[SLIP] Step %d: %.1f ms behind schedule (dt=%.1f ms)",
                        step_index,
                        slip_s * 1000.0,
                        dt_s * 1000.0,
                    )
                    if slip_s > 0.100:
                        self._log_runtime_diag(
                            "[SLIP_DIAG]",
                            segment_index,
                            segment_direction,
                        )
            else:
                remaining = next_time - now
                if remaining > 0.001:
                    time.sleep(remaining)

            q_seed = q_target
            step_index += 1

        self._log.info("Trajectory execution complete: %d steps", len(trajectory))
        self._log.info(
            "Final trajectory target segment=%d direction=%s: "
            "J1=%.2f J2=%.2f J3=%.2f J4=%.2f J5=%.2f J6=%.2f",
            int(segment_index),
            str(segment_direction),
            final_target[0],
            final_target[1],
            final_target[2],
            final_target[3],
            final_target[4],
            final_target[5],
        )
        self._log_segment_boundary(
            "Segment end diag",
            segment_index,
            segment_direction,
            final_target,
        )

        q_seed, hold_err = self._run_terminal_capture_hold(
            final_target=final_target,
            dt_s=dt_s,
            planned_step_count=len(trajectory),
            segment_index=segment_index,
            segment_direction=segment_direction,
            step_cb=step_cb,
        )
        if hold_err is not None:
            return q_seed, hold_err

        if self._joint_command_cb is not None and send_stop_burst:
            self._log.info("Sending synchronous stop burst")
            try:
                for _ in range(3):
                    self._joint_command_cb(-1, list(trajectory[-1]))
                    time.sleep(0.01)
                self._log.info("Synchronous stop burst complete")
            except Exception as exc:
                self._log.warning("Synchronous stop burst failed: %s", exc)
        elif self._joint_command_cb is not None:
            self._log.debug(
                "Synchronous stop burst skipped for segment=%d direction=%s loop continuity.",
                int(segment_index),
                str(segment_direction),
            )

        self._progress(100.0)
        return q_seed, None

    def _precompute_trajectory(self, point_start, point_end, q_seed, speed, accel, update_rate_hz):
        """
        Pre-compute entire joint-space trajectory from start to end.

        Args:
            point_start: Start TCP position [x, y, z] in mm
            point_end: End TCP position [x, y, z] in mm
            q_seed: Initial joint configuration for IK seeding
            speed: TCP speed in mm/s
            accel: TCP acceleration in mm/s2 (or None)
            update_rate_hz: Control loop update rate

        Returns:
            (trajectory, error) where:
            - trajectory: List of joint configurations (one per time step), or None if failed
            - error: Error message if failed, None if successful
        """
        from ik.kinematics import build_pose_target, ik_solve, ik_solve_multiseed, within_limits
        from ik.config import get_config

        # Get joint limits from config
        config = get_config()
        joint_limits_deg = config.joint_limits_deg

        # Calculate path parameters
        ax, ay, az = point_start
        bx, by, bz = point_end
        dx = bx - ax
        dy = by - ay
        dz = bz - az
        dist = math.sqrt(dx * dx + dy * dy + dz * dz)

        if dist <= 1e-6:
            self._log.info("Distance too small (%.4f mm). Returning seed position.", dist)
            return [q_seed], None

        dt_s = 1.0 / update_rate_hz

        # Estimate trajectory duration using trapezoidal profile
        if accel and accel > 0.0:
            t_accel = speed / accel
            d_accel = 0.5 * accel * t_accel * t_accel
            if dist <= 2.0 * d_accel:
                # Triangle profile (doesn't reach cruise speed)
                duration_s = 2.0 * math.sqrt(dist / accel)
            else:
                # Trapezoidal profile
                cruise_dist = dist - 2.0 * d_accel
                duration_s = 2.0 * t_accel + cruise_dist / speed
        else:
            # Constant velocity
            duration_s = dist / speed

        num_steps = max(1, int(duration_s / dt_s))

        self._log.info(
            "Pre-computing trajectory: start=(%.1f, %.1f, %.1f) end=(%.1f, %.1f, %.1f) "
            "dist=%.1f mm duration=%.2f s steps=%d dt=%.3f s",
            ax, ay, az, bx, by, bz, dist, duration_s, num_steps, dt_s
        )

        trajectory = []
        traveled = 0.0
        v_prev = 0.0
        step_index = 0

        # Generate trajectory using trapezoidal velocity profile
        while traveled < dist and step_index <= num_steps:
            remaining = max(dist - traveled, 0.0)

            # Compute velocity using trapezoidal profile
            v_cap = speed
            if accel and accel > 0.0:
                v_cap = min(v_cap, math.sqrt(max(0.0, 2.0 * accel * remaining)))
                dv = accel * dt_s
                if v_prev < v_cap:
                    v_next = min(v_prev + dv, v_cap)
                else:
                    v_next = max(v_prev - dv, v_cap)
                step = 0.5 * (v_prev + v_next) * dt_s
            else:
                v_next = v_cap
                step = v_next * dt_s

            step = min(step, remaining)
            if step <= 0.0:
                break

            # Compute interpolated TCP position
            next_dist = min(traveled + step, dist)
            s = next_dist / dist
            tx = ax + dx * s
            ty = ay + dy * s
            tz = az + dz * s

            # Build target pose
            T_target = build_pose_target(tx, ty, tz, 0.0, 0.0, 0.0)

            # Solve IK with joint limits
            ok, q_sol, iters, pos_err, _ = ik_solve(
                T_target=T_target,
                q0_deg=q_seed,
                position_only=USE_POSITION_ONLY_IK,
                max_iters=DEFAULT_IK_MAX_ITERS,
                tol_pos_mm=DEFAULT_POS_TOL_MM,
                tol_ori_rad=1e-3,
                damping=0.05,
                step_scale=1.0,
                joint_limits_deg=joint_limits_deg,
                dh_table=None,
                verbose=False,
                max_dq_deg=PRECOMPUTE_MAX_DQ_DEG,
            )

            if not ok:
                # Log which joints violated limits
                lim = [(lo, hi) for lo, hi in joint_limits_deg]
                for j in range(6):
                    if q_sol[j] < lim[j][0] or q_sol[j] > lim[j][1]:
                        over = max(lim[j][0] - q_sol[j], q_sol[j] - lim[j][1])
                        self._log.warning(
                            "Single-seed IK: J%d=%.4f° violates [%.1f, %.1f] (over by %.4f°)",
                            j + 1, q_sol[j], lim[j][0], lim[j][1], over
                        )
                self._log.warning(
                    "Single-seed IK failed at step %d/%d: tcp=(%.1f, %.1f, %.1f) pos_err=%.2f mm — trying multi-seed fallback",
                    step_index, num_steps, tx, ty, tz, pos_err
                )
                ms_ok, ms_q, ms_iters, ms_pos_err, _, _, ms_n_seeds = ik_solve_multiseed(
                    T_target=T_target,
                    q0_deg=q_seed,
                    position_only=USE_POSITION_ONLY_IK,
                    max_iters=DEFAULT_IK_MAX_ITERS,
                    tol_pos_mm=DEFAULT_POS_TOL_MM,
                    tol_ori_rad=1e-3,
                    damping=0.05,
                    step_scale=1.0,
                    joint_limits_deg=joint_limits_deg,
                    dh_table=None,
                    max_dq_deg=PRECOMPUTE_MAX_DQ_DEG,
                    verbose=False,
                    max_attempts=MULTISEED_MAX_ATTEMPTS,
                    prefer_closest_to_q0=True,
                )
                if not ms_ok:
                    self._log.error(
                        "Multi-seed IK also failed at step %d/%d: tcp=(%.1f, %.1f, %.1f) pos_err=%.2f mm",
                        step_index, num_steps, tx, ty, tz, ms_pos_err
                    )
                    return None, f"ik_failed at step {step_index}: pos_err={ms_pos_err:.2f}mm (multi-seed)"
                self._log.info(
                    "Multi-seed fallback succeeded at step %d/%d: pos_err=%.2f mm, seeds=%d",
                    step_index, num_steps, ms_pos_err, ms_n_seeds
                )
                ok, q_sol, pos_err = ms_ok, ms_q, ms_pos_err

            # Check for branch jump: if solution is far from seed, try PyBullet fallback
            if ok:
                max_jump = max(abs(q_sol[j] - q_seed[j]) for j in range(min(len(q_sol), len(q_seed))))
                if max_jump > 30.0:  # 30 deg threshold for branch jump detection
                    if self._ik_fallback_fn is not None:
                        self._log.warning(
                            "Branch jump detected at step %d: max_jump=%.1f deg from seed. Trying PyBullet fallback.",
                            step_index, max_jump,
                        )
                        fb = self._ik_fallback_fn(tx, ty, tz, q_seed)
                        if fb is not None and fb.ok:
                            fb_jump = max(abs(fb.q_deg[j] - q_seed[j]) for j in range(min(len(fb.q_deg), len(q_seed))))
                            if fb_jump < max_jump:
                                self._log.info(
                                    "PyBullet fallback accepted: max_jump=%.1f deg (was %.1f)",
                                    fb_jump, max_jump,
                                )
                                q_sol = list(fb.q_deg)
                                pos_err = fb.pos_err_mm
                            else:
                                self._log.warning(
                                    "PyBullet fallback also far: max_jump=%.1f deg (was %.1f). Using PyBullet anyway (better pos_err).",
                                    fb_jump, max_jump,
                                )
                                q_sol = list(fb.q_deg)
                                pos_err = fb.pos_err_mm
                        else:
                            # PyBullet fallback also failed — both solvers produced branch jump.
                            # Use q_seed (previous step's solution) to maintain branch consistency.
                            # The small TCP movement between steps means q_seed is still close to correct,
                            # and far better than a 74+ deg branch jump.
                            self._log.warning(
                                "PyBullet fallback failed at step %d. Using previous seed to maintain branch consistency.",
                                step_index,
                            )
                            q_sol = list(q_seed)
                            pos_err = 0.0
                    else:
                        self._log.warning(
                            "Branch jump at step %d: max_jump=%.1f deg. No fallback available.",
                            step_index, max_jump,
                        )

            # Validate solution is within limits
            if not within_limits(q_sol, joint_limits_deg):
                self._log.error(
                    "Joint limits violated at step %d/%d: q=%s limits=%s",
                    step_index, num_steps, q_sol, joint_limits_deg
                )
                return None, f"limits_violated at step {step_index}"

            # Store solution and use as seed for next step (continuity)
            trajectory.append([float(v) for v in q_sol])
            q_seed = q_sol
            traveled = next_dist
            v_prev = v_next
            step_index += 1

        # Append zero-velocity dwell waypoints for clean stop
        DWELL_DURATION_S = 0.15  # 3 waypoints at 20 Hz
        dwell_steps = max(1, int(DWELL_DURATION_S / dt_s))

        self._log.info(
            "Appending %d zero-velocity dwell waypoints (%.3f s)",
            dwell_steps, dwell_steps * dt_s
        )

        # Hold final position for dwell duration
        final_q = trajectory[-1] if trajectory else q_seed
        for _ in range(dwell_steps):
            trajectory.append([float(v) for v in final_q])

        self._log.info(
            "Trajectory pre-computation complete: %d waypoints, final_dist=%.2f mm",
            len(trajectory), traveled
        )

        # Store final trajectory target for PRELOCK comparison (before execution starts)
        # This ensures PRELOCK compares against what the trajectory actually commanded,
        # not a geometrically different IK solution from re-solving with measured angles as seed
        with self._shared_state.lock:
            self._shared_state.trajectory_final_target = list(final_q)

        return trajectory, None

    def _run(self, point_a_mm, point_b_mm, loop_enabled, overrides):
        try:
            self._set_streaming_active(True)
            self._log.info("Streaming started loop=%s", loop_enabled)
            self._status("running")
            self._progress(0.0)

            with self._shared_state.lock:
                seed = list(self._shared_state.joint_deg)
            if len(seed) < 6:
                seed = [0.0] * 6

            forward = True
            segment_index = 0

            while not self._stop.is_set():
                start = point_a_mm if forward else point_b_mm
                end = point_b_mm if forward else point_a_mm
                segment_direction = "A->B" if forward else "B->A"
                send_stop_burst = not bool(loop_enabled)
                seed, err = self._run_segment(
                    start,
                    end,
                    seed,
                    overrides,
                    segment_index=segment_index,
                    segment_direction=segment_direction,
                    send_stop_burst=send_stop_burst,
                )
                if err == "paused":
                    self._status("paused")
                    self._log.info("Streaming paused")
                    return
                if err:
                    self._status("error")
                    self._log.warning("Streaming error: %s", err)
                    return
                if not loop_enabled:
                    break
                forward = not forward
                segment_index += 1

            self._status("idle")
            self._log.info("Streaming finished")
        except Exception:
            self._status("error")
            self._log.exception("Streaming crashed")
        finally:
            self._set_streaming_active(False)

    def _run_precomputed(self, trajectory, update_rate_hz, overrides, segment_meta, send_stop_burst, step_cb):
        try:
            self._set_streaming_active(True)
            self._log.info(
                "Precomputed trajectory started samples=%d rate_hz=%.3f send_stop_burst=%s",
                len(trajectory),
                update_rate_hz,
                send_stop_burst,
            )
            self._status("running")
            self._progress(0.0)

            if not trajectory:
                self._status("error")
                self._log.warning("Precomputed trajectory rejected: empty trajectory")
                return
            if not update_rate_hz or update_rate_hz <= 0.0:
                self._status("error")
                self._log.warning("Precomputed trajectory rejected: invalid update rate %s", update_rate_hz)
                return

            dt_s = 1.0 / float(update_rate_hz)
            segment_index = int(segment_meta.get("segment_index", 0) or 0)
            segment_direction = str(segment_meta.get("segment_direction") or "precomputed")
            final_target = self._publish_trajectory_meta(
                trajectory=trajectory,
                dt_s=dt_s,
                segment_index=segment_index,
                segment_direction=segment_direction,
                overrides=overrides,
            )
            if final_target:
                self._log.info(
                    "Executing precomputed trajectory segment=%d direction=%s "
                    "waypoints=%d rate=%.2f dt=%.4f",
                    segment_index,
                    segment_direction,
                    len(trajectory),
                    update_rate_hz,
                    dt_s,
                )

            q_seed = list(trajectory[0][:6])
            q_seed, err = self._execute_trajectory(
                trajectory=trajectory,
                dt_s=dt_s,
                q_seed=q_seed,
                segment_index=segment_index,
                segment_direction=segment_direction,
                send_stop_burst=send_stop_burst,
                step_cb=step_cb,
            )
            if err == "paused":
                self._status("paused")
                self._log.info("Precomputed trajectory paused")
                return
            if err:
                self._status("error")
                self._log.warning("Precomputed trajectory error: %s", err)
                return

            self._status("idle")
            self._log.info("Precomputed trajectory finished")
        except Exception:
            self._status("error")
            self._log.exception("Precomputed trajectory crashed")
        finally:
            self._set_streaming_active(False)

    def _run_segment(
        self,
        point_start,
        point_end,
        q_seed,
        overrides,
        segment_index: int = 0,
        segment_direction: str = "A->B",
        send_stop_burst: bool = True,
    ):
        """Execute point-to-point motion using pre-computed trajectory."""
        ax, ay, az = point_start
        bx, by, bz = point_end
        dx = bx - ax
        dy = by - ay
        dz = bz - az
        dist = math.sqrt(dx * dx + dy * dy + dz * dz)
        if dist <= 1e-6:
            self._log.info(
                "Segment distance too small (%.4f mm). Skipping.", dist
            )
            self._progress(100.0)
            return q_seed, None

        speed = _parse_float(overrides.get("speed_mm_s"), self._motion.get("tcp_speed_mm_s"))
        accel = _parse_float(overrides.get("accel_mm_s2"), self._motion.get("tcp_acc_mm_s2"))
        update_rate_hz = _parse_float(
            overrides.get("update_rate_hz"),
            self._motion.get("update_rate_hz"),
        )

        if not speed or speed <= 0:
            self._log.warning("Invalid speed: %s", speed)
            return q_seed, "invalid speed"
        if not update_rate_hz or update_rate_hz <= 0:
            self._log.warning("Invalid update rate: %s", update_rate_hz)
            return q_seed, "invalid update rate"
        dt_s = 1.0 / update_rate_hz

        # PRE-COMPUTE TRAJECTORY (with reverse-cache check)
        trajectory = None
        error = None
        cache = self._trajectory_cache
        if (cache["trajectory"] is not None
                and cache["speed"] == speed
                and cache["accel"] == accel
                and cache["rate_hz"] == update_rate_hz):
            cached_a = cache["point_a"]
            cached_b = cache["point_b"]
            eps = 0.01
            is_reverse = (
                all(abs(point_start[i] - cached_b[i]) < eps for i in range(3))
                and all(abs(point_end[i] - cached_a[i]) < eps for i in range(3))
            )
            if is_reverse:
                self._log.info(
                    "Reversing cached %s trajectory (%d waypoints) for %s direction",
                    cache["direction"], len(cache["trajectory"]), segment_direction,
                )
                trajectory = list(reversed(cache["trajectory"]))
                error = None
                if trajectory:
                    self._log_segment_boundary(
                        "Reverse cache reuse diag",
                        segment_index,
                        segment_direction,
                        trajectory[0],
                        seed_angles=q_seed,
                    )

        if trajectory is None:
            trajectory, error = self._precompute_trajectory(
                point_start, point_end, q_seed, speed, accel, update_rate_hz
            )
        if error:
            self._log.error("Trajectory pre-computation failed: %s", error)
            return q_seed, error
        if not trajectory:
            self._log.error("Trajectory pre-computation returned empty trajectory")
            return q_seed, "empty_trajectory"

        self._trajectory_cache = {
            "point_a": list(point_start),
            "point_b": list(point_end),
            "speed": speed,
            "accel": accel,
            "rate_hz": update_rate_hz,
            "trajectory": [list(wp) for wp in trajectory],
            "direction": segment_direction,
        }

        final_target = self._publish_trajectory_meta(
            trajectory=trajectory,
            dt_s=dt_s,
            segment_index=segment_index,
            segment_direction=segment_direction,
            overrides=overrides,
        )

        self._log.info(
            "Executing trajectory segment=%d direction=%s: "
            "A=(%.2f, %.2f, %.2f) B=(%.2f, %.2f, %.2f) dist=%.2f mm "
            "waypoints=%d speed=%.2f accel=%.2f rate=%.2f dt=%.4f",
            int(segment_index),
            str(segment_direction),
            ax, ay, az, bx, by, bz, dist, len(trajectory),
            speed, accel or 0.0, update_rate_hz, dt_s,
        )

        return self._execute_trajectory(
            trajectory=trajectory,
            dt_s=dt_s,
            q_seed=q_seed,
            segment_index=int(segment_index),
            segment_direction=str(segment_direction),
            send_stop_burst=send_stop_burst,
        )
