import logging
import math
import threading
import time

from ik.kinematics import compute_cartesian_pose, euler_zyx_to_rotation_matrix, rotation_matrix_to_axis_angle

from .feedback_worker import MultiJointFeedbackWorker
from .runtime import run_can_joint_command


def _clamp_value(value, lo, hi):
    return lo if value < lo else (hi if value > hi else value)


def _compute_cartesian_pose_error(measured_angles_deg, target_angles_deg):
    if not measured_angles_deg or not target_angles_deg:
        return None

    measured_pose = compute_cartesian_pose(list(measured_angles_deg[:6]))
    target_pose = compute_cartesian_pose(list(target_angles_deg[:6]))

    pos_err_mm = math.dist(measured_pose[:3], target_pose[:3])

    measured_rot = euler_zyx_to_rotation_matrix(
        float(measured_pose[3]),
        float(measured_pose[4]),
        float(measured_pose[5]),
    )
    target_rot = euler_zyx_to_rotation_matrix(
        float(target_pose[3]),
        float(target_pose[4]),
        float(target_pose[5]),
    )
    rel_rot = target_rot @ measured_rot.T
    _axis, angle_rad = rotation_matrix_to_axis_angle(rel_rot)
    ori_err_deg = float(math.degrees(angle_rad))

    return {
        "measured_pose": tuple(float(v) for v in measured_pose),
        "target_pose": tuple(float(v) for v in target_pose),
        "position_error_mm": float(pos_err_mm),
        "orientation_error_deg": float(ori_err_deg),
    }


def create_can_stream_state():
    return {
        "lock": threading.Lock(),
        "current_angles": None,
        "dt_s": None,
        "overrides": None,
        "min_velocity_deg_s": 0.0,
        "max_joint_velocity_deg_s": None,
        "stop_burst": 2,
        "phase": "IDLE",
        "active": False,
        "run_id": 0,
        "last_velocity_targets": None,
        "segment_index": 0,
        "segment_direction": None,
        "manual_stop": False,
        "finalize_in_progress": False,
        "fault_reason": None,
        "last_target_angles": None,
        "final_target_angles": None,
        "final_target_ready": False,
        "final_target_source": None,
        "final_target_latched_mono": None,
        "final_target_waypoint_count": 0,
        "last_final_target_wait_log_mono": None,
        "stream_started_mono": None,
        "last_step_mono": None,
        "feedback_every_n_steps": 1,
        "last_feedback_step": -1,
        "last_feedback_mono": None,
        "last_feedback_log_mono": None,
        "feedback_log_interval_s": 2.0,
        "stale_feedback_timeout_s": 0.35,
        "stale_startup_grace_s": 1.5,
        "stale_consecutive_threshold": 2,
        "stale_feedback_consecutive_count": 0,
        "feedback_mode": "background",
        "background_vector_require_all_joints": True,
        "background_vector_min_valid_joints": 6,
        "background_vector_max_coherence_window_s": 0.30,
        "background_speed_min_valid_joints": 6,
        "background_speed_min_samples": 2,
        "background_speed_invalid_log_interval_s": 0.5,
        "background_prev_angles": None,
        "background_prev_sample_mono": None,
        "background_prev_total_samples": None,
        "background_feedback_speeds": None,
        "background_last_speed_log_mono": None,
        "perf_metrics": None,
        "last_command_send_ms": None,
        "last_feedback_age_ms": None,
        "last_feedback_coherence_ms": None,
        "last_feedback_valid_count": None,
        "last_feedback_sample_count": None,
        "last_feedback_required_valid": None,
        "last_feedback_valid": None,
        "last_feedback_mode": None,
        "measured_angles": None,
        "measured_speeds": None,
        "measured_speed_valid": False,
        "in_capture": False,
        "capture_started_mono": None,
        "handoff_requested": False,
        "capture_dist_deg": 3.0,
        "capture_taper_min": 0.1,
        "continuous_settle_enabled": False,
        "continuous_settle_handoff_min_s": 0.0,
        "continuous_settle_blend_ramp_s": 0.0,
        "continuous_settle_target_blend_max": 0.0,
        "capture_feedback_boost_enabled": False,
        "capture_feedback_boost_joints_per_cycle": 0,
        "capture_feedback_boost_timeout_s": None,
        "capture_feedback_boost_applied": False,
        "capture_expand_max_extra_deg": 0.0,
        "capture_correction_ramp_s": 0.0,
        "capture_handoff_ready_dwell_s": 0.0,
        "capture_handoff_max_joint_error_deg": 0.0,
        "capture_handoff_max_cart_error_mm": 0.0,
        "late_capture_freeze_remaining_deg": 0.0,
        "capture_ready_since_mono": None,
        "late_capture_freeze_active": False,
        "transition_window_s": 0.8,
        "lock_entry_speed_deg_s": 4.0,
        "lock_entry_dist_max_deg": 8.0,
        "lock_entry_expand_gain": 0.6,
        "recovery_start_mono": None,
        "measured_handoff_recovery_window_s": 0.8,
        "measured_handoff_recovery_speed_deg_s": 2.0,
        "measured_handoff_recovery_kp_vel_per_deg": 0.8,
        "outer_loop_enabled": True,
        "kp_vel_per_deg": 5.0,
        "corr_vel_max_deg_s": 30.0,
        "correction_filter_alpha": 0.0,
        "v_corr_filtered": None,
        "ki_vel_per_deg_s": 0.0,
        "ki_windup_limit_deg_s": 0.0,
        "v_corr_integral": None,
        "max_tracking_error_deg": 0.0,
        "tracking_error_holdoff_s": 0.25,
        "tracking_fault_max_age_ms": 500,
        "tracking_error_since_mono": None,
        "lock_measured_pos_tol_deg": 2.0,
        "lock_measured_speed_tol_deg_s": 6.0,
        "measured_ready_samples": 2,
        "measured_ready_count": 0,
        "allow_lock_without_measured_ready": True,
        "max_consecutive_send_fail": 5,
        "send_fail_counts": None,
        "finalize_enabled": True,
        "finalize_lock_enabled": True,
        "lock_overrides": None,
        "pre_lock_settle_timeout_s": 0.6,
        "pre_lock_settle_dt_s": 0.05,
        "pre_lock_settle_samples": 2,
        "post_stop_settle_s": 0.12,
        "rx_drain_ms": 120,
        "verify_timeout_s": 2.5,
        "position_tolerance_deg": 0.35,
        "speed_tolerance_deg_s": 1.2,
        "done_consecutive_samples": 4,
        "loop_enabled": False,
    }


class CanStreamController:
    def __init__(
        self,
        shared_state,
        comm_client,
        stream_log,
        set_path_status,
        update_encoder_display,
        set_encoder_status,
    ):
        self.shared_state = shared_state
        self.comm_client = comm_client
        self.stream_log = stream_log
        self.set_path_status = set_path_status
        self.update_encoder_display = update_encoder_display
        self.set_encoder_status = set_encoder_status
        self.state = create_can_stream_state()
        self._streamer = None

    def attach_streamer(self, streamer):
        self._streamer = streamer

    def stop_streamer(self):
        if self._streamer is not None:
            self._streamer.stop()

    def set_can_phase(self, phase: str):
        phase_txt = str(phase or "UNKNOWN").upper()
        changed = False
        with self.state["lock"]:
            if self.state.get("phase") != phase_txt:
                self.state["phase"] = phase_txt
                changed = True
        if changed:
            self.set_path_status(phase_txt.lower())
            self.stream_log.info("CAN phase -> %s", phase_txt)

    def sync_runtime_diagnostics(self, perf_metrics, last_feedback_log_mono):
        with self.state["lock"]:
            self.state["perf_metrics"] = dict(perf_metrics)
            self.state["last_feedback_log_mono"] = last_feedback_log_mono

    def compute_cartesian_pose_error(self, measured_angles_deg, target_angles_deg):
        return _compute_cartesian_pose_error(measured_angles_deg, target_angles_deg)

    def send_can_velocity_stop(self, reason: str):
        with self.state["lock"]:
            overrides = dict(self.state["overrides"] or {})
            stop_burst = int(self.state.get("stop_burst", 2) or 2)
        if not self.comm_client.is_connected or not self.comm_client.is_homed:
            return
        try:
            ok = self.comm_client.stop_joint_velocities(
                overrides=overrides,
                burst=max(1, stop_burst),
            )
            self.stream_log.info(
                "CAN velocity stop (%s): ok=%s burst=%s",
                reason,
                ok,
                stop_burst,
            )
        except Exception as exc:
            self.stream_log.warning("CAN velocity stop failed (%s): %s", reason, exc)

    def get_runtime_snapshot(self):
        with self.state["lock"]:
            measured_angles = self.state.get("measured_angles")
            current_angles = self.state.get("current_angles")
            return {
                "run_id": int(self.state.get("run_id", 0) or 0),
                "active": bool(self.state.get("active", False)),
                "phase": self.state.get("phase"),
                "segment_index": int(self.state.get("segment_index", 0) or 0),
                "segment_direction": self.state.get("segment_direction"),
                "in_capture": bool(self.state.get("in_capture", False)),
                "handoff_requested": bool(self.state.get("handoff_requested", False)),
                "manual_stop": bool(self.state.get("manual_stop", False)),
                "fault_reason": self.state.get("fault_reason"),
                "measured_handoff_recovery_window_s": self.state.get(
                    "measured_handoff_recovery_window_s"
                ),
                "measured_angles": list(measured_angles) if measured_angles is not None else None,
                "current_angles": list(current_angles) if current_angles is not None else None,
                "last_command_send_ms": self.state.get("last_command_send_ms"),
                "last_feedback_age_ms": self.state.get("last_feedback_age_ms"),
                "last_feedback_coherence_ms": self.state.get("last_feedback_coherence_ms"),
                "last_feedback_valid_count": self.state.get("last_feedback_valid_count"),
                "last_feedback_sample_count": self.state.get("last_feedback_sample_count"),
                "last_feedback_required_valid": self.state.get("last_feedback_required_valid"),
                "last_feedback_valid": self.state.get("last_feedback_valid"),
                "last_feedback_mode": self.state.get("last_feedback_mode"),
            }

    def can_joint_command(self, step_index, target_angles):
        return run_can_joint_command(self, step_index, target_angles)

    def cleanup_feedback_worker(self):
        feedback_worker = self.state.get("feedback_worker")
        if feedback_worker is not None:
            try:
                feedback_worker.stop()
                self.stream_log.info("Feedback worker stopped")
            except Exception as exc:
                self.stream_log.warning("Feedback worker stop failed: %s", exc)
            with self.state["lock"]:
                self.state["feedback_worker"] = None

        with self.state["lock"]:
            self.state["inline_feedback_index"] = 0
            self.state["inline_measured_angles"] = [None] * 6
            self.state["inline_measured_timestamps"] = [None] * 6
            self.state["inline_measured_total_samples"] = [0] * 6
            self.state["prev_inline_angles"] = [None] * 6
            self.state["prev_inline_timestamps"] = [None] * 6
            self.state["feedback_speeds"] = [None] * 6
            self.state["background_prev_angles"] = [None] * 6
            self.state["background_prev_sample_mono"] = [None] * 6
            self.state["background_prev_total_samples"] = [0] * 6
            self.state["background_feedback_speeds"] = [0.0] * 6
            self.state["background_last_speed_log_mono"] = None
            self.state["last_velocity_targets"] = [0.0] * 6
            self.state["recovery_start_mono"] = None
            self.state["capture_feedback_boost_applied"] = False
            self.state["capture_ready_since_mono"] = None
            self.state["late_capture_freeze_active"] = False

    def begin_run(
        self,
        seed_angles,
        final_target_angles,
        can_overrides,
        can_lock_overrides,
        runtime_cfg,
    ):
        now_mono = time.monotonic()
        with self.state["lock"]:
            run_id = int(self.state.get("run_id", 0) or 0) + 1
            self.state["run_id"] = run_id
            self.state["last_velocity_targets"] = [0.0] * len(seed_angles[:6])
            self.state["segment_index"] = 0
            self.state["segment_direction"] = None
            self.state["active"] = True
            self.state["manual_stop"] = False
            self.state["finalize_in_progress"] = False
            self.state["fault_reason"] = None
            self.state["handoff_requested"] = False
            self.state["in_capture"] = False
            self.state["capture_started_mono"] = None
            self.state["measured_ready_count"] = 0
            self.state["recovery_start_mono"] = None
            self.state["tracking_error_since_mono"] = None
            self.state["current_angles"] = [float(v) for v in seed_angles[:6]]
            self.state["measured_angles"] = [float(v) for v in seed_angles[:6]]
            self.state["measured_speeds"] = [0.0] * len(seed_angles[:6])
            self.state["measured_speed_valid"] = False
            self.state["last_feedback_step"] = 0
            self.state["last_feedback_mono"] = now_mono
            self.state["last_feedback_log_mono"] = None
            self.state["stream_started_mono"] = now_mono
            self.state["last_step_mono"] = None
            self.state["last_target_angles"] = None
            self.state["final_target_angles"] = final_target_angles
            self.state["final_target_ready"] = False
            self.state["final_target_source"] = None
            self.state["final_target_latched_mono"] = None
            self.state["final_target_waypoint_count"] = 0
            self.state["last_final_target_wait_log_mono"] = None
            self.state["send_fail_counts"] = [0] * len(seed_angles[:6])
            self.state["overrides"] = dict(can_overrides)
            self.state["lock_overrides"] = dict(can_lock_overrides)
            self.state["min_velocity_deg_s"] = runtime_cfg["min_velocity_deg_s"]
            self.state["max_joint_velocity_deg_s"] = runtime_cfg["max_joint_velocity_deg_s"]
            self.state["stop_burst"] = runtime_cfg["stop_burst"]
            self.state["feedback_every_n_steps"] = runtime_cfg["feedback_every_n_steps"]
            self.state["feedback_log_interval_s"] = runtime_cfg["feedback_log_interval_s"]
            self.state["stale_feedback_timeout_s"] = runtime_cfg["stale_feedback_timeout_s"]
            self.state["stale_startup_grace_s"] = runtime_cfg["stale_startup_grace_s"]
            self.state["stale_consecutive_threshold"] = runtime_cfg["stale_consecutive_threshold"]
            self.state["stale_feedback_consecutive_count"] = 0
            self.state["perf_metrics"] = {
                "command_count": 0,
                "feedback_stale_count": 0,
                "feedback_miss_count": 0,
                "slip_count": 0,
                "max_slip_ms": 0.0,
                "last_report_time": now_mono,
            }
            self.state["capture_dist_deg"] = runtime_cfg["capture_dist_deg"]
            self.state["capture_taper_min"] = runtime_cfg["capture_taper_min"]
            self.state["continuous_settle_enabled"] = runtime_cfg["continuous_settle_enabled"]
            self.state["continuous_settle_handoff_min_s"] = runtime_cfg["continuous_settle_handoff_min_s"]
            self.state["continuous_settle_blend_ramp_s"] = runtime_cfg["continuous_settle_blend_ramp_s"]
            self.state["continuous_settle_target_blend_max"] = runtime_cfg["continuous_settle_target_blend_max"]
            self.state["capture_feedback_boost_enabled"] = bool(
                runtime_cfg.get("capture_feedback_boost_enabled", False)
            )
            self.state["capture_feedback_boost_joints_per_cycle"] = int(
                runtime_cfg.get("capture_feedback_boost_joints_per_cycle", 0) or 0
            )
            self.state["capture_feedback_boost_timeout_s"] = runtime_cfg.get(
                "capture_feedback_boost_timeout_s"
            )
            self.state["capture_feedback_boost_applied"] = False
            self.state["capture_expand_max_extra_deg"] = float(
                runtime_cfg.get("capture_expand_max_extra_deg", 0.0) or 0.0
            )
            self.state["capture_correction_ramp_s"] = float(
                runtime_cfg.get("capture_correction_ramp_s", 0.0) or 0.0
            )
            self.state["capture_handoff_ready_dwell_s"] = float(
                runtime_cfg.get("capture_handoff_ready_dwell_s", 0.0) or 0.0
            )
            self.state["capture_handoff_max_joint_error_deg"] = float(
                runtime_cfg.get("capture_handoff_max_joint_error_deg", 0.0) or 0.0
            )
            self.state["capture_handoff_max_cart_error_mm"] = float(
                runtime_cfg.get("capture_handoff_max_cart_error_mm", 0.0) or 0.0
            )
            self.state["late_capture_freeze_remaining_deg"] = float(
                runtime_cfg.get("late_capture_freeze_remaining_deg", 0.0) or 0.0
            )
            self.state["capture_ready_since_mono"] = None
            self.state["late_capture_freeze_active"] = False
            self.state["transition_window_s"] = runtime_cfg["transition_window_s"]
            self.state["lock_entry_speed_deg_s"] = runtime_cfg["lock_entry_speed_deg_s"]
            self.state["lock_entry_dist_max_deg"] = runtime_cfg["lock_entry_dist_max_deg"]
            self.state["lock_entry_expand_gain"] = runtime_cfg["lock_entry_expand_gain"]
            self.state["measured_handoff_recovery_window_s"] = runtime_cfg["measured_handoff_recovery_window_s"]
            self.state["measured_handoff_recovery_speed_deg_s"] = runtime_cfg["measured_handoff_recovery_speed_deg_s"]
            self.state["measured_handoff_recovery_kp_vel_per_deg"] = runtime_cfg["measured_handoff_recovery_kp_vel_per_deg"]
            self.state["outer_loop_enabled"] = runtime_cfg["outer_loop_enabled"]
            self.state["kp_vel_per_deg"] = runtime_cfg["kp_vel_per_deg"]
            self.state["corr_vel_max_deg_s"] = runtime_cfg["corr_vel_max_deg_s"]
            self.state["correction_filter_alpha"] = runtime_cfg["correction_filter_alpha"]
            self.state["v_corr_filtered"] = [0.0] * 6
            self.state["ki_vel_per_deg_s"] = runtime_cfg["ki_vel_per_deg_s"]
            self.state["ki_windup_limit_deg_s"] = runtime_cfg["ki_windup_limit_deg_s"]
            self.state["v_corr_integral"] = [0.0] * 6
            self.state["max_tracking_error_deg"] = runtime_cfg["max_tracking_error_deg"]
            self.state["tracking_error_holdoff_s"] = runtime_cfg["tracking_error_holdoff_s"]
            self.state["tracking_fault_max_age_ms"] = runtime_cfg["tracking_fault_max_age_ms"]
            self.state["lock_measured_pos_tol_deg"] = runtime_cfg["lock_measured_pos_tol_deg"]
            self.state["lock_measured_speed_tol_deg_s"] = runtime_cfg["lock_measured_speed_tol_deg_s"]
            self.state["measured_ready_samples"] = runtime_cfg["measured_ready_samples"]
            self.state["allow_lock_without_measured_ready"] = runtime_cfg["allow_lock_without_measured_ready"]
            self.state["max_consecutive_send_fail"] = runtime_cfg["max_consecutive_send_fail"]
            self.state["finalize_enabled"] = runtime_cfg["finalize_enabled"]
            self.state["finalize_lock_enabled"] = runtime_cfg["finalize_lock_enabled"]
            self.state["pre_lock_settle_timeout_s"] = runtime_cfg["pre_lock_settle_timeout_s"]
            self.state["pre_lock_settle_dt_s"] = runtime_cfg["pre_lock_settle_dt_s"]
            self.state["pre_lock_settle_samples"] = runtime_cfg["pre_lock_settle_samples"]
            self.state["post_stop_settle_s"] = runtime_cfg["post_stop_settle_s"]
            self.state["rx_drain_ms"] = runtime_cfg["rx_drain_ms"]
            self.state["verify_timeout_s"] = runtime_cfg["verify_timeout_s"]
            self.state["position_tolerance_deg"] = runtime_cfg["position_tolerance_deg"]
            self.state["speed_tolerance_deg_s"] = runtime_cfg["speed_tolerance_deg_s"]
            self.state["done_consecutive_samples"] = runtime_cfg["done_consecutive_samples"]
            self.state["loop_enabled"] = runtime_cfg["loop_enabled"]
            self.state["phase"] = "PRECHECK"
            self.state["feedback_mode"] = runtime_cfg["feedback_during_stream"]
            self.state["inline_feedback_enabled"] = runtime_cfg["feedback_during_stream"] == "inline"
            self.state["inline_feedback_index"] = 0
            self.state["inline_feedback_budget_ms"] = runtime_cfg["inline_feedback_time_budget_ms"]
            self.state["inline_feedback_joints_per_step"] = runtime_cfg["inline_feedback_joints_per_step"]
            self.state["inline_feedback_timeout_s"] = runtime_cfg["inline_feedback_timeout_s"]
            self.state["inline_measured_angles"] = [None] * 6
            self.state["inline_measured_timestamps"] = [None] * 6
            self.state["inline_measured_total_samples"] = [0] * 6
            self.state["prev_inline_angles"] = [None] * 6
            self.state["prev_inline_timestamps"] = [None] * 6
            self.state["feedback_speeds"] = [None] * 6
            self.state["background_vector_require_all_joints"] = runtime_cfg["background_vector_require_all_joints"]
            self.state["background_vector_min_valid_joints"] = runtime_cfg["background_vector_min_valid_joints"]
            self.state["background_vector_max_coherence_window_s"] = runtime_cfg["background_vector_max_coherence_window_s"]
            self.state["background_speed_min_valid_joints"] = runtime_cfg["background_speed_min_valid_joints"]
            self.state["background_speed_min_samples"] = runtime_cfg["background_speed_min_samples"]
            self.state["background_speed_invalid_log_interval_s"] = runtime_cfg["background_speed_invalid_log_interval_s"]
            self.state["background_prev_angles"] = [None] * 6
            self.state["background_prev_sample_mono"] = [None] * 6
            self.state["background_prev_total_samples"] = [0] * 6
            self.state["background_feedback_speeds"] = [0.0] * 6
            self.state["background_last_speed_log_mono"] = None
        return run_id

    def set_dt(self, dt_s):
        with self.state["lock"]:
            self.state["dt_s"] = dt_s

    def attach_feedback_runtime(
        self,
        feedback_worker,
        feedback_speed_min_dt_s,
        feedback_speed_max_deg_s,
    ):
        with self.state["lock"]:
            self.state["feedback_worker"] = feedback_worker
            self.state["feedback_speed_min_dt_s"] = feedback_speed_min_dt_s
            self.state["feedback_speed_max_deg_s"] = feedback_speed_max_deg_s

    def setup_feedback_worker(
        self,
        enable_feedback,
        feedback_during_stream,
        feedback_rate_hz,
        feedback_timeout_s,
        stale_feedback_timeout_s,
        feedback_log_interval_s,
        background_worker_joints_per_cycle,
        background_worker_inter_joint_gap_s,
        background_worker_slow_cycle_warn_s,
        background_worker_cycle_log_interval_s,
        background_worker_warmup_s,
        inline_feedback_time_budget_ms,
        inline_feedback_joints_per_step,
        seed_joint_angles,
    ):
        feedback_worker = None
        if enable_feedback:
            if feedback_during_stream == "background":
                feedback_worker = MultiJointFeedbackWorker(
                    comm_client=self.comm_client,
                    joint_ids=list(range(1, 7)),
                    poll_rate_hz=feedback_rate_hz,
                    timeout_s=feedback_timeout_s,
                    joints_per_cycle=background_worker_joints_per_cycle,
                    inter_joint_gap_s=background_worker_inter_joint_gap_s,
                    slow_cycle_warn_s=background_worker_slow_cycle_warn_s,
                    cycle_log_interval_s=background_worker_cycle_log_interval_s,
                    logger=self.stream_log,
                )
                seed_angles = {
                    jid: float(seed_joint_angles[jid - 1])
                    for jid in range(1, 7)
                    if jid - 1 < len(seed_joint_angles) and seed_joint_angles[jid - 1] is not None
                }
                if seed_angles:
                    feedback_worker.seed(seed_angles)
                feedback_worker.start()
                if background_worker_warmup_s > 0.0:
                    self.stream_log.info(
                        "Feedback worker warmup: waiting %.0f ms before starting command stream",
                        background_worker_warmup_s * 1000.0,
                    )
                    time.sleep(background_worker_warmup_s)
                self.stream_log.info(
                    "Feedback mode: background worker @ %.1f Hz "
                    "(timeout=%.0f ms stale=%.0f ms log_interval=%.1fs "
                    "reads_per_cycle=%d inter_joint_gap=%.1f ms slow_warn=%.1f ms warmup=%.0f ms)",
                    feedback_rate_hz,
                    feedback_timeout_s * 1000,
                    stale_feedback_timeout_s * 1000,
                    feedback_log_interval_s,
                    background_worker_joints_per_cycle,
                    background_worker_inter_joint_gap_s * 1000.0,
                    background_worker_slow_cycle_warn_s * 1000.0,
                    background_worker_warmup_s * 1000.0,
                )
            elif feedback_during_stream == "inline":
                self.stream_log.warning(
                    "Feedback mode: inline (DEGRADED fallback). Background worker mode is recommended."
                )
                self.stream_log.info(
                    "Feedback mode: inline (budget=%.1fms, joints_per_step=%d, timeout=%.1fms)",
                    inline_feedback_time_budget_ms,
                    inline_feedback_joints_per_step,
                    min(0.02, feedback_timeout_s) * 1000,
                )
            else:
                self.stream_log.warning(
                    "Feedback mode: disabled (feedback_during_stream=%s).",
                    feedback_during_stream,
                )
        else:
            self.stream_log.info("Feedback: disabled (enable_feedback=False)")
        return feedback_worker

    def mark_start_failed(self):
        with self.state["lock"]:
            self.state["active"] = False

    def mark_manual_stop(self):
        with self.state["lock"]:
            self.state["manual_stop"] = True
            self.state["handoff_requested"] = False

    def handle_stream_status(self, state_name_raw, finalize_callback):
        state_name = str(state_name_raw or "").lower()
        self.set_path_status(state_name)
        if state_name == "running":
            self.set_can_phase("STREAM")
            self.set_encoder_status("CAN stream: running")
            return

        if state_name_raw in {"idle", "paused", "error"}:
            self.send_can_velocity_stop(f"state={state_name_raw}")

        should_finalize = False
        start_finalize_thread = False
        do_cleanup_feedback = False
        phase_after_lock = None
        encoder_status_after_lock = None
        state_terminal = False
        run_id = None
        with self.state["lock"]:
            if not self.state.get("active"):
                return
            run_id = int(self.state.get("run_id", 0) or 0)
            manual_stop = bool(self.state.get("manual_stop", False))
            handoff_requested = bool(self.state.get("handoff_requested", False))
            finalize_enabled = bool(self.state.get("finalize_enabled", True))
            finalize_in_progress = bool(self.state.get("finalize_in_progress", False))
            fault_reason = self.state.get("fault_reason")

            if state_name == "error":
                self.state["active"] = False
                self.state["finalize_in_progress"] = False
                do_cleanup_feedback = True
                phase_after_lock = "FAULT"
                encoder_status_after_lock = (
                    f"CAN stream: fault ({fault_reason if fault_reason else 'stream error'})"
                )
                state_terminal = True

            if state_name == "paused":
                if manual_stop:
                    self.state["active"] = False
                    self.state["finalize_in_progress"] = False
                    phase_after_lock = "PAUSED"
                    encoder_status_after_lock = "CAN stream: stopped."
                    state_terminal = True
                should_finalize = bool(finalize_enabled and handoff_requested)
            elif state_name == "idle":
                should_finalize = bool(finalize_enabled)

            if not state_terminal:
                if should_finalize and finalize_in_progress:
                    return
                if should_finalize:
                    self.state["finalize_in_progress"] = True
                    start_finalize_thread = True
                else:
                    self.state["active"] = False
                    self.state["finalize_in_progress"] = False
                    phase_after_lock = "COMPLETE"
                    encoder_status_after_lock = "CAN stream: complete."

        if do_cleanup_feedback:
            self.cleanup_feedback_worker()
            if phase_after_lock is not None:
                self.set_can_phase(phase_after_lock)
            if encoder_status_after_lock is not None:
                self.set_encoder_status(encoder_status_after_lock)
            return

        if phase_after_lock is not None:
            self.set_can_phase(phase_after_lock)
            if encoder_status_after_lock is not None:
                self.set_encoder_status(encoder_status_after_lock)
            return

        if start_finalize_thread:
            threading.Thread(
                target=finalize_callback,
                args=(run_id, state_name),
                daemon=True,
            ).start()

    def finalize_can_stream(self, run_id: int, trigger_state: str):
        self.set_can_phase("STOP_AND_SETTLE")
        self.send_can_velocity_stop(f"finalize_trigger={trigger_state}")

        with self.state["lock"]:
            measured_angles_at_transition = (
                list(self.state["measured_angles"])
                if self.state["measured_angles"] is not None
                else None
            )
            feedback_mode_at_transition = str(
                self.state.get("feedback_mode", "unknown")
            ).lower()

        if measured_angles_at_transition is not None:
            sparse_samples = any(v is None for v in measured_angles_at_transition[:6])
            self.stream_log.info(
                "Transition to STOP_AND_SETTLE | Last measured (mode=%s sparse=%s): "
                "J1=%.2f J2=%.2f J3=%.2f J4=%.2f J5=%.2f J6=%.2f",
                feedback_mode_at_transition,
                sparse_samples,
                measured_angles_at_transition[0] if len(measured_angles_at_transition) > 0 and measured_angles_at_transition[0] is not None else -999,
                measured_angles_at_transition[1] if len(measured_angles_at_transition) > 1 and measured_angles_at_transition[1] is not None else -999,
                measured_angles_at_transition[2] if len(measured_angles_at_transition) > 2 and measured_angles_at_transition[2] is not None else -999,
                measured_angles_at_transition[3] if len(measured_angles_at_transition) > 3 and measured_angles_at_transition[3] is not None else -999,
                measured_angles_at_transition[4] if len(measured_angles_at_transition) > 4 and measured_angles_at_transition[4] is not None else -999,
                measured_angles_at_transition[5] if len(measured_angles_at_transition) > 5 and measured_angles_at_transition[5] is not None else -999,
            )
            if feedback_mode_at_transition == "inline":
                self.stream_log.info(
                    "Inline mode note: measurements may be mixed-timestamp due to round-robin reads."
                )
            elif feedback_mode_at_transition == "background":
                self.stream_log.info(
                    "Background mode note: measurements come from worker snapshot cache."
                )
        else:
            self.stream_log.info(
                "Transition to STOP_AND_SETTLE | Last measured: No feedback data available"
            )

        with self.state["lock"]:
            if run_id != self.state.get("run_id"):
                self.state["finalize_in_progress"] = False
                return
            state_final_target = (
                list(self.state["final_target_angles"])
                if self.state["final_target_angles"] is not None
                else None
            )
            state_final_target_ready = bool(self.state.get("final_target_ready", False))
            state_final_target_source = self.state.get("final_target_source")
            finalize_enabled = bool(self.state.get("finalize_enabled", True))
            finalize_lock_enabled = bool(self.state.get("finalize_lock_enabled", True))
            lock_overrides = dict(self.state.get("lock_overrides") or {})
            pre_lock_settle_timeout_s = float(self.state.get("pre_lock_settle_timeout_s", 0.6) or 0.6)
            pre_lock_settle_dt_s = float(self.state.get("pre_lock_settle_dt_s", 0.05) or 0.05)
            pre_lock_settle_samples = int(self.state.get("pre_lock_settle_samples", 2) or 2)
            lock_pos_tol = float(self.state.get("lock_measured_pos_tol_deg", 2.0) or 2.0)
            lock_speed_tol = float(self.state.get("lock_measured_speed_tol_deg_s", 6.0) or 6.0)
            allow_lock_without_measured_ready = bool(
                self.state.get("allow_lock_without_measured_ready", True)
            )
            verify_timeout_s = float(self.state.get("verify_timeout_s", 2.5) or 2.5)
            position_tolerance_deg = float(self.state.get("position_tolerance_deg", 0.35) or 0.35)
            speed_tolerance_deg_s = float(self.state.get("speed_tolerance_deg_s", 1.2) or 1.2)
            done_consecutive_samples = int(self.state.get("done_consecutive_samples", 4) or 4)
            post_stop_settle_s = float(self.state.get("post_stop_settle_s", 0.12) or 0.12)
            rx_drain_ms = int(self.state.get("rx_drain_ms", 120) or 120)

        if state_final_target_ready and state_final_target is not None and len(state_final_target) >= 6:
            final_target_angles = [float(v) for v in state_final_target[:6]]
            self.stream_log.info(
                "PRELOCK using latched trajectory target (source=%s): "
                "J1=%.2f J2=%.2f J3=%.2f J4=%.2f J5=%.2f J6=%.2f",
                state_final_target_source,
                final_target_angles[0],
                final_target_angles[1],
                final_target_angles[2],
                final_target_angles[3],
                final_target_angles[4],
                final_target_angles[5],
            )
        else:
            with self.shared_state.lock:
                trajectory_target = getattr(self.shared_state, "trajectory_final_target", None)
            if trajectory_target and len(trajectory_target) >= 6:
                final_target_angles = [float(v) for v in trajectory_target[:6]]
                self.stream_log.warning(
                    "PRELOCK fallback: can_stream_state target unavailable "
                    "(ready=%s source=%s). Using shared_state trajectory target.",
                    state_final_target_ready,
                    state_final_target_source,
                )
                self.stream_log.info(
                    "PRELOCK using shared_state trajectory target: "
                    "J1=%.2f J2=%.2f J3=%.2f J4=%.2f J5=%.2f J6=%.2f",
                    final_target_angles[0],
                    final_target_angles[1],
                    final_target_angles[2],
                    final_target_angles[3],
                    final_target_angles[4],
                    final_target_angles[5],
                )
            else:
                final_target_angles = None

        if not finalize_enabled:
            with self.state["lock"]:
                if run_id == self.state.get("run_id"):
                    self.state["active"] = False
                    self.state["finalize_in_progress"] = False
            self.set_can_phase("COMPLETE")
            self.set_encoder_status("CAN stream: complete (no finalize).")
            return

        if not final_target_angles:
            self.cleanup_feedback_worker()
            with self.state["lock"]:
                if run_id == self.state.get("run_id"):
                    self.state["active"] = False
                    self.state["finalize_in_progress"] = False
                    self.state["fault_reason"] = "missing final target for lock"
            self.set_can_phase("FAULT")
            self.set_encoder_status("CAN stream: fault (missing final target).")
            return

        if post_stop_settle_s > 0.0:
            try:
                self.comm_client.ensure_ready()
                self.comm_client._controller._drain_rx(drain_ms=max(1, rx_drain_ms))
            except Exception as exc:
                self.stream_log.warning("CAN RX drain skipped: %s", exc)
            time.sleep(post_stop_settle_s)

        def is_cancelled() -> bool:
            with self.state["lock"]:
                if run_id != self.state.get("run_id"):
                    return True
                if bool(self.state.get("manual_stop", False)):
                    return True
            return False

        if is_cancelled():
            with self.state["lock"]:
                if run_id == self.state.get("run_id"):
                    self.state["active"] = False
                    self.state["finalize_in_progress"] = False
            self.set_can_phase("PAUSED")
            self.set_encoder_status("CAN stream: stopped.")
            return

        self.cleanup_feedback_worker()
        self.set_can_phase("PRELOCK")
        self.set_encoder_status("CAN stream: pre-lock settle...")

        settle_count = 0
        settle_prev_angles = None
        settle_prev_t = None
        last_prelock_measured_vals = None
        settle_start = time.monotonic()
        settle_ok = False
        while (time.monotonic() - settle_start) <= max(0.0, pre_lock_settle_timeout_s):
            if is_cancelled():
                self.cleanup_feedback_worker()
                with self.state["lock"]:
                    if run_id == self.state.get("run_id"):
                        self.state["active"] = False
                        self.state["finalize_in_progress"] = False
                self.set_can_phase("PAUSED")
                self.set_encoder_status("CAN stream: stopped.")
                return

            try:
                measured = self.comm_client.read_joint_angles()
                raw_encoder_ticks = self.comm_client.read_encoders(timeout_s=None)
            except Exception as exc:
                self.stream_log.warning("PRELOCK read failed: %s", exc)
                measured = None
                raw_encoder_ticks = None

            if measured and all(v is not None for v in measured[:len(final_target_angles)]):
                measured_vals = [float(v) for v in measured[:len(final_target_angles)]]
                last_prelock_measured_vals = list(measured_vals)
                self.update_encoder_display(measured_vals)
                now_t = time.monotonic()
                if settle_prev_angles is None or settle_prev_t is None:
                    max_speed = float("inf")
                else:
                    dt_local = max(1e-6, now_t - settle_prev_t)
                    max_speed = max(
                        abs((measured_vals[idx] - settle_prev_angles[idx]) / dt_local)
                        for idx in range(len(measured_vals))
                    )
                max_pos_err = max(
                    abs(final_target_angles[idx] - measured_vals[idx])
                    for idx in range(len(measured_vals))
                )
                settle_now = (max_pos_err <= lock_pos_tol) and (max_speed <= lock_speed_tol)
                settle_count = (settle_count + 1) if settle_now else 0
                settle_prev_angles = measured_vals
                settle_prev_t = now_t

                joint_errors = []
                for idx in range(len(measured_vals)):
                    err = abs(float(final_target_angles[idx]) - float(measured_vals[idx]))
                    joint_errors.append((idx, err, float(measured_vals[idx]), float(final_target_angles[idx])))
                cartesian_error = _compute_cartesian_pose_error(measured_vals, final_target_angles)

                if joint_errors:
                    max_err_joint = max(joint_errors, key=lambda x: x[1])
                    idx, err, meas, tgt = max_err_joint
                    if raw_encoder_ticks:
                        home_offsets = self.comm_client._home_offsets if hasattr(self.comm_client, "_home_offsets") else [None] * 6
                        self.stream_log.info(
                            "[PRELOCK_DIAG] Raw encoder ticks: J1=%s J2=%s J3=%s J4=%s J5=%s J6=%s",
                            raw_encoder_ticks[0] if len(raw_encoder_ticks) > 0 else None,
                            raw_encoder_ticks[1] if len(raw_encoder_ticks) > 1 else None,
                            raw_encoder_ticks[2] if len(raw_encoder_ticks) > 2 else None,
                            raw_encoder_ticks[3] if len(raw_encoder_ticks) > 3 else None,
                            raw_encoder_ticks[4] if len(raw_encoder_ticks) > 4 else None,
                            raw_encoder_ticks[5] if len(raw_encoder_ticks) > 5 else None,
                        )
                        self.stream_log.info(
                            "[PRELOCK_DIAG] Home offsets: J1=%s J2=%s J3=%s J4=%s J5=%s J6=%s",
                            home_offsets[0] if len(home_offsets) > 0 else None,
                            home_offsets[1] if len(home_offsets) > 1 else None,
                            home_offsets[2] if len(home_offsets) > 2 else None,
                            home_offsets[3] if len(home_offsets) > 3 else None,
                            home_offsets[4] if len(home_offsets) > 4 else None,
                            home_offsets[5] if len(home_offsets) > 5 else None,
                        )

                    self.stream_log.info(
                        "PRELOCK | Per-joint errors (deg): J1=%.2f J2=%.2f J3=%.2f J4=%.2f J5=%.2f J6=%.2f",
                        joint_errors[0][1] if len(joint_errors) > 0 else -999,
                        joint_errors[1][1] if len(joint_errors) > 1 else -999,
                        joint_errors[2][1] if len(joint_errors) > 2 else -999,
                        joint_errors[3][1] if len(joint_errors) > 3 else -999,
                        joint_errors[4][1] if len(joint_errors) > 4 else -999,
                        joint_errors[5][1] if len(joint_errors) > 5 else -999,
                    )

                    self.stream_log.info(
                        "PRELOCK | Max error: J%d measured=%.2f deg target=%.2f deg error=%.2f deg",
                        idx + 1, meas, tgt, err
                    )
                if cartesian_error is not None:
                    self.stream_log.info(
                        "PRELOCK | Cartesian error: pos_mm=%.3f ori_deg=%.3f "
                        "measured_xyz=(%.2f, %.2f, %.2f) target_xyz=(%.2f, %.2f, %.2f)",
                        cartesian_error["position_error_mm"],
                        cartesian_error["orientation_error_deg"],
                        cartesian_error["measured_pose"][0],
                        cartesian_error["measured_pose"][1],
                        cartesian_error["measured_pose"][2],
                        cartesian_error["target_pose"][0],
                        cartesian_error["target_pose"][1],
                        cartesian_error["target_pose"][2],
                    )

                self.stream_log.info(
                    "PRELOCK | max_pos_err=%.3f max_speed=%.3f settle_now=%s settle_count=%d/%d",
                    max_pos_err,
                    max_speed,
                    settle_now,
                    settle_count,
                    max(1, pre_lock_settle_samples),
                )
                if settle_count >= max(1, pre_lock_settle_samples):
                    settle_ok = True
                    break
            else:
                settle_count = 0
            time.sleep(max(0.01, pre_lock_settle_dt_s))

        if not settle_ok and not allow_lock_without_measured_ready:
            self.cleanup_feedback_worker()
            with self.state["lock"]:
                if run_id == self.state.get("run_id"):
                    self.state["active"] = False
                    self.state["finalize_in_progress"] = False
                    self.state["fault_reason"] = "pre-lock readiness not met"
            self.set_can_phase("FAULT")
            self.set_encoder_status("CAN stream: fault (pre-lock readiness not met).")
            return

        if last_prelock_measured_vals is not None:
            final_lock_joint_err_deg = max(
                abs(float(final_target_angles[idx]) - float(last_prelock_measured_vals[idx]))
                for idx in range(len(last_prelock_measured_vals))
            )
            final_lock_cart_error = _compute_cartesian_pose_error(last_prelock_measured_vals, final_target_angles)
            final_lock_cart_mm = (
                float(final_lock_cart_error["position_error_mm"])
                if final_lock_cart_error is not None
                else -1.0
            )
            final_lock_ori_deg = (
                float(final_lock_cart_error["orientation_error_deg"])
                if final_lock_cart_error is not None
                else -1.0
            )
            self.stream_log.info(
                "FINAL_LOCK_CORRECTION | max_joint_err=%.3f cart_mm=%.3f ori_deg=%.3f action=%s",
                final_lock_joint_err_deg,
                final_lock_cart_mm,
                final_lock_ori_deg,
                "lock" if finalize_lock_enabled else "skip",
            )

        if finalize_lock_enabled:
            self.set_can_phase("FINAL_LOCK")
            self.set_encoder_status("CAN stream: final lock...")
            self.stream_log.info(
                "FINAL_LOCK now uses F5 absolute-axis commands with home offsets + CANRSP wait."
            )
            try:
                lock_ok, lock_details = self.comm_client.send_joint_targets_abs_wait_detailed(
                    target_angles_deg=list(final_target_angles),
                    overrides=lock_overrides,
                    clamp_limits=True,
                    timeout_s=8.0,
                )
                detail_parts = []
                for info in lock_details:
                    detail_parts.append(
                        f"{info.get('joint_id')}:{info.get('target_deg'):.2f} "
                        f"ok={info.get('ok')} cmd={info.get('lock_cmd')} "
                        f"axis={info.get('target_axis')} "
                        f"wait_s={info.get('wait_elapsed_s', 0.0):.3f} "
                        f"reason={info.get('reason_code')}"
                    )
                self.stream_log.info(
                    "FINAL_LOCK result ok=%s details=%s",
                    lock_ok,
                    ", ".join(detail_parts),
                )
                if not lock_ok:
                    self.stream_log.warning("FINAL_LOCK reported failure on one or more joints.")
            except Exception as exc:
                self.stream_log.warning("FINAL_LOCK exception: %s", exc)

        self.set_can_phase("VERIFY")
        self.set_encoder_status("CAN stream: verify...")

        verify_start = time.monotonic()
        verify_prev = None
        verify_prev_t = None
        done_count = 0
        verify_dt_s = max(0.02, pre_lock_settle_dt_s)
        verify_ok = False
        while (time.monotonic() - verify_start) <= max(0.0, verify_timeout_s):
            if is_cancelled():
                with self.state["lock"]:
                    if run_id == self.state.get("run_id"):
                        self.state["active"] = False
                        self.state["finalize_in_progress"] = False
                self.set_can_phase("PAUSED")
                self.set_encoder_status("CAN stream: stopped.")
                return

            try:
                measured = self.comm_client.read_joint_angles()
                raw_encoder_ticks = self.comm_client.read_encoders(timeout_s=None)
            except Exception as exc:
                self.stream_log.warning("VERIFY read failed: %s", exc)
                measured = None
                raw_encoder_ticks = None

            if measured and all(v is not None for v in measured[:len(final_target_angles)]):
                measured_vals = [float(v) for v in measured[:len(final_target_angles)]]
                self.update_encoder_display(measured_vals)
                now_t = time.monotonic()
                if verify_prev is None or verify_prev_t is None:
                    max_speed = float("inf")
                else:
                    dt_local = max(1e-6, now_t - verify_prev_t)
                    max_speed = max(
                        abs((measured_vals[idx] - verify_prev[idx]) / dt_local)
                        for idx in range(len(measured_vals))
                    )
                joint_errors = []
                for idx in range(len(measured_vals)):
                    err = abs(float(final_target_angles[idx]) - float(measured_vals[idx]))
                    joint_errors.append((idx, err, float(measured_vals[idx]), float(final_target_angles[idx])))
                cartesian_error = _compute_cartesian_pose_error(measured_vals, final_target_angles)

                max_pos_err = max(err for _, err, _, _ in joint_errors)

                if joint_errors:
                    max_err_joint = max(joint_errors, key=lambda x: x[1])
                    idx, err, meas, tgt = max_err_joint
                    if raw_encoder_ticks:
                        home_offsets = self.comm_client._home_offsets if hasattr(self.comm_client, "_home_offsets") else [None] * 6
                        self.stream_log.info(
                            "[VERIFY_DIAG] Raw encoder ticks: J1=%s J2=%s J3=%s J4=%s J5=%s J6=%s",
                            raw_encoder_ticks[0] if len(raw_encoder_ticks) > 0 else None,
                            raw_encoder_ticks[1] if len(raw_encoder_ticks) > 1 else None,
                            raw_encoder_ticks[2] if len(raw_encoder_ticks) > 2 else None,
                            raw_encoder_ticks[3] if len(raw_encoder_ticks) > 3 else None,
                            raw_encoder_ticks[4] if len(raw_encoder_ticks) > 4 else None,
                            raw_encoder_ticks[5] if len(raw_encoder_ticks) > 5 else None,
                        )
                        self.stream_log.info(
                            "[VERIFY_DIAG] Home offsets: J1=%s J2=%s J3=%s J4=%s J5=%s J6=%s",
                            home_offsets[0] if len(home_offsets) > 0 else None,
                            home_offsets[1] if len(home_offsets) > 1 else None,
                            home_offsets[2] if len(home_offsets) > 2 else None,
                            home_offsets[3] if len(home_offsets) > 3 else None,
                            home_offsets[4] if len(home_offsets) > 4 else None,
                            home_offsets[5] if len(home_offsets) > 5 else None,
                        )

                    self.stream_log.info(
                        "VERIFY | Per-joint errors (deg): J1=%.2f J2=%.2f J3=%.2f J4=%.2f J5=%.2f J6=%.2f",
                        joint_errors[0][1] if len(joint_errors) > 0 else -999,
                        joint_errors[1][1] if len(joint_errors) > 1 else -999,
                        joint_errors[2][1] if len(joint_errors) > 2 else -999,
                        joint_errors[3][1] if len(joint_errors) > 3 else -999,
                        joint_errors[4][1] if len(joint_errors) > 4 else -999,
                        joint_errors[5][1] if len(joint_errors) > 5 else -999,
                    )

                    self.stream_log.info(
                        "VERIFY | Max error: J%d measured=%.2f deg target=%.2f deg error=%.2f deg",
                        idx + 1, meas, tgt, err
                    )
                if cartesian_error is not None:
                    self.stream_log.info(
                        "VERIFY | Cartesian error: pos_mm=%.3f ori_deg=%.3f "
                        "measured_xyz=(%.2f, %.2f, %.2f) target_xyz=(%.2f, %.2f, %.2f)",
                        cartesian_error["position_error_mm"],
                        cartesian_error["orientation_error_deg"],
                        cartesian_error["measured_pose"][0],
                        cartesian_error["measured_pose"][1],
                        cartesian_error["measured_pose"][2],
                        cartesian_error["target_pose"][0],
                        cartesian_error["target_pose"][1],
                        cartesian_error["target_pose"][2],
                    )

                done_now = (max_pos_err <= position_tolerance_deg) and (max_speed <= speed_tolerance_deg_s)
                done_count = (done_count + 1) if done_now else 0
                self.stream_log.info(
                    "VERIFY | max_pos_err=%.3f max_speed=%.3f done_now=%s done_count=%d/%d",
                    max_pos_err,
                    max_speed,
                    done_now,
                    done_count,
                    max(1, done_consecutive_samples),
                )
                verify_prev = measured_vals
                verify_prev_t = now_t
                if done_count >= max(1, done_consecutive_samples):
                    verify_ok = True
                    break
            else:
                done_count = 0
            time.sleep(verify_dt_s)

        with self.state["lock"]:
            if run_id == self.state.get("run_id"):
                self.state["active"] = False
                self.state["finalize_in_progress"] = False
                if not verify_ok and not self.state.get("fault_reason"):
                    self.state["fault_reason"] = "verify incomplete"

        self.cleanup_feedback_worker()

        if verify_ok:
            self.set_can_phase("COMPLETE")
            self.set_encoder_status("CAN stream: complete.")
            self.stream_log.info("CAN stream complete: verify success.")
        else:
            self.set_can_phase("FAULT")
            self.set_encoder_status("CAN stream: verify incomplete.")
            self.stream_log.warning("CAN stream verify incomplete.")

    def on_can_segment_meta(self, meta):
        if not isinstance(meta, dict):
            return

        meta_run_id_raw = meta.get("run_id")
        try:
            meta_run_id = int(meta_run_id_raw) if meta_run_id_raw is not None else None
        except (TypeError, ValueError):
            meta_run_id = None

        final_target_raw = meta.get("final_target_angles")
        if not isinstance(final_target_raw, (list, tuple)) or len(final_target_raw) < 6:
            self.stream_log.warning(
                "Segment metadata ignored: invalid final_target_angles payload (%s)",
                type(final_target_raw).__name__,
            )
            return
        try:
            final_target = [float(v) for v in list(final_target_raw)[:6]]
        except (TypeError, ValueError):
            self.stream_log.warning("Segment metadata ignored: non-numeric final_target_angles.")
            return

        waypoint_count_raw = meta.get("waypoint_count", 0)
        try:
            waypoint_count = max(0, int(waypoint_count_raw))
        except (TypeError, ValueError):
            waypoint_count = 0
        segment_index_raw = meta.get("segment_index", 0)
        try:
            segment_index = max(0, int(segment_index_raw))
        except (TypeError, ValueError):
            segment_index = 0
        segment_direction = str(meta.get("segment_direction") or "unknown")

        latched = False
        active_run_id = None
        with self.state["lock"]:
            active_run_id = int(self.state.get("run_id", 0) or 0)
            if not self.state.get("active"):
                return
            if meta_run_id is None or meta_run_id != active_run_id:
                return

            self.state["final_target_angles"] = list(final_target)
            self.state["final_target_ready"] = True
            self.state["final_target_source"] = "trajectory_meta_cb"
            self.state["final_target_latched_mono"] = float(
                meta.get("created_mono", time.monotonic()) or time.monotonic()
            )
            self.state["final_target_waypoint_count"] = waypoint_count
            self.state["segment_index"] = int(segment_index)
            self.state["segment_direction"] = segment_direction
            self.state["last_final_target_wait_log_mono"] = None
            latched = True

        if latched:
            self.stream_log.info(
                "Trajectory target latched run_id=%d segment=%d direction=%s "
                "source=trajectory_meta_cb waypoints=%d "
                "J1=%.2f J2=%.2f J3=%.2f J4=%.2f J5=%.2f J6=%.2f",
                active_run_id,
                segment_index,
                segment_direction,
                waypoint_count,
                final_target[0],
                final_target[1],
                final_target[2],
                final_target[3],
                final_target[4],
                final_target[5],
            )
