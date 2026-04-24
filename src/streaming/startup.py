import math
import time


def _clamp_value(value, lo, hi):
    return lo if value < lo else (hi if value > hi else value)


class CanStreamStartupHelper:
    def __init__(
        self,
        shared_state,
        comm_client,
        pybullet_ik,
        stream_log,
        can_controller,
        can_streamer,
        solve_ik_for_pose,
    ):
        self.shared_state = shared_state
        self.comm_client = comm_client
        self.pybullet_ik = pybullet_ik
        self.stream_log = stream_log
        self.can_controller = can_controller
        self.can_streamer = can_streamer
        self.solve_ik_for_pose = solve_ik_for_pose

    def start_keyframe_can_stream(
        self,
        plan,
        playback_settings,
        set_encoder_status,
        update_encoder_display,
        step_cb=None,
    ):
        def _parse_float(value, default=None):
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        if self.can_streamer.is_running():
            self.stream_log.warning("Keyframe CAN stream blocked: another stream is already running")
            return False, "another stream is already running"
        if not self.comm_client.is_connected:
            set_encoder_status("Keyframes: robot playback requires CAN connection")
            self.stream_log.warning("Keyframe CAN stream failed: CAN not connected")
            return False, "robot playback requires CAN connection"
        if not self.comm_client.is_homed:
            set_encoder_status("Keyframes: robot playback requires homing")
            self.stream_log.warning("Keyframe CAN stream failed: robot not homed")
            return False, "robot playback requires homing"

        stream_settings = dict((playback_settings or {}).get("settings") or {})
        velocity_cfg = stream_settings.get("can_velocity_stream", {})

        enable_feedback = bool(velocity_cfg.get("enable_feedback", True))
        feedback_rate_hz = 25.0 if _parse_float(velocity_cfg.get("feedback_rate_hz", 25.0)) is None else _parse_float(velocity_cfg.get("feedback_rate_hz", 25.0))
        feedback_timeout_s = max(0.001, float(_parse_float(velocity_cfg.get("feedback_timeout_s", 0.02), 0.02)))
        feedback_log_interval_s = max(0.1, float(_parse_float(velocity_cfg.get("feedback_log_interval_s", 2.0), 2.0)))
        feedback_speed_min_dt_s = float(_parse_float(velocity_cfg.get("feedback_speed_min_dt_s", 0.005), 0.005))
        feedback_speed_max_deg_s = float(_parse_float(velocity_cfg.get("feedback_speed_max_deg_s", 250.0), 250.0))

        min_velocity_deg_s = float(_parse_float(velocity_cfg.get("min_velocity_deg_s", 0.0), 0.0))
        max_joint_velocity_deg_s = _parse_float(velocity_cfg.get("max_joint_velocity_deg_s"))
        if max_joint_velocity_deg_s is not None and max_joint_velocity_deg_s <= 0.0:
            max_joint_velocity_deg_s = None
        stop_burst = int(_parse_float(velocity_cfg.get("stop_burst", 2), 2))
        if stop_burst < 1:
            stop_burst = 1

        feedback_every_n_steps = max(1, int(_parse_float(velocity_cfg.get("feedback_every_n_steps", 1), 1)))
        stale_feedback_timeout_s = float(
            _parse_float(
                velocity_cfg.get("stale_feedback_s") or velocity_cfg.get("stale_feedback_timeout_s", 0.35),
                0.35,
            )
        )
        stale_startup_grace_s = max(
            0.0,
            float(
                _parse_float(
                    velocity_cfg.get("startup_grace_s", velocity_cfg.get("stale_startup_grace_s", 1.5)),
                    1.5,
                )
            ),
        )
        stale_consecutive_threshold = max(
            1,
            int(
                _parse_float(
                    velocity_cfg.get("stale_consecutive_threshold", velocity_cfg.get("consecutive_stale_threshold", 2)),
                    2,
                )
            ),
        )

        outer_loop_enabled = bool(velocity_cfg.get("outer_loop_enabled", False))
        kp_vel_per_deg = float(_parse_float(velocity_cfg.get("kp_vel_per_deg", 0.0), 0.0))
        corr_vel_max_deg_s = float(_parse_float(velocity_cfg.get("corr_vel_max_deg_s", 0.0), 0.0))
        correction_filter_alpha = _clamp_value(float(_parse_float(velocity_cfg.get("correction_filter_alpha", 0.0), 0.0)), 0.0, 1.0)
        ki_vel_per_deg_s = max(0.0, float(_parse_float(velocity_cfg.get("ki_vel_per_deg_s", 0.0), 0.0)))
        ki_windup_limit_deg_s = max(0.0, float(_parse_float(velocity_cfg.get("ki_windup_limit_deg_s", 0.0), 0.0)))
        capture_dist_deg = float(_parse_float(velocity_cfg.get("capture_dist_deg", 3.0), 3.0))
        capture_taper_min = _clamp_value(float(_parse_float(velocity_cfg.get("capture_taper_min", 0.1), 0.1)), 0.01, 1.0)
        continuous_settle_enabled = bool(velocity_cfg.get("continuous_settle_enabled", False))
        continuous_settle_handoff_min_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("continuous_settle_handoff_min_s", 0.0), 0.0)),
        )
        continuous_settle_blend_ramp_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("continuous_settle_blend_ramp_s", 0.0), 0.0)),
        )
        continuous_settle_target_blend_max = _clamp_value(
            float(_parse_float(velocity_cfg.get("continuous_settle_target_blend_max", 0.0), 0.0)),
            0.0,
            1.0,
        )
        capture_feedback_boost_enabled = bool(
            velocity_cfg.get("capture_feedback_boost_enabled", False)
        )
        capture_feedback_boost_joints_per_cycle = max(
            0,
            int(_parse_float(velocity_cfg.get("capture_feedback_boost_joints_per_cycle", 0), 0)),
        )
        capture_feedback_boost_timeout_val = _parse_float(
            velocity_cfg.get("capture_feedback_boost_timeout_s"),
            None,
        )
        capture_feedback_boost_timeout_s = None
        if capture_feedback_boost_timeout_val is not None:
            capture_feedback_boost_timeout_s = max(
                0.001,
                float(capture_feedback_boost_timeout_val),
            )
        capture_expand_max_extra_deg = max(
            0.0,
            float(_parse_float(velocity_cfg.get("capture_expand_max_extra_deg", 0.0), 0.0)),
        )
        capture_correction_ramp_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("capture_correction_ramp_s", 0.0), 0.0)),
        )
        capture_handoff_ready_dwell_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("capture_handoff_ready_dwell_s", 0.0), 0.0)),
        )
        capture_handoff_max_joint_error_deg = max(
            0.0,
            float(_parse_float(velocity_cfg.get("capture_handoff_max_joint_error_deg", 0.0), 0.0)),
        )
        capture_handoff_max_cart_error_mm = max(
            0.0,
            float(_parse_float(velocity_cfg.get("capture_handoff_max_cart_error_mm", 0.0), 0.0)),
        )
        late_capture_freeze_remaining_deg = max(
            0.0,
            float(_parse_float(velocity_cfg.get("late_capture_freeze_remaining_deg", 0.0), 0.0)),
        )
        transition_window_s = float(_parse_float(velocity_cfg.get("transition_window_s", 0.8), 0.8))
        lock_entry_speed_deg_s = float(_parse_float(velocity_cfg.get("lock_entry_speed_deg_s", 4.0), 4.0))
        lock_entry_dist_max_deg = float(_parse_float(velocity_cfg.get("lock_entry_dist_max_deg", 8.0), 8.0))
        lock_entry_expand_gain = float(_parse_float(velocity_cfg.get("lock_entry_expand_gain", 0.6), 0.6))
        measured_handoff_recovery_window_s = float(_parse_float(velocity_cfg.get("measured_handoff_recovery_window_s", 0.8), 0.8))
        measured_handoff_recovery_speed_deg_s = float(_parse_float(velocity_cfg.get("measured_handoff_recovery_speed_deg_s", 2.0), 2.0))
        measured_handoff_recovery_kp_vel_per_deg = float(_parse_float(velocity_cfg.get("measured_handoff_recovery_kp_vel_per_deg", 0.8), 0.8))
        lock_measured_pos_tol_deg = float(_parse_float(velocity_cfg.get("lock_measured_pos_tol_deg", 2.0), 2.0))
        lock_measured_speed_tol_deg_s = float(_parse_float(velocity_cfg.get("lock_measured_speed_tol_deg_s", 6.0), 6.0))
        measured_ready_samples = max(1, int(_parse_float(velocity_cfg.get("measured_ready_samples", 2), 2)))
        allow_lock_without_measured_ready = bool(velocity_cfg.get("allow_lock_without_measured_ready", True))
        max_tracking_error_deg = float(_parse_float(velocity_cfg.get("max_tracking_error_deg", 0.0), 0.0))
        tracking_error_holdoff_s = float(_parse_float(velocity_cfg.get("tracking_error_holdoff_s", 0.25), 0.25))
        tracking_fault_max_age_ms = float(_parse_float(velocity_cfg.get("tracking_fault_max_age_ms", 500), 500))
        max_consecutive_send_fail = max(1, int(_parse_float(velocity_cfg.get("max_consecutive_send_fail", 5), 5)))
        finalize_lock_enabled = bool(velocity_cfg.get("finalize_lock_enabled", True))
        pre_lock_settle_timeout_s = float(_parse_float(velocity_cfg.get("pre_lock_settle_timeout_s", 0.6), 0.6))
        pre_lock_settle_dt_s = max(0.01, float(_parse_float(velocity_cfg.get("pre_lock_settle_dt_s", 0.05), 0.05)))
        pre_lock_settle_samples = max(1, int(_parse_float(velocity_cfg.get("pre_lock_settle_samples", 2), 2)))
        post_stop_settle_s = float(_parse_float(velocity_cfg.get("post_stop_settle_s", 0.12), 0.12))
        rx_drain_ms = int(_parse_float(velocity_cfg.get("rx_drain_ms", 120), 120))
        verify_timeout_s = float(_parse_float(velocity_cfg.get("verify_timeout_s", 2.5), 2.5))
        position_tolerance_deg = float(_parse_float(velocity_cfg.get("position_tolerance_deg", 0.35), 0.35))
        speed_tolerance_deg_s = float(_parse_float(velocity_cfg.get("speed_tolerance_deg_s", 1.2), 1.2))
        done_consecutive_samples = max(1, int(_parse_float(velocity_cfg.get("done_consecutive_samples", 4), 4)))

        feedback_during_stream = str(velocity_cfg.get("feedback_during_stream", "background")).lower()
        if feedback_during_stream not in {"background", "inline", "off"}:
            feedback_during_stream = "background"
        if not enable_feedback:
            feedback_during_stream = "off"
            outer_loop_enabled = False

        inline_feedback_time_budget_ms = float(_parse_float(velocity_cfg.get("inline_feedback_time_budget_ms", 30), 30))
        inline_feedback_joints_per_step = max(1, int(_parse_float(velocity_cfg.get("inline_feedback_joints_per_step", 1), 1)))

        background_worker_joints_per_cycle = max(1, int(_parse_float(velocity_cfg.get("background_worker_joints_per_cycle", 1), 1)))
        background_worker_inter_joint_gap_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("background_worker_inter_joint_gap_ms", 0.0), 0.0)) / 1000.0,
        )
        background_worker_slow_cycle_warn_s = max(
            0.001,
            float(_parse_float(velocity_cfg.get("background_worker_slow_cycle_warn_ms", 60.0), 60.0)) / 1000.0,
        )
        background_worker_cycle_log_interval_s = max(
            0.2,
            float(_parse_float(velocity_cfg.get("background_worker_cycle_log_interval_s", 2.0), 2.0)),
        )
        background_worker_warmup_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("background_worker_warmup_s", 0.25), 0.25)),
        )
        background_vector_require_all_joints = bool(velocity_cfg.get("background_vector_require_all_joints", True))
        background_vector_min_valid_joints = max(1, int(_parse_float(velocity_cfg.get("background_vector_min_valid_joints", 6), 6)))
        background_vector_max_coherence_window_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("background_vector_max_coherence_window_s", 0.30), 0.30)),
        )
        background_speed_min_valid_joints = max(1, int(_parse_float(velocity_cfg.get("background_speed_min_valid_joints", 6), 6)))
        background_speed_min_samples = max(1, int(_parse_float(velocity_cfg.get("background_speed_min_samples", 2), 2)))
        background_speed_invalid_log_interval_s = max(
            0.0,
            float(_parse_float(velocity_cfg.get("background_speed_invalid_log_interval_s", 0.5), 0.5)),
        )

        can_param_entries = stream_settings.get("can_stream_joint_params", [])
        can_overrides = {}
        can_lock_overrides = {}
        for entry in can_param_entries:
            try:
                joint_id = int(entry.get("joint_id"))
            except (TypeError, ValueError):
                continue
            stream_acc = entry.get("stream_acc", entry.get("acc"))
            if stream_acc is None:
                continue
            can_overrides[joint_id] = {"stream_acc": int(stream_acc)}
            lock_speed_rpm = entry.get("lock_speed_rpm", entry.get("speed_rpm", 300))
            lock_acc = entry.get("lock_acc", entry.get("acc", 120))
            can_lock_overrides[joint_id] = {
                "speed_rpm": int(lock_speed_rpm),
                "acc": int(lock_acc),
            }
        missing_joint_ids = [joint.id for joint in self.comm_client.config.joints if joint.id not in can_overrides]
        if missing_joint_ids:
            set_encoder_status("Keyframes: missing joint stream_acc settings.")
            self.stream_log.warning(
                "Keyframe CAN stream failed: missing can_stream_joint_params.stream_acc for joints %s",
                missing_joint_ids,
            )
            return False, "missing joint stream_acc settings"

        current_angles = self.comm_client.read_joint_angles()
        if not current_angles or any(v is None for v in current_angles[:6]):
            set_encoder_status("Keyframes: encoder read incomplete.")
            self.stream_log.warning("Keyframe CAN stream failed: encoder read incomplete")
            return False, "encoder read incomplete"
        current_angles = [float(v) for v in current_angles[:6]]
        update_encoder_display(current_angles)
        with self.shared_state.lock:
            self.shared_state.joint_deg = list(current_angles)

        trajectory = [list(sample[:6]) for sample in getattr(plan, "full_joint_trajectory_deg", [])]
        if len(trajectory) < 2:
            set_encoder_status("Keyframes: interpolation trajectory too short.")
            self.stream_log.warning("Keyframe CAN stream failed: trajectory too short")
            return False, "interpolation trajectory too short"

        speed_mm_s = float((playback_settings or {}).get("speed_mm_s") or 0.0)
        accel_mm_s2 = float((playback_settings or {}).get("accel_mm_s2") or 0.0)
        update_rate_hz = float((playback_settings or {}).get("update_rate_hz") or getattr(plan, "update_rate_hz", 0.0) or 0.0)
        if update_rate_hz <= 0.0:
            set_encoder_status("Keyframes: invalid interpolation update rate.")
            self.stream_log.warning("Keyframe CAN stream failed: invalid update rate %s", update_rate_hz)
            return False, "invalid interpolation update rate"
        planner_stats = getattr(plan, "planner_stats", None)
        planner_actual_speed_mm_s = getattr(planner_stats, "actual_speed_mm_s", None)
        planner_requested_speed_mm_s = getattr(planner_stats, "requested_speed_mm_s", None)
        self.stream_log.info(
            "Keyframe CAN stream config samples=%d segments=%d plan_rate_hz=%.3f plan_duration_s=%.3f "
            "playback_speed_mm_s=%.3f playback_accel_mm_s2=%.3f playback_rate_hz=%.3f "
            "planner_requested_speed_mm_s=%s planner_actual_speed_mm_s=%s "
            "feedback_mode=%s feedback_rate_hz=%.3f background_worker_joints_per_cycle=%d "
            "outer_loop_enabled=%s kp_vel_per_deg=%.3f corr_vel_max_deg_s=%.3f "
            "continuous_settle_enabled=%s capture_boost_enabled=%s",
            len(trajectory),
            len(getattr(plan, "segments", [])),
            float(getattr(plan, "update_rate_hz", 0.0) or 0.0),
            float(getattr(plan, "estimated_duration_s", 0.0) or 0.0),
            speed_mm_s,
            accel_mm_s2,
            update_rate_hz,
            planner_requested_speed_mm_s,
            planner_actual_speed_mm_s,
            feedback_during_stream,
            feedback_rate_hz,
            background_worker_joints_per_cycle,
            outer_loop_enabled,
            kp_vel_per_deg,
            corr_vel_max_deg_s,
            continuous_settle_enabled,
            capture_feedback_boost_enabled,
        )
        self.stream_log.info(
            "Keyframe CAN stream handoff capture_dist_deg=%.3f capture_taper_min=%.3f "
            "capture_expand_max_extra_deg=%.3f capture_correction_ramp_s=%.3f "
            "capture_handoff_ready_dwell_s=%.3f capture_handoff_max_joint_error_deg=%.3f "
            "capture_handoff_max_cart_error_mm=%.3f measured_handoff_recovery_window_s=%.3f "
            "late_capture_freeze_remaining_deg=%.3f allow_lock_without_measured_ready=%s",
            capture_dist_deg,
            capture_taper_min,
            capture_expand_max_extra_deg,
            capture_correction_ramp_s,
            capture_handoff_ready_dwell_s,
            capture_handoff_max_joint_error_deg,
            capture_handoff_max_cart_error_mm,
            measured_handoff_recovery_window_s,
            late_capture_freeze_remaining_deg,
            allow_lock_without_measured_ready,
        )

        run_id = self.can_controller.begin_run(
            seed_angles=current_angles,
            final_target_angles=None,
            can_overrides=can_overrides,
            can_lock_overrides=can_lock_overrides,
            runtime_cfg={
                "min_velocity_deg_s": min_velocity_deg_s,
                "max_joint_velocity_deg_s": max_joint_velocity_deg_s,
                "stop_burst": stop_burst,
                "feedback_every_n_steps": feedback_every_n_steps,
                "feedback_log_interval_s": feedback_log_interval_s,
                "stale_feedback_timeout_s": stale_feedback_timeout_s,
                "stale_startup_grace_s": stale_startup_grace_s,
                "stale_consecutive_threshold": stale_consecutive_threshold,
                "capture_dist_deg": capture_dist_deg,
                "capture_taper_min": capture_taper_min,
                "continuous_settle_enabled": continuous_settle_enabled,
                "continuous_settle_handoff_min_s": continuous_settle_handoff_min_s,
                "continuous_settle_blend_ramp_s": continuous_settle_blend_ramp_s,
                "continuous_settle_target_blend_max": continuous_settle_target_blend_max,
                "capture_feedback_boost_enabled": capture_feedback_boost_enabled,
                "capture_feedback_boost_joints_per_cycle": capture_feedback_boost_joints_per_cycle,
                "capture_feedback_boost_timeout_s": capture_feedback_boost_timeout_s,
                "capture_expand_max_extra_deg": capture_expand_max_extra_deg,
                "capture_correction_ramp_s": capture_correction_ramp_s,
                "capture_handoff_ready_dwell_s": capture_handoff_ready_dwell_s,
                "capture_handoff_max_joint_error_deg": capture_handoff_max_joint_error_deg,
                "capture_handoff_max_cart_error_mm": capture_handoff_max_cart_error_mm,
                "late_capture_freeze_remaining_deg": late_capture_freeze_remaining_deg,
                "transition_window_s": transition_window_s,
                "lock_entry_speed_deg_s": lock_entry_speed_deg_s,
                "lock_entry_dist_max_deg": lock_entry_dist_max_deg,
                "lock_entry_expand_gain": lock_entry_expand_gain,
                "measured_handoff_recovery_window_s": measured_handoff_recovery_window_s,
                "measured_handoff_recovery_speed_deg_s": measured_handoff_recovery_speed_deg_s,
                "measured_handoff_recovery_kp_vel_per_deg": measured_handoff_recovery_kp_vel_per_deg,
                "outer_loop_enabled": outer_loop_enabled,
                "kp_vel_per_deg": kp_vel_per_deg,
                "corr_vel_max_deg_s": corr_vel_max_deg_s,
                "correction_filter_alpha": correction_filter_alpha,
                "ki_vel_per_deg_s": ki_vel_per_deg_s,
                "ki_windup_limit_deg_s": ki_windup_limit_deg_s,
                "max_tracking_error_deg": max_tracking_error_deg,
                "tracking_error_holdoff_s": tracking_error_holdoff_s,
                "tracking_fault_max_age_ms": tracking_fault_max_age_ms,
                "lock_measured_pos_tol_deg": lock_measured_pos_tol_deg,
                "lock_measured_speed_tol_deg_s": lock_measured_speed_tol_deg_s,
                "measured_ready_samples": measured_ready_samples,
                "allow_lock_without_measured_ready": allow_lock_without_measured_ready,
                "max_consecutive_send_fail": max_consecutive_send_fail,
                "finalize_enabled": True,
                "finalize_lock_enabled": finalize_lock_enabled,
                "pre_lock_settle_timeout_s": pre_lock_settle_timeout_s,
                "pre_lock_settle_dt_s": pre_lock_settle_dt_s,
                "pre_lock_settle_samples": pre_lock_settle_samples,
                "post_stop_settle_s": post_stop_settle_s,
                "rx_drain_ms": rx_drain_ms,
                "verify_timeout_s": verify_timeout_s,
                "position_tolerance_deg": position_tolerance_deg,
                "speed_tolerance_deg_s": speed_tolerance_deg_s,
                "done_consecutive_samples": done_consecutive_samples,
                "loop_enabled": False,
                "feedback_during_stream": feedback_during_stream,
                "inline_feedback_time_budget_ms": inline_feedback_time_budget_ms,
                "inline_feedback_joints_per_step": inline_feedback_joints_per_step,
                "inline_feedback_timeout_s": min(0.02, feedback_timeout_s),
                "background_vector_require_all_joints": background_vector_require_all_joints,
                "background_vector_min_valid_joints": background_vector_min_valid_joints,
                "background_vector_max_coherence_window_s": background_vector_max_coherence_window_s,
                "background_speed_min_valid_joints": background_speed_min_valid_joints,
                "background_speed_min_samples": background_speed_min_samples,
                "background_speed_invalid_log_interval_s": background_speed_invalid_log_interval_s,
            },
        )

        self.can_controller.set_dt(1.0 / update_rate_hz)

        feedback_worker = self.can_controller.setup_feedback_worker(
            enable_feedback=enable_feedback,
            feedback_during_stream=feedback_during_stream,
            feedback_rate_hz=feedback_rate_hz,
            feedback_timeout_s=feedback_timeout_s,
            stale_feedback_timeout_s=stale_feedback_timeout_s,
            feedback_log_interval_s=feedback_log_interval_s,
            background_worker_joints_per_cycle=background_worker_joints_per_cycle,
            background_worker_inter_joint_gap_s=background_worker_inter_joint_gap_s,
            background_worker_slow_cycle_warn_s=background_worker_slow_cycle_warn_s,
            background_worker_cycle_log_interval_s=background_worker_cycle_log_interval_s,
            background_worker_warmup_s=background_worker_warmup_s,
            inline_feedback_time_budget_ms=inline_feedback_time_budget_ms,
            inline_feedback_joints_per_step=inline_feedback_joints_per_step,
            seed_joint_angles=current_angles,
        )
        self.can_controller.attach_feedback_runtime(
            feedback_worker=feedback_worker,
            feedback_speed_min_dt_s=feedback_speed_min_dt_s,
            feedback_speed_max_deg_s=feedback_speed_max_deg_s,
        )

        overrides = {
            "speed_mm_s": speed_mm_s,
            "accel_mm_s2": accel_mm_s2,
            "update_rate_hz": update_rate_hz,
            "run_id": run_id,
        }
        segment_meta = {
            "segment_index": max(0, len(getattr(plan, "segments", [])) - 1),
            "segment_direction": "KF",
        }
        started = self.can_streamer.start_precomputed(
            trajectory=trajectory,
            update_rate_hz=update_rate_hz,
            overrides=overrides,
            segment_meta=segment_meta,
            send_stop_burst=True,
            step_cb=step_cb,
        )
        if not started:
            self.stream_log.warning("Keyframe CAN stream ignored: streaming already running")
            self.can_controller.mark_start_failed()
            self.can_controller.cleanup_feedback_worker()
            return False, "streaming already running"

        self.can_controller.set_can_phase("STREAM")
        self.stream_log.info(
            "Keyframe CAN velocity stream started samples=%d segments=%d speed=%.3f accel=%.3f rate=%.3f",
            len(trajectory),
            len(getattr(plan, "segments", [])),
            speed_mm_s,
            accel_mm_s2,
            update_rate_hz,
        )
        set_encoder_status("Keyframes: interpolated robot playback active")
        return True, None


