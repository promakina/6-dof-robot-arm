import logging
import math
import time


def _clamp_value(value, lo, hi):
    return lo if value < lo else (hi if value > hi else value)


def run_can_joint_command(controller, step_index, target_angles):
    now_mono = time.monotonic()

    # Handle synchronous stop burst (step_index=-1)
    if step_index == -1:
        with controller.state["lock"]:
            if not controller.state.get("active"):
                return False
            overrides = dict(controller.state["overrides"] or {})

        joint_count = len(target_angles)
        velocity_targets = [0.0] * joint_count
        ok, details = controller.comm_client.send_joint_velocities_detailed(
            joint_velocities_deg_s=velocity_targets,
            overrides=overrides
        )
        controller.stream_log.debug("Stop burst sent: ok=%s", ok)
        return ok

    with controller.state["lock"]:
        if not controller.state.get("active"):
            return False
        current_angles = (
            list(controller.state["current_angles"])
            if controller.state["current_angles"] is not None
            else None
        )
        dt_s = controller.state["dt_s"]
        overrides = dict(controller.state["overrides"] or {})
        val = controller.state["min_velocity_deg_s"]
        min_velocity_deg_s = float(0.0 if val is None else val)
        max_joint_velocity_deg_s = controller.state["max_joint_velocity_deg_s"]
        val = controller.state.get("feedback_every_n_steps", 1)
        feedback_every_n_steps = int(1 if val is None else val)
        val = controller.state.get("last_feedback_step", -1)
        last_feedback_step = int(-1 if val is None else val)
        val = controller.state.get("stale_feedback_timeout_s", 0.35)
        stale_feedback_timeout_s = float(0.35 if val is None else val)
        val = controller.state.get("stale_startup_grace_s", 1.5)
        stale_startup_grace_s = float(1.5 if val is None else val)
        val = controller.state.get("stale_consecutive_threshold", 2)
        stale_consecutive_threshold = int(2 if val is None else val)
        val = controller.state.get("stale_feedback_consecutive_count", 0)
        stale_feedback_consecutive_count = int(0 if val is None else val)
        measured_angles_prev = (
            list(controller.state["measured_angles"])
            if controller.state["measured_angles"] is not None
            else None
        )
        measured_speeds_prev = (
            list(controller.state["measured_speeds"])
            if controller.state["measured_speeds"] is not None
            else None
        )
        measured_speed_valid_prev = bool(controller.state.get("measured_speed_valid", False))
        background_prev_angles = list(controller.state.get("background_prev_angles") or [])
        background_prev_sample_mono = list(controller.state.get("background_prev_sample_mono") or [])
        background_prev_total_samples = list(controller.state.get("background_prev_total_samples") or [])
        background_feedback_speeds = list(controller.state.get("background_feedback_speeds") or [])
        background_last_speed_log_mono = controller.state.get("background_last_speed_log_mono")
        last_feedback_mono = controller.state.get("last_feedback_mono")
        last_feedback_log_mono = controller.state.get("last_feedback_log_mono")
        val = controller.state.get("feedback_log_interval_s", 2.0)
        feedback_log_interval_s = float(2.0 if val is None else val)
        val = controller.state.get("feedback_speed_min_dt_s", 0.005)
        feedback_speed_min_dt_s = float(0.005 if val is None else val)
        val = controller.state.get("feedback_speed_max_deg_s", 250.0)
        feedback_speed_max_deg_s = float(250.0 if val is None else val)
        background_vector_require_all_joints = bool(
            controller.state.get("background_vector_require_all_joints", True)
        )
        val = controller.state.get("background_vector_min_valid_joints", 6)
        background_vector_min_valid_joints = int(6 if val is None else val)
        val = controller.state.get("background_vector_max_coherence_window_s", 0.30)
        background_vector_max_coherence_window_s = float(0.30 if val is None else val)
        val = controller.state.get("background_speed_min_valid_joints", 6)
        background_speed_min_valid_joints = int(6 if val is None else val)
        val = controller.state.get("background_speed_min_samples", 2)
        background_speed_min_samples = int(2 if val is None else val)
        val = controller.state.get("background_speed_invalid_log_interval_s", 0.5)
        background_speed_invalid_log_interval_s = float(0.5 if val is None else val)
        perf_metrics = dict(controller.state.get("perf_metrics") or {})
        final_target_angles = (
            list(controller.state["final_target_angles"])
            if controller.state["final_target_angles"] is not None
            else None
        )
        final_target_ready = bool(controller.state.get("final_target_ready", False))
        final_target_source = controller.state.get("final_target_source")
        last_final_target_wait_log_mono = controller.state.get("last_final_target_wait_log_mono")
        in_capture = bool(controller.state.get("in_capture", False))
        capture_started_mono = controller.state.get("capture_started_mono")
        val = controller.state.get("capture_dist_deg", 2.0)
        capture_dist_deg = float(2.0 if val is None else val)
        val = controller.state.get("capture_taper_min", 0.02)
        capture_taper_min = float(0.02 if val is None else val)
        val = controller.state.get("capture_velocity_threshold_deg_s", 0.5)
        capture_velocity_threshold_deg_s = float(0.5 if val is None else val)
        val = controller.state.get("capture_position_threshold_deg", 0.5)
        capture_position_threshold_deg = float(0.5 if val is None else val)
        continuous_settle_enabled = bool(controller.state.get("continuous_settle_enabled", False))
        val = controller.state.get("continuous_settle_handoff_min_s", 0.0)
        continuous_settle_handoff_min_s = float(0.0 if val is None else val)
        val = controller.state.get("continuous_settle_blend_ramp_s", 0.0)
        continuous_settle_blend_ramp_s = float(0.0 if val is None else val)
        val = controller.state.get("continuous_settle_target_blend_max", 0.0)
        continuous_settle_target_blend_max = float(0.0 if val is None else val)
        capture_feedback_boost_enabled = bool(
            controller.state.get("capture_feedback_boost_enabled", False)
        )
        val = controller.state.get("capture_feedback_boost_joints_per_cycle", 0)
        capture_feedback_boost_joints_per_cycle = int(0 if val is None else val)
        capture_feedback_boost_timeout_s = controller.state.get("capture_feedback_boost_timeout_s")
        capture_feedback_boost_applied = bool(
            controller.state.get("capture_feedback_boost_applied", False)
        )
        val = controller.state.get("capture_expand_max_extra_deg", 0.0)
        capture_expand_max_extra_deg = float(0.0 if val is None else val)
        val = controller.state.get("capture_correction_ramp_s", 0.0)
        capture_correction_ramp_s = float(0.0 if val is None else val)
        val = controller.state.get("capture_handoff_ready_dwell_s", 0.0)
        capture_handoff_ready_dwell_s = float(0.0 if val is None else val)
        val = controller.state.get("capture_handoff_max_joint_error_deg", 0.0)
        capture_handoff_max_joint_error_deg = float(0.0 if val is None else val)
        val = controller.state.get("capture_handoff_max_cart_error_mm", 0.0)
        capture_handoff_max_cart_error_mm = float(0.0 if val is None else val)
        val = controller.state.get("late_capture_freeze_remaining_deg", 0.0)
        late_capture_freeze_remaining_deg = float(0.0 if val is None else val)
        capture_ready_since_mono = controller.state.get("capture_ready_since_mono")
        late_capture_freeze_active = bool(
            controller.state.get("late_capture_freeze_active", False)
        )
        val = controller.state.get("transition_window_s", 0.8)
        transition_window_s = float(0.8 if val is None else val)
        val = controller.state.get("lock_entry_speed_deg_s", 4.0)
        lock_entry_speed_deg_s = float(4.0 if val is None else val)
        val = controller.state.get("lock_entry_dist_max_deg", 8.0)
        lock_entry_dist_max_deg = float(8.0 if val is None else val)
        val = controller.state.get("lock_entry_expand_gain", 0.6)
        lock_entry_expand_gain = float(0.6 if val is None else val)
        recovery_start_mono = controller.state.get("recovery_start_mono")
        val = controller.state.get("measured_handoff_recovery_window_s", 0.8)
        measured_handoff_recovery_window_s = float(0.8 if val is None else val)
        val = controller.state.get("measured_handoff_recovery_speed_deg_s", 2.0)
        measured_handoff_recovery_speed_deg_s = float(2.0 if val is None else val)
        val = controller.state.get("measured_handoff_recovery_kp_vel_per_deg", 0.8)
        measured_handoff_recovery_kp_vel_per_deg = float(0.8 if val is None else val)
        outer_loop_enabled = bool(controller.state.get("outer_loop_enabled", False))
        val = controller.state.get("kp_vel_per_deg", 0.0)
        kp_vel_per_deg = float(0.0 if val is None else val)
        val = controller.state.get("corr_vel_max_deg_s", 0.0)
        corr_vel_max_deg_s = float(0.0 if val is None else val)
        val = controller.state.get("correction_filter_alpha", 0.0)
        correction_filter_alpha = float(0.0 if val is None else val)
        _n = len(target_angles)
        v_corr_filtered = list(controller.state.get("v_corr_filtered") or [0.0] * _n)
        if len(v_corr_filtered) < _n:
            v_corr_filtered = v_corr_filtered + [0.0] * (_n - len(v_corr_filtered))
        val = controller.state.get("ki_vel_per_deg_s", 0.0)
        ki_vel_per_deg_s = float(0.0 if val is None else val)
        val = controller.state.get("ki_windup_limit_deg_s", 0.0)
        ki_windup_limit_deg_s = float(0.0 if val is None else val)
        v_corr_integral = list(controller.state.get("v_corr_integral") or [0.0] * _n)
        if len(v_corr_integral) < _n:
            v_corr_integral = v_corr_integral + [0.0] * (_n - len(v_corr_integral))
        val = controller.state.get("max_tracking_error_deg", 0.0)
        max_tracking_error_deg = float(0.0 if val is None else val)
        val = controller.state.get("tracking_error_holdoff_s", 0.25)
        tracking_error_holdoff_s = float(0.25 if val is None else val)
        val = controller.state.get("tracking_fault_max_age_ms", 500)
        tracking_fault_max_age_ms = float(500 if val is None else val)
        tracking_error_since_mono = controller.state.get("tracking_error_since_mono")
        val = controller.state.get("lock_measured_pos_tol_deg", 2.0)
        lock_measured_pos_tol_deg = float(2.0 if val is None else val)
        val = controller.state.get("lock_measured_speed_tol_deg_s", 6.0)
        lock_measured_speed_tol_deg_s = float(6.0 if val is None else val)
        val = controller.state.get("measured_ready_samples", 2)
        measured_ready_samples = int(2 if val is None else val)
        val = controller.state.get("measured_ready_count", 0)
        measured_ready_count = int(0 if val is None else val)
        allow_lock_without_measured_ready = bool(
            controller.state.get("allow_lock_without_measured_ready", True)
        )
        val = controller.state.get("max_consecutive_send_fail", 5)
        max_consecutive_send_fail = int(5 if val is None else val)
        send_fail_counts = list(controller.state.get("send_fail_counts") or [])
        last_velocity_targets = list(controller.state.get("last_velocity_targets") or [])
        finalize_enabled = bool(controller.state.get("finalize_enabled", True))
        run_id = int(controller.state.get("run_id", 0) or 0)
        last_step_mono = controller.state.get("last_step_mono")
        stream_started_mono = controller.state.get("stream_started_mono")
    if not perf_metrics:
        perf_metrics = {
            "command_count": 0,
            "feedback_stale_count": 0,
            "feedback_miss_count": 0,
            "slip_count": 0,
            "max_slip_ms": 0.0,
            "last_report_time": now_mono,
        }
    if current_angles is None:
        raise RuntimeError("CAN stream missing current angles.")
    if dt_s is None or dt_s <= 0.0:
        raise RuntimeError("CAN stream missing dt.")
    stale_startup_grace_s = max(0.0, float(stale_startup_grace_s))
    stale_consecutive_threshold = max(1, int(stale_consecutive_threshold))
    stream_elapsed_s = (
        (now_mono - float(stream_started_mono))
        if stream_started_mono is not None
        else 0.0
    )
    in_stale_startup_grace = stream_elapsed_s < stale_startup_grace_s
    feedback_log_due = (
        last_feedback_log_mono is None
        or (now_mono - float(last_feedback_log_mono)) >= max(0.1, feedback_log_interval_s)
    )
    perf_metrics["command_count"] = int(perf_metrics.get("command_count", 0)) + 1

    joint_count = min(
        len(target_angles),
        len(current_angles),
        len(controller.comm_client.config.joints),
    )
    if len(background_prev_angles) < joint_count:
        background_prev_angles.extend([None] * (joint_count - len(background_prev_angles)))
    if len(background_prev_sample_mono) < joint_count:
        background_prev_sample_mono.extend([None] * (joint_count - len(background_prev_sample_mono)))
    if len(background_prev_total_samples) < joint_count:
        background_prev_total_samples.extend([0] * (joint_count - len(background_prev_total_samples)))
    if len(background_feedback_speeds) < joint_count:
        background_feedback_speeds.extend([0.0] * (joint_count - len(background_feedback_speeds)))
    if len(background_prev_angles) > joint_count:
        background_prev_angles = background_prev_angles[:joint_count]
    if len(background_prev_sample_mono) > joint_count:
        background_prev_sample_mono = background_prev_sample_mono[:joint_count]
    if len(background_prev_total_samples) > joint_count:
        background_prev_total_samples = background_prev_total_samples[:joint_count]
    if len(background_feedback_speeds) > joint_count:
        background_feedback_speeds = background_feedback_speeds[:joint_count]
    background_vector_min_valid_joints = max(
        1, min(joint_count, int(background_vector_min_valid_joints))
    )
    background_speed_min_valid_joints = max(
        1, min(joint_count, int(background_speed_min_valid_joints))
    )
    background_speed_min_samples = max(1, int(background_speed_min_samples))
    background_speed_invalid_log_interval_s = max(
        0.1, float(background_speed_invalid_log_interval_s)
    )
    if background_vector_require_all_joints:
        background_vector_min_valid_joints = joint_count
    if len(send_fail_counts) < joint_count:
        send_fail_counts.extend([0] * (joint_count - len(send_fail_counts)))
    if len(last_velocity_targets) < joint_count:
        last_velocity_targets.extend([0.0] * (joint_count - len(last_velocity_targets)))
    if len(last_velocity_targets) > joint_count:
        last_velocity_targets = last_velocity_targets[:joint_count]

    target_slice = [float(target_angles[idx]) for idx in range(joint_count)]
    with controller.state["lock"]:
        controller.state["last_target_angles"] = list(target_slice)

    terminal_valid_joint_count = 0
    terminal_max_joint_err = None
    terminal_cart_error_mm = None
    terminal_ori_error_deg = None
    terminal_speed_deg_s = None
    explicit_terminal_gate_enabled = (
        capture_handoff_max_joint_error_deg > 0.0
        or capture_handoff_max_cart_error_mm > 0.0
    )
    explicit_terminal_joint_ok = not (capture_handoff_max_joint_error_deg > 0.0)
    explicit_terminal_cart_ok = not (capture_handoff_max_cart_error_mm > 0.0)
    explicit_terminal_error_ok = not explicit_terminal_gate_enabled

    has_final_target = final_target_angles is not None and len(final_target_angles) >= joint_count
    effective_finalize = finalize_enabled and final_target_ready and has_final_target
    if finalize_enabled and not effective_finalize:
        should_log_final_target_wait = (
            last_final_target_wait_log_mono is None
            or (now_mono - float(last_final_target_wait_log_mono)) >= 1.0
        )
        if should_log_final_target_wait:
            controller.stream_log.warning(
                "Finalize gated: waiting for trajectory target handoff "
                "(run_id=%d ready=%s has_target=%s source=%s).",
                run_id,
                final_target_ready,
                has_final_target,
                final_target_source,
            )
            with controller.state["lock"]:
                if run_id == int(controller.state.get("run_id", 0) or 0):
                    controller.state["last_final_target_wait_log_mono"] = now_mono

    feedback_angles = None
    feedback_speeds = measured_speeds_prev if measured_speeds_prev is not None else [0.0] * joint_count
    feedback_speed_valid = measured_speed_valid_prev
    feedback_due = (
        (step_index == 0)
        or (feedback_every_n_steps <= 1)
        or ((step_index - last_feedback_step) >= feedback_every_n_steps)
    )

    # Get feedback worker and mode from state
    feedback_worker = controller.state.get("feedback_worker")
    feedback_mode = controller.state.get("feedback_mode", "background")
    feedback_stale_count_step = 0
    feedback_miss_count_step = 0
    fresh_joint_flags = [0] * joint_count
    max_age_s = None
    coherence_window_s = None
    sample_count = 0
    fresh_count = 0
    required_valid_count = joint_count
    feedback_valid = False

    if feedback_mode == "background" and feedback_worker is not None and feedback_due:
        active_joint_ids = list(range(1, joint_count + 1))
        vector_snapshot = feedback_worker.get_vector_snapshot(active_joint_ids)

        has_samples = list(vector_snapshot.get("has_sample") or [])
        sample_ages_s = list(vector_snapshot.get("sample_ages_s") or [])
        sample_mono_s = list(vector_snapshot.get("sample_mono_s") or [])
        total_samples_vec = list(vector_snapshot.get("total_samples") or [])
        angles_vec = list(vector_snapshot.get("angles_deg") or [])
        max_age_s = vector_snapshot.get("max_age_s")
        coherence_window_s = vector_snapshot.get("coherence_window_s")

        sample_count = sum(1 for idx in range(joint_count) if idx < len(has_samples) and has_samples[idx])
        fresh_count = 0
        stale_count = 0
        for idx in range(joint_count):
            has_sample = idx < len(has_samples) and has_samples[idx]
            age_s = sample_ages_s[idx] if idx < len(sample_ages_s) else None
            if has_sample and age_s is not None and age_s <= stale_feedback_timeout_s:
                fresh_count += 1
                fresh_joint_flags[idx] = 1
            elif has_sample:
                stale_count += 1

        miss_count = max(0, joint_count - sample_count)
        feedback_miss_count_step += miss_count
        feedback_stale_count_step += stale_count

        coherence_ok = (
            background_vector_max_coherence_window_s <= 0.0
            or coherence_window_s is None
            or float(coherence_window_s) <= background_vector_max_coherence_window_s
        )
        if not coherence_ok:
            feedback_stale_count_step += 1

        required_valid_count = joint_count if background_vector_require_all_joints else background_vector_min_valid_joints
        feedback_valid = fresh_count >= required_valid_count and coherence_ok

        if feedback_valid:
            feedback_angles = []
            for idx in range(joint_count):
                angle_raw = angles_vec[idx] if idx < len(angles_vec) else None
                if angle_raw is None:
                    fallback_val = None
                    if measured_angles_prev is not None and idx < len(measured_angles_prev):
                        fallback_val = measured_angles_prev[idx]
                    elif idx < len(current_angles):
                        fallback_val = current_angles[idx]
                    feedback_angles.append(float(0.0 if fallback_val is None else fallback_val))
                else:
                    feedback_angles.append(float(angle_raw))

            new_speed_samples = 0
            valid_speed_count = 0
            for idx in range(joint_count):
                sample_mono = sample_mono_s[idx] if idx < len(sample_mono_s) else None
                total_samples = (
                    int(total_samples_vec[idx])
                    if idx < len(total_samples_vec) and total_samples_vec[idx] is not None
                    else 0
                )
                prev_total = int(background_prev_total_samples[idx])
                prev_angle = background_prev_angles[idx]
                prev_sample_mono = background_prev_sample_mono[idx]
                current_angle = feedback_angles[idx]

                if total_samples < prev_total:
                    background_prev_total_samples[idx] = total_samples
                    background_prev_angles[idx] = current_angle
                    background_prev_sample_mono[idx] = sample_mono
                    background_feedback_speeds[idx] = 0.0
                elif total_samples > prev_total:
                    new_speed_samples += 1
                    if (
                        sample_mono is not None
                        and prev_angle is not None
                        and prev_sample_mono is not None
                    ):
                        dt_fb = float(sample_mono) - float(prev_sample_mono)
                        if dt_fb >= feedback_speed_min_dt_s:
                            speed_raw = abs((float(current_angle) - float(prev_angle)) / dt_fb)
                            speed_finite = math.isfinite(speed_raw)
                            speed_within_cap = (
                                feedback_speed_max_deg_s <= 0.0
                                or speed_raw <= feedback_speed_max_deg_s
                            )
                            if speed_finite and speed_within_cap:
                                background_feedback_speeds[idx] = float(speed_raw)
                            else:
                                background_feedback_speeds[idx] = 0.0
                                if (
                                    background_last_speed_log_mono is None
                                    or (now_mono - float(background_last_speed_log_mono))
                                    >= background_speed_invalid_log_interval_s
                                ):
                                    controller.stream_log.warning(
                                        "[FeedbackBG] invalid speed sample J%d speed_raw=%.3f dt=%.6f max=%.3f",
                                        idx + 1,
                                        speed_raw,
                                        dt_fb,
                                        feedback_speed_max_deg_s,
                                    )
                                    background_last_speed_log_mono = now_mono
                        else:
                            background_feedback_speeds[idx] = 0.0
                    else:
                        background_feedback_speeds[idx] = 0.0

                    background_prev_total_samples[idx] = total_samples
                    background_prev_angles[idx] = current_angle
                    background_prev_sample_mono[idx] = sample_mono

                if (
                    total_samples >= background_speed_min_samples
                    and math.isfinite(float(background_feedback_speeds[idx]))
                ):
                    valid_speed_count += 1

            feedback_speeds = list(background_feedback_speeds[:joint_count])
            feedback_speed_valid = valid_speed_count >= background_speed_min_valid_joints

            if feedback_log_due:
                controller.stream_log.info(
                    "[FeedbackBG] valid=%d/%d fresh=%d stale=%d miss=%d "
                    "max_age=%.1f ms coherence=%.1f ms speeds=%d/%d new_speed_samples=%d "
                    "worker_cycle=%.1f ms worker_reads=%d/%d",
                    sample_count,
                    joint_count,
                    fresh_count,
                    stale_count,
                    miss_count,
                    (0.0 if max_age_s is None else float(max_age_s) * 1000.0),
                    (0.0 if coherence_window_s is None else float(coherence_window_s) * 1000.0),
                    valid_speed_count,
                    joint_count,
                    new_speed_samples,
                    float(vector_snapshot.get("last_cycle_elapsed_s", 0.0)) * 1000.0,
                    int(vector_snapshot.get("last_cycle_valid_count", 0)),
                    int(vector_snapshot.get("last_cycle_target_reads", 0)),
                )
                last_feedback_log_mono = now_mono
            controller.update_encoder_display(feedback_angles)
        elif feedback_log_due:
            controller.stream_log.warning(
                "[FeedbackBG] snapshot rejected: sample=%d/%d fresh=%d required=%d miss=%d stale=%d "
                "max_age=%.1f ms coherence=%.1f ms limit=%.1f ms require_all=%s",
                sample_count,
                joint_count,
                fresh_count,
                required_valid_count,
                miss_count,
                stale_count,
                (0.0 if max_age_s is None else float(max_age_s) * 1000.0),
                (0.0 if coherence_window_s is None else float(coherence_window_s) * 1000.0),
                max(0.0, float(background_vector_max_coherence_window_s)) * 1000.0,
                background_vector_require_all_joints,
            )
            last_feedback_log_mono = now_mono

    elif feedback_mode == "background" and feedback_due:
        if feedback_log_due:
            controller.stream_log.warning(
                "[FeedbackBG] worker unavailable: feedback_worker=%s enable_feedback should be true",
                feedback_worker is not None,
            )
            last_feedback_log_mono = now_mono

    elif feedback_mode == "inline" and feedback_due:
        # P1 NEW CODE: Inline round-robin encoder reads
        # Skip first N steps to avoid CAN bus initialization overhead
        inline_startup_skip_steps = 5
        if step_index < inline_startup_skip_steps:
            if step_index == 0:
                controller.stream_log.info(
                    "[INLINE] Skipping first %d steps to avoid initialization overhead",
                    inline_startup_skip_steps
                )
            # Skip inline feedback during startup, update last_feedback_step to avoid spam
            last_feedback_step = step_index
        else:
            inline_start = time.perf_counter()
            inline_elapsed_ms = 0.0
            inline_budget_ms = controller.state.get("inline_feedback_budget_ms", 30)
            inline_joints_per_step = controller.state.get("inline_feedback_joints_per_step", 1)
            inline_timeout_s = controller.state.get("inline_feedback_timeout_s", 0.02)
            inline_index = controller.state.get("inline_feedback_index", 0)

            inline_measured_angles = controller.state.get("inline_measured_angles")
            inline_measured_timestamps = controller.state.get("inline_measured_timestamps")
            inline_measured_total_samples = controller.state.get("inline_measured_total_samples")

            reads_completed = 0
            for _ in range(inline_joints_per_step):
                # Check budget before each read
                inline_elapsed_ms = (time.perf_counter() - inline_start) * 1000
                if inline_elapsed_ms >= inline_budget_ms:
                    controller.stream_log.debug(
                        "[INLINE] Budget exhausted: %.2fms / %.0fms",
                        inline_elapsed_ms, inline_budget_ms
                    )
                    break

                # Round-robin joint selection
                joint_id = (inline_index % 6) + 1
                inline_index += 1

                # Fast-fail encoder read
                angle_deg = controller.comm_client.read_encoder(joint_id, timeout_s=inline_timeout_s)

                if angle_deg is not None:
                    joint_idx = joint_id - 1
                    inline_measured_angles[joint_idx] = angle_deg
                    inline_measured_timestamps[joint_idx] = time.monotonic()
                    inline_measured_total_samples[joint_idx] += 1
                    reads_completed += 1

            # Update state
            with controller.state["lock"]:
                controller.state["inline_feedback_index"] = inline_index

            inline_elapsed_ms = (time.perf_counter() - inline_start) * 1000

            if reads_completed > 0:
                if feedback_log_due:
                    controller.stream_log.info(
                        "[INLINE] Read %d joints in %.2fms (budget=%.0fms)",
                        reads_completed, inline_elapsed_ms, inline_budget_ms
                    )
                    last_feedback_log_mono = now_mono

            # Update last feedback step
            last_feedback_step = step_index

            # P1: Calculate speeds for inline mode
            now_mono_inline = time.monotonic()
            feedback_speeds_inline = controller.state.get("feedback_speeds", [None] * 6)
            prev_inline_angles = controller.state.get("prev_inline_angles", [None] * 6)
            prev_inline_timestamps = controller.state.get("prev_inline_timestamps", [None] * 6)

            for joint_idx in range(6):
                current_angle = inline_measured_angles[joint_idx]
                current_time = inline_measured_timestamps[joint_idx]
                prev_angle = prev_inline_angles[joint_idx]
                prev_time = prev_inline_timestamps[joint_idx]

                if (current_angle is not None and prev_angle is not None and
                    current_time is not None and prev_time is not None):
                    dt = current_time - prev_time
                    if dt >= 0.01:  # Minimum 10ms between samples
                        speed = abs((current_angle - prev_angle) / dt)
                        if speed <= 250.0:  # Speed sanity cap
                            feedback_speeds_inline[joint_idx] = speed

            # Update previous samples and speeds
            with controller.state["lock"]:
                controller.state["prev_inline_angles"] = list(inline_measured_angles)
                controller.state["prev_inline_timestamps"] = list(inline_measured_timestamps)
                controller.state["feedback_speeds"] = feedback_speeds_inline

            # Set feedback_angles for downstream processing if we have valid samples
            # Use inline_measured_angles if at least one joint has been sampled
            if any(angle is not None for angle in inline_measured_angles):
                feedback_angles = list(inline_measured_angles)
                # Also set speeds if available (for capture readiness checks)
                feedback_speeds = feedback_speeds_inline
                feedback_speed_valid = any(s is not None for s in feedback_speeds_inline)

                if feedback_log_due:
                    valid_count = sum(1 for a in inline_measured_angles if a is not None)
                    controller.stream_log.debug(
                        "[INLINE] Using %d/%d measured angles for feedback",
                        valid_count, 6
                    )

    perf_metrics["feedback_stale_count"] = (
        int(perf_metrics.get("feedback_stale_count", 0)) + feedback_stale_count_step
    )
    perf_metrics["feedback_miss_count"] = (
        int(perf_metrics.get("feedback_miss_count", 0)) + feedback_miss_count_step
    )

    if feedback_angles is not None:
        stale_feedback_consecutive_count = 0
        if feedback_mode != "background":
            if (
                measured_angles_prev is not None
                and len(measured_angles_prev) >= joint_count
                and last_feedback_mono is not None
            ):
                dt_fb = now_mono - float(last_feedback_mono)
                if dt_fb > 1e-4:
                    # P1.1: Handle None values in feedback_angles (inline mode with round-robin)
                    feedback_speeds = []
                    for idx in range(joint_count):
                        if (feedback_angles[idx] is not None and
                            measured_angles_prev[idx] is not None):
                            speed = abs((feedback_angles[idx] - measured_angles_prev[idx]) / dt_fb)
                            feedback_speeds.append(speed)
                        else:
                            feedback_speeds.append(None)
                    feedback_speed_valid = any(s is not None for s in feedback_speeds)
                else:
                    feedback_speeds = [0.0] * joint_count
                    feedback_speed_valid = False
            else:
                feedback_speeds = [0.0] * joint_count
                feedback_speed_valid = False
        with controller.state["lock"]:
            controller.state["measured_angles"] = list(feedback_angles)
            controller.state["measured_speeds"] = list(feedback_speeds)
            controller.state["measured_speed_valid"] = bool(feedback_speed_valid)
            controller.state["last_feedback_step"] = int(step_index)
            controller.state["last_feedback_mono"] = now_mono
    else:
        if feedback_log_due and last_feedback_mono is not None:
            feedback_age_ms = (now_mono - float(last_feedback_mono)) * 1000.0
            controller.stream_log.debug(
                "[Feedback] Age: %.1f ms (stale threshold: %.1f ms)",
                feedback_age_ms,
                stale_feedback_timeout_s * 1000.0,
            )
            last_feedback_log_mono = now_mono

        if (
            stale_feedback_timeout_s > 0.0
            and last_feedback_mono is not None
            and (now_mono - float(last_feedback_mono)) > stale_feedback_timeout_s
        ):
            feedback_age_ms = (now_mono - float(last_feedback_mono)) * 1000.0
            stale_feedback_consecutive_count += 1
            if in_stale_startup_grace:
                controller.stream_log.warning(
                    "[Feedback] STALE during startup grace: age %.1f ms > threshold %.1f ms "
                    "(count=%d/%d elapsed=%.2fs grace=%.2fs)",
                    feedback_age_ms,
                    stale_feedback_timeout_s * 1000.0,
                    stale_feedback_consecutive_count,
                    stale_consecutive_threshold,
                    stream_elapsed_s,
                    stale_startup_grace_s,
                )
            else:
                controller.stream_log.warning(
                    "[Feedback] STALE: age %.1f ms > threshold %.1f ms (count=%d/%d)",
                    feedback_age_ms,
                    stale_feedback_timeout_s * 1000.0,
                    stale_feedback_consecutive_count,
                    stale_consecutive_threshold,
                )
                if stale_feedback_consecutive_count >= stale_consecutive_threshold:
                    with controller.state["lock"]:
                        controller.state["fault_reason"] = (
                            "stale feedback sustained "
                            f"({stale_feedback_consecutive_count}/{stale_consecutive_threshold}, "
                            f"{now_mono - float(last_feedback_mono):.3f}s)"
                        )
                        controller.state["stale_feedback_consecutive_count"] = int(
                            stale_feedback_consecutive_count
                        )
                    controller.stream_log.warning(
                        "CAN stream fault: stale feedback sustained count=%d threshold=%d",
                        stale_feedback_consecutive_count,
                        stale_consecutive_threshold,
                    )
                    controller.sync_runtime_diagnostics(perf_metrics, last_feedback_log_mono)
                    return False
        elif stale_feedback_consecutive_count != 0:
            if feedback_log_due:
                controller.stream_log.debug(
                    "[Feedback] STALE counter reset: %d -> 0",
                    stale_feedback_consecutive_count,
                )
            stale_feedback_consecutive_count = 0

    measured_angles = feedback_angles if feedback_angles is not None else measured_angles_prev
    if measured_angles is None or len(measured_angles) < joint_count:
        measured_angles = None
        feedback_speeds = [0.0] * joint_count
        feedback_speed_valid = False

    if effective_finalize and final_target_angles is not None and measured_angles is not None:
        valid_terminal_joint_errors = []
        measured_terminal_angles = []
        for idx in range(joint_count):
            measured_val = measured_angles[idx] if idx < len(measured_angles) else None
            if measured_val is None:
                continue
            terminal_valid_joint_count += 1
            measured_terminal_angles.append(float(measured_val))
            valid_terminal_joint_errors.append(
                abs(float(final_target_angles[idx]) - float(measured_val))
            )
        if valid_terminal_joint_errors:
            terminal_max_joint_err = max(valid_terminal_joint_errors)
        if terminal_valid_joint_count >= joint_count:
            terminal_pose_error = controller.compute_cartesian_pose_error(
                measured_terminal_angles[:joint_count],
                final_target_angles[:joint_count],
            )
            if terminal_pose_error is not None:
                terminal_cart_error_mm = float(terminal_pose_error["position_error_mm"])
                terminal_ori_error_deg = float(terminal_pose_error["orientation_error_deg"])
        valid_terminal_speeds = [
            abs(float(feedback_speeds[idx]))
            for idx in range(min(joint_count, len(feedback_speeds)))
            if feedback_speeds[idx] is not None
        ]
        if valid_terminal_speeds:
            terminal_speed_deg_s = max(valid_terminal_speeds)

    if explicit_terminal_gate_enabled:
        if capture_handoff_max_joint_error_deg > 0.0:
            explicit_terminal_joint_ok = (
                terminal_max_joint_err is not None
                and terminal_max_joint_err <= capture_handoff_max_joint_error_deg
            )
        if capture_handoff_max_cart_error_mm > 0.0:
            explicit_terminal_cart_ok = (
                terminal_cart_error_mm is not None
                and terminal_cart_error_mm <= capture_handoff_max_cart_error_mm
            )
        explicit_terminal_error_ok = (
            explicit_terminal_joint_ok and explicit_terminal_cart_ok
        )

    feedback_age_ms_diag = None
    if feedback_mode == "background" and max_age_s is not None:
        feedback_age_ms_diag = float(max_age_s) * 1000.0
    elif last_feedback_mono is not None:
        feedback_age_ms_diag = (now_mono - float(last_feedback_mono)) * 1000.0
    feedback_coherence_ms_diag = None
    if coherence_window_s is not None:
        feedback_coherence_ms_diag = float(coherence_window_s) * 1000.0

    if last_step_mono is not None:
        slip_s = (now_mono - float(last_step_mono)) - dt_s
        slip_ms = abs(slip_s) * 1000.0
        if slip_ms > 10.0:
            perf_metrics["slip_count"] = int(perf_metrics.get("slip_count", 0)) + 1
            perf_metrics["max_slip_ms"] = max(
                float(perf_metrics.get("max_slip_ms", 0.0)),
                slip_ms,
            )
            if slip_ms > 100.0:
                controller.stream_log.warning(
                    "CAN stream schedule slip: step=%d slip=%.1f ms dt=%.1f ms",
                    step_index + 1,
                    slip_s * 1000.0,
                    dt_s * 1000.0,
                )
            elif feedback_log_due:
                controller.stream_log.debug(
                    "CAN stream schedule slip: step=%d slip=%.1f ms dt=%.1f ms",
                    step_index + 1,
                    slip_s * 1000.0,
                    dt_s * 1000.0,
                )

    remaining_max = 0.0
    dynamic_capture_dist = capture_dist_deg
    capture_elapsed_s = 0.0
    if effective_finalize:
        remaining_max = max(
            abs(float(final_target_angles[idx]) - float(target_slice[idx]))
            for idx in range(joint_count)
        )
        # Dynamic capture zone: expand based on measured tracking error (golden F3)
        if measured_angles is not None and any(m is not None for m in measured_angles):
            measured_gap = max(
                (abs(float(final_target_angles[i]) - float(measured_angles[i]))
                 for i in range(joint_count)
                 if i < len(measured_angles) and measured_angles[i] is not None),
                default=0.0,
            )
        else:
            measured_gap = 0.0
        capture_extra = lock_entry_expand_gain * measured_gap
        if capture_expand_max_extra_deg > 0.0:
            capture_extra = min(capture_extra, capture_expand_max_extra_deg)
        dynamic_capture_dist = capture_dist_deg + capture_extra
        dynamic_capture_dist = _clamp_value(dynamic_capture_dist, capture_dist_deg, lock_entry_dist_max_deg)

        if (not in_capture) and capture_dist_deg > 0.0 and remaining_max <= dynamic_capture_dist:
            in_capture = True
            capture_started_mono = now_mono
            measured_ready_count = 0
            capture_ready_since_mono = None
            controller.set_can_phase("CAPTURE")
            if (
                capture_feedback_boost_enabled
                and not capture_feedback_boost_applied
                and feedback_mode == "background"
                and feedback_worker is not None
                and capture_feedback_boost_joints_per_cycle > 0
            ):
                try:
                    feedback_worker.update_schedule(
                        joints_per_cycle=capture_feedback_boost_joints_per_cycle,
                        timeout_s=capture_feedback_boost_timeout_s,
                    )
                    capture_feedback_boost_applied = True
                    controller.stream_log.info(
                        "TERMINAL_EVENT | feedback_boost reads_per_cycle=%d timeout_ms=%s",
                        capture_feedback_boost_joints_per_cycle,
                        "inherit"
                        if capture_feedback_boost_timeout_s is None
                        else f"{float(capture_feedback_boost_timeout_s) * 1000.0:.1f}",
                    )
                except Exception as exc:
                    controller.stream_log.warning(
                        "Capture feedback boost failed: %s",
                        exc,
                    )
            controller.stream_log.info(
                "Capture enter step=%d remaining_max=%.3f dynamic_capture_dist=%.3f "
                "(base=%.3f gap=%.3f source=%s)",
                step_index + 1,
                remaining_max,
                dynamic_capture_dist,
                capture_dist_deg,
                measured_gap,
                final_target_source,
            )
    if in_capture and capture_started_mono is not None:
        capture_elapsed_s = max(0.0, now_mono - float(capture_started_mono))

    taper = 1.0
    if in_capture and dynamic_capture_dist > 1e-6:
        taper = _clamp_value(remaining_max / dynamic_capture_dist, 0.0, 1.0)
        taper = max(taper, capture_taper_min)

    terminal_blend = 0.0
    command_target_slice = list(target_slice)
    if (
        continuous_settle_enabled
        and effective_finalize
        and in_capture
        and final_target_angles is not None
    ):
        dist_blend = 1.0 - _clamp_value(taper, 0.0, 1.0)
        time_blend = 1.0
        if continuous_settle_blend_ramp_s > 1e-6:
            time_blend = _clamp_value(capture_elapsed_s / continuous_settle_blend_ramp_s, 0.0, 1.0)
        terminal_blend = _clamp_value(
            max(dist_blend, time_blend) * continuous_settle_target_blend_max,
            0.0,
            continuous_settle_target_blend_max,
        )
        if terminal_blend > 0.0:
            command_target_slice = [
                ((1.0 - terminal_blend) * float(target_slice[idx]))
                + (terminal_blend * float(final_target_angles[idx]))
                for idx in range(joint_count)
            ]
    feedforward_target_slice = list(command_target_slice)
    planned_progress_complete = (
        effective_finalize
        and in_capture
        and final_target_angles is not None
        and remaining_max <= 1e-6
    )
    late_capture_freeze_now = False
    freeze_for_residual_gate = (
        effective_finalize
        and in_capture
        and final_target_angles is not None
        and late_capture_freeze_remaining_deg > 0.0
        and remaining_max <= late_capture_freeze_remaining_deg
        and explicit_terminal_gate_enabled
        and not explicit_terminal_error_ok
    )
    freeze_for_measured_convergence = planned_progress_complete
    if freeze_for_residual_gate or freeze_for_measured_convergence:
        late_capture_freeze_now = True
        feedforward_target_slice = [float(current_angles[idx]) for idx in range(joint_count)]
        command_target_slice = [
            float(final_target_angles[idx]) for idx in range(joint_count)
        ]
        if freeze_for_residual_gate and not late_capture_freeze_active:
            controller.stream_log.info(
                "TERMINAL_EVENT | late_capture_freeze remaining=%.3f max_joint_err=%.3f "
                "cart_mm=%.3f gate_joint=%.3f gate_cart_mm=%.3f",
                remaining_max,
                -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err),
                -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm),
                float(capture_handoff_max_joint_error_deg),
                float(capture_handoff_max_cart_error_mm),
            )
        elif freeze_for_measured_convergence and not late_capture_freeze_active:
            controller.stream_log.info(
                "TERMINAL_EVENT | terminal_target_hold capture_t=%.3f remaining=%.3f "
                "max_joint_err=%.3f cart_mm=%.3f",
                capture_elapsed_s,
                remaining_max,
                -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err),
                -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm),
            )
    elif late_capture_freeze_active:
        controller.stream_log.info(
            "TERMINAL_EVENT | late_capture_freeze_release remaining=%.3f max_joint_err=%.3f "
            "cart_mm=%.3f",
            remaining_max,
            -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err),
            -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm),
        )

    # Calculate actual elapsed time since last step for velocity scaling
    if last_step_mono is not None:
        actual_dt = now_mono - float(last_step_mono)
        # Guard: never use dt smaller than half the nominal interval
        # This prevents extreme velocities if steps fire in rapid succession
        actual_dt = max(dt_s * 0.5, actual_dt)
    else:
        # First step: use nominal dt
        actual_dt = dt_s

    velocity_targets = [0.0] * joint_count
    max_abs_tracking_err = 0.0
    max_abs_velocity = 0.0
    capture_correction_scale = 1.0
    if in_capture and capture_correction_ramp_s > 1e-6:
        capture_correction_scale = _clamp_value(
            capture_elapsed_s / capture_correction_ramp_s,
            0.0,
            1.0,
        )
    for idx in range(joint_count):
        # Use actual elapsed time instead of ideal dt_s
        # This automatically compensates for CAN bus delays
        v_ff = (feedforward_target_slice[idx] - float(current_angles[idx])) / actual_dt
        if in_capture:
            v_ff *= taper
        v_corr = 0.0
        # P1.1: Handle sparse inline feedback - only use non-None measured angles
        if (outer_loop_enabled and measured_angles is not None and
            idx < len(measured_angles) and measured_angles[idx] is not None):
            pos_err = command_target_slice[idx] - float(measured_angles[idx])
            max_abs_tracking_err = max(max_abs_tracking_err, abs(pos_err))
            # Accumulate integral (only outside capture zone to avoid windup near endpoint)
            if ki_vel_per_deg_s > 0.0 and not in_capture:
                v_corr_integral[idx] += pos_err * actual_dt
                if ki_windup_limit_deg_s > 0.0:
                    limit_deg = ki_windup_limit_deg_s / ki_vel_per_deg_s
                    v_corr_integral[idx] = _clamp_value(v_corr_integral[idx], -limit_deg, limit_deg)
            corr_limit = max(0.0, corr_vel_max_deg_s * (taper if in_capture else 1.0))
            if late_capture_freeze_now:
                corr_limit = max(0.0, corr_vel_max_deg_s)
            if in_capture:
                corr_limit *= capture_correction_scale
            v_p = kp_vel_per_deg * pos_err
            v_i = ki_vel_per_deg_s * v_corr_integral[idx]
            v_corr = _clamp_value(v_p + v_i, -corr_limit, corr_limit)
            if correction_filter_alpha > 0.0:
                v_corr_filtered[idx] = (
                    correction_filter_alpha * v_corr
                    + (1.0 - correction_filter_alpha) * v_corr_filtered[idx]
                )
                v_corr = v_corr_filtered[idx]
        else:
            # Decay the filter state toward zero when no feedback is available,
            # so it does not carry stale correction into the next valid measurement.
            if correction_filter_alpha > 0.0:
                v_corr_filtered[idx] *= (1.0 - correction_filter_alpha)
        vel = v_ff + v_corr
        if max_joint_velocity_deg_s is not None and max_joint_velocity_deg_s > 0.0:
            vel = max(min(vel, max_joint_velocity_deg_s), -max_joint_velocity_deg_s)

        # Apply absolute velocity threshold in capture zone
        if in_capture and (not late_capture_freeze_now) and abs(vel) < capture_velocity_threshold_deg_s:
            vel = 0.0
        elif abs(vel) < min_velocity_deg_s:
            vel = 0.0

        velocity_targets[idx] = vel
        max_abs_velocity = max(max_abs_velocity, abs(vel))

    # Phase 2: Recovery crawl override - slow Kp toward final target (golden F2)
    if recovery_start_mono is not None and final_target_angles is not None:
        max_abs_velocity = 0.0
        for idx in range(joint_count):
            if (measured_angles is not None and idx < len(measured_angles)
                    and measured_angles[idx] is not None):
                err = float(final_target_angles[idx]) - float(measured_angles[idx])
                vel = _clamp_value(
                    measured_handoff_recovery_kp_vel_per_deg * err,
                    -measured_handoff_recovery_speed_deg_s,
                    measured_handoff_recovery_speed_deg_s,
                )
            else:
                vel = 0.0
            velocity_targets[idx] = vel
            max_abs_velocity = max(max_abs_velocity, abs(vel))

    if outer_loop_enabled and max_tracking_error_deg > 0.0:
        if max_abs_tracking_err > max_tracking_error_deg:
            if tracking_error_since_mono is None:
                tracking_error_since_mono = now_mono
            elif (now_mono - tracking_error_since_mono) >= tracking_error_holdoff_s:
                # Check feedback age before faulting
                feedback_age_ms = None
                if feedback_mode == "background" and max_age_s is not None:
                    feedback_age_ms = float(max_age_s) * 1000.0
                elif last_feedback_mono is not None:
                    feedback_age_ms = (now_mono - float(last_feedback_mono)) * 1000.0

                # Only fault if feedback is fresh enough
                if feedback_age_ms is not None and feedback_age_ms > tracking_fault_max_age_ms:
                    controller.stream_log.warning(
                        "Tracking error %.3f deg ignored: feedback stale (age=%.0f ms > threshold=%.0f ms)",
                        max_abs_tracking_err,
                        feedback_age_ms,
                        tracking_fault_max_age_ms,
                    )
                    tracking_error_since_mono = None
                else:
                    with controller.state["lock"]:
                        controller.state["fault_reason"] = (
                            f"tracking error {max_abs_tracking_err:.3f} deg"
                        )
                    controller.stream_log.warning(
                        "CAN stream fault: tracking error max=%.3f deg",
                        max_abs_tracking_err,
                    )
                    controller.sync_runtime_diagnostics(perf_metrics, last_feedback_log_mono)
                    return False
        else:
            tracking_error_since_mono = None

    # Check position-based capture completion
    # P1.1: Handle sparse inline feedback - only use non-None values
    if in_capture and measured_angles is not None and final_target_angles is not None:
        # Compute position errors only for joints with valid measurements
        pos_errors = []
        for i in range(min(len(final_target_angles), len(measured_angles))):
            if measured_angles[i] is not None and final_target_angles[i] is not None:
                pos_errors.append(abs(float(final_target_angles[i]) - float(measured_angles[i])))

        max_pos_err = max(pos_errors) if pos_errors else float('inf')

        # Trigger early completion when close enough AND slow enough
        # Require at least some valid samples before allowing early completion
        min_valid_for_completion = max(3, len(measured_angles) // 2)
        completion_error_ok = (
            (not explicit_terminal_gate_enabled) or explicit_terminal_error_ok
        )
        if (len(pos_errors) >= min_valid_for_completion and
            max_pos_err < capture_position_threshold_deg and
            max_abs_velocity < capture_velocity_threshold_deg_s and
            completion_error_ok and
            not planned_progress_complete):

            controller.stream_log.info(
                "Position-based capture complete: pos_err=%.3f deg, vel=%.3f deg/s",
                max_pos_err, max_abs_velocity
            )
            # Send zero velocities and return success
            velocity_targets = [0.0] * joint_count
            ok, _ = controller.comm_client.send_joint_velocities_detailed(
                joint_velocities_deg_s=velocity_targets,
                overrides=overrides
            )
            controller.sync_runtime_diagnostics(perf_metrics, last_feedback_log_mono)
            return ok
        if (
            len(pos_errors) >= min_valid_for_completion
            and max_pos_err < capture_position_threshold_deg
            and max_abs_velocity < capture_velocity_threshold_deg_s
            and (
                not completion_error_ok
                or planned_progress_complete
            )
        ):
            controller.stream_log.debug(
                "Position-based capture completion blocked "
                "(planned_progress_complete=%s max_joint_err=%.3f cart_mm=%.3f gate_ok=%s)",
                planned_progress_complete,
                -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err),
                -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm),
                explicit_terminal_error_ok,
            )

    # Periodic timing diagnostics
    if feedback_log_due and last_step_mono is not None:
        elapsed_since_last = now_mono - float(last_step_mono)
        timing_ratio = elapsed_since_last / dt_s
        controller.stream_log.info(
            "Timing: step=%d ideal_dt=%.0fms actual_dt=%.0fms ratio=%.2fx max_vel=%.1f°/s",
            step_index, dt_s * 1000, elapsed_since_last * 1000, timing_ratio, max_abs_velocity
        )

        # Log position tracking (measured vs target)
        if measured_angles is not None and len(command_target_slice) >= joint_count:
            valid_pos_errors = []
            for idx in range(joint_count):
                if measured_angles[idx] is not None and command_target_slice[idx] is not None:
                    pos_err = abs(float(measured_angles[idx]) - float(command_target_slice[idx]))
                    valid_pos_errors.append(pos_err)

            if valid_pos_errors:
                max_tracking_err_log = max(valid_pos_errors)
                avg_tracking_err = sum(valid_pos_errors) / len(valid_pos_errors)
                controller.stream_log.info(
                    "Position tracking: step=%d max_err=%.2f° avg_err=%.2f° valid=%d/%d",
                    step_index, max_tracking_err_log, avg_tracking_err, len(valid_pos_errors), joint_count
                )

    command_delta_max = 0.0
    if joint_count > 0:
        command_delta_max = max(
            abs(float(velocity_targets[idx]) - float(last_velocity_targets[idx]))
            for idx in range(joint_count)
        )

    if in_capture and final_target_angles is not None:
        terminal_joint_err = -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err)
        terminal_cart_mm = -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm)
        terminal_measured_speed = -1.0 if terminal_speed_deg_s is None else float(terminal_speed_deg_s)
        controller.stream_log.info(
            "TERMINAL_STEP | capture_t=%.3f remaining=%.3f blend=%.3f max_joint_err=%.3f "
            "cart_mm=%.3f meas_speed=%.3f max_cmd=%.3f cmd_delta=%.3f fb_valid=%d/%d "
            "fb_age_ms=%.1f recovery=%d freeze=%d joint_fresh=%s",
            capture_elapsed_s,
            remaining_max,
            terminal_blend,
            terminal_joint_err,
            terminal_cart_mm,
            terminal_measured_speed,
            max_abs_velocity,
            command_delta_max,
            fresh_count,
            joint_count,
            -1.0 if feedback_age_ms_diag is None else float(feedback_age_ms_diag),
            1 if recovery_start_mono is not None else 0,
            1 if late_capture_freeze_now else 0,
            ",".join(str(int(flag)) for flag in fresh_joint_flags),
        )

    joint_ids = [joint.id for joint in controller.comm_client.config.joints]
    if controller.stream_log.isEnabledFor(logging.DEBUG):
        ik_parts = []
        for idx, angle in enumerate(command_target_slice):
            if idx >= len(joint_ids):
                break
            ik_parts.append("%d:%.2fdeg" % (joint_ids[idx], angle))
        controller.stream_log.debug(
            "IK step %d targets: %s",
            step_index + 1,
            ", ".join(ik_parts),
        )
    send_started = time.perf_counter()
    ok, details = controller.comm_client.send_joint_velocities_detailed(
        joint_velocities_deg_s=velocity_targets,
        overrides=overrides,
    )
    command_send_ms = (time.perf_counter() - send_started) * 1000.0
    if command_send_ms > max(40.0, dt_s * 500.0):
        controller.stream_log.warning(
            "[SEND] step=%d send_time=%.1f ms dt=%.1f ms feedback_age=%s coherence=%s phase=%s",
            step_index + 1,
            command_send_ms,
            dt_s * 1000.0,
            "n/a" if feedback_age_ms_diag is None else f"{feedback_age_ms_diag:.1f} ms",
            "n/a" if feedback_coherence_ms_diag is None else f"{feedback_coherence_ms_diag:.1f} ms",
            controller.state.get("phase"),
        )
    if controller.stream_log.isEnabledFor(logging.DEBUG):
        cmd_parts = []
        for idx, info in enumerate(details):
            vel_cmd = float(info.get("joint_vel_deg_s", 0.0))
            part = (
                "%s:%+.2fdeg/s rpm=%s acc=%s"
                % (
                    info["joint_id"],
                    vel_cmd,
                    info.get("motor_speed_rpm"),
                    info.get("motor_acc"),
                )
            )
            if not info.get("ok", False):
                part += "(fail)"
            cmd_parts.append(part)
        controller.stream_log.debug(
            "CAN velocity step %d dt=%.4fs commands: %s",
            step_index + 1,
            dt_s,
            ", ".join(cmd_parts),
        )

    next_angles = list(current_angles)
    send_failed = False
    for idx in range(joint_count):
        info = details[idx] if idx < len(details) else {}
        joint_ok = bool(info.get("ok", False))
        if joint_ok:
            next_angles[idx] = feedforward_target_slice[idx]
            send_fail_counts[idx] = 0
        else:
            send_fail_counts[idx] = int(send_fail_counts[idx]) + 1
            send_failed = True

    fault_from_send = False
    if send_failed:
        max_fail = max(send_fail_counts) if send_fail_counts else 0
        if max_fail >= max(1, max_consecutive_send_fail):
            fault_from_send = True
            with controller.state["lock"]:
                controller.state["fault_reason"] = (
                    f"send failures (max consecutive {max_fail})"
                )
            controller.stream_log.warning("CAN stream fault: send failures max=%d", max_fail)

    if (
        effective_finalize
        and in_capture
        and measured_angles is not None
        and len(measured_angles) >= joint_count
        and feedback_speed_valid
        and len(feedback_speeds) >= joint_count
    ):
        # P1.1: Handle sparse inline feedback - only use non-None values
        # Check if we have enough valid samples for readiness evaluation
        valid_angles = [measured_angles[idx] for idx in range(joint_count)
                       if measured_angles[idx] is not None and final_target_angles[idx] is not None]
        valid_speeds = [feedback_speeds[idx] for idx in range(joint_count)
                       if feedback_speeds[idx] is not None]

        # Require at least half of joints to have valid measurements for readiness check
        min_valid_joints = max(3, joint_count // 2)

        if len(valid_angles) >= min_valid_joints and len(valid_speeds) >= min_valid_joints:
            # Compute position errors only for valid joints
            pos_errors = []
            for idx in range(joint_count):
                if measured_angles[idx] is not None and final_target_angles[idx] is not None:
                    pos_errors.append(abs(float(final_target_angles[idx]) - float(measured_angles[idx])))

            max_pos_err = max(pos_errors) if pos_errors else float('inf')
            max_speed = max(abs(float(s)) for s in valid_speeds) if valid_speeds else float('inf')

            measured_ready_now = (
                max_pos_err <= lock_measured_pos_tol_deg
                and max_speed <= lock_measured_speed_tol_deg_s
            )
            measured_ready_count = (measured_ready_count + 1) if measured_ready_now else 0
            controller.stream_log.debug(
                "CAPTURE gate step=%d max_pos_err=%.3f max_speed=%.3f ready_now=%s "
                "ready_count=%d/%d valid_joints=%d/%d gate_joint=%.3f gate_cart_mm=%.3f gate_ok=%s",
                step_index + 1,
                max_pos_err,
                max_speed,
                measured_ready_now,
                measured_ready_count,
                measured_ready_samples,
                len(valid_angles),
                joint_count,
                -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err),
                -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm),
                explicit_terminal_error_ok,
            )
        else:
            # Not enough valid samples yet for readiness check
            controller.stream_log.debug(
                "CAPTURE gate step=%d: insufficient valid samples (angles=%d speeds=%d, need %d)",
                step_index + 1,
                len(valid_angles),
                len(valid_speeds),
                min_valid_joints,
            )

    request_handoff = False
    if effective_finalize and in_capture:
        velocity_ok = max_abs_velocity <= lock_entry_speed_deg_s
        capture_dwell_ok = (
            (not continuous_settle_enabled)
            or capture_elapsed_s >= max(0.0, continuous_settle_handoff_min_s)
        )
        error_gate_ok = (
            (not explicit_terminal_gate_enabled)
            or explicit_terminal_error_ok
        )
        ready_and_slow_now = (
            measured_ready_count >= max(1, measured_ready_samples)
            and velocity_ok
            and capture_dwell_ok
            and error_gate_ok
        )
        if ready_and_slow_now:
            if capture_ready_since_mono is None:
                capture_ready_since_mono = now_mono
        else:
            capture_ready_since_mono = None
        ready_hold_ok = (
            capture_handoff_ready_dwell_s <= 0.0
            or (
                capture_ready_since_mono is not None
                and (now_mono - float(capture_ready_since_mono)) >= capture_handoff_ready_dwell_s
            )
        )
        if ready_and_slow_now and ready_hold_ok:
            request_handoff = True
            controller.stream_log.info(
                "Lock transition%s: measured-ready + residual-error gate passed "
                "(vel=%.2f <= %.2f joint_err=%.3f cart_mm=%.3f)",
                " during measured convergence" if recovery_start_mono is not None else "",
                max_abs_velocity,
                lock_entry_speed_deg_s,
                -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err),
                -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm),
            )
            recovery_start_mono = None
        elif planned_progress_complete:
            if recovery_start_mono is None:
                recovery_start_mono = now_mono
                with controller.state["lock"]:
                    controller.state["recovery_start_mono"] = recovery_start_mono
                controller.stream_log.info(
                    "Late CAPTURE measured convergence started: planned trajectory exhausted "
                    "(window=%.2fs)",
                    measured_handoff_recovery_window_s,
                )
                controller.stream_log.info(
                    "TERMINAL_EVENT | recovery_start capture_t=%.3f",
                    capture_elapsed_s,
                )
            elif (now_mono - float(recovery_start_mono)) >= measured_handoff_recovery_window_s:
                request_handoff = True
                controller.stream_log.warning(
                    "Late CAPTURE convergence timeout: proceeding to FINAL_LOCK "
                    "(joint_err=%.3f cart_mm=%.3f ready=%s gate_ok=%s)",
                    -1.0 if terminal_max_joint_err is None else float(terminal_max_joint_err),
                    -1.0 if terminal_cart_error_mm is None else float(terminal_cart_error_mm),
                    measured_ready_count >= max(1, measured_ready_samples),
                    error_gate_ok,
                )
        else:
            recovery_start_mono = None

    # Detect overshoot during finalize
    # P1.1: Handle sparse inline feedback - only check joints with valid measurements
    if effective_finalize and in_capture and measured_angles is not None and final_target_angles is not None:
        if measured_angles_prev is not None:
            for idx in range(min(joint_count, len(measured_angles), len(measured_angles_prev), len(final_target_angles))):
                # Skip if any value is None (sparse inline feedback)
                if (measured_angles[idx] is None or measured_angles_prev[idx] is None or
                    final_target_angles[idx] is None):
                    continue

                target = float(final_target_angles[idx])
                measured = float(measured_angles[idx])
                prev = float(measured_angles_prev[idx])

                # Check if we crossed the target
                if (prev < target < measured) or (prev > target > measured):
                    overshoot = abs(measured - target)
                    controller.stream_log.warning(
                        "[OVERSHOOT] J%d crossed target: %.2f° → %.2f° (target %.2f°), overshoot=%.2f°",
                        idx + 1, prev, measured, target, overshoot
                    )

                    # Track in metrics
                    perf_metrics['overshoot_count'] = perf_metrics.get('overshoot_count', 0) + 1
                    perf_metrics['overshoot_max_deg'] = max(
                        perf_metrics.get('overshoot_max_deg', 0.0), overshoot
                    )

    with controller.state["lock"]:
        controller.state["current_angles"] = next_angles
        controller.state["last_target_angles"] = list(command_target_slice)
        controller.state["last_velocity_targets"] = list(velocity_targets)
        controller.state["send_fail_counts"] = list(send_fail_counts)
        controller.state["in_capture"] = bool(in_capture)
        controller.state["capture_started_mono"] = capture_started_mono
        controller.state["measured_ready_count"] = int(measured_ready_count)
        controller.state["capture_ready_since_mono"] = capture_ready_since_mono
        controller.state["late_capture_freeze_active"] = bool(late_capture_freeze_now)
        controller.state["recovery_start_mono"] = recovery_start_mono
        controller.state["last_step_mono"] = now_mono
        controller.state["tracking_error_since_mono"] = tracking_error_since_mono
        controller.state["last_feedback_log_mono"] = last_feedback_log_mono
        controller.state["stale_feedback_consecutive_count"] = int(
            stale_feedback_consecutive_count
        )
        controller.state["background_prev_angles"] = list(background_prev_angles)
        controller.state["background_prev_sample_mono"] = list(background_prev_sample_mono)
        controller.state["background_prev_total_samples"] = list(background_prev_total_samples)
        controller.state["background_feedback_speeds"] = list(background_feedback_speeds)
        controller.state["background_last_speed_log_mono"] = background_last_speed_log_mono
        controller.state["perf_metrics"] = dict(perf_metrics)
        controller.state["last_command_send_ms"] = float(command_send_ms)
        controller.state["last_feedback_age_ms"] = feedback_age_ms_diag
        controller.state["last_feedback_coherence_ms"] = feedback_coherence_ms_diag
        controller.state["last_feedback_valid_count"] = int(fresh_count)
        controller.state["last_feedback_sample_count"] = int(sample_count)
        controller.state["last_feedback_required_valid"] = int(required_valid_count)
        controller.state["last_feedback_valid"] = bool(feedback_valid)
        controller.state["last_feedback_mode"] = str(feedback_mode)
        controller.state["capture_feedback_boost_applied"] = bool(capture_feedback_boost_applied)
        controller.state["v_corr_filtered"] = list(v_corr_filtered)
        controller.state["v_corr_integral"] = list(v_corr_integral)

    if fault_from_send:
        controller.sync_runtime_diagnostics(perf_metrics, last_feedback_log_mono)
        return False

    if request_handoff:
        with controller.state["lock"]:
            controller.state["handoff_requested"] = True
        controller.stream_log.info(
            "TERMINAL_EVENT | handoff_request capture_t=%.3f recovery=%d",
            capture_elapsed_s,
            1 if recovery_start_mono is not None else 0,
        )
        controller.set_can_phase("CAPTURE")
        controller.stream_log.info("Capture handoff requested at step=%d", step_index + 1)
        controller.stop_streamer()
    else:
        if in_capture:
            controller.set_can_phase("CAPTURE")
        else:
            controller.set_can_phase("STREAM")

    last_report_time = float(perf_metrics.get("last_report_time", now_mono))
    if (now_mono - last_report_time) >= 5.0:
        controller.stream_log.info(
            "Performance: commands=%d slips=%d max_slip=%.1f ms stale=%d misses=%d",
            int(perf_metrics.get("command_count", 0)),
            int(perf_metrics.get("slip_count", 0)),
            float(perf_metrics.get("max_slip_ms", 0.0)),
            int(perf_metrics.get("feedback_stale_count", 0)),
            int(perf_metrics.get("feedback_miss_count", 0)),
        )
        perf_metrics["last_report_time"] = now_mono
        controller.sync_runtime_diagnostics(perf_metrics, last_feedback_log_mono)

    return True


