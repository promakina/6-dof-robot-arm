from __future__ import annotations

from typing import List, Optional

from .config import CommConfig, get_comm_config
from .controller import CanConnectResult, RobotCommController


class RobotCommClient:
    def __init__(self, simulation_mode: bool = False, config: Optional[CommConfig] = None):
        self._config = config or get_comm_config()
        self._controller = RobotCommController(simulation_mode=simulation_mode, config=self._config)
        self._setup_done = False
        self._home_offsets: List[Optional[int]] = [None] * len(self._config.joints)
        self._home_mode: Optional[int] = None
        self._homed = False
        self._rehome_required = False
        self._rehome_reason: Optional[str] = None
        self._last_encoder_ticks: List[Optional[int]] = [None] * len(self._config.joints)
        self._last_encoder_mode: Optional[int] = None
        self._last_port: Optional[str] = None

    def _apply_config(self, config: CommConfig) -> None:
        old_joint_ids = [joint.id for joint in self._config.joints]
        new_joint_ids = [joint.id for joint in config.joints]
        old_offsets = {
            joint_id: self._home_offsets[idx]
            for idx, joint_id in enumerate(old_joint_ids)
            if idx < len(self._home_offsets)
        }
        old_ticks = {
            joint_id: self._last_encoder_ticks[idx]
            for idx, joint_id in enumerate(old_joint_ids)
            if idx < len(self._last_encoder_ticks)
        }

        self._config = config
        self._controller.update_config(config)
        self._home_offsets = [old_offsets.get(joint_id) for joint_id in new_joint_ids]
        self._last_encoder_ticks = [old_ticks.get(joint_id) for joint_id in new_joint_ids]

    def refresh_config(self) -> CommConfig:
        config = get_comm_config(force_reload=True)
        self._apply_config(config)
        return self._config

    @property
    def is_connected(self) -> bool:
        return self._controller.is_connected

    @property
    def default_port(self) -> str:
        self.refresh_config()
        return self._config.can_port

    @property
    def is_homed(self) -> bool:
        return self._homed

    @property
    def config(self) -> CommConfig:
        self.refresh_config()
        return self._config

    def connect(self, port: Optional[str] = None) -> bool:
        return self.connect_with_checks(port).ok

    def connect_with_checks(
        self,
        port: Optional[str] = None,
        init_timeout_s: float = 5.0,
        listen_timeout_s: float = 0.2,
    ) -> CanConnectResult:
        self.refresh_config()
        if self._controller.is_connected:
            return CanConnectResult(
                ok=True,
                message="CAN bus already connected.",
                interface=str(self._config.can_interface),
                channel=str(port or self._last_port or self._config.can_port),
                bitrate=int(self._config.can_bitrate),
                init_timeout_s=float(init_timeout_s),
                listen_timeout_s=float(listen_timeout_s),
                stage="already_connected",
                frames_received=0,
            )
        self._setup_done = False
        self._last_port = port
        return self._controller.connect_with_checks(
            port_override=port,
            init_timeout_s=init_timeout_s,
            listen_timeout_s=listen_timeout_s,
        )

    def disconnect(self) -> None:
        if self._controller.is_connected:
            self._controller.disconnect()
        self._setup_done = False
        self._homed = False
        self._home_offsets = [None] * len(self._config.joints)
        self._home_mode = None
        self._rehome_required = False
        self._rehome_reason = None
        self._last_encoder_ticks = [None] * len(self._config.joints)
        self._last_encoder_mode = None

    def get_encoder_domain_state(self) -> dict:
        current_mode = self._controller.get_encoder_read_mode()
        mismatch = (
            self._home_mode is not None
            and current_mode is not None
            and int(self._home_mode) != int(current_mode)
        )
        return {
            "current_mode": current_mode,
            "current_mode_hex": self._controller.mode_to_hex(current_mode),
            "home_mode": self._home_mode,
            "home_mode_hex": self._controller.mode_to_hex(self._home_mode),
            "mode_locked": self._controller.is_encoder_mode_locked(),
            "mismatch": mismatch,
            "rehome_required": bool(self._rehome_required),
            "rehome_reason": self._rehome_reason,
            "last_ticks": list(self._last_encoder_ticks),
        }

    def ensure_ready(self) -> None:
        self.refresh_config()
        if not self._controller.is_connected:
            raise RuntimeError("CAN bus not connected.")
        if not self._setup_done:
            self._controller.setup_all_motors()
            self._setup_done = True

    def read_encoder(self, joint_id: int, timeout_s: Optional[float] = None) -> Optional[float]:
        """Read a single encoder value and convert to degrees"""
        self.ensure_ready()
        encoder_ticks = self._controller.read_encoder(joint_id, timeout_s=timeout_s)
        if encoder_ticks is None:
            return None

        # Convert to angle using same logic as read_joint_angles
        joint_idx = joint_id - 1
        if joint_idx < 0 or joint_idx >= len(self._config.joints):
            return None

        joint_cfg = self._config.joints[joint_idx]
        home_offset = self._home_offsets[joint_idx] if joint_idx < len(self._home_offsets) else None

        if home_offset is None:
            return None

        ticks_from_home = encoder_ticks - home_offset
        ticks_per_rev = self._config.encoder_ticks_per_rev
        deg_per_tick = 360.0 / (ticks_per_rev * joint_cfg.gear_ratio)
        angle_deg = ticks_from_home * deg_per_tick * joint_cfg.sign_map

        return angle_deg

    def read_encoders(self, timeout_s: Optional[float] = None) -> List[Optional[int]]:
        self.ensure_ready()
        encoder_values = self._controller.read_all_encoders(timeout_s=timeout_s)
        padded = list(encoder_values[: len(self._config.joints)])
        if len(padded) < len(self._config.joints):
            padded.extend([None] * (len(self._config.joints) - len(padded)))
        self._last_encoder_ticks = padded
        self._last_encoder_mode = self._controller.get_encoder_read_mode()
        return list(encoder_values)

    def set_home(self) -> dict:
        encoder_values = self.read_encoders()
        padded = list(encoder_values[: len(self._config.joints)])
        if len(padded) < len(self._config.joints):
            padded.extend([None] * (len(self._config.joints) - len(padded)))

        failed_joint_ids = [
            joint.id for idx, joint in enumerate(self._config.joints) if padded[idx] is None
        ]
        mode = self._controller.get_encoder_read_mode()
        mode_locked = self._controller.is_encoder_mode_locked()
        success = (not failed_joint_ids) and mode is not None and mode_locked

        if success:
            self._home_offsets = [int(v) for v in padded]
            self._home_mode = int(mode)
            self._homed = True
            self._rehome_required = False
            self._rehome_reason = None
        else:
            self._home_offsets = [None] * len(self._config.joints)
            self._home_mode = None
            self._homed = False
            self._rehome_required = False
            if failed_joint_ids:
                self._rehome_reason = "home read failed"
            elif mode is None:
                self._rehome_reason = "encoder mode unresolved"
            elif not mode_locked:
                self._rehome_reason = "encoder mode not locked"
            else:
                self._rehome_reason = "home failed"

        joint_results = []
        for idx, joint in enumerate(self._config.joints):
            joint_results.append(
                {
                    "joint_id": joint.id,
                    "ok": padded[idx] is not None,
                    "ticks": padded[idx],
                }
            )

        return {
            "success": bool(success),
            "offsets": list(self._home_offsets),
            "failed_joint_ids": failed_joint_ids,
            "joint_results": joint_results,
            "home_mode": self._home_mode,
            "home_mode_hex": self._controller.mode_to_hex(self._home_mode),
            "reason": self._rehome_reason,
        }

    def read_joint_angles(self, timeout_s: Optional[float] = None) -> List[Optional[float]]:
        encoder_values = self.read_encoders(timeout_s=timeout_s)
        if self._rehome_required:
            return [None] * len(self._config.joints)
        current_mode = self._controller.get_encoder_read_mode()
        if (
            self._home_mode is not None
            and current_mode is not None
            and int(self._home_mode) != int(current_mode)
        ):
            self._rehome_required = True
            self._rehome_reason = (
                "encoder domain mismatch: home=%s current=%s"
                % (
                    self._controller.mode_to_hex(self._home_mode),
                    self._controller.mode_to_hex(current_mode),
                )
            )
            self._homed = False
            return [None] * len(self._config.joints)

        angles: List[Optional[float]] = []
        for idx, joint in enumerate(self._config.joints):
            enc = encoder_values[idx] if idx < len(encoder_values) else None
            offset = self._home_offsets[idx] if idx < len(self._home_offsets) else None
            if enc is None or offset is None:
                angles.append(None)
            else:
                angles.append(self._controller.encoder_ticks_to_angle(joint, enc, offset))
        return angles

    def move_to_angles(self, target_angles_deg: List[float]) -> None:
        if not self._homed:
            raise RuntimeError("Robot not homed. Use Set Home first.")

        current_angles = self.read_joint_angles()
        for idx, joint in enumerate(self._config.joints):
            if idx >= len(target_angles_deg):
                break
            current = current_angles[idx]
            if current is None:
                raise RuntimeError(f"Current angle for joint {joint.id} is unknown.")
            target = float(target_angles_deg[idx])
            ok = self._controller.move_joint_to_angle(joint, target, current)
            if not ok:
                raise RuntimeError(f"Failed to move joint {joint.id}.")

    def send_joint_targets(
        self,
        target_angles_deg: List[float],
        current_angles_deg: List[float],
        clamp_limits: bool = False,
    ) -> List[bool]:
        if not self._homed:
            raise RuntimeError("Robot not homed. Use Set Home first.")
        self.ensure_ready()
        results: List[bool] = []
        for idx, joint in enumerate(self._config.joints):
            if idx >= len(target_angles_deg) or idx >= len(current_angles_deg):
                break
            current = current_angles_deg[idx]
            if current is None:
                raise RuntimeError(f"Current angle for joint {joint.id} is unknown.")
            target = float(target_angles_deg[idx])
            ok, _, _, _, _ = self._controller.send_joint_angle(
                joint,
                target,
                float(current),
                clamp_limits=clamp_limits,
            )
            results.append(ok)
        return results

    def send_joint_targets_detailed(
        self,
        target_angles_deg: List[float],
        current_angles_deg: List[float],
        clamp_limits: bool = False,
        overrides: Optional[dict[int, dict]] = None,
        joint_ids: Optional[List[int]] = None,
    ) -> tuple[bool, List[dict]]:
        if not self._homed:
            raise RuntimeError("Robot not homed. Use Set Home first.")
        self.ensure_ready()
        ok_all = True
        details: List[dict] = []
        selected_joint_ids = set(int(joint_id) for joint_id in (joint_ids or []))
        for idx, joint in enumerate(self._config.joints):
            if idx >= len(target_angles_deg) or idx >= len(current_angles_deg):
                break
            if selected_joint_ids and joint.id not in selected_joint_ids:
                continue
            current = current_angles_deg[idx]
            if current is None:
                raise RuntimeError(f"Current angle for joint {joint.id} is unknown.")
            target = float(target_angles_deg[idx])
            override = overrides.get(joint.id, {}) if overrides else {}
            speed_rpm = override.get("speed_rpm")
            acc = override.get("acc")
            ok, dir_ccw, motor_speed, motor_acc, pulses = self._controller.send_joint_angle(
                joint,
                target,
                float(current),
                clamp_limits=clamp_limits,
                speed_rpm=speed_rpm,
                acc=acc,
            )
            details.append(
                {
                    "joint_id": joint.id,
                    "target_deg": target,
                    "dir_ccw": dir_ccw,
                    "motor_speed_rpm": motor_speed,
                    "motor_acc": motor_acc,
                    "pulses": pulses,
                    "ok": ok,
                }
            )
            ok_all = ok_all and ok
        return ok_all, details

    def move_home(self) -> None:
        targets = [joint.home_pos for joint in self._config.joints]
        self.move_to_angles(targets)

    def send_joint_velocities_detailed(
        self,
        joint_velocities_deg_s: List[float],
        overrides: Optional[dict[int, dict]] = None,
    ) -> tuple[bool, List[dict]]:
        if not self._homed:
            raise RuntimeError("Robot not homed. Use Set Home first.")
        self.ensure_ready()
        ok_all = True
        details: List[dict] = []
        for idx, joint in enumerate(self._config.joints):
            if idx >= len(joint_velocities_deg_s):
                break
            joint_vel = float(joint_velocities_deg_s[idx])
            override = overrides.get(joint.id, {}) if overrides else {}
            stream_acc = override.get("stream_acc", override.get("acc"))
            ok, dir_ccw, motor_speed, motor_acc = self._controller.send_joint_velocity(
                joint,
                joint_vel_deg_s=joint_vel,
                acc=stream_acc,
            )
            details.append(
                {
                    "joint_id": joint.id,
                    "joint_vel_deg_s": joint_vel,
                    "dir_ccw": dir_ccw,
                    "motor_speed_rpm": motor_speed,
                    "motor_acc": motor_acc,
                    "ok": ok,
                }
            )
            ok_all = ok_all and ok
        return ok_all, details

    def stop_joint_velocities(
        self,
        overrides: Optional[dict[int, dict]] = None,
        default_acc: int = 120,
        burst: int = 2,
    ) -> bool:
        if not self._homed:
            raise RuntimeError("Robot not homed. Use Set Home first.")
        self.ensure_ready()
        ok_all = True
        for joint in self._config.joints:
            override = overrides.get(joint.id, {}) if overrides else {}
            stop_acc = int(override.get("stream_acc", override.get("acc", default_acc)))
            ok = self._controller.stop_motor_velocity(joint.id, acc=stop_acc, burst=burst)
            ok_all = ok_all and ok
        return ok_all

    def send_joint_targets_abs_wait_detailed(
        self,
        target_angles_deg: List[float],
        overrides: Optional[dict[int, dict]] = None,
        clamp_limits: bool = False,
        timeout_s: float = 10.0,
    ) -> tuple[bool, List[dict]]:
        """
        Send F5 absolute-axis position commands to all joints using home offsets.
        All commands are sent in parallel (back-to-back), then waits for all motors to complete.

        This is more robust than FD relative-pulse commands after a velocity stream,
        because it doesn't depend on the accuracy of current encoder reads.

        Returns: (all_ok, details_list)
        """
        if not self._homed:
            raise RuntimeError("Robot not homed. Use Set Home first.")
        self.ensure_ready()

        # Enable CANRSP for all joints to receive completion responses
        motor_ids = [joint.id for joint in self._config.joints]
        self._controller.ensure_canrsp(motor_ids, True)

        import logging
        _diag_log = logging.getLogger("streaming")

        ok_all = True
        details: List[dict] = []
        try:
            # Phase 1: Compute targets and read BEFORE encoders for all joints
            send_queue = []
            for idx, joint in enumerate(self._config.joints):
                if idx >= len(target_angles_deg):
                    break
                target = float(target_angles_deg[idx])

                # P0.5: _home_offsets is a list, not a dict - use index access with defensive guards
                joint_idx = joint.id - 1  # Convert 1-based joint ID to 0-based index
                if joint_idx < 0 or joint_idx >= len(self._home_offsets):
                    # Invalid joint ID - out of range
                    _diag_log.error(
                        "FINAL_LOCK: Invalid joint_id %d (index %d out of range 0-%d)",
                        joint.id, joint_idx, len(self._home_offsets) - 1
                    )
                    details.append({
                        "ok": False,
                        "reason": f"invalid joint_id {joint.id} (index {joint_idx} out of range)",
                        "joint_id": joint.id,
                        "target_deg": target,
                        "dir_ccw": None,
                        "motor_speed_rpm": None,
                        "motor_acc": None,
                        "pulses": None,
                        "target_axis": None,
                        "lock_cmd": "F5",
                    })
                    ok_all = False
                    continue

                home_offset = self._home_offsets[joint_idx]
                if home_offset is None:
                    # Joint not homed - missing home offset
                    _diag_log.error(
                        "FINAL_LOCK: Joint %d not homed (home_offset is None at index %d)",
                        joint.id, joint_idx
                    )
                    details.append({
                        "ok": False,
                        "reason": f"joint {joint.id} not homed (missing home offset)",
                        "joint_id": joint.id,
                        "target_deg": target,
                        "dir_ccw": None,
                        "motor_speed_rpm": None,
                        "motor_acc": None,
                        "pulses": None,
                        "target_axis": None,
                        "lock_cmd": "F5",
                    })
                    ok_all = False
                    continue

                # Clamp target to limits if requested
                if clamp_limits:
                    from .controller import clamp
                    target = clamp(target, joint.min_angle, joint.max_angle)

                # Get speed/acc overrides
                override = overrides.get(joint.id, {}) if overrides else {}
                speed_rpm = override.get("speed_rpm")
                acc = override.get("acc")
                if speed_rpm is None:
                    speed_rpm = joint.speed_rpm
                if acc is None:
                    acc = joint.acc

                # Compute motor speed/acc
                motor_speed = self._controller._joint_speed_to_motor_rpm(speed_rpm, joint.gear_ratio)
                motor_acc = self._controller._joint_acc_to_motor_acc(acc, joint.gear_ratio)

                # Compute F5 target from home offset directly
                raw_target_axis = self._controller.joint_angle_to_absolute_axis_from_home(joint, target, home_offset)
                target_axis = self._controller._clamp_int24(raw_target_axis)

                encoder_target = self._controller._f5_command_to_encoder_target(joint, target_axis)

                # Compute pulses for diagnostic/FD-compatibility (not used in F5 command)
                angle_diff = target - joint.home_pos  # From home position
                motor_revs = (abs(angle_diff) / 360.0) * joint.gear_ratio
                pulses = int(motor_revs * self._config.encoder_ticks_per_rev)

                # Read BEFORE encoder for diagnostics
                before_ticks = self._controller.read_encoder(joint.id, timeout_s=0.2)
                _diag_log.info(
                    "[F5_BEFORE] J%d encoder_ticks=%s home=%d f5_target=%d invert=%s",
                    joint.id, before_ticks, home_offset, target_axis, joint.invert_dir,
                )

                # Use the live encoder when deciding whether an absolute target move is required.
                delta_axis = encoder_target - (before_ticks if before_ticks is not None else home_offset)
                axis_counts = abs(delta_axis)
                dir_ccw = 1 if delta_axis >= 0 else 0

                if axis_counts <= self._controller.F5_NO_MOTION_TOLERANCE_COUNTS:
                    _diag_log.info(
                        "FINAL_LOCK: J%d skipped (no motion required) before=%s target=%d home=%d invert=%s",
                        joint.id,
                        before_ticks,
                        target_axis,
                        home_offset,
                        joint.invert_dir,
                    )
                    details.append({
                        "joint_id": joint.id,
                        "target_deg": target,
                        "ok": True,
                        "dir_ccw": dir_ccw,
                        "motor_speed_rpm": motor_speed,
                        "motor_acc": motor_acc,
                        "pulses": pulses,
                        "target_axis": target_axis,
                        "encoder_target": encoder_target,
                        "lock_cmd": "F5",
                        "reason_code": "no_motion_required",
                        "wait_elapsed_s": 0.0,
                        "resp_status": None,
                        "resp_cmd": None,
                        "resp_raw": None,
                        "exception": None,
                        "target_angle": target,
                        "current_angle": joint.home_pos,
                        "target_axis_counts": target_axis,
                        "raw_target_axis": raw_target_axis,
                        "raw_target_axis_counts": raw_target_axis,
                        "home_encoder": int(home_offset),
                        "home_encoder_counts": int(home_offset),
                        "delta_axis": int(delta_axis),
                        "delta_axis_counts": int(delta_axis),
                        "fd_pulses": pulses,
                        "axis_counts": int(axis_counts),
                        "diag_before_ticks": before_ticks,
                        "diag_after_ticks": before_ticks,
                        "diag_expected_delta": int(delta_axis),
                        "diag_actual_delta": 0,
                    })
                    continue

                # Queue this joint for sending
                send_queue.append({
                    "joint": joint,
                    "target": target,
                    "target_axis": target_axis,
                    "encoder_target": encoder_target,
                    "motor_speed": motor_speed,
                    "motor_acc": motor_acc,
                    "dir_ccw": dir_ccw,
                    "pulses": pulses,
                    "raw_target_axis": raw_target_axis,
                    "home_offset": home_offset,
                    "delta_axis": delta_axis,
                    "axis_counts": axis_counts,
                    "before_ticks": before_ticks,
                })

            # Phase 2: Send all F5 commands back-to-back (no wait)
            sent_joint_ids = []
            for item in send_queue:
                ok, send_details = self._controller.send_position_abs_axis_no_wait(
                    can_id=item["joint"].id,
                    speed_rpm=item["motor_speed"],
                    acc=item["motor_acc"],
                    abs_axis=item["target_axis"],
                )
                if not ok:
                    # Send failed - mark this joint as failed
                    _diag_log.error(
                        "FINAL_LOCK: F5 send failed for J%d: %s",
                        item["joint"].id, send_details.get("reason_code")
                    )
                    ok_all = False
                else:
                    sent_joint_ids.append(item["joint"].id)

            # Phase 3: Wait for all CANRSP completions
            wait_results = self._controller.wait_canrsp_multi(sent_joint_ids, timeout_s=timeout_s)

            # Phase 4: Read AFTER encoders and build final details
            for item in send_queue:
                joint = item["joint"]
                wait_result = wait_results.get(joint.id, {
                    "ok": False,
                    "reason_code": "not_sent",
                    "wait_elapsed_s": 0.0,
                    "resp_status": None,
                    "resp_cmd": None,
                    "resp_raw": None,
                })

                # Read AFTER encoder for diagnostics
                after_ticks = self._controller.read_encoder(joint.id, timeout_s=0.2)
                before_ticks = item["before_ticks"]
                target_axis = item["target_axis"]
                encoder_target = item["encoder_target"]
                expected_delta = encoder_target - (before_ticks if before_ticks is not None else 0)
                actual_delta = (after_ticks - before_ticks) if (before_ticks is not None and after_ticks is not None) else None
                _diag_log.info(
                    "[F5_AFTER] J%d encoder_ticks=%s f5_target=%d "
                    "expected_delta=%s actual_delta=%s sign_match=%s "
                    "landed_vs_target=%s invert=%s",
                    joint.id,
                    after_ticks,
                    target_axis,
                    expected_delta if before_ticks is not None else "N/A",
                    actual_delta if actual_delta is not None else "N/A",
                    (expected_delta > 0) == (actual_delta > 0) if (before_ticks is not None and actual_delta is not None and expected_delta != 0 and actual_delta != 0) else "N/A",
                    (after_ticks - encoder_target) if after_ticks is not None else "N/A",
                    joint.invert_dir,
                )

                # Build final details dict
                details.append({
                    "joint_id": joint.id,
                    "target_deg": item["target"],
                    "ok": wait_result["ok"],
                    "dir_ccw": item["dir_ccw"],
                    "motor_speed_rpm": item["motor_speed"],
                    "motor_acc": item["motor_acc"],
                    "pulses": item["pulses"],
                    "target_axis": item["target_axis"],
                    "encoder_target": encoder_target,
                    "lock_cmd": "F5",
                    "reason_code": wait_result.get("reason_code"),
                    "wait_elapsed_s": wait_result.get("wait_elapsed_s"),
                    "resp_status": wait_result.get("resp_status"),
                    "resp_cmd": wait_result.get("resp_cmd"),
                    "resp_raw": wait_result.get("resp_raw"),
                    "exception": wait_result.get("exception"),
                    "target_angle": item["target"],
                    "current_angle": joint.home_pos,  # We don't know current angle reliably
                    "target_axis_counts": item["target_axis"],
                    "raw_target_axis": item["raw_target_axis"],
                    "raw_target_axis_counts": item["raw_target_axis"],
                    "home_encoder": int(item["home_offset"]),
                    "home_encoder_counts": int(item["home_offset"]),
                    "delta_axis": int(item["delta_axis"]),
                    "delta_axis_counts": int(item["delta_axis"]),
                    "fd_pulses": item["pulses"],
                    "axis_counts": int(item["axis_counts"]),
                    "diag_before_ticks": before_ticks,
                    "diag_after_ticks": after_ticks,
                    "diag_expected_delta": expected_delta if before_ticks is not None else None,
                    "diag_actual_delta": actual_delta,
                })
                if not wait_result["ok"]:
                    _diag_log.warning(
                        "FINAL_LOCK: J%d failed reason=%s before=%s after=%s target=%s expected_delta=%s actual_delta=%s resp_status=%s resp_raw=%s",
                        joint.id,
                        wait_result.get("reason_code"),
                        before_ticks,
                        after_ticks,
                        target_axis,
                        expected_delta if before_ticks is not None else None,
                        actual_delta,
                        wait_result.get("resp_status"),
                        wait_result.get("resp_raw"),
                    )
                ok_all = ok_all and wait_result["ok"]

            return ok_all, details
        finally:
            # FINAL_LOCK is the only phase that requires CANRSP; keep it disabled otherwise.
            self._controller.ensure_canrsp(motor_ids, False)

    def set_canrsp_enabled(
        self,
        enable: bool,
        joint_ids: Optional[List[int]] = None,
    ) -> None:
        self.ensure_ready()
        ids = list(joint_ids) if joint_ids is not None else [joint.id for joint in self._config.joints]
        self._controller.ensure_canrsp(ids, bool(enable))

