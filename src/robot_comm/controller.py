from __future__ import annotations

import logging
import time
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .config import CommConfig, JointConfig, get_comm_config

try:
    import can
    CAN_AVAILABLE = True
except ImportError:
    CAN_AVAILABLE = False


def clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else (hi if v > hi else v)


def crc8(can_id: int, payload: bytes) -> int:
    return (can_id + sum(payload)) & 0xFF


@dataclass
class MotorStatus:
    can_id: int
    enabled: bool = False
    encoder_value: Optional[int] = None


@dataclass
class CanConnectResult:
    ok: bool
    message: str
    interface: str
    channel: str
    bitrate: int
    init_timeout_s: float
    listen_timeout_s: float
    stage: str
    frames_received: int = 0


class RobotCommController:
    CMD_MODE = 0x82
    CMD_CURRENT = 0x83
    CMD_MICROSTEP = 0x84
    CMD_PROTECT = 0x88
    CMD_CANRSP = 0x8C
    CMD_ENABLE = 0xF3
    CMD_POSITION_ABS_AXIS = 0xF5
    CMD_SPEED = 0xF6
    CMD_POSITION = 0xFD
    CMD_READ_ENCODER = 0x30
    CMD_READ_ENCODER_CARRY = 0x33
    MOTOR_MAX_RPM = 3000            # Max RPM SERVO42D can take
    F5_NO_MOTION_TOLERANCE_COUNTS = 16

    def __init__(self, simulation_mode: bool = False, config: Optional[CommConfig] = None):
        self._config = config or get_comm_config()
        self._simulation_mode = simulation_mode or not CAN_AVAILABLE
        self._bus: Optional["can.BusABC"] = None
        self._connected = False
        self._motors: Dict[int, MotorStatus] = {
            joint.id: MotorStatus(can_id=joint.id) for joint in self._config.joints
        }
        self._bus_lock = threading.Lock()
        self._canrsp_state: Dict[int, Optional[bool]] = {}
        self._encoder_read_mode: Optional[int] = None
        self._encoder_mode_locked = False
        self._log = logging.getLogger("robot_comm_controller")

    def update_config(self, config: CommConfig) -> None:
        existing = self._motors
        self._config = config
        self._motors = {
            joint.id: existing.get(joint.id, MotorStatus(can_id=joint.id))
            for joint in self._config.joints
        }

    @property
    def is_connected(self) -> bool:
        return self._connected

    def _open_can_bus(self, channel: str):
        interface = str(self._config.can_interface).strip().lower()
        if interface == "slcan":
            try:
                return can.Bus(
                    interface=self._config.can_interface,
                    channel=channel,
                    bitrate=self._config.can_bitrate,
                    tty_baudrate=self._config.can_tty_baudrate,
                )
            except TypeError:
                # Compatibility with python-can variants that use camelCase.
                return can.Bus(
                    interface=self._config.can_interface,
                    channel=channel,
                    bitrate=self._config.can_bitrate,
                    ttyBaudrate=self._config.can_tty_baudrate,
                )
        return can.Bus(
            interface=self._config.can_interface,
            channel=channel,
            bitrate=self._config.can_bitrate,
        )

    def _build_connect_failure(
        self,
        *,
        channel: str,
        init_timeout_s: float,
        listen_timeout_s: float,
        stage: str,
        message: str,
    ) -> CanConnectResult:
        return CanConnectResult(
            ok=False,
            message=message,
            interface=str(self._config.can_interface),
            channel=channel,
            bitrate=int(self._config.can_bitrate),
            init_timeout_s=float(init_timeout_s),
            listen_timeout_s=float(listen_timeout_s),
            stage=stage,
            frames_received=0,
        )

    def connect_with_checks(
        self,
        port_override: Optional[str] = None,
        init_timeout_s: float = 5.0,
        listen_timeout_s: float = 0.2,
    ) -> CanConnectResult:
        if self._simulation_mode:
            self._connected = True
            self._encoder_read_mode = None
            self._encoder_mode_locked = False
            channel = str(port_override or self._config.can_port)
            return CanConnectResult(
                ok=True,
                message="Simulation mode active.",
                interface=str(self._config.can_interface),
                channel=channel,
                bitrate=int(self._config.can_bitrate),
                init_timeout_s=float(init_timeout_s),
                listen_timeout_s=float(listen_timeout_s),
                stage="simulation",
                frames_received=0,
            )

        channel = str(port_override or self._config.can_port).strip()
        interface = str(self._config.can_interface).strip()
        bitrate = int(self._config.can_bitrate)
        init_timeout_s = max(0.1, float(init_timeout_s))
        listen_timeout_s = max(0.0, float(listen_timeout_s))

        if not CAN_AVAILABLE:
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="import",
                message="python-can is not available.",
            )
        if not interface:
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="config",
                message="CAN interface is not configured.",
            )
        if not channel:
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="config",
                message="CAN port/channel is not configured.",
            )
        if bitrate <= 0:
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="config",
                message=f"Invalid CAN bitrate: {bitrate}.",
            )

        result: Dict[str, object] = {"bus": None, "exc": None}
        cancel_event = threading.Event()

        def bus_creator():
            bus = None
            try:
                bus = self._open_can_bus(channel)
                if cancel_event.is_set():
                    try:
                        bus.shutdown()
                    except Exception:
                        pass
                    return
                result["bus"] = bus
            except Exception as exc:
                if bus is not None:
                    try:
                        bus.shutdown()
                    except Exception:
                        pass
                result["exc"] = exc

        thread = threading.Thread(target=bus_creator, daemon=True)
        thread.start()
        thread.join(init_timeout_s)
        if thread.is_alive():
            cancel_event.set()
            self._connected = False
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="open_timeout",
                message=(
                    f"CAN bus initialization timed out after {init_timeout_s:.1f}s. "
                    "Check hardware power, USB/CAN adapter, and COM port settings."
                ),
            )

        open_exc = result.get("exc")
        if open_exc is not None:
            self._connected = False
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="open_error",
                message=f"Failed to open CAN bus: {open_exc}",
            )

        bus = result.get("bus")
        if bus is None:
            self._connected = False
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="open_error",
                message="Failed to open CAN bus for an unknown reason.",
            )

        try:
            motor_ids = list(self._motors.keys())
            try:
                bus.set_filters(
                    [
                        {"can_id": cid, "can_mask": 0x7FF, "extended": False}
                        for cid in motor_ids
                    ]
                )
            except Exception:
                pass

            frames_received = 0
            if listen_timeout_s > 0.0:
                t_end = time.time() + listen_timeout_s
                while time.time() < t_end:
                    remaining = max(0.0, min(0.05, t_end - time.time()))
                    if remaining <= 0.0:
                        break
                    msg = bus.recv(timeout=remaining)
                    if msg is None:
                        continue
                    frames_received += 1

            self._bus = bus
            self._connected = True
            self._encoder_read_mode = None
            self._encoder_mode_locked = False
            message = (
                f"CAN connected on {channel} ({interface}, {bitrate} bps). "
                f"Observed {frames_received} frame(s) during startup listen."
                if listen_timeout_s > 0.0
                else f"CAN connected on {channel} ({interface}, {bitrate} bps)."
            )
            return CanConnectResult(
                ok=True,
                message=message,
                interface=interface,
                channel=channel,
                bitrate=bitrate,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="connected",
                frames_received=frames_received,
            )
        except Exception as exc:
            try:
                bus.shutdown()
            except Exception:
                pass
            self._bus = None
            self._connected = False
            self._encoder_read_mode = None
            self._encoder_mode_locked = False
            return self._build_connect_failure(
                channel=channel,
                init_timeout_s=init_timeout_s,
                listen_timeout_s=listen_timeout_s,
                stage="preflight",
                message=f"CAN preflight failed: {exc}",
            )

    def connect(self, port_override: Optional[str] = None) -> bool:
        if self._connected:
            return True
        return self.connect_with_checks(port_override=port_override).ok

    def disconnect(self) -> None:
        if self._bus is not None:
            try:
                self._bus.shutdown()
            except Exception:
                pass
            self._bus = None
        self._connected = False
        self._encoder_read_mode = None
        self._encoder_mode_locked = False

    def _mk_msg(self, can_id: int, *data: int) -> "can.Message":
        payload = bytes(data)
        return can.Message(
            arbitration_id=can_id,
            is_extended_id=False,
            data=payload + bytes([crc8(can_id, payload)]),
        )

    def _send_locked(self, can_id: int, *data: int) -> bool:
        if self._simulation_mode:
            return True
        if self._bus is None:
            return False
        try:
            msg = self._mk_msg(can_id, *data)
            self._bus.send(msg)
            return True
        except Exception:
            return False

    def _send(self, can_id: int, *data: int) -> bool:
        with self._bus_lock:
            return self._send_locked(can_id, *data)

    def _recv_match_locked(self, want_id: int, want_code: int, timeout: float):
        if self._simulation_mode or self._bus is None:
            return None
        t0 = time.time()
        while True:
            remaining = max(0.0, timeout - (time.time() - t0))
            if remaining <= 0:
                return None
            msg = self._bus.recv(remaining)
            if not msg or not msg.data:
                continue
            if msg.arbitration_id == want_id and msg.data[0] == want_code:
                return msg

    def _request_response(
        self,
        can_id: int,
        want_code: int,
        timeout_s: Optional[float] = None,
        *data: int,
    ):
        if self._simulation_mode or self._bus is None:
            time.sleep(0.01)
            return None
        effective_timeout = self._config.can_recv_timeout
        if timeout_s is not None:
            effective_timeout = max(0.001, float(timeout_s))

        # Measure lock acquisition time
        lock_start = time.perf_counter()
        self._bus_lock.acquire()
        lock_wait_ms = (time.perf_counter() - lock_start) * 1000

        try:
            # Measure CAN operation time
            op_start = time.perf_counter()

            if not self._send_locked(can_id, *data):
                return None
            result = self._recv_match_locked(can_id, want_code, effective_timeout)

            op_elapsed_ms = (time.perf_counter() - op_start) * 1000

            # Log if operation is slow or lock was contested
            if lock_wait_ms > 5.0 or op_elapsed_ms > 50.0:
                if self._log:
                    self._log.debug(
                        "[CAN] ID=0x%03X lock_wait=%.1f ms op_time=%.1f ms",
                        can_id, lock_wait_ms, op_elapsed_ms
                    )

            return result
        finally:
            self._bus_lock.release()

    def _drain_rx(self, drain_ms: int = 120) -> None:
        if self._simulation_mode or self._bus is None:
            return
        with self._bus_lock:
            t_end = time.time() + (drain_ms / 1000.0)
            while time.time() < t_end:
                _ = self._bus.recv(timeout=0)
                time.sleep(0.001)

    def set_canrsp(self, can_id: int, enable: bool) -> bool:
        if self._simulation_mode:
            self._canrsp_state[can_id] = enable
            return True
        ack = self._request_response(
            can_id,
            self.CMD_CANRSP,
            self._config.can_recv_timeout,
            self.CMD_CANRSP,
            0x01 if enable else 0x00,
        )
        if ack:
            self._canrsp_state[can_id] = enable
            return True
        return False

    def ensure_canrsp(self, can_ids: List[int], enable: bool) -> None:
        for cid in can_ids:
            if self._canrsp_state.get(cid) is not enable:
                self.set_canrsp(cid, enable)
                time.sleep(0.01)

    def setup_motor(self, joint: JointConfig) -> None:
        can_id = joint.id
        self._send(can_id, self.CMD_PROTECT, 0x00)
        time.sleep(0.01)
        self._send(can_id, self.CMD_MODE, 0x05)
        time.sleep(0.01)
        mA = clamp(joint.work_current_mA, 0, 5200)
        self._send(can_id, self.CMD_CURRENT, (int(mA) >> 8) & 0xFF, int(mA) & 0xFF)
        time.sleep(0.01)
        if self._config.motor_enable_on_start:
            self._send(can_id, self.CMD_ENABLE, 0x01)
            self._motors[can_id].enabled = True
            time.sleep(0.01)

    def setup_all_motors(self) -> None:
        motor_ids = list(self._motors.keys())
        self.ensure_canrsp(motor_ids, True)
        self._drain_rx()
        for joint in self._config.joints:
            self.setup_motor(joint)
        for joint in self._config.joints:
            self.set_microstep(joint.id, self._config.motor_microstep)
        self.ensure_encoder_mode(timeout_s=self._config.can_recv_timeout, probe_can_ids=motor_ids)
        self.ensure_canrsp(motor_ids, False)
        self._drain_rx()

    def set_microstep(self, can_id: int, microstep: int) -> bool:
        microstep = int(clamp(microstep, 1, 255))
        if self._simulation_mode:
            return True
        ack = self._request_response(
            can_id,
            self.CMD_MICROSTEP,
            self._config.can_recv_timeout,
            self.CMD_MICROSTEP,
            microstep,
        )
        if ack and len(ack.data) >= 3:
            return ack.data[1] == 1
        return False

    @staticmethod
    def mode_to_hex(mode: Optional[int]) -> str:
        if mode is None:
            return "None"
        return "0x%02X" % int(mode)

    def get_encoder_read_mode(self) -> Optional[int]:
        return self._encoder_read_mode

    def is_encoder_mode_locked(self) -> bool:
        return bool(self._encoder_mode_locked)

    def _parse_encoder_response(
        self,
        can_id: int,
        resp,
        expected_cmd: Optional[int] = None,
    ) -> tuple[Optional[int], Optional[int], str]:
        if resp is None:
            return None, None, "timeout"
        if not resp.data:
            return None, None, "empty_payload"
        cmd = int(resp.data[0])
        if expected_cmd is not None and cmd != expected_cmd:
            return None, cmd, "unexpected_cmd"
        if cmd == self.CMD_READ_ENCODER:
            if len(resp.data) < 8:
                return None, cmd, "invalid_len_0x30"
            carry = int.from_bytes(resp.data[1:5], byteorder="big", signed=True)
            value = int.from_bytes(resp.data[5:7], byteorder="big", signed=False)
            ticks_per_rev = self._config.encoder_ticks_per_rev
            val = carry * ticks_per_rev + value
            self._motors[can_id].encoder_value = val
            return val, cmd, "ok"
        if cmd == self.CMD_READ_ENCODER_CARRY:
            if len(resp.data) < 6:
                return None, cmd, "invalid_len_0x33"
            sign = resp.data[1]
            val = (resp.data[2] << 16) | (resp.data[3] << 8) | resp.data[4]
            if sign != 0:
                val = -val
            self._motors[can_id].encoder_value = val
            return val, cmd, "ok"
        return None, cmd, "unexpected_cmd"

    def _read_encoder_once(self, can_id: int, cmd: int, timeout_s: float) -> tuple[Optional[int], str]:
        resp = self._request_response(
            can_id,
            cmd,
            timeout_s,
            cmd,
        )
        value, _, reason = self._parse_encoder_response(can_id=can_id, resp=resp, expected_cmd=cmd)
        return value, reason

    def ensure_encoder_mode(
        self,
        timeout_s: Optional[float] = None,
        probe_can_ids: Optional[List[int]] = None,
    ) -> Optional[int]:
        if self._encoder_mode_locked and self._encoder_read_mode in (
            self.CMD_READ_ENCODER,
            self.CMD_READ_ENCODER_CARRY,
        ):
            return self._encoder_read_mode

        if self._simulation_mode:
            self._encoder_read_mode = self.CMD_READ_ENCODER
            self._encoder_mode_locked = True
            return self._encoder_read_mode

        effective_timeout = self._config.can_recv_timeout
        if timeout_s is not None:
            effective_timeout = max(0.001, float(timeout_s))

        can_ids = list(probe_can_ids or [joint.id for joint in self._config.joints])
        if not can_ids:
            return None

        for cmd in (self.CMD_READ_ENCODER, self.CMD_READ_ENCODER_CARRY):
            for can_id in can_ids:
                value, reason = self._read_encoder_once(can_id=can_id, cmd=cmd, timeout_s=effective_timeout)
                if value is not None:
                    self._encoder_read_mode = cmd
                    self._encoder_mode_locked = True
                    self._log.info(
                        "Encoder read mode locked: mode=%s probe_joint=%d",
                        self.mode_to_hex(cmd),
                        can_id,
                    )
                    return cmd
                self._log.debug(
                    "Encoder mode probe failed: mode=%s joint=%d reason=%s timeout=%.3f",
                    self.mode_to_hex(cmd),
                    can_id,
                    reason,
                    effective_timeout,
                )
        self._log.warning("Encoder mode probe failed for all candidate commands/joints.")
        return None

    def read_encoder(self, can_id: int, timeout_s: Optional[float] = None) -> Optional[int]:
        if self._simulation_mode:
            return 0

        effective_timeout = self._config.can_recv_timeout
        if timeout_s is not None:
            effective_timeout = max(0.001, float(timeout_s))

        mode = self.ensure_encoder_mode(timeout_s=effective_timeout, probe_can_ids=[can_id])
        if mode is None:
            self._log.warning("Encoder read failed: unresolved encoder mode (joint=%d).", can_id)
            return None

        value, reason = self._read_encoder_once(can_id=can_id, cmd=mode, timeout_s=effective_timeout)
        if value is None:
            self._log.debug(
                "Encoder read failed: joint=%d mode=%s reason=%s timeout=%.3f",
                can_id,
                self.mode_to_hex(mode),
                reason,
                effective_timeout,
            )
        else:
            self._log.debug(
                "Encoder read ok: joint=%d mode=%s ticks=%d timeout=%.3f",
                can_id,
                self.mode_to_hex(mode),
                value,
                effective_timeout,
            )
        return value

    def read_all_encoders(self, timeout_s: Optional[float] = None) -> List[Optional[int]]:
        motor_ids = [joint.id for joint in self._config.joints]
        values = []
        for can_id in motor_ids:
            values.append(self.read_encoder(can_id, timeout_s=timeout_s))
        return values

    def _joint_speed_to_motor_rpm(self, joint_speed_rpm: float, gear_ratio: float) -> int:
        requested_joint_rpm = abs(float(joint_speed_rpm))
        motor_rpm = int(requested_joint_rpm * abs(gear_ratio) + 0.5)
        if requested_joint_rpm > 0.0 and motor_rpm <= 0:
            motor_rpm = 1
        return int(clamp(motor_rpm, 0, self.MOTOR_MAX_RPM))

    def _joint_acc_to_motor_acc(self, joint_acc: float, gear_ratio: float) -> int:
        motor_acc = int(abs(joint_acc) * abs(gear_ratio) + 0.5)
        return int(clamp(motor_acc, 0, 255))

    def _driver_acc_to_motor_acc(self, driver_acc: float) -> int:
        motor_acc = int(abs(driver_acc) + 0.5)
        return int(clamp(motor_acc, 0, 255))

    def _joint_deg_s_to_motor_rpm(self, joint_vel_deg_s: float, gear_ratio: float) -> int:
        motor_rpm = (abs(joint_vel_deg_s) * abs(gear_ratio)) / 6.0
        return int(clamp(int(motor_rpm + 0.5), 0, self.MOTOR_MAX_RPM))

    def encoder_ticks_to_angle(self, joint: JointConfig, encoder_ticks: int, home_offset: int = 0) -> float:
        relative_ticks = encoder_ticks - home_offset
        ticks_per_rev = self._config.encoder_ticks_per_rev
        motor_revs = relative_ticks / ticks_per_rev
        joint_angle = (motor_revs / joint.gear_ratio) * 360.0
        joint_angle *= joint.sign_map
        return joint_angle

    def angle_to_pulses(self, joint: JointConfig, target_angle: float, current_angle: float) -> Tuple[int, int]:
        angle_diff = target_angle - current_angle
        angle_diff *= joint.sign_map
        dir_ccw = 1 if angle_diff >= 0 else 0
        ticks_per_rev = self._config.motor_ticks_per_rev
        motor_revs = (abs(angle_diff) / 360.0) * joint.gear_ratio
        pulses = int(motor_revs * ticks_per_rev)
        return dir_ccw, pulses

    @staticmethod
    def _clamp_int24(value: int) -> int:
        """Clamp value to 24-bit signed range for F5 absolute-axis commands"""
        return int(clamp(value, -0x7FFFFF, 0x7FFFFF))

    @staticmethod
    def _encode_int24(value: int) -> Tuple[int, int, int]:
        """Encode a 24-bit signed integer into three bytes for CAN payload"""
        value_clamped = int(clamp(value, -0x7FFFFF, 0x7FFFFF))
        if value_clamped < 0:
            value_clamped = (1 << 24) + value_clamped
        b5 = (value_clamped >> 16) & 0xFF
        b6 = (value_clamped >> 8) & 0xFF
        b7 = value_clamped & 0xFF
        return b5, b6, b7

    @staticmethod
    def _f5_command_to_encoder_target(joint: JointConfig, f5_target: int) -> int:
        """
        Convert an F5 command-space target into the encoder-space landing target.
        Inverted joints are commanded with a negated F5 axis value but still land on
        the corresponding positive encoder-side absolute position.
        """
        return -int(f5_target) if joint.invert_dir else int(f5_target)

    def joint_angle_to_absolute_axis_from_home(
        self,
        joint: JointConfig,
        target_angle_deg: float,
        home_encoder: int,
    ) -> int:
        """
        Compute F5 absolute axis target from home offset directly, bypassing current encoder read.
        This is more robust than current_encoder + delta when encoder reads may be stale/corrupted.

        Formula: home_encoder + int(target_angle * sign_map * gear_ratio / 360 * ticks_per_rev)

        For invert_dir joints, the motor controller hardware negates the F5 axis target
        (motor physically inverted). To compensate, we negate the ENTIRE f5_target (not
        just axis counts) so the hardware double-negation lands at the correct encoder
        position (home + axis_counts). encoder_ticks_to_angle() needs no changes — the
        motor physically ends up at the correct position and the encoder reads it directly.

        CRITICAL: Uses encoder_ticks_per_rev (16384) to match hardware 0x30 protocol.
        The encoder value field wraps at 16384 (14-bit magnetic encoder), not 12800.
        """
        motor_revs = (target_angle_deg * joint.sign_map / 360.0) * joint.gear_ratio
        axis_counts = int(motor_revs * self._config.encoder_ticks_per_rev)
        f5_target = home_encoder + axis_counts
        if joint.invert_dir:
            f5_target = -f5_target
        
        # DIAGNOSTIC: Round-trip check - what angle would we read at the expected landing position?
        # For inverted joints, motor lands at -f5_target (i.e. home + axis_counts).
        expected_encoder = home_encoder + axis_counts
        roundtrip_angle = self.encoder_ticks_to_angle(joint, expected_encoder, home_encoder)
        
        import logging
        logger = logging.getLogger("streaming")
        logger.info(
            f"[F5_DIAG] J{joint.id} target={target_angle_deg:.2f}° "
            f"sign_map={joint.sign_map} gear={joint.gear_ratio} invert={joint.invert_dir} "
            f"home={home_encoder} motor_revs={motor_revs:.6f} "
            f"axis_counts={axis_counts} "
            f"f5_target={f5_target} roundtrip_angle={roundtrip_angle:.2f}°"
        )
        
        return f5_target

    def send_position(self, can_id: int, dir_ccw: int, speed_rpm: int, acc: int, pulses: int) -> bool:
        if self._simulation_mode:
            return True
        if not self._connected or self._bus is None:
            return False
        diag_log = logging.getLogger("streaming")
        speed_rpm = int(clamp(speed_rpm, 0, self.MOTOR_MAX_RPM))
        acc = int(clamp(acc, 0, 255))
        pulses = max(0, min(pulses, 0xFFFFFF))
        b2 = ((1 if dir_ccw else 0) << 7) | ((speed_rpm >> 8) & 0x0F)
        b3 = speed_rpm & 0xFF
        b4 = acc
        b5 = (pulses >> 16) & 0xFF
        b6 = (pulses >> 8) & 0xFF
        b7 = pulses & 0xFF
        payload = bytes([self.CMD_POSITION, b2, b3, b4, b5, b6, b7])
        frame_hex = (payload + bytes([crc8(can_id, payload)])).hex()
        diag_log.info(
            "[FD_SEND] J%d dir=%d speed=%d acc=%d pulses=%d frame=%s",
            can_id,
            dir_ccw,
            speed_rpm,
            acc,
            pulses,
            frame_hex,
        )
        try:
            with self._bus_lock:
                msg = can.Message(
                    arbitration_id=can_id,
                    is_extended_id=False,
                    data=payload + bytes([crc8(can_id, payload)]),
                )
                self._bus.send(msg)
            return True
        except Exception as exc:
            diag_log.warning(
                "[FD_SEND_FAIL] J%d dir=%d speed=%d acc=%d pulses=%d err=%s",
                can_id,
                dir_ccw,
                speed_rpm,
                acc,
                pulses,
                exc,
            )
            return False

    def send_speed(self, can_id: int, dir_ccw: int, speed_rpm: int, acc: int) -> bool:
        if self._simulation_mode:
            return True
        if not self._connected or self._bus is None:
            return False
        speed_rpm = int(clamp(speed_rpm, 0, self.MOTOR_MAX_RPM))
        acc = int(clamp(acc, 0, 255))
        b2 = ((1 if dir_ccw else 0) << 7) | ((speed_rpm >> 8) & 0x0F)
        b3 = speed_rpm & 0xFF
        b4 = acc
        payload = bytes([self.CMD_SPEED, b2, b3, b4])
        try:
            with self._bus_lock:
                msg = can.Message(
                    arbitration_id=can_id,
                    is_extended_id=False,
                    data=payload + bytes([crc8(can_id, payload)]),
                )
                self._bus.send(msg)
            return True
        except Exception:
            return False

    def move_joint_to_angle(
        self,
        joint: JointConfig,
        target_angle: float,
        current_angle: float,
        speed_rpm: Optional[float] = None,
    ) -> bool:
        target_angle = clamp(target_angle, joint.min_angle, joint.max_angle)
        if speed_rpm is None:
            speed_rpm = joint.speed_rpm
        motor_speed = self._joint_speed_to_motor_rpm(speed_rpm, joint.gear_ratio)
        motor_acc = self._joint_acc_to_motor_acc(joint.acc, joint.gear_ratio)
        dir_ccw, pulses = self.angle_to_pulses(joint, target_angle, current_angle)
        if joint.invert_dir:
            dir_ccw = 1 - dir_ccw
        if pulses == 0:
            return True
        return self.send_position(joint.id, dir_ccw, motor_speed, motor_acc, pulses)

    def send_joint_angle(
        self,
        joint: JointConfig,
        target_angle: float,
        current_angle: float,
        speed_rpm: Optional[float] = None,
        acc: Optional[int] = None,
        clamp_limits: bool = False,
    ) -> Tuple[bool, int, int, int]:
        if clamp_limits:
            target_angle = clamp(target_angle, joint.min_angle, joint.max_angle)
        if speed_rpm is None:
            speed_rpm = joint.speed_rpm
        if acc is None:
            acc = joint.acc
        motor_speed = self._joint_speed_to_motor_rpm(speed_rpm, joint.gear_ratio)
        motor_acc = self._joint_acc_to_motor_acc(acc, joint.gear_ratio)
        dir_ccw, pulses = self.angle_to_pulses(joint, target_angle, current_angle)
        if joint.invert_dir:
            dir_ccw = 1 - dir_ccw
        diag_log = logging.getLogger("streaming")
        diag_log.info(
            "[FD_CMD] J%d current=%.3f target=%.3f delta=%.3f dir=%d speed=%d acc=%d pulses=%d invert=%s",
            joint.id,
            float(current_angle),
            float(target_angle),
            float(target_angle) - float(current_angle),
            dir_ccw,
            motor_speed,
            motor_acc,
            pulses,
            joint.invert_dir,
        )
        if pulses == 0:
            return True, dir_ccw, motor_speed, motor_acc, pulses
        ok = self.send_position(joint.id, dir_ccw, motor_speed, motor_acc, pulses)
        diag_log.info(
            "[FD_CMD_RESULT] J%d ok=%s pulses=%d",
            joint.id,
            ok,
            pulses,
        )
        return ok, dir_ccw, motor_speed, motor_acc, pulses

    def send_joint_velocity(
        self,
        joint: JointConfig,
        joint_vel_deg_s: float,
        acc: Optional[int] = None,
    ) -> Tuple[bool, int, int, int]:
        if acc is None:
            acc = joint.acc
        motor_acc = self._driver_acc_to_motor_acc(acc)
        motor_speed = self._joint_deg_s_to_motor_rpm(joint_vel_deg_s, joint.gear_ratio)
        motor_vel_signed = joint_vel_deg_s * joint.sign_map
        dir_ccw = 1 if motor_vel_signed >= 0.0 else 0
        if joint.invert_dir:
            dir_ccw = 1 - dir_ccw
        ok = self.send_speed(joint.id, dir_ccw, motor_speed, motor_acc)
        return ok, dir_ccw, motor_speed, motor_acc

    def stop_motor_velocity(self, can_id: int, acc: int, burst: int = 2) -> bool:
        ok = True
        for _ in range(max(1, int(burst))):
            ok = self.send_speed(can_id, 1, 0, acc) and ok
        return ok

    def send_position_abs_axis_no_wait(
        self,
        can_id: int,
        speed_rpm: int,
        acc: int,
        abs_axis: int,
    ) -> Tuple[bool, Dict[str, object]]:
        """
        Send F5 absolute-axis position command WITHOUT waiting for CANRSP completion.
        Used for parallel command dispatch - wait for all with wait_canrsp_multi().

        Returns: (ok, details_dict)
        """
        details: Dict[str, object] = {
            "reason_code": "unknown",
            "send_ok": False,
            "exception": None,
            "speed_rpm": int(speed_rpm),
            "acc": int(acc),
            "abs_axis": int(abs_axis),
            "abs_axis_counts": int(abs_axis),
        }
        if self._simulation_mode:
            details["reason_code"] = "simulation_mode"
            details["send_ok"] = True
            return True, details
        if not self._connected or self._bus is None:
            details["reason_code"] = "not_connected"
            details["send_ok"] = False
            return False, details
        speed_rpm = int(clamp(speed_rpm, 0, self.MOTOR_MAX_RPM))
        acc = int(clamp(acc, 0, 255))
        abs_axis_clamped = self._clamp_int24(abs_axis)
        details["speed_rpm"] = speed_rpm
        details["acc"] = acc
        details["abs_axis"] = abs_axis_clamped
        details["abs_axis_counts"] = abs_axis_clamped
        b5, b6, b7 = self._encode_int24(abs_axis_clamped)
        b2 = (speed_rpm >> 8) & 0x0F
        b3 = speed_rpm & 0xFF
        b4 = acc
        payload = bytes([self.CMD_POSITION_ABS_AXIS, b2, b3, b4, b5, b6, b7])
        try:
            with self._bus_lock:
                msg = can.Message(
                    arbitration_id=can_id,
                    is_extended_id=False,
                    data=payload + bytes([crc8(can_id, payload)]),
                )
                self._bus.send(msg)
            details["reason_code"] = "send_ok"
            details["send_ok"] = True
            return True, details
        except Exception as exc:
            details["reason_code"] = "can_exception"
            details["exception"] = str(exc)
            details["send_ok"] = False
            return False, details

    def wait_canrsp_multi(
        self,
        joint_ids: List[int],
        timeout_s: float = 10.0,
    ) -> Dict[int, Dict[str, object]]:
        """
        Wait for CANRSP completion from multiple joints after sending parallel F5 commands.

        Listens on the CAN bus for F5 CANRSP responses from all joint_ids.
        Tracks which joints have completed (status==2) or failed (status==0).
        Returns when all joints have responded or timeout expires.

        Args:
            joint_ids: List of CAN IDs to wait for
            timeout_s: Total timeout for all joints to complete

        Returns:
            Dict mapping joint_id -> {ok, reason_code, wait_elapsed_s, resp_status, resp_raw}
        """
        results: Dict[int, Dict[str, object]] = {}
        pending = set(joint_ids)

        if self._simulation_mode:
            time.sleep(0.01)
            for jid in joint_ids:
                results[jid] = {
                    "ok": True,
                    "reason_code": "simulation_mode",
                    "wait_elapsed_s": 0.01,
                    "resp_status": 2,
                    "resp_cmd": self.CMD_POSITION_ABS_AXIS,
                    "resp_raw": None,
                }
            return results

        if not self._connected or self._bus is None:
            for jid in joint_ids:
                results[jid] = {
                    "ok": False,
                    "reason_code": "not_connected",
                    "wait_elapsed_s": 0.0,
                    "resp_status": None,
                    "resp_cmd": None,
                    "resp_raw": None,
                }
            return results

        t0 = time.time()
        try:
            with self._bus_lock:
                while pending:
                    remaining = max(0.0, timeout_s - (time.time() - t0))
                    if remaining <= 0:
                        # Timeout - mark all pending joints as failed
                        for jid in pending:
                            results[jid] = {
                                "ok": False,
                                "reason_code": "wait_timeout",
                                "wait_elapsed_s": time.time() - t0,
                                "resp_status": None,
                                "resp_cmd": None,
                                "resp_raw": None,
                            }
                        break

                    resp = self._bus.recv(remaining)
                    if not resp or not resp.data:
                        continue

                    can_id = resp.arbitration_id
                    if can_id not in pending:
                        continue

                    if resp.data[0] != self.CMD_POSITION_ABS_AXIS:
                        continue

                    status = resp.data[1] if len(resp.data) > 1 else None
                    elapsed = time.time() - t0

                    if status == 2:
                        results[can_id] = {
                            "ok": True,
                            "reason_code": "run_complete",
                            "wait_elapsed_s": elapsed,
                            "resp_status": status,
                            "resp_cmd": int(resp.data[0]),
                            "resp_raw": bytes(resp.data).hex(),
                        }
                        pending.remove(can_id)
                    elif status == 0:
                        results[can_id] = {
                            "ok": False,
                            "reason_code": "driver_status_fail",
                            "wait_elapsed_s": elapsed,
                            "resp_status": status,
                            "resp_cmd": int(resp.data[0]),
                            "resp_raw": bytes(resp.data).hex(),
                        }
                        pending.remove(can_id)
        except Exception as exc:
            # Exception during wait - mark all pending as failed
            elapsed = time.time() - t0
            for jid in pending:
                results[jid] = {
                    "ok": False,
                    "reason_code": "can_exception",
                    "wait_elapsed_s": elapsed,
                    "resp_status": None,
                    "resp_cmd": None,
                    "resp_raw": None,
                    "exception": str(exc),
                }

        return results

    def send_position_abs_axis_wait_detailed(
        self,
        can_id: int,
        speed_rpm: int,
        acc: int,
        abs_axis: int,
        timeout_s: float = 10.0,
    ) -> Tuple[bool, Dict[str, object]]:
        """
        Send F5 absolute-axis position command and wait for CANRSP completion.

        This is more robust than FD relative-pulse commands after a velocity stream,
        because it doesn't depend on the accuracy of a current encoder read.

        Returns: (ok, details_dict)
        """
        details: Dict[str, object] = {
            "reason_code": "unknown",
            "wait_elapsed_s": 0.0,
            "resp_status": None,
            "resp_cmd": None,
            "resp_raw": None,
            "exception": None,
            "speed_rpm": int(speed_rpm),
            "acc": int(acc),
            "abs_axis": int(abs_axis),
            "abs_axis_counts": int(abs_axis),
            "timeout_s": float(timeout_s),
        }
        if self._simulation_mode:
            time.sleep(0.01)
            details["reason_code"] = "simulation_mode"
            details["wait_elapsed_s"] = 0.01
            return True, details
        if not self._connected or self._bus is None:
            details["reason_code"] = "not_connected"
            return False, details
        speed_rpm = int(clamp(speed_rpm, 0, self.MOTOR_MAX_RPM))
        acc = int(clamp(acc, 0, 255))
        abs_axis_clamped = self._clamp_int24(abs_axis)
        details["speed_rpm"] = speed_rpm
        details["acc"] = acc
        details["abs_axis"] = abs_axis_clamped
        details["abs_axis_counts"] = abs_axis_clamped
        b5, b6, b7 = self._encode_int24(abs_axis_clamped)
        b2 = (speed_rpm >> 8) & 0x0F
        b3 = speed_rpm & 0xFF
        b4 = acc
        payload = bytes([self.CMD_POSITION_ABS_AXIS, b2, b3, b4, b5, b6, b7])
        t0 = time.time()
        try:
            with self._bus_lock:
                msg = can.Message(
                    arbitration_id=can_id,
                    is_extended_id=False,
                    data=payload + bytes([crc8(can_id, payload)]),
                )
                self._bus.send(msg)
                while True:
                    remaining = max(0.0, timeout_s - (time.time() - t0))
                    if remaining <= 0:
                        details["reason_code"] = "wait_timeout"
                        details["wait_elapsed_s"] = time.time() - t0
                        return False, details
                    resp = self._bus.recv(remaining)
                    if not resp or not resp.data:
                        continue
                    if resp.arbitration_id != can_id:
                        continue
                    if resp.data[0] != self.CMD_POSITION_ABS_AXIS:
                        continue
                    status = resp.data[1] if len(resp.data) > 1 else None
                    details["resp_status"] = status
                    details["resp_cmd"] = int(resp.data[0])
                    details["resp_raw"] = bytes(resp.data).hex()
                    details["wait_elapsed_s"] = time.time() - t0
                    if status == 2:
                        details["reason_code"] = "run_complete"
                        return True, details
                    if status == 0:
                        details["reason_code"] = "driver_status_fail"
                        return False, details
        except Exception as exc:
            details["reason_code"] = "can_exception"
            details["wait_elapsed_s"] = time.time() - t0
            details["exception"] = str(exc)
            return False, details

        details["reason_code"] = "unexpected_exit"
        details["wait_elapsed_s"] = time.time() - t0
        return False, details

    def send_joint_angle_abs_from_home_wait_detailed(
        self,
        joint: JointConfig,
        target_angle: float,
        home_encoder: int,
        speed_rpm: Optional[float] = None,
        acc: Optional[int] = None,
        clamp_limits: bool = False,
        timeout_s: float = 10.0,
    ) -> Tuple[bool, int, int, int, int, int, Dict[str, object]]:
        """
        Send F5 absolute axis position command using home-offset-based target calculation.
        This is more robust than current-encoder-based delta when encoder reads may be stale/corrupted.

        Returns: (ok, dir_ccw, motor_speed, motor_acc, pulses, target_axis, details)
        """
        if clamp_limits:
            target_angle = clamp(target_angle, joint.min_angle, joint.max_angle)
        if speed_rpm is None:
            speed_rpm = joint.speed_rpm
        if acc is None:
            acc = joint.acc
        motor_speed = self._joint_speed_to_motor_rpm(speed_rpm, joint.gear_ratio)
        motor_acc = self._joint_acc_to_motor_acc(acc, joint.gear_ratio)

        # Compute absolute axis target from home offset directly
        raw_target_axis = self.joint_angle_to_absolute_axis_from_home(joint, target_angle, home_encoder)
        target_axis = self._clamp_int24(raw_target_axis)

        # Compute motion in encoder space. For inverted joints, target_axis is command space
        # and the physical landing target is the negated encoder-side absolute position.
        encoder_target = self._f5_command_to_encoder_target(joint, target_axis)
        # Compute pulses for diagnostic/FD-compatibility (not used in F5 command)
        # Diagnostic pulse calc uses encoder_ticks_per_rev to match F5/encoder domain
        angle_diff = target_angle - joint.home_pos  # From home position
        motor_revs = (abs(angle_diff) / 360.0) * joint.gear_ratio
        pulses = int(motor_revs * self._config.encoder_ticks_per_rev)

        # Use the live encoder when deciding whether an absolute target move is required.
        import logging
        _diag_log = logging.getLogger("streaming")
        before_ticks = self.read_encoder(joint.id, timeout_s=0.2)
        delta_axis = encoder_target - (before_ticks if before_ticks is not None else home_encoder)
        axis_counts = abs(delta_axis)
        dir_ccw = 1 if delta_axis >= 0 else 0

        if axis_counts <= self.F5_NO_MOTION_TOLERANCE_COUNTS:
            details = {
                "reason_code": "no_motion_required",
                "wait_elapsed_s": 0.0,
                "resp_status": None,
                "resp_cmd": None,
                "resp_raw": None,
                "exception": None,
                "target_angle": target_angle,
                "current_angle": joint.home_pos,  # We don't know current angle reliably
                "target_axis": target_axis,
                "encoder_target": encoder_target,
                "target_axis_counts": target_axis,
                "raw_target_axis": raw_target_axis,
                "raw_target_axis_counts": raw_target_axis,
                "home_encoder": int(home_encoder),
                "home_encoder_counts": int(home_encoder),
                "fd_pulses": int(pulses),
                "axis_counts": int(axis_counts),
                "diag_before_ticks": before_ticks,
                "diag_after_ticks": before_ticks,
                "diag_expected_delta": int(delta_axis),
                "diag_actual_delta": 0,
                "timeout_s": timeout_s,
            }
            return True, dir_ccw, motor_speed, motor_acc, pulses, target_axis, details

        # --- Before/after encoder diagnostic for F5 invert analysis ---
        _diag_log.info(
            "[F5_BEFORE] J%d encoder_ticks=%s home=%d f5_target=%d invert=%s",
            joint.id, before_ticks, home_encoder, target_axis, joint.invert_dir,
        )

        ok, wait_details = self.send_position_abs_axis_wait_detailed(
            can_id=joint.id,
            speed_rpm=motor_speed,
            acc=motor_acc,
            abs_axis=target_axis,
            timeout_s=timeout_s,
        )

        after_ticks = self.read_encoder(joint.id, timeout_s=0.2)
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

        wait_details["target_angle"] = target_angle
        wait_details["current_angle"] = joint.home_pos  # We don't know current angle reliably
        wait_details["target_axis"] = target_axis
        wait_details["encoder_target"] = encoder_target
        wait_details["target_axis_counts"] = target_axis
        wait_details["raw_target_axis"] = raw_target_axis
        wait_details["raw_target_axis_counts"] = raw_target_axis
        wait_details["home_encoder"] = int(home_encoder)
        wait_details["home_encoder_counts"] = int(home_encoder)
        wait_details["delta_axis"] = int(delta_axis)
        wait_details["delta_axis_counts"] = int(delta_axis)
        wait_details["fd_pulses"] = int(pulses)
        wait_details["axis_counts"] = int(axis_counts)
        wait_details["diag_before_ticks"] = before_ticks
        wait_details["diag_after_ticks"] = after_ticks
        wait_details["diag_expected_delta"] = expected_delta if before_ticks is not None else None
        wait_details["diag_actual_delta"] = actual_delta
        return ok, dir_ccw, motor_speed, motor_acc, pulses, target_axis, wait_details
