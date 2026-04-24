import math
import threading
import json
import time
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog

import pybullet as p

from app.state import SharedState as AppSharedState
from app.logging_utils import get_stream_logger
from app.logging_utils import get_keyframe_logger
from app_paths import get_settings_path
from demos.orientation_sweep import SweepConfig, plan_orientation_sweep, probe_reachability
from gui.prototype_layout import Tooltip, build_main_layout
from gui.refresh import GuiRefreshController
from ik.adapter import IKSolution, solve_ik_for_pose, solve_ik_for_pose_single_seed
from ik.config import get_config
from ik.kinematics import build_pose_target, compute_cartesian_pose, get_fk, ik_solve, ik_solve_multiseed, pose_error
from ik.logging import get_logger
from keyframes.interpolation import (
    InterpolatedSegment,
    InterpolationPlanningCancelled,
    KeyframeInterpolationPlan,
    extend_plan_terminal_hold,
    plan_interpolated_keyframe_path,
)
from robot_comm import RobotCommClient
from sim.runtime import PyBulletDirectIK as SimPyBulletDirectIK
from sim.runtime import PYBULLET_WINDOW_WIDTH
from sim.runtime import run_simulation as run_simulation_thread
from streaming.controller import CanStreamController
from streaming.interpolation import StreamingInterpolator
from streaming.startup import CanStreamStartupHelper

MM_TO_M = 0.001
M_TO_MM = 1000.0
KEYFRAME_SIM_CHECK_MS = 100
KEYFRAME_SIM_SETTLE_TOL_DEG = 3.0
KEYFRAME_SIM_STABLE_SAMPLES = 2
KEYFRAME_SIM_MIN_TIMEOUT_S = 5.0
KEYFRAME_SIM_PROGRESS_EPS_DEG = 0.5
KEYFRAME_SIM_STALL_TIMEOUT_S = 4.0
KEYFRAME_SIM_CLOSE_ENOUGH_TOL_DEG = 5.0
KEYFRAME_ROBOT_CHECK_MS = 250
KEYFRAME_ROBOT_SETTLE_TOL_DEG = 1.0
KEYFRAME_ROBOT_STABLE_SAMPLES = 2
KEYFRAME_ROBOT_TIMEOUT_S = 20.0
KEYFRAME_ROBOT_PROGRESS_EPS_DEG = 0.5
KEYFRAME_ROBOT_STALL_TIMEOUT_S = 3.0
KEYFRAME_EXPORT_DIRNAME = "keyframes"
KEYFRAME_SIM_INTERP_MAX_RATE_HZ = 6.0
CAN_CONNECT_INIT_TIMEOUT_S = 5.0
CAN_CONNECT_LISTEN_TIMEOUT_S = 0.2
ROBOT_MOVE_COMPLETE_TIMEOUT_S = 15.0
MANUAL_JOG_MOVE_COMPLETE_TIMEOUT_S = 10.0
POST_MOTION_ENCODER_SETTLE_S = 0.2
STATUS_UPDATE_INTERVAL_S = 0.12
TERMINAL_FLUSH_INTERVAL_MS = 40
TERMINAL_MAX_LINES = 400
COMM_TASK_DEFAULT_TIMEOUT_S = 8.0
COMM_TASK_WATCHDOG_MS = 250
COMM_READ_TIMEOUT_S = 0.75
_MODULE_LOG = get_stream_logger("streaming")


@dataclass
class KeyframeRecord:
    source: str
    angles_deg: list[float]
    delay_s: float = 0.0


def load_streaming_settings():
    settings_path = get_settings_path("streaming_interpolation_settings.json")
    if not settings_path.exists():
        return {}
    try:
        return json.loads(settings_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _MODULE_LOG.warning("Failed to load streaming_interpolation_settings.json: %s", exc)
        return {}


def load_simulation_tuning_settings():
    settings_path = get_settings_path("simulation_tuning.json")
    if not settings_path.exists():
        return {}
    try:
        return json.loads(settings_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _MODULE_LOG.warning("Failed to load simulation_tuning.json: %s", exc)
        return {}


def get_keyframe_export_dir() -> Path:
    return Path(__file__).resolve().parents[1] / KEYFRAME_EXPORT_DIRNAME


def run_gui(shared_state, stop_event):
    ik_log = get_logger("ik_solution")
    comm_client = RobotCommClient()
    pybullet_ik = SimPyBulletDirectIK()
    stream_log = get_stream_logger("streaming")
    keyframe_log = get_keyframe_logger("keyframes")
    stream_settings = load_streaming_settings()
    root = tk.Tk()
    layout = build_main_layout(root, comm_client.default_port)
    root.update_idletasks()
    root_width = int(root.winfo_width())
    root_height = int(root.winfo_height())
    screen_width = int(root.winfo_screenwidth())
    screen_height = int(root.winfo_screenheight())
    root_x = min(max(0, PYBULLET_WINDOW_WIDTH), max(0, screen_width - root_width))
    root_y = max(0, (screen_height - root_height) // 2)
    root.geometry(f"{root_width}x{root_height}+{root_x}+{root_y}")

    header = layout.header
    terminal_panel = layout.terminal
    pose_panel = layout.pose_panel
    manual_jog_panel = layout.manual_jog_panel
    keyframes_panel = layout.keyframes_panel
    demo_panel = layout.demo_panel
    state_panel = layout.robot_state_panel
    robot_control_panel = layout.robot_control_panel
    status_bar = layout.status_bar

    x_var = pose_panel.x_var
    y_var = pose_panel.y_var
    z_var = pose_panel.z_var
    rx_var = pose_panel.rx_var
    ry_var = pose_panel.ry_var
    rz_var = pose_panel.rz_var
    only_pos_var = pose_panel.only_pos_var
    pose_pos_err_var = pose_panel.pos_err_var
    joint_entry_vars = pose_panel.joint_entry_vars
    home_target_btn = pose_panel.home_target_btn
    custom_ik_settings_btn = pose_panel.custom_ik_settings_btn
    custom_ik_go_sim_btn = pose_panel.custom_ik_go_sim_btn
    custom_ik_go_robot_btn = pose_panel.custom_ik_go_robot_btn
    custom_ik_joint_vars = pose_panel.custom_ik_joint_vars
    custom_ik_tcp_pos_vars = pose_panel.custom_ik_tcp_pos_vars
    custom_ik_tcp_rpy_vars = pose_panel.custom_ik_tcp_rpy_vars
    custom_ik_pos_err_var = pose_panel.custom_ik_pos_err_var
    custom_ik_ori_err_var = pose_panel.custom_ik_ori_err_var
    custom_ik_meta_var = pose_panel.custom_ik_meta_var
    pose_status_var = pose_panel.status_var

    terminal_text = terminal_panel.text
    terminal_command_var = terminal_panel.command_var
    terminal_command_entry = terminal_panel.command_entry
    terminal_send_btn = terminal_panel.send_btn

    comm_port_var = header.comm_port_var
    connect_btn = header.connect_btn
    read_btn = header.read_btn
    set_home_btn = header.set_home_btn
    home_btn = header.home_btn
    encoder_vars = state_panel.encoder_joint_vars
    encoder_tcp_pos_vars = state_panel.encoder_tcp_pos_vars
    encoder_tcp_rpy_vars = state_panel.encoder_tcp_rpy_vars
    encoder_status_var = robot_control_panel.status_var
    keyframe_sim_btn = robot_control_panel.keyframe_sim_btn
    keyframe_enc_btn = robot_control_panel.keyframe_enc_btn

    keyframe_tree = keyframes_panel.tree
    keyframes: list[KeyframeRecord] = []
    keyframe_delay_editor = {"widget": None, "index": None}
    keyframe_terminal_state = {"last": None}
    terminal_buffer: list[tuple[str, str]] = []
    terminal_flush_after_id = {"value": None}
    encoder_display_state = {"after_id": None, "latest": None}
    throttled_status_state: dict[str, dict] = {}
    fk_solver = get_fk()
    keyframe_playback = {
        "active": False,
        "mode": None,
        "after_id": None,
        "generation": 0,
        "execution_mode": None,
        "plan": None,
        "settings": None,
        "trajectory_index": 0,
        "segment_index": 0,
        "segment_end_indices": [],
        "plan_estimated_duration_s": 0.0,
        "started_mono": None,
        "stop_event": None,
        "comm_reserved": False,
        "plan_ready": False,
        "preroll_done": False,
        "step_direction": 1,
        "interpolation_direction": 1,
        "plan_cache": {},
    }
    demo_sweep = {
        "active": False,
        "generation": 0,
        "plan": None,
        "plan_cache": {},
        "plan_cache_key": None,
        "trajectory_index": 0,
        "mode": None,
        "timer_id": None,
        "cancel_event": None,
    }

    comm_busy = {"value": False}
    comm_task_state = {
        "next_id": 0,
        "active_id": None,
        "tasks": {},
        "watchdog_after_id": None,
    }
    interpolated_robot_monitor_state = {"final_read_pending": False}

    def _set_stringvar_if_changed(var: tk.StringVar, text: str):
        if var.get() != text:
            var.set(text)

    def _wrap_angle_deg(angle_deg: float) -> float:
        wrapped = (float(angle_deg) + 180.0) % 360.0 - 180.0
        if abs(wrapped) < 5e-4:
            return 0.0
        if abs(wrapped + 180.0) < 5e-4:
            return 180.0
        return wrapped

    def _quat_multiply(q1, q2):
        x1, y1, z1, w1 = q1
        x2, y2, z2, w2 = q2
        return (
            w1 * x2 + x1 * w2 + y1 * z2 - z1 * y2,
            w1 * y2 - x1 * z2 + y1 * w2 + z1 * x2,
            w1 * z2 + x1 * y2 - y1 * x2 + z1 * w2,
            w1 * w2 - x1 * x2 - y1 * y2 - z1 * z2,
        )

    def _stabilize_display_rpy(rx_deg: float, ry_deg: float, rz_deg: float) -> tuple[float, float, float]:
        """
        Canonicalize ZYX Euler output near gimbal lock for stable UI display.

        At |Ry| ~= 90 deg, many equivalent (Rx, Ry, Rz) triplets represent the same
        orientation. Small encoder jitter can therefore flip the displayed values
        between branches such as (0, -90, 0) and (-90, -89.9998, 90). For display,
        collapse the singular family to a consistent form with Rz = 0.
        """
        rx = float(rx_deg)
        ry = float(ry_deg)
        rz = float(rz_deg)
        if abs(abs(ry) - 90.0) <= 0.05:
            if ry < 0.0:
                rx = _wrap_angle_deg(rx + rz)
                ry = -90.0
            else:
                rx = _wrap_angle_deg(rx - rz)
                ry = 90.0
            rz = 0.0
        else:
            rx = _wrap_angle_deg(rx)
            ry = _wrap_angle_deg(ry)
            rz = _wrap_angle_deg(rz)
        return (rx, ry, rz)

    demo_terminal_state = {"last": None}

    def set_demo_status(text: str, echo_terminal: bool = True):
        text = str(text)
        _set_stringvar_if_changed(demo_panel.status_var, "Demo: see Terminal")
        if not echo_terminal:
            return
        if demo_terminal_state.get("last") == text:
            return
        demo_terminal_state["last"] = text
        lower_text = text.lower()
        tag = "output"
        if any(
            token in lower_text
            for token in (
                "error",
                "failed",
                "invalid",
                "unavailable",
                "incomplete",
                "not implemented",
                "cancelled",
            )
        ):
            tag = "error"
        elif any(
            token in lower_text
            for token in (
                "planning",
                "probing",
                "captured",
                "complete",
                "active",
                "stop requested",
            )
        ):
            tag = "dim"
        append_terminal_line(text, tag)

    def _schedule_status_update(key: str, var: tk.StringVar, text: str, min_interval_s: float = STATUS_UPDATE_INTERVAL_S):
        state = throttled_status_state.setdefault(
            key,
            {"after_id": None, "latest": None, "current": None, "last_applied_mono": 0.0},
        )
        text = str(text)
        state["latest"] = text
        if state["current"] == text and state["after_id"] is None:
            return
        if state["after_id"] is not None:
            return
        elapsed_s = time.monotonic() - float(state["last_applied_mono"] or 0.0)
        delay_ms = 0
        if min_interval_s > 0.0:
            delay_ms = max(0, int(round(max(0.0, float(min_interval_s) - elapsed_s) * 1000.0)))

        def apply_update():
            state["after_id"] = None
            latest = str(state.get("latest") or "")
            _set_stringvar_if_changed(var, latest)
            state["current"] = latest
            state["last_applied_mono"] = time.monotonic()

        state["after_id"] = root.after(delay_ms, apply_update)

    def set_keyframe_status(text: str, echo_terminal: bool = True):
        text = str(text)
        if not echo_terminal:
            return
        if keyframe_terminal_state.get("last") == text:
            return
        keyframe_terminal_state["last"] = text
        lower_text = text.lower()
        tag = "output"
        if any(
            token in lower_text
            for token in (
                "error",
                "failed",
                "invalid",
                "timeout",
                "requires",
                "unavailable",
                "incomplete",
                "stopped",
            )
        ):
            tag = "error"
        elif any(
            token in lower_text
            for token in (
                "planning",
                "planned",
                "complete",
                "added",
                "loaded",
                "saved",
                "moved",
                "cleared",
                "removed",
                "stop requested",
                "interp",
            )
        ):
            tag = "dim"
        append_terminal_line(text, tag)

    def get_selected_keyframe_index() -> int | None:
        selection = keyframe_tree.selection()
        if not selection:
            return None
        try:
            return int(selection[0])
        except (TypeError, ValueError):
            return None

    def select_keyframe(index: int | None):
        if index is None or index < 0 or index >= len(keyframes):
            keyframe_tree.selection_remove(keyframe_tree.selection())
            return
        iid = str(index)
        if keyframe_tree.exists(iid):
            keyframe_tree.selection_set(iid)
            keyframe_tree.focus(iid)
            keyframe_tree.see(iid)

    def format_keyframe_delay(delay_s: float) -> str:
        value = max(0.0, float(delay_s))
        text = f"{value:.3f}".rstrip("0").rstrip(".")
        return text or "0"

    def get_keyframe_delay_seconds(index: int) -> float:
        if index < 0 or index >= len(keyframes):
            return 0.0
        value = parse_float_entry(getattr(keyframes[index], "delay_s", 0.0))
        if value is None or value <= 0.0:
            return 0.0
        return float(value)

    def close_keyframe_delay_editor(commit: bool = False):
        editor = keyframe_delay_editor.get("widget")
        index = keyframe_delay_editor.get("index")
        if editor is None:
            return
        raw_value = editor.get()
        try:
            editor.destroy()
        except Exception:
            pass
        keyframe_delay_editor["widget"] = None
        keyframe_delay_editor["index"] = None
        if not commit or index is None or index < 0 or index >= len(keyframes):
            return
        delay_s = parse_float_entry(str(raw_value).strip())
        if delay_s is None or delay_s < 0.0:
            set_keyframe_status("Keyframes: delay must be a non-negative number of seconds")
            refresh_keyframe_tree(select_index=index)
            return
        keyframes[index].delay_s = float(delay_s)
        refresh_keyframe_tree(select_index=index)
        set_keyframe_status(
            f"Keyframes: frame {index + 1} delay set to {format_keyframe_delay(float(delay_s))} s"
        )

    def open_keyframe_delay_editor(index: int):
        if index < 0 or index >= len(keyframes):
            return
        if keyframe_playback["active"]:
            set_keyframe_status("Keyframes: stop playback before editing delays")
            return
        iid = str(index)
        if not keyframe_tree.exists(iid):
            return
        bbox = keyframe_tree.bbox(iid, "delay")
        if not bbox:
            return
        close_keyframe_delay_editor(commit=False)
        x, y, width, height = bbox
        editor = tk.Entry(keyframe_tree, justify="center")
        editor.insert(0, format_keyframe_delay(get_keyframe_delay_seconds(index)))
        editor.place(x=x + 1, y=y + 1, width=width - 2, height=height - 2)
        editor.focus_set()
        editor.selection_range(0, "end")
        editor.icursor("end")
        editor.bind("<Return>", lambda _event: (close_keyframe_delay_editor(commit=True), "break")[1])
        editor.bind("<Escape>", lambda _event: (close_keyframe_delay_editor(commit=False), "break")[1])
        editor.bind("<FocusOut>", lambda _event: close_keyframe_delay_editor(commit=True))
        keyframe_delay_editor["widget"] = editor
        keyframe_delay_editor["index"] = index

    def on_keyframe_tree_double_click(event):
        item_id = keyframe_tree.identify_row(event.y)
        column_id = keyframe_tree.identify_column(event.x)
        if column_id != "#8" or not item_id:
            return
        try:
            index = int(item_id)
        except (TypeError, ValueError):
            return
        select_keyframe(index)
        open_keyframe_delay_editor(index)
        return "break"

    def refresh_keyframe_tree(select_index: int | None = None):
        close_keyframe_delay_editor(commit=False)
        current_selection = get_selected_keyframe_index() if select_index is None else select_index
        for iid in keyframe_tree.get_children():
            keyframe_tree.delete(iid)
        for index, record in enumerate(keyframes, start=1):
            tag = "odd" if index % 2 == 1 else "even"
            angles = [f"{float(value):+.1f}" for value in record.angles_deg[:6]]
            delay_value = format_keyframe_delay(getattr(record, "delay_s", 0.0))
            keyframe_tree.insert("", "end", iid=str(index - 1), values=(index, *angles, delay_value), tags=(tag,))
        with shared_state.lock:
            shared_state.keyframe_marker_joint_sets_deg = [list(record.angles_deg[:6]) for record in keyframes]
            shared_state.keyframe_markers_visible = bool(keyframes_panel.show_markers_var.get())
            shared_state.keyframe_trace_visible = bool(keyframes_panel.trace_markers_var.get())
            shared_state.keyframe_markers_dirty = True
        if current_selection is not None and 0 <= current_selection < len(keyframes):
            select_keyframe(current_selection)
        else:
            keyframe_tree.selection_remove(keyframe_tree.selection())

    def refresh_keyframe_controls():
        is_playing = bool(keyframe_playback["active"])
        has_keyframes = bool(keyframes)
        has_selection = get_selected_keyframe_index() is not None

        keyframes_panel.remove_selected_btn.configure(
            state="normal" if has_keyframes and has_selection and not is_playing else "disabled"
        )
        keyframes_panel.remove_last_btn.configure(
            state="normal" if has_keyframes and not is_playing else "disabled"
        )
        keyframes_panel.clear_all_btn.configure(
            state="normal" if has_keyframes and not is_playing else "disabled"
        )
        keyframes_panel.move_up_btn.configure(
            state="normal" if has_keyframes and has_selection and not is_playing else "disabled"
        )
        keyframes_panel.move_down_btn.configure(
            state="normal" if has_keyframes and has_selection and not is_playing else "disabled"
        )
        keyframes_panel.save_btn.configure(
            state="normal" if has_keyframes and not is_playing else "disabled"
        )
        keyframes_panel.load_btn.configure(
            state="normal" if not is_playing else "disabled"
        )
        keyframes_panel.play_sim_btn.configure(
            state="normal" if has_keyframes and not is_playing else "disabled"
        )
        keyframes_panel.play_robot_btn.configure(
            state="normal" if has_keyframes and not is_playing and not comm_busy["value"] else "disabled"
        )
        keyframes_panel.move_sim_selected_btn.configure(
            state="normal" if has_keyframes and has_selection and not is_playing else "disabled"
        )
        keyframes_panel.move_robot_selected_btn.configure(
            state="normal" if has_keyframes and has_selection and not is_playing and not comm_busy["value"] else "disabled"
        )
        keyframes_panel.stop_btn.configure(state="normal" if is_playing else "disabled")
        keyframe_sim_btn.configure(state="normal" if not is_playing else "disabled")
        keyframe_enc_btn.configure(
            state="normal" if not is_playing and not comm_busy["value"] else "disabled"
        )

    def refresh_comm_controls():
        robot_keyframe_active = bool(keyframe_playback["active"]) and keyframe_playback.get("mode") == "robot"
        state = "disabled" if comm_busy["value"] or robot_keyframe_active else "normal"
        read_btn.configure(state=state)
        set_home_btn.configure(state=state)
        home_btn.configure(state=state)
        pose_panel.go_robot_btn.configure(state=state)
        manual_jog_state = "disabled" if comm_busy["value"] else "normal"
        manual_jog_panel.joint_step_entry.configure(state=manual_jog_state)
        manual_jog_panel.cart_step_entry.configure(state=manual_jog_state)
        manual_jog_panel.sim_btn.configure(state=manual_jog_state)
        manual_jog_panel.robot_btn.configure(state=manual_jog_state)
        for minus_btn, plus_btn in manual_jog_panel.joint_jog_buttons.values():
            minus_btn.configure(state=manual_jog_state)
            plus_btn.configure(state=manual_jog_state)
        for minus_btn, plus_btn in manual_jog_panel.cart_jog_buttons.values():
            minus_btn.configure(state=manual_jog_state)
            plus_btn.configure(state=manual_jog_state)
        if comm_busy["value"]:
            if not manual_jog_panel.busy_badge.winfo_ismapped():
                manual_jog_panel.busy_badge.grid()
            if not manual_jog_panel.busy_progress.winfo_ismapped():
                manual_jog_panel.busy_progress.grid()
                manual_jog_panel.busy_progress.start(12)
        else:
            manual_jog_panel.busy_progress.stop()
            if manual_jog_panel.busy_badge.winfo_ismapped():
                manual_jog_panel.busy_badge.grid_remove()
            if manual_jog_panel.busy_progress.winfo_ismapped():
                manual_jog_panel.busy_progress.grid_remove()

    def stop_keyframe_playback(status_text: str | None = None):
        keyframe_log.info(
            "Keyframe playback stop requested generation=%d mode=%s execution_mode=%s status=%s",
            int(keyframe_playback.get("generation") or 0),
            keyframe_playback.get("mode"),
            keyframe_playback.get("execution_mode"),
            status_text,
        )
        current_mode = keyframe_playback.get("mode")
        current_execution_mode = keyframe_playback.get("execution_mode")
        if current_mode == "robot":
            cancel_active_comm_task(
                "robot playback stopping",
                status_text="Encoders: recovering from cancelled robot task...",
            )
        if current_mode == "robot" and current_execution_mode == "interpolated":
            status_lower = str(status_text or "").lower()
            if "stop requested" in status_lower or "stopped" in status_lower:
                can_controller.mark_manual_stop()
            can_streamer.stop()
        after_id = keyframe_playback.get("after_id")
        if after_id is not None:
            try:
                root.after_cancel(after_id)
            except Exception:
                pass
        stop_event = keyframe_playback.get("stop_event")
        if stop_event is not None:
            stop_event.set()
        keyframe_playback["after_id"] = None
        keyframe_playback["active"] = False
        keyframe_playback["mode"] = None
        keyframe_playback["execution_mode"] = None
        keyframe_playback["plan"] = None
        keyframe_playback["settings"] = None
        keyframe_playback["trajectory_index"] = 0
        keyframe_playback["segment_index"] = 0
        keyframe_playback["segment_end_indices"] = []
        keyframe_playback["plan_estimated_duration_s"] = 0.0
        keyframe_playback["started_mono"] = None
        keyframe_playback["stop_event"] = None
        keyframe_playback["plan_ready"] = False
        keyframe_playback["preroll_done"] = False
        keyframe_playback["step_direction"] = 1
        keyframe_playback["interpolation_direction"] = 1
        if keyframe_playback.get("comm_reserved"):
            keyframe_playback["comm_reserved"] = False
            set_comm_busy(False)
        with shared_state.lock:
            shared_state.streaming_active = False
            shared_state.hardware_streaming_active = False
        keyframe_playback["generation"] = int(keyframe_playback["generation"]) + 1
        if status_text is not None:
            set_keyframe_status(status_text)
        refresh_comm_controls()
        refresh_keyframe_controls()
        if status_text == "Keyframes: robot playback complete":
            queue_post_motion_encoder_refresh(
                "Keyframes: robot playback complete",
                busy_status="Keyframes: refreshing encoders...",
                error_prefix="Keyframes: robot playback complete, encoder refresh failed",
            )

    def set_comm_busy(is_busy: bool, status_text: str | None = None):
        comm_busy["value"] = is_busy
        refresh_comm_controls()
        refresh_keyframe_controls()
        if status_text is not None:
            encoder_status_var.set(status_text)

    def _schedule_comm_task_watchdog():
        if comm_task_state.get("watchdog_after_id") is not None:
            return

        def tick():
            comm_task_state["watchdog_after_id"] = None
            active_id = comm_task_state.get("active_id")
            if active_id is None:
                return
            task_info = comm_task_state["tasks"].get(active_id)
            if not task_info:
                comm_task_state["active_id"] = None
                set_comm_busy(False)
                return
            if task_info.get("released"):
                return
            timeout_s = task_info.get("timeout_s")
            elapsed_s = time.monotonic() - float(task_info.get("started_mono") or 0.0)
            if timeout_s is not None and elapsed_s >= float(timeout_s):
                task_info["released"] = True
                task_info["timed_out"] = True
                if comm_task_state.get("active_id") == active_id:
                    comm_task_state["active_id"] = None
                    set_comm_busy(False)
                stream_log.warning(
                    "Comm task timeout id=%s name=%s elapsed_s=%.3f timeout_s=%.3f",
                    active_id,
                    task_info.get("name"),
                    elapsed_s,
                    float(timeout_s),
                )
                on_timeout = task_info.get("on_timeout")
                if callable(on_timeout):
                    root.after(0, lambda cb=on_timeout: cb())
                return
            comm_task_state["watchdog_after_id"] = root.after(COMM_TASK_WATCHDOG_MS, tick)

        comm_task_state["watchdog_after_id"] = root.after(COMM_TASK_WATCHDOG_MS, tick)

    def cancel_active_comm_task(reason: str, status_text: str | None = None) -> bool:
        active_id = comm_task_state.get("active_id")
        if active_id is None:
            return False
        task_info = comm_task_state["tasks"].get(active_id)
        if task_info is None or task_info.get("released"):
            return False
        task_info["released"] = True
        task_info["cancelled"] = True
        task_info["cancel_reason"] = str(reason)
        comm_task_state["active_id"] = None
        set_comm_busy(False)
        stream_log.warning(
            "Comm task cancelled id=%s name=%s reason=%s",
            active_id,
            task_info.get("name"),
            reason,
        )
        if status_text is not None:
            set_encoder_status(status_text)
        return True

    def run_background_comm_task(task, on_done=None, task_name: str = "background comm task", ui_callback: bool = True):
        def worker():
            try:
                result = task()
                err = None
            except Exception as exc:
                result = None
                err = str(exc)

            if on_done is None:
                if err:
                    stream_log.warning("Background comm task failed name=%s error=%s", task_name, err)
                return

            def finish():
                on_done(result, err)

            if ui_callback:
                root.after(0, finish)
            else:
                try:
                    finish()
                except Exception:
                    stream_log.exception("Background comm callback failed name=%s", task_name)

        threading.Thread(target=worker, daemon=True).start()

    def run_comm_task(
        task,
        on_done,
        busy_status: str = "Encoders: working...",
        *,
        task_name: str | None = None,
        timeout_s: float | None = COMM_TASK_DEFAULT_TIMEOUT_S,
        on_timeout=None,
    ):
        if comm_busy["value"]:
            stream_log.info("Comm task ignored while busy name=%s", task_name or busy_status)
            return False

        for stale_task_id, stale_task in list(comm_task_state["tasks"].items()):
            if stale_task.get("released"):
                comm_task_state["tasks"].pop(stale_task_id, None)

        comm_task_state["next_id"] = int(comm_task_state.get("next_id") or 0) + 1
        task_id = int(comm_task_state["next_id"])
        task_label = str(task_name or busy_status or f"comm_task_{task_id}")
        comm_task_state["active_id"] = task_id
        comm_task_state["tasks"][task_id] = {
            "id": task_id,
            "name": task_label,
            "started_mono": time.monotonic(),
            "timeout_s": None if timeout_s is None else float(timeout_s),
            "released": False,
            "timed_out": False,
            "cancelled": False,
            "on_timeout": on_timeout,
        }
        set_comm_busy(True, busy_status)
        stream_log.info(
            "Comm task start id=%d name=%s timeout_s=%s",
            task_id,
            task_label,
            "none" if timeout_s is None else f"{float(timeout_s):.3f}",
        )
        _schedule_comm_task_watchdog()

        def worker():
            try:
                result = task()
                err = None
            except Exception as exc:
                result = None
                err = str(exc)

            def finish():
                task_info = comm_task_state["tasks"].get(task_id)
                if task_info is None:
                    return
                if task_info.get("released"):
                    stream_log.warning(
                        "Comm task late completion ignored id=%d name=%s cancelled=%s timed_out=%s",
                        task_id,
                        task_label,
                        bool(task_info.get("cancelled")),
                        bool(task_info.get("timed_out")),
                    )
                    comm_task_state["tasks"].pop(task_id, None)
                    return
                task_info["released"] = True
                if comm_task_state.get("active_id") == task_id:
                    comm_task_state["active_id"] = None
                    set_comm_busy(False)
                stream_log.info(
                    "Comm task finish id=%d name=%s err=%s",
                    task_id,
                    task_label,
                    "none" if err is None else err,
                )
                on_done(result, err)
                comm_task_state["tasks"].pop(task_id, None)

            root.after(0, finish)

        threading.Thread(target=worker, daemon=True).start()
        return True

    def update_comm_status():
        if comm_client.is_connected:
            header.can_status_var.set("CAN: connected")
            connect_btn.configure(text="Disconnect")
            header.can_dot_label.configure(fg="#2e7d32")
        else:
            header.can_status_var.set("CAN: disconnected")
            connect_btn.configure(text="Connect")
            header.can_dot_label.configure(fg="#b71c1c")

    def set_pose_status(text: str):
        _schedule_status_update("pose_status", pose_status_var, text)

    def set_jog_status(text: str):
        _schedule_status_update("jog_status", manual_jog_panel.status_var, text)

    def set_encoder_status(text: str):
        _schedule_status_update("encoder_status", encoder_status_var, text)

    def append_keyframe(angles_deg: list[float], source: str):
        keyframes.append(
            KeyframeRecord(
                source=source,
                angles_deg=[float(value) for value in angles_deg[:6]],
                delay_s=0.0,
            )
        )
        select_index = len(keyframes) - 1
        refresh_keyframe_tree(select_index=select_index)
        refresh_keyframe_controls()
        set_keyframe_status(f"Keyframes: added {source} frame #{len(keyframes)}")

    def replace_keyframes(records: list[KeyframeRecord]):
        keyframes.clear()
        keyframes.extend(records)
        refresh_keyframe_tree(select_index=0 if keyframes else None)
        refresh_keyframe_controls()

    def build_keyframe_export_payload() -> dict:
        now_local = time.localtime()
        return {
            "format": "robot_arm_keyframes",
            "version": 2,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", now_local),
            "count": len(keyframes),
            "keyframes": [
                {
                    "index": idx + 1,
                    "source": record.source,
                    "angles_deg": [float(value) for value in record.angles_deg[:6]],
                    "delay_s": float(getattr(record, "delay_s", 0.0)),
                }
                for idx, record in enumerate(keyframes)
            ],
        }

    def parse_keyframe_payload(payload: object) -> list[KeyframeRecord]:
        items = payload.get("keyframes") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise ValueError("Missing 'keyframes' list.")
        records: list[KeyframeRecord] = []
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("Each keyframe entry must be an object.")
            angles = item.get("angles_deg")
            if not isinstance(angles, list) or len(angles) < 6:
                raise ValueError("Each keyframe must include 6 joint angles.")
            try:
                parsed_angles = [float(value) for value in angles[:6]]
            except (TypeError, ValueError) as exc:
                raise ValueError("Keyframe joint angles must be numeric.") from exc
            source = str(item.get("source") or "loaded")
            delay_s = parse_float_entry(item.get("delay_s"))
            if delay_s is None:
                delay_s = 0.0
            if delay_s < 0.0:
                raise ValueError("Keyframe delay must be non-negative.")
            records.append(KeyframeRecord(source=source, angles_deg=parsed_angles, delay_s=float(delay_s)))
        return records

    def continue_keyframe_step_after_delay(generation: int, mode: str, index: int, on_done):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        delay_s = get_keyframe_delay_seconds(index)
        if delay_s <= 0.0:
            on_done()
            return
        total = len(keyframes)
        delay_text = format_keyframe_delay(delay_s)
        status_text = f"Keyframes: {mode} delay after frame {index + 1}/{total} ({delay_text} s)"
        set_keyframe_status(status_text)
        if mode == "robot":
            set_encoder_status(status_text)
        keyframe_log.info(
            "Step %s delay generation=%d frame=%d/%d delay_s=%.3f",
            mode,
            generation,
            index + 1,
            total,
            delay_s,
        )

        def resume():
            if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                return
            on_done()

        keyframe_playback["after_id"] = root.after(max(1, int(round(delay_s * 1000.0))), resume)

    def parse_displayed_encoder_angles() -> list[float] | None:
        angles = []
        for var in encoder_vars[:6]:
            value = parse_float_entry(var.get())
            if value is None:
                return None
            angles.append(float(value))
        return angles if len(angles) == 6 else None

    def get_sim_joint_angles() -> list[float] | None:
        with shared_state.lock:
            joint_deg = list(shared_state.joint_deg)
        if len(joint_deg) < 6:
            return None
        return [float(value) for value in joint_deg[:6]]

    def apply_sim_keyframe(angles_deg: list[float], motion_overrides: dict | None = None):
        with shared_state.lock:
            shared_state.manual_joint_target_deg = list(angles_deg[:6])
            shared_state.manual_joint_pending = True
            shared_state.manual_joint_motion_overrides = dict(motion_overrides or {})
            shared_state.ik_enabled = False

    def get_keyframe_start_index(mode: str) -> int:
        if mode == "sim":
            return 0
        selected_index = get_selected_keyframe_index()
        if selected_index is not None and 0 <= selected_index < len(keyframes):
            return selected_index
        return 0

    def resolve_keyframe_sim_motion(current_angles: list[float], target_angles: list[float]) -> dict | None:
        percentages = resolve_keyframe_ui_percentages()
        if percentages is None:
            return None
        speed_pct, accel_pct = percentages

        tuning = load_simulation_tuning_settings()
        motor_tuning = tuning.get("joint_motor_control", {})
        base_max_velocity = parse_float_entry(motor_tuning.get("maxVelocity_rad_s"))
        base_position_gain = parse_float_entry(motor_tuning.get("positionGain"))
        base_velocity_gain = parse_float_entry(motor_tuning.get("velocityGain"))

        base_max_velocity = base_max_velocity if base_max_velocity and base_max_velocity > 0.0 else 5.0
        base_position_gain = base_position_gain if base_position_gain and base_position_gain > 0.0 else 0.8
        base_velocity_gain = base_velocity_gain if base_velocity_gain and base_velocity_gain > 0.0 else 1.5

        speed_scale = max(0.01, min(1.0, speed_pct / 100.0))
        accel_scale = max(0.01, min(1.0, accel_pct / 100.0))
        max_velocity_rad_s = max(0.05, min(20.0, base_max_velocity * speed_scale))
        position_gain = max(0.05, min(1.0, base_position_gain * math.sqrt(accel_scale)))
        velocity_gain = max(0.05, min(10.0, base_velocity_gain * accel_scale))

        padded_current = list(current_angles[:6]) + [0.0] * max(0, 6 - len(current_angles))
        padded_target = list(target_angles[:6]) + [0.0] * max(0, 6 - len(target_angles))
        max_delta_deg = max(abs(float(tgt) - float(cur)) for cur, tgt in zip(padded_current[:6], padded_target[:6]))
        max_speed_deg_s = math.degrees(max_velocity_rad_s)
        accel_deg_s2 = max(30.0, max_speed_deg_s * accel_scale * 0.75)
        estimated_move_s = 0.0
        if max_delta_deg > 0.0:
            t_accel = max_speed_deg_s / accel_deg_s2
            dist_accel = 0.5 * accel_deg_s2 * t_accel * t_accel
            if max_delta_deg <= 2.0 * dist_accel:
                estimated_move_s = 2.0 * math.sqrt(max_delta_deg / accel_deg_s2)
            else:
                estimated_move_s = 2.0 * t_accel + (max_delta_deg - 2.0 * dist_accel) / max_speed_deg_s
        timeout_s = max(
            KEYFRAME_SIM_MIN_TIMEOUT_S,
            (estimated_move_s * 4.0) + 2.0 + (0.5 / math.sqrt(accel_scale)),
        )

        return {
            "speed_pct": float(speed_pct),
            "accel_pct": float(accel_pct),
            "max_delta_deg": float(max_delta_deg),
            "timeout_s": float(timeout_s),
            "overrides": {
                "maxVelocity": float(max_velocity_rad_s),
                "positionGain": float(position_gain),
                "velocityGain": float(velocity_gain),
            },
        }

    def set_keyframe_streaming_active(active: bool):
        with shared_state.lock:
            shared_state.streaming_active = bool(active)

    def set_keyframe_hardware_streaming_active(active: bool):
        with shared_state.lock:
            shared_state.hardware_streaming_active = bool(active)

    def resolve_keyframe_interpolation_settings(mode: str) -> dict | None:
        percentages = resolve_keyframe_ui_percentages()
        if percentages is None:
            return None
        speed_pct, accel_pct = percentages
        settings = load_streaming_settings()
        motion = settings.get("streaming_motion", {})
        keyframe_interp = settings.get("keyframe_interpolation", {})
        requested_update_rate_hz = parse_float_entry(motion.get("update_rate_hz"))
        if requested_update_rate_hz is None or requested_update_rate_hz <= 0.0:
            requested_update_rate_hz = 20.0
        update_rate_hz = float(requested_update_rate_hz)
        if mode == "sim":
            update_rate_hz = min(update_rate_hz, KEYFRAME_SIM_INTERP_MAX_RATE_HZ)
        ik_mode = str(keyframe_interp.get("ik_mode") or "default").strip().lower()
        if ik_mode == "single_seed_feedback":
            ik_mode = "single_seed_fallback"
        sampling_mode = str(keyframe_interp.get("sampling_mode") or "update_rate").strip().lower()
        planner_kind = str(keyframe_interp.get("planner_kind") or "baseline").strip().lower()
        position_tolerance_mm = parse_float_entry(keyframe_interp.get("position_tolerance_mm"))
        orientation_tolerance_deg = parse_float_entry(keyframe_interp.get("orientation_tolerance_deg"))
        max_joint_step_deg = parse_float_entry(keyframe_interp.get("max_joint_step_deg"))
        speed_scale = max(0.01, min(1.0, speed_pct / 100.0))
        accel_scale = max(0.01, min(1.0, accel_pct / 100.0))
        sim_speed_ref_mm_s = parse_float_entry(motion.get("tcp_speed_mm_s"))
        if sim_speed_ref_mm_s is None or sim_speed_ref_mm_s <= 0.0:
            sim_speed_ref_mm_s = 40.0
        sim_accel_ref_mm_s2 = parse_float_entry(motion.get("tcp_acc_mm_s2"))
        if sim_accel_ref_mm_s2 is None or sim_accel_ref_mm_s2 <= 0.0:
            sim_accel_ref_mm_s2 = 80.0
        robot_speed_ref_mm_s = parse_float_entry(keyframe_interp.get("robot_playback_speed_mm_s"))
        if robot_speed_ref_mm_s is None or robot_speed_ref_mm_s <= 0.0:
            robot_speed_ref_mm_s = float(sim_speed_ref_mm_s)
        robot_accel_ref_mm_s2 = parse_float_entry(keyframe_interp.get("robot_playback_accel_mm_s2"))
        if robot_accel_ref_mm_s2 is None or robot_accel_ref_mm_s2 <= 0.0:
            robot_accel_ref_mm_s2 = float(sim_accel_ref_mm_s2)
        sim_playback_speed_mm_s = float(sim_speed_ref_mm_s) * speed_scale
        sim_playback_accel_mm_s2 = float(sim_accel_ref_mm_s2) * accel_scale
        robot_playback_speed_mm_s = float(robot_speed_ref_mm_s) * speed_scale
        robot_playback_accel_mm_s2 = float(robot_accel_ref_mm_s2) * accel_scale
        robot_playback_update_rate_hz = parse_float_entry(keyframe_interp.get("robot_playback_update_rate_hz"))
        if robot_playback_update_rate_hz is None or robot_playback_update_rate_hz <= 0.0:
            robot_playback_update_rate_hz = float(update_rate_hz)
        terminal_hold_s = parse_float_entry(keyframe_interp.get("terminal_hold_s"))
        pause_simulation_during_robot_playback = parse_bool_entry(
            keyframe_interp.get("pause_simulation_during_robot_playback"),
            default=True,
        )
        return {
            "speed_pct": float(speed_pct),
            "accel_pct": float(accel_pct),
            "sim_speed_ref_mm_s": float(sim_speed_ref_mm_s),
            "sim_accel_ref_mm_s2": float(sim_accel_ref_mm_s2),
            "robot_speed_ref_mm_s": float(robot_speed_ref_mm_s),
            "robot_accel_ref_mm_s2": float(robot_accel_ref_mm_s2),
            "speed_mm_s": float(sim_playback_speed_mm_s),
            "accel_mm_s2": float(sim_playback_accel_mm_s2),
            "update_rate_hz": float(update_rate_hz),
            "requested_update_rate_hz": float(requested_update_rate_hz),
            "ik_mode": ik_mode,
            "sampling_mode": sampling_mode,
            "planner_kind": planner_kind,
            "position_tolerance_mm": float(position_tolerance_mm or 0.0),
            "orientation_tolerance_deg": float(orientation_tolerance_deg or 0.0),
            "max_joint_step_deg": float(max_joint_step_deg or 10.0),
            "robot_playback_speed_mm_s": float(robot_playback_speed_mm_s),
            "robot_playback_accel_mm_s2": float(robot_playback_accel_mm_s2),
            "robot_playback_update_rate_hz": float(robot_playback_update_rate_hz),
            "terminal_hold_s": max(0.0, float(terminal_hold_s or 0.0)),
            "pause_simulation_during_robot_playback": bool(pause_simulation_during_robot_playback),
            "settings": settings,
        }

    def resolve_keyframe_step_playback_settings() -> dict:
        settings = load_streaming_settings()
        step_cfg = settings.get("keyframe_step_playback", {})
        sim_handoff_error_deg = parse_float_entry(step_cfg.get("sim_handoff_error_deg"))
        robot_handoff_error_deg = parse_float_entry(step_cfg.get("robot_handoff_error_deg"))
        min_handoff_samples = step_cfg.get("min_handoff_samples")
        try:
            min_handoff_samples = int(min_handoff_samples)
        except (TypeError, ValueError):
            min_handoff_samples = 1
        min_handoff_samples = max(1, min_handoff_samples)
        require_improving_error = parse_bool_entry(step_cfg.get("require_improving_error"), default=True)
        progress_epsilon_deg = parse_float_entry(step_cfg.get("progress_epsilon_deg"))
        stall_retry_enabled = parse_bool_entry(step_cfg.get("stall_retry_enabled"), default=True)
        stall_retry_poll_threshold = step_cfg.get("stall_retry_poll_threshold")
        try:
            stall_retry_poll_threshold = int(stall_retry_poll_threshold)
        except (TypeError, ValueError):
            stall_retry_poll_threshold = 2
        stall_retry_poll_threshold = max(1, stall_retry_poll_threshold)
        stall_retry_max_attempts = step_cfg.get("stall_retry_max_attempts")
        try:
            stall_retry_max_attempts = int(stall_retry_max_attempts)
        except (TypeError, ValueError):
            stall_retry_max_attempts = 1
        stall_retry_max_attempts = max(0, stall_retry_max_attempts)
        stall_retry_min_command_delta_deg = parse_float_entry(step_cfg.get("stall_retry_min_command_delta_deg"))
        stall_retry_min_error_deg = parse_float_entry(step_cfg.get("stall_retry_min_error_deg"))
        return {
            "sim_handoff_error_deg": float(sim_handoff_error_deg if sim_handoff_error_deg and sim_handoff_error_deg > 0.0 else 5.0),
            "robot_handoff_error_deg": float(
                robot_handoff_error_deg if robot_handoff_error_deg and robot_handoff_error_deg > 0.0 else 2.0
            ),
            "min_handoff_samples": int(min_handoff_samples),
            "require_improving_error": bool(require_improving_error),
            "progress_epsilon_deg": float(progress_epsilon_deg if progress_epsilon_deg and progress_epsilon_deg > 0.0 else 0.2),
            "stall_retry_enabled": bool(stall_retry_enabled),
            "stall_retry_poll_threshold": int(stall_retry_poll_threshold),
            "stall_retry_max_attempts": int(stall_retry_max_attempts),
            "stall_retry_min_command_delta_deg": float(
                stall_retry_min_command_delta_deg
                if stall_retry_min_command_delta_deg and stall_retry_min_command_delta_deg > 0.0
                else 0.5
            ),
            "stall_retry_min_error_deg": float(
                stall_retry_min_error_deg if stall_retry_min_error_deg and stall_retry_min_error_deg > 0.0 else 0.5
            ),
        }

    def build_keyframe_robot_playback_settings(settings: dict) -> dict:
        playback_settings = dict(settings)
        playback_settings["speed_mm_s"] = float(
            settings.get("robot_playback_speed_mm_s") or settings.get("speed_mm_s") or 0.0
        )
        playback_settings["accel_mm_s2"] = float(
            settings.get("robot_playback_accel_mm_s2") or settings.get("accel_mm_s2") or 0.0
        )
        playback_settings["update_rate_hz"] = float(
            settings.get("robot_playback_update_rate_hz") or settings.get("update_rate_hz") or 0.0
        )
        return playback_settings

    def build_keyframe_plan_settings(mode: str, settings: dict) -> dict:
        plan_settings = dict(settings)
        if mode == "robot":
            plan_settings["speed_mm_s"] = float(
                settings.get("robot_playback_speed_mm_s") or settings.get("speed_mm_s") or 0.0
            )
            plan_settings["accel_mm_s2"] = float(
                settings.get("robot_playback_accel_mm_s2") or settings.get("accel_mm_s2") or 0.0
            )
            plan_settings["update_rate_hz"] = float(
                settings.get("robot_playback_update_rate_hz") or settings.get("update_rate_hz") or 0.0
            )
        return plan_settings

    def resolve_keyframe_robot_command_overrides(settings: dict | None) -> dict[int, dict]:
        overrides: dict[int, dict] = {}
        for entry in (settings or {}).get("can_stream_joint_params", []):
            if not isinstance(entry, dict):
                continue
            joint_id = entry.get("joint_id")
            try:
                joint_id = int(joint_id)
            except (TypeError, ValueError):
                continue
            if joint_id <= 0:
                continue
            override = {}
            speed_rpm = entry.get("speed_rpm")
            accel = entry.get("stream_acc", entry.get("acc"))
            if speed_rpm is not None:
                override["speed_rpm"] = float(speed_rpm)
            if accel is not None:
                override["acc"] = int(accel)
            if override:
                overrides[joint_id] = override
        return overrides

    def resolve_keyframe_robot_step_motion(
        current_angles_deg: list[float],
        target_angles_deg: list[float],
    ) -> dict | None:
        percentages = resolve_keyframe_ui_percentages()
        if percentages is None:
            return None
        speed_pct, accel_pct = percentages

        speed_scale = max(0.01, min(1.0, speed_pct / 100.0))
        accel_scale = max(0.01, min(1.0, accel_pct / 100.0))
        auto_sync_enabled = bool(keyframes_panel.auto_sync_var.get())
        overrides: dict[int, dict] = {}
        sync_duration_s = 0.0
        joint_delta_deg: dict[int, float] = {}
        joint_scaled_acc: dict[int, int] = {}

        def rpm_to_rev_s(speed_rpm_value: float) -> float:
            return max(0.0, float(speed_rpm_value) / 60.0)

        def acc_to_rev_s2(acc_value: float) -> float:
            return max(0.0, float(acc_value) / 60.0)

        def move_time_s(distance_deg: float, speed_rpm_value: float, acc_value: float) -> float:
            distance_rev = max(0.0, float(distance_deg) / 360.0)
            if distance_rev <= 0.0:
                return 0.0
            v_max = rpm_to_rev_s(speed_rpm_value)
            a_max = acc_to_rev_s2(acc_value)
            if v_max <= 0.0:
                return float("inf")
            if a_max <= 0.0:
                return distance_rev / v_max
            t_accel = v_max / a_max
            d_accel = 0.5 * a_max * t_accel * t_accel
            if distance_rev <= 2.0 * d_accel:
                return 2.0 * math.sqrt(distance_rev / a_max)
            cruise_rev = distance_rev - (2.0 * d_accel)
            return (2.0 * t_accel) + (cruise_rev / v_max)

        def solve_speed_rpm_for_duration(distance_deg: float, duration_s: float, acc_value: float, max_speed_rpm: float) -> float:
            distance_rev = max(0.0, float(distance_deg) / 360.0)
            if distance_rev <= 0.0:
                return 0.0
            duration_s = max(0.0, float(duration_s))
            if duration_s <= 0.0:
                return max(0.0, float(max_speed_rpm))
            a_max = acc_to_rev_s2(acc_value)
            v_limit = rpm_to_rev_s(max_speed_rpm)
            if a_max <= 0.0:
                required_v = distance_rev / duration_s
            else:
                discriminant = max(0.0, (duration_s * duration_s) - ((4.0 * distance_rev) / a_max))
                required_v = 0.5 * a_max * (duration_s - math.sqrt(discriminant))
                if required_v <= 0.0:
                    required_v = distance_rev / duration_s
            required_v = min(v_limit, max(0.0, required_v))
            return min(float(max_speed_rpm), max(0.0, required_v * 60.0))

        if auto_sync_enabled:
            for idx, joint in enumerate(comm_client.config.joints):
                if idx >= len(current_angles_deg) or idx >= len(target_angles_deg):
                    break
                delta_deg = abs(float(target_angles_deg[idx]) - float(current_angles_deg[idx]))
                joint_delta_deg[int(joint.id)] = delta_deg
                scaled_acc = int(max(1, min(255, round(max(1, int(joint.acc)) * accel_scale))))
                joint_scaled_acc[int(joint.id)] = scaled_acc
                requested_speed_rpm = max(1, round(max(1, int(joint.max_speed_rpm)) * speed_scale))
                sync_duration_s = max(
                    sync_duration_s,
                    move_time_s(delta_deg, requested_speed_rpm, scaled_acc),
                )

        for joint in comm_client.config.joints:
            max_speed_rpm = max(1, int(joint.max_speed_rpm))
            base_acc = max(1, int(joint.acc))
            scaled_acc = int(max(1, min(255, round(base_acc * accel_scale))))
            requested_speed_rpm = max(1, min(max_speed_rpm, round(max_speed_rpm * speed_scale)))
            speed_rpm = float(requested_speed_rpm)
            if auto_sync_enabled and sync_duration_s > 0.0:
                delta_deg = float(joint_delta_deg.get(int(joint.id), 0.0))
                speed_rpm = solve_speed_rpm_for_duration(
                    delta_deg,
                    sync_duration_s,
                    float(joint_scaled_acc.get(int(joint.id), scaled_acc)),
                    float(requested_speed_rpm),
                )
            overrides[int(joint.id)] = {
                "speed_rpm": speed_rpm,
                "acc": scaled_acc,
            }

        return {
            "speed_pct": float(speed_pct),
            "accel_pct": float(accel_pct),
            "auto_sync": bool(auto_sync_enabled),
            "sync_duration_s": float(sync_duration_s),
            "overrides": overrides,
        }

    def resolve_keyframe_robot_tracking_limit(settings: dict | None) -> float:
        runtime_cfg = (settings or {}).get("can_velocity_stream", {})
        tracking_limit = parse_float_entry(runtime_cfg.get("max_tracking_error_deg"))
        if tracking_limit is None or tracking_limit <= 0.0:
            tracking_limit = 15.0
        return float(tracking_limit)

    def parse_robot_joint_angles(values) -> list[float] | None:
        angles = []
        for value in values or []:
            if value is None:
                return None
            angles.append(float(value))
            if len(angles) >= 6:
                break
        return angles if len(angles) >= 6 else None

    def read_encoder_angles_after_settle(settle_s: float = POST_MOTION_ENCODER_SETTLE_S) -> list[float]:
        if settle_s > 0.0:
            time.sleep(float(settle_s))
        final_angles = parse_robot_joint_angles(comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S))
        if final_angles is None:
            raise RuntimeError("Encoder read incomplete.")
        return final_angles[:6]

    def execute_blocking_robot_move(
        target_angles: list[float],
        *,
        timeout_s: float = ROBOT_MOVE_COMPLETE_TIMEOUT_S,
        overrides: dict[int, dict] | None = None,
    ) -> dict:
        resolved_target_angles = parse_robot_joint_angles(target_angles)
        if resolved_target_angles is None:
            raise RuntimeError("Target joint angles unavailable.")
        ok_all, details = comm_client.send_joint_targets_abs_wait_detailed(
            target_angles_deg=resolved_target_angles,
            overrides=overrides,
            clamp_limits=True,
            timeout_s=float(timeout_s),
        )
        if not ok_all:
            failed_details = [detail for detail in details if not detail.get("ok")]
            failed_joint_ids = [detail.get("joint_id") for detail in failed_details]
            stream_log.warning(
                "Blocking robot move failed target=%s failures=%s",
                [round(float(v), 3) for v in resolved_target_angles[:6]],
                [
                    {
                        "joint_id": detail.get("joint_id"),
                        "reason_code": detail.get("reason_code"),
                        "resp_status": detail.get("resp_status"),
                        "target_axis": detail.get("target_axis"),
                        "before": detail.get("diag_before_ticks"),
                        "after": detail.get("diag_after_ticks"),
                        "expected_delta": detail.get("diag_expected_delta"),
                        "actual_delta": detail.get("diag_actual_delta"),
                    }
                    for detail in failed_details
                ],
            )
            raise RuntimeError(f"Move failed for joints {failed_joint_ids}.")
        final_angles = read_encoder_angles_after_settle()
        return {
            "target_angles": resolved_target_angles,
            "final_angles": final_angles,
            "details": details,
        }

    def queue_post_motion_encoder_refresh(
        success_status: str,
        *,
        busy_status: str | None = None,
        error_prefix: str | None = None,
    ):
        if not comm_client.is_connected:
            return

        def start_refresh():
            if comm_busy["value"]:
                root.after(50, start_refresh)
                return

            def task():
                return read_encoder_angles_after_settle()

            def done(result, err):
                if err:
                    if error_prefix:
                        set_encoder_status(f"{error_prefix} ({err})")
                    return
                final_angles = parse_robot_joint_angles(result)
                if final_angles is None:
                    if error_prefix:
                        set_encoder_status(f"{error_prefix} (encoder read incomplete)")
                    return
                update_encoder_display(final_angles)
                set_encoder_status(success_status)

            run_comm_task(
                task,
                done,
                busy_status=busy_status or success_status,
                task_name="post_motion_encoder_refresh",
                timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S,
            )

        root.after(0, start_refresh)

    ik_config = get_config()
    custom_ik_settings_popup = {"window": None}
    custom_ik_result_state = {"angles": None}
    custom_ik_settings = {
        "mode_var": tk.StringVar(value="multi_seed"),
        "single_seed": {
            "max_iters": tk.StringVar(value=str(int(ik_config.solver.single_seed.max_iters))),
            "tol_pos_mm": tk.StringVar(value=str(float(ik_config.solver.single_seed.tol_pos_mm))),
            "tol_ori_rad": tk.StringVar(value=f"{math.degrees(float(ik_config.solver.single_seed.tol_ori_rad)):.3f}"),
            "damping": tk.StringVar(value=str(float(ik_config.solver.single_seed.damping))),
            "step_scale": tk.StringVar(value=str(float(ik_config.solver.single_seed.step_scale))),
            "max_dq_deg": tk.StringVar(
                value=""
                if ik_config.solver.single_seed.max_dq_deg is None
                else str(float(ik_config.solver.single_seed.max_dq_deg))
            ),
        },
        "multi_seed": {
            "max_attempts": tk.StringVar(value=str(int(ik_config.solver.multi_seed.max_attempts))),
            "prefer_closest_to_q0": tk.BooleanVar(value=bool(ik_config.solver.multi_seed.prefer_closest_to_q0)),
            "n_random": tk.StringVar(value=str(int(ik_config.solver.multi_seed.seed_params.n_random))),
            "jitter_deg": tk.StringVar(value=str(float(ik_config.solver.multi_seed.seed_params.jitter_deg))),
            "rng_seed": tk.StringVar(value=str(int(ik_config.solver.multi_seed.seed_params.rng_seed))),
        },
    }
    custom_ik_knob_help = {
        "mode": "Selects which solver path Run Custom IK uses. Single-seed follows one initial guess, while multi-seed tries several guesses and picks the best valid result.",
        "max_iters": "Maximum iterations for one solve attempt. Higher values can recover harder poses but increase solve time.",
        "tol_pos_mm": "Position tolerance in millimeters. Smaller values require the FK result to land closer to the target position.",
        "tol_ori_rad": "Orientation tolerance in degrees. Smaller values require tighter TCP orientation accuracy.",
        "damping": "Damped least-squares regularization strength. Higher damping is more stable near singularities but can slow convergence.",
        "step_scale": "Scales each per-iteration joint update. Lower values are safer; higher values make the solver more aggressive.",
        "max_dq_deg": "Maximum joint change allowed in one iteration, in degrees. Leave blank to disable the clamp.",
        "max_attempts": "Maximum number of seed guesses to try in multi-seed mode. More attempts improve coverage but cost more time.",
        "prefer_closest_to_q0": "When several valid solutions exist, prefer the one closest to the current seed posture. This usually avoids unnecessary branch jumps.",
        "n_random": "Number of additional random seed guesses to generate. More random seeds can find alternate branches more reliably.",
        "jitter_deg": "Random perturbation size around the current seed, in degrees. Larger jitter explores farther from the current posture.",
        "rng_seed": "Random generator seed for multi-seed exploration. Keeping it fixed makes the solver repeatable from run to run.",
    }

    def add_popup_info_button(parent, row: int, tooltip_text: str):
        info_btn = tk.Button(
            parent,
            text="i",
            width=2,
            relief="flat",
            bd=0,
            bg="#1565c0",
            fg="#ffffff",
            activebackground="#2b7bd6",
            activeforeground="#ffffff",
            font=("Helvetica", 8, "bold"),
            cursor="hand2",
            padx=4,
            pady=1,
        )
        info_btn.grid(row=row, column=1, sticky="w", padx=(4, 8), pady=2)
        Tooltip(info_btn, tooltip_text)
        return info_btn

    def add_popup_entry_row(parent, row: int, label_text: str, var: tk.StringVar, unit_text: str, tooltip_text: str):
        tk.Label(parent, text=label_text, bg="#f7f7f7", fg="#4a4a4a", font=("Helvetica", 9)).grid(
            row=row, column=0, sticky="w", pady=2
        )
        add_popup_info_button(parent, row, tooltip_text)
        tk.Entry(
            parent,
            textvariable=var,
            width=10,
            bg="#ffffff",
            fg="#111111",
            insertbackground="#111111",
            relief="flat",
            font=("Courier", 10),
            highlightthickness=1,
            highlightbackground="#cccccc",
            highlightcolor="#1565c0",
        ).grid(row=row, column=2, sticky="w", pady=2)
        tk.Label(parent, text=unit_text, bg="#f7f7f7", fg="#888888", font=("Helvetica", 9)).grid(
            row=row, column=3, sticky="w", padx=(4, 0), pady=2
        )

    def add_popup_bool_row(parent, row: int, label_text: str, var: tk.BooleanVar, tooltip_text: str):
        tk.Label(parent, text=label_text, bg="#f7f7f7", fg="#4a4a4a", font=("Helvetica", 9)).grid(
            row=row, column=0, sticky="w", pady=2
        )
        add_popup_info_button(parent, row, tooltip_text)
        tk.Checkbutton(
            parent,
            variable=var,
            bg="#f7f7f7",
            activebackground="#f7f7f7",
            selectcolor="#ffffff",
            cursor="hand2",
        ).grid(row=row, column=2, sticky="w", pady=2)

    def read_custom_ik_runtime_settings():
        single_seed_vars = custom_ik_settings["single_seed"]
        multi_seed_vars = custom_ik_settings["multi_seed"]
        max_dq_text = str(single_seed_vars["max_dq_deg"].get()).strip()
        max_dq_deg = None if max_dq_text == "" else float(max_dq_text)
        return {
            "mode": str(custom_ik_settings["mode_var"].get() or "multi_seed"),
            "single_seed": {
                "max_iters": int(str(single_seed_vars["max_iters"].get()).strip()),
                "tol_pos_mm": float(str(single_seed_vars["tol_pos_mm"].get()).strip()),
                "tol_ori_rad": math.radians(float(str(single_seed_vars["tol_ori_rad"].get()).strip())),
                "damping": float(str(single_seed_vars["damping"].get()).strip()),
                "step_scale": float(str(single_seed_vars["step_scale"].get()).strip()),
                "max_dq_deg": max_dq_deg,
            },
            "multi_seed": {
                "max_attempts": int(str(multi_seed_vars["max_attempts"].get()).strip()),
                "prefer_closest_to_q0": bool(multi_seed_vars["prefer_closest_to_q0"].get()),
                "seed_params": {
                    "n_random": int(str(multi_seed_vars["n_random"].get()).strip()),
                    "jitter_deg": float(str(multi_seed_vars["jitter_deg"].get()).strip()),
                    "rng_seed": int(str(multi_seed_vars["rng_seed"].get()).strip()),
                },
            },
        }

    def show_custom_ik_settings():
        existing = custom_ik_settings_popup["window"]
        if existing is not None and existing.winfo_exists():
            existing.deiconify()
            existing.lift()
            existing.focus_force()
            return

        popup = tk.Toplevel(root)
        popup.title("Custom IK Settings")
        popup.transient(root)
        popup.resizable(False, False)
        popup.configure(bg="#f7f7f7", padx=10, pady=10)
        custom_ik_settings_popup["window"] = popup

        tk.Label(
            popup,
            text="These settings affect only the Run Custom IK button for this session. They do not update ik_settings.json.",
            bg="#f7f7f7",
            fg="#888888",
            font=("Helvetica", 9),
            justify="left",
            anchor="w",
            wraplength=430,
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))

        mode_frame = tk.Frame(popup, bg="#f7f7f7")
        mode_frame.grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 10))
        tk.Label(mode_frame, text="Mode", bg="#f7f7f7", fg="#4a4a4a", font=("Helvetica", 9, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        add_popup_info_button(mode_frame, 0, custom_ik_knob_help["mode"])
        tk.Radiobutton(
            mode_frame,
            text="Single-Seed",
            value="single_seed",
            variable=custom_ik_settings["mode_var"],
            bg="#f7f7f7",
            activebackground="#f7f7f7",
            cursor="hand2",
        ).grid(row=0, column=2, sticky="w", padx=(4, 8))
        tk.Radiobutton(
            mode_frame,
            text="Multi-Seed",
            value="multi_seed",
            variable=custom_ik_settings["mode_var"],
            bg="#f7f7f7",
            activebackground="#f7f7f7",
            cursor="hand2",
        ).grid(row=0, column=3, sticky="w")

        single_seed_frame = tk.LabelFrame(
            popup,
            text="Single-Seed Knobs",
            bg="#f7f7f7",
            fg="#1a1a1a",
            padx=8,
            pady=6,
            font=("Helvetica", 9, "bold"),
        )
        single_seed_frame.grid(row=2, column=0, sticky="nsew", padx=(0, 8))
        add_popup_entry_row(
            single_seed_frame, 0, "max_iters", custom_ik_settings["single_seed"]["max_iters"], "iters", custom_ik_knob_help["max_iters"]
        )
        add_popup_entry_row(
            single_seed_frame, 1, "tol_pos_mm", custom_ik_settings["single_seed"]["tol_pos_mm"], "mm", custom_ik_knob_help["tol_pos_mm"]
        )
        add_popup_entry_row(
            single_seed_frame, 2, "tol_ori_deg", custom_ik_settings["single_seed"]["tol_ori_rad"], "deg", custom_ik_knob_help["tol_ori_rad"]
        )
        add_popup_entry_row(
            single_seed_frame, 3, "damping", custom_ik_settings["single_seed"]["damping"], "", custom_ik_knob_help["damping"]
        )
        add_popup_entry_row(
            single_seed_frame, 4, "step_scale", custom_ik_settings["single_seed"]["step_scale"], "", custom_ik_knob_help["step_scale"]
        )
        add_popup_entry_row(
            single_seed_frame, 5, "max_dq_deg", custom_ik_settings["single_seed"]["max_dq_deg"], "deg", custom_ik_knob_help["max_dq_deg"]
        )

        multi_seed_frame = tk.LabelFrame(
            popup,
            text="Multi-Seed Knobs",
            bg="#f7f7f7",
            fg="#1a1a1a",
            padx=8,
            pady=6,
            font=("Helvetica", 9, "bold"),
        )
        multi_seed_frame.grid(row=2, column=1, sticky="nsew")
        add_popup_entry_row(
            multi_seed_frame, 0, "max_attempts", custom_ik_settings["multi_seed"]["max_attempts"], "tries", custom_ik_knob_help["max_attempts"]
        )
        add_popup_bool_row(
            multi_seed_frame, 1, "prefer_closest_to_q0", custom_ik_settings["multi_seed"]["prefer_closest_to_q0"], custom_ik_knob_help["prefer_closest_to_q0"]
        )
        add_popup_entry_row(
            multi_seed_frame, 2, "n_random", custom_ik_settings["multi_seed"]["n_random"], "seeds", custom_ik_knob_help["n_random"]
        )
        add_popup_entry_row(
            multi_seed_frame, 3, "jitter_deg", custom_ik_settings["multi_seed"]["jitter_deg"], "deg", custom_ik_knob_help["jitter_deg"]
        )
        add_popup_entry_row(
            multi_seed_frame, 4, "rng_seed", custom_ik_settings["multi_seed"]["rng_seed"], "", custom_ik_knob_help["rng_seed"]
        )

        button_row = tk.Frame(popup, bg="#f7f7f7")
        button_row.grid(row=3, column=0, columnspan=2, sticky="e", pady=(10, 0))

        def close_popup():
            custom_ik_settings_popup["window"] = None
            popup.destroy()

        def apply_popup_settings():
            try:
                settings = read_custom_ik_runtime_settings()
            except ValueError as exc:
                append_terminal_line(f"Custom IK settings invalid: {exc}", "error")
                set_pose_status("Status: custom IK settings invalid")
                return
            custom_ik_settings["single_seed"]["tol_ori_rad"].set(
                f"{math.degrees(float(settings['single_seed']['tol_ori_rad'])):.3f}"
            )
            append_terminal_line(
                (
                    "Custom IK settings applied: "
                    f"mode={settings['mode']} "
                    f"single={settings['single_seed']} "
                    f"multi={settings['multi_seed']}"
                ),
                "dim",
            )
            set_pose_status("Status: custom IK settings updated")
            close_popup()

        tk.Button(
            button_row,
            text="Close",
            command=close_popup,
            bg="#5a5a5a",
            fg="#ffffff",
            activebackground="#7a7a7a",
            activeforeground="#ffffff",
            relief="flat",
            bd=0,
            padx=8,
            pady=3,
            font=("Helvetica", 9, "bold"),
            cursor="hand2",
        ).grid(row=0, column=0, padx=(0, 4))
        tk.Button(
            button_row,
            text="Apply",
            command=apply_popup_settings,
            bg="#1565c0",
            fg="#ffffff",
            activebackground="#2b7bd6",
            activeforeground="#ffffff",
            relief="flat",
            bd=0,
            padx=8,
            pady=3,
            font=("Helvetica", 9, "bold"),
            cursor="hand2",
        ).grid(row=0, column=1)

        popup.protocol("WM_DELETE_WINDOW", close_popup)

    def solve_fast_keyframe_sim_ik(
        x_mm: float,
        y_mm: float,
        z_mm: float,
        rx_deg: float,
        ry_deg: float,
        rz_deg: float,
        q0_deg: list[float],
        position_only: bool = False,
    ) -> IKSolution:
        _ = position_only
        target = build_pose_target(x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg)
        ok, q_sol, iters, pos_err, ori_err, seed_used, attempts = ik_solve_multiseed(
            T_target=target,
            q0_deg=list(q0_deg[:6]),
            position_only=False,
            joint_limits_deg=ik_config.joint_limits_deg,
            dh_table=ik_config.dh_table,
            max_attempts=6,
            max_iters=50,
            tol_pos_mm=1.5,
            tol_ori_rad=0.03,
            damping=0.08,
            step_scale=1.0,
            max_dq_deg=6.0,
            prefer_closest_to_q0=True,
            seed_params={"n_random": 2, "jitter_deg": 12.0, "rng_seed": 0},
        )
        return IKSolution(
            ok=bool(ok),
            q_deg=[float(value) for value in q_sol[:6]],
            iters=int(iters),
            pos_err_mm=float(pos_err),
            ori_err_rad=float(ori_err),
            seed_used=[float(value) for value in seed_used[:6]],
            attempts=int(attempts),
        )

    def solve_keyframe_robot_interp_ik(
        x_mm: float,
        y_mm: float,
        z_mm: float,
        rx_deg: float,
        ry_deg: float,
        rz_deg: float,
        q0_deg: list[float],
        position_only: bool = False,
    ) -> IKSolution:
        fast_solution = solve_ik_for_pose_single_seed(
            x_mm=x_mm,
            y_mm=y_mm,
            z_mm=z_mm,
            rx_deg=rx_deg,
            ry_deg=ry_deg,
            rz_deg=rz_deg,
            q0_deg=list(q0_deg[:6]),
            position_only=position_only,
            max_iters=60,
            tol_pos_mm=0.75,
            tol_ori_rad=0.02,
            damping=0.08,
            step_scale=1.0,
            max_dq_deg=6.0,
        )
        if fast_solution.ok:
            return fast_solution
        return solve_ik_for_pose(
            x_mm=x_mm,
            y_mm=y_mm,
            z_mm=z_mm,
            rx_deg=rx_deg,
            ry_deg=ry_deg,
            rz_deg=rz_deg,
            q0_deg=list(q0_deg[:6]),
            position_only=position_only,
        )

    def append_terminal_line(text: str, tag: str = "output"):
        terminal_buffer.append((str(text), str(tag)))
        if terminal_flush_after_id["value"] is not None:
            return

        def apply_update():
            terminal_flush_after_id["value"] = None
            pending = list(terminal_buffer)
            terminal_buffer.clear()
            if not pending:
                return
            terminal_text.configure(state="normal")
            for line_text, line_tag in pending:
                terminal_text.insert("end", line_text + "\n", line_tag)
            line_count = int(terminal_text.index("end-1c").split(".")[0])
            if line_count > TERMINAL_MAX_LINES:
                trim_to_line = line_count - TERMINAL_MAX_LINES
                terminal_text.delete("1.0", f"{trim_to_line + 1}.0")
            terminal_text.see("end")
            terminal_text.configure(state="disabled")

        terminal_flush_after_id["value"] = root.after(TERMINAL_FLUSH_INTERVAL_MS, apply_update)

    terminal_docs_path = Path("docs") / "terminal_commands.md"

    def normalize_terminal_command(command: str) -> str:
        return "_".join((command or "").strip().lower().replace("-", " ").split())

    def execute_terminal_command():
        raw_command = terminal_command_var.get().strip()
        if not raw_command:
            return
        terminal_command_var.set("")
        append_terminal_line(f"> {raw_command}", "prompt")
        command = normalize_terminal_command(raw_command)

        if command == "robot_window_size":
            root.update_idletasks()
            width = int(root.winfo_width())
            height = int(root.winfo_height())
            append_terminal_line(f"Robot Arm Control window size: {width} x {height}", "output")
            return

        if command == "pybullet_window_size":
            with shared_state.lock:
                pybullet_window_size = list(shared_state.pybullet_window_size)
            width = int(pybullet_window_size[0]) if len(pybullet_window_size) > 0 else 0
            height = int(pybullet_window_size[1]) if len(pybullet_window_size) > 1 else 0
            if width > 0 and height > 0:
                append_terminal_line(f"PyBullet window size: {width} x {height}", "output")
            else:
                append_terminal_line("PyBullet window size is not available yet.", "error")
            return

        append_terminal_line(
            f"Unknown command: {raw_command}. See {terminal_docs_path.as_posix()}",
            "error",
        )

    terminal_send_btn.configure(command=execute_terminal_command)
    terminal_command_entry.bind("<Return>", lambda _event: (execute_terminal_command(), "break")[1])

    def update_encoder_display(angles):
        encoder_display_state["latest"] = list(angles or [])
        pending_after_id = encoder_display_state.get("after_id")
        if pending_after_id is not None:
            try:
                root.after_cancel(pending_after_id)
            except Exception:
                pass
            encoder_display_state["after_id"] = None

        def apply_update():
            encoder_display_state["after_id"] = None
            latest_angles = list(encoder_display_state.get("latest") or [])
            stream_log.info(
                "Encoder display apply latest=%s",
                [
                    None if angle is None else round(float(angle), 4)
                    for angle in latest_angles[:6]
                ],
            )
            valid_angles = []
            for i, angle in enumerate(latest_angles):
                if i >= len(encoder_vars):
                    break
                if angle is None:
                    _set_stringvar_if_changed(encoder_vars[i], "--")
                else:
                    angle_value = float(angle)
                    valid_angles.append(angle_value)
                    _set_stringvar_if_changed(encoder_vars[i], f"{angle_value:+.2f}")
            for i in range(len(latest_angles), len(encoder_vars)):
                _set_stringvar_if_changed(encoder_vars[i], "--")

            if len(valid_angles) >= 6:
                try:
                    x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg = fk_solver.get_full_pose(valid_angles[:6])
                    rx_deg, ry_deg, rz_deg = _stabilize_display_rpy(rx_deg, ry_deg, rz_deg)
                    pose_values = (x_mm, y_mm, z_mm)
                    rpy_values = (rx_deg, ry_deg, rz_deg)
                    for i, value in enumerate(pose_values):
                        _set_stringvar_if_changed(encoder_tcp_pos_vars[i], f"{float(value):+.2f}")
                    for i, value in enumerate(rpy_values):
                        _set_stringvar_if_changed(encoder_tcp_rpy_vars[i], f"{float(value):+.2f}")
                except Exception:
                    for var in encoder_tcp_pos_vars + encoder_tcp_rpy_vars:
                        _set_stringvar_if_changed(var, "--")
            else:
                for var in encoder_tcp_pos_vars + encoder_tcp_rpy_vars:
                    _set_stringvar_if_changed(var, "--")

        encoder_display_state["after_id"] = root.after(0, apply_update)

    def parse_float_entry(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def parse_int_entry(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def parse_keyframe_percent(value, label: str) -> float | None:
        percent = parse_float_entry(value)
        if percent is None or percent < 1.0 or percent > 100.0:
            set_keyframe_status(f"Keyframes: invalid {label} percent")
            return None
        return float(percent)

    def resolve_keyframe_ui_percentages() -> tuple[float, float] | None:
        speed_pct = parse_keyframe_percent(keyframes_panel.speed_var.get(), "speed")
        if speed_pct is None:
            return None
        accel_pct = parse_keyframe_percent(keyframes_panel.accel_var.get(), "accel")
        if accel_pct is None:
            return None
        return float(speed_pct), float(accel_pct)

    def parse_bool_entry(value, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
        if value is None:
            return bool(default)
        return bool(value)

    def clear_custom_ik_display(meta_text: str = "Idle"):
        custom_ik_result_state["angles"] = None
        for var in custom_ik_joint_vars + custom_ik_tcp_pos_vars + custom_ik_tcp_rpy_vars:
            _set_stringvar_if_changed(var, "--")
        _set_stringvar_if_changed(custom_ik_pos_err_var, "--")
        _set_stringvar_if_changed(custom_ik_ori_err_var, "--")
        _set_stringvar_if_changed(custom_ik_meta_var, meta_text)

    clear_custom_ik_display()
    _set_stringvar_if_changed(pose_pos_err_var, "-- mm")

    def parse_pose_inputs():
        try:
            x = float(x_var.get()) * MM_TO_M
            y = float(y_var.get()) * MM_TO_M
            z = float(z_var.get()) * MM_TO_M
            rx = math.radians(float(rx_var.get()))
            ry = math.radians(float(ry_var.get()))
            rz = math.radians(float(rz_var.get()))
        except ValueError:
            return None
        return x, y, z, rx, ry, rz

    def set_target(enable_ik: bool):
        values = parse_pose_inputs()
        if values is None:
            set_pose_status("Status: invalid target values")
            return False
        x, y, z, rx, ry, rz = values
        target_orn = p.getQuaternionFromEuler([rx, ry, rz])
        with shared_state.lock:
            shared_state.target_pos_m = [x, y, z]
            shared_state.target_orn = target_orn
            shared_state.target_rpy_deg = [
                math.degrees(rx),
                math.degrees(ry),
                math.degrees(rz),
            ]
            shared_state.ik_enabled = enable_ik
        ik_log.info(
            "Target updated: x=%.2f mm y=%.2f mm z=%.2f mm rpy=(%.2f, %.2f, %.2f) deg ik_enabled=%s",
            x * M_TO_MM,
            y * M_TO_MM,
            z * M_TO_MM,
            math.degrees(rx),
            math.degrees(ry),
            math.degrees(rz),
            enable_ik,
        )
        set_pose_status("Status: target applied" if not enable_ik else "Status: simulation moving")
        return True

    def send_joint_command():
        try:
            values = [float(var.get()) for var in joint_entry_vars]
        except ValueError:
            set_pose_status("Status: invalid joint values")
            return False
        with shared_state.lock:
            shared_state.manual_joint_target_deg = values
            shared_state.manual_joint_pending = True
            shared_state.manual_joint_motion_overrides = {}
            shared_state.ik_enabled = False
        set_pose_status("Status: joint command sent")
        return True

    def on_only_position_toggle(*_args):
        with shared_state.lock:
            shared_state.only_position = bool(only_pos_var.get())
        ik_log.info(
            "Custom IK mode set to %s",
            "position-only" if only_pos_var.get() else "full pose",
        )

    only_pos_var.trace_add("write", on_only_position_toggle)

    def get_step_values():
        try:
            pos_step = float(pose_panel.pos_step_var.get())
            ang_step = float(pose_panel.ang_step_var.get())
        except ValueError:
            return None
        return pos_step, ang_step

    def jog_pose_position(var, delta):
        try:
            current = float(var.get())
        except ValueError:
            current = 0.0
        var.set(f"{current + delta:.3f}")
        with shared_state.lock:
            ik_enabled = shared_state.ik_enabled
        set_target(ik_enabled)

    def jog_pose_orientation_local(axis_name: str, delta_deg: float):
        with shared_state.lock:
            current_target_orn = shared_state.target_orn
            ik_enabled = shared_state.ik_enabled

        if current_target_orn is None or len(current_target_orn) != 4:
            values = parse_pose_inputs()
            if values is None:
                set_pose_status("Status: invalid target values")
                return
            _, _, _, rx, ry, rz = values
            current_target_orn = p.getQuaternionFromEuler([rx, ry, rz])

        local_axis_vectors = {
            "rx": (1.0, 0.0, 0.0),
            "ry": (0.0, 1.0, 0.0),
            "rz": (0.0, 0.0, 1.0),
        }
        axis_local = local_axis_vectors.get(axis_name)
        if axis_local is None:
            set_pose_status("Status: invalid orientation jog axis")
            return

        half_angle_rad = math.radians(delta_deg) * 0.5
        sin_half = math.sin(half_angle_rad)
        delta_orn = (
            axis_local[0] * sin_half,
            axis_local[1] * sin_half,
            axis_local[2] * sin_half,
            math.cos(half_angle_rad),
        )
        target_orn = _quat_multiply(current_target_orn, delta_orn)
        norm = math.sqrt(sum(float(value) * float(value) for value in target_orn))
        if norm <= 1e-9:
            set_pose_status("Status: invalid orientation jog result")
            return
        target_orn = tuple(float(value) / norm for value in target_orn)
        target_rpy_deg = [math.degrees(value) for value in p.getEulerFromQuaternion(target_orn)]
        rx_deg, ry_deg, rz_deg = _stabilize_display_rpy(*target_rpy_deg[:3])
        rx_var.set(f"{rx_deg:.3f}")
        ry_var.set(f"{ry_deg:.3f}")
        rz_var.set(f"{rz_deg:.3f}")
        set_target(ik_enabled)

    def bind_jog_buttons():
        for axis_name, is_angle in {
            "x": False,
            "y": False,
            "z": False,
            "rx": True,
            "ry": True,
            "rz": True,
        }.items():
            minus_btn, plus_btn = pose_panel.axis_jog_buttons[axis_name]
            var = getattr(pose_panel, f"{axis_name}_var")

            def make_command(direction, axis_var=var, angle_axis=is_angle, axis_key=axis_name):
                def command():
                    steps = get_step_values()
                    if steps is None:
                        set_pose_status("Status: invalid jog step")
                        return
                    pos_step, ang_step = steps
                    delta = ang_step if angle_axis else pos_step
                    if angle_axis:
                        jog_pose_orientation_local(axis_key, direction * delta)
                    else:
                        jog_pose_position(axis_var, direction * delta)

                return command

            minus_btn.configure(command=make_command(-1))
            plus_btn.configure(command=make_command(1))

    bind_jog_buttons()

    def on_toggle_connect():
        if comm_busy["value"]:
            return
        if comm_client.is_connected:
            comm_client.disconnect()
            update_comm_status()
            append_terminal_line("CAN disconnected.", "dim")
            return
        port = comm_port_var.get().strip()
        if not port:
            append_terminal_line("CAN connect failed: COM port is required.", "error")
            return
        append_terminal_line(
            (
                f"CAN connecting: port={port} "
                f"timeout={CAN_CONNECT_INIT_TIMEOUT_S:.1f}s "
                f"listen={CAN_CONNECT_LISTEN_TIMEOUT_S:.1f}s"
            ),
            "dim",
        )

        def task():
            return comm_client.connect_with_checks(
                port,
                init_timeout_s=CAN_CONNECT_INIT_TIMEOUT_S,
                listen_timeout_s=CAN_CONNECT_LISTEN_TIMEOUT_S,
            )

        def done(result, err):
            update_comm_status()
            if err:
                append_terminal_line(f"CAN connect failed: {err}", "error")
                return
            connect_result = result
            if not connect_result or not connect_result.ok:
                message = getattr(connect_result, "message", None) or "Connect failed."
                append_terminal_line(message, "error")
                return
            append_terminal_line(connect_result.message, "dim")

        run_comm_task(
            task,
            done,
            task_name="connect_can_bus",
            timeout_s=CAN_CONNECT_INIT_TIMEOUT_S + 2.0,
        )

    connect_btn.configure(command=on_toggle_connect)
    update_comm_status()

    def on_read_encoders():
        if not comm_client.is_connected:
            encoder_status_var.set("Encoders: CAN not connected.")
            return

        def task():
            return comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)

        def done(result, err):
            if err:
                encoder_status_var.set(f"Encoders: error ({err})")
                return
            stream_log.info(
                "Read Encoders angles=%s",
                [
                    None if angle is None else round(float(angle), 4)
                    for angle in (result or [])[:6]
                ],
            )
            update_encoder_display(result or [])
            domain_state = comm_client.get_encoder_domain_state()
            stream_log.info(
                "Read Encoders: mode=%s home_mode=%s mismatch=%s ticks=%s",
                domain_state.get("current_mode_hex"),
                domain_state.get("home_mode_hex"),
                domain_state.get("mismatch"),
                domain_state.get("last_ticks"),
            )
            if domain_state.get("mismatch"):
                encoder_status_var.set("Encoders: domain mismatch detected, re-home required.")
            elif domain_state.get("rehome_required"):
                encoder_status_var.set("Encoders: re-home required.")
            else:
                encoder_status_var.set(
                    "Encoders: homed" if comm_client.is_homed else "Encoders: not homed"
                )

        run_comm_task(task, done, task_name="read_encoders", timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S)

    def on_set_home():
        if not comm_client.is_connected:
            encoder_status_var.set("Home: CAN not connected.")
            return

        def task():
            return comm_client.set_home()

        def done(result, err):
            if err:
                encoder_status_var.set(f"Home: error ({err})")
                return
            data = result or {}
            success = bool(data.get("success", False))
            failed_joint_ids = list(data.get("failed_joint_ids") or [])
            home_mode_hex = data.get("home_mode_hex", "None")
            joint_results = list(data.get("joint_results") or [])
            raw_ticks = [item.get("ticks") for item in joint_results]
            stream_log.info(
                "Set Home: success=%s mode=%s failed_joints=%s raw_ticks=%s",
                success,
                home_mode_hex,
                failed_joint_ids,
                raw_ticks,
            )
            if success:
                update_encoder_display([0.0] * 6)
                encoder_status_var.set(f"Home set (mode {home_mode_hex}).")
            else:
                if failed_joint_ids:
                    encoder_status_var.set(
                        "Home failed: encoder read failed for joints %s."
                        % failed_joint_ids
                    )
                else:
                    reason = data.get("reason") or "unknown reason"
                    encoder_status_var.set(f"Home failed: {reason}.")

        run_comm_task(task, done, task_name="set_home", timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S)

    def on_move_robot():
        if not comm_client.is_connected:
            encoder_status_var.set("Move: CAN not connected.")
            return
        with shared_state.lock:
            target_angles = list(shared_state.joint_deg)
        if len(target_angles) < 6:
            encoder_status_var.set("Move: joint angles not ready.")
            return

        def task():
            return execute_blocking_robot_move(
                target_angles[:6],
                timeout_s=ROBOT_MOVE_COMPLETE_TIMEOUT_S,
            )

        def done(result, err):
            if err:
                encoder_status_var.set(f"Move: error ({err})")
                return
            final_angles = list((result or {}).get("final_angles") or [])
            if final_angles:
                update_encoder_display(final_angles)
            encoder_status_var.set("Move: complete.")

        run_comm_task(
            task,
            done,
            busy_status="Move: running...",
            task_name="move_robot_pose",
            timeout_s=ROBOT_MOVE_COMPLETE_TIMEOUT_S + 2.0,
        )

    def on_move_home():
        if not comm_client.is_connected:
            encoder_status_var.set("Home: CAN not connected.")
            return

        def task():
            return execute_blocking_robot_move(
                [0.0] * 6,
                timeout_s=ROBOT_MOVE_COMPLETE_TIMEOUT_S,
            )

        def done(result, err):
            if err:
                encoder_status_var.set(f"Home: error ({err})")
                return
            final_angles = list((result or {}).get("final_angles") or [])
            if final_angles:
                update_encoder_display(final_angles)
            encoder_status_var.set("Home: complete.")

        run_comm_task(
            task,
            done,
            busy_status="Home: moving...",
            task_name="move_home",
            timeout_s=ROBOT_MOVE_COMPLETE_TIMEOUT_S + 2.0,
        )

    def on_pose_set_target():
        if pose_panel.mode_var.get() == 0:
            set_target(False)
        else:
            send_joint_command()

    def on_pose_go_sim():
        if pose_panel.mode_var.get() == 0:
            set_target(True)
        else:
            send_joint_command()

    def on_pose_go_robot():
        set_pose_status("Status: Go Robot uses the current simulated joint state")
        on_move_robot()

    def on_pose_home_target():
        try:
            home_pose = fk_solver.get_full_pose([0.0] * 6)
        except Exception as exc:
            set_pose_status(f"Status: home target unavailable ({exc})")
            return
        _set_stringvar_if_changed(x_var, f"{float(home_pose[0]):.2f}")
        _set_stringvar_if_changed(y_var, f"{float(home_pose[1]):.2f}")
        _set_stringvar_if_changed(z_var, f"{float(home_pose[2]):.2f}")
        _set_stringvar_if_changed(rx_var, f"{float(home_pose[3]):.2f}")
        _set_stringvar_if_changed(ry_var, f"{float(home_pose[4]):.2f}")
        _set_stringvar_if_changed(rz_var, f"{float(home_pose[5]):.2f}")
        set_target(False)
        set_pose_status("Status: target set to home pose")

    def on_pose_home():
        with shared_state.lock:
            shared_state.manual_joint_target_deg = [0.0] * 6
            shared_state.manual_joint_pending = True
            shared_state.manual_joint_motion_overrides = {}
            shared_state.ik_enabled = False
        set_pose_status("Status: moving simulation home")

    def on_pose_stop():
        with shared_state.lock:
            shared_state.ik_enabled = False
        set_pose_status("Status: IK stopped")

    def on_run_custom_ik():
        target_values = [
            parse_float_entry(x_var.get()),
            parse_float_entry(y_var.get()),
            parse_float_entry(z_var.get()),
            parse_float_entry(rx_var.get()),
            parse_float_entry(ry_var.get()),
            parse_float_entry(rz_var.get()),
        ]
        if any(value is None for value in target_values):
            clear_custom_ik_display("Invalid target pose")
            set_pose_status("Status: invalid target values")
            append_terminal_line("Custom IK failed: invalid target pose values.", "error")
            return

        x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg = [float(value) for value in target_values]
        position_only = bool(only_pos_var.get())
        with shared_state.lock:
            q_seed = list(shared_state.joint_deg)
        if len(q_seed) < 6:
            q_seed = [0.0] * 6
        else:
            q_seed = [float(value) for value in q_seed[:6]]
        try:
            runtime_settings = read_custom_ik_runtime_settings()
        except ValueError as exc:
            clear_custom_ik_display("Invalid settings")
            set_pose_status("Status: custom IK settings invalid")
            append_terminal_line(f"Custom IK settings invalid: {exc}", "error")
            return

        solver_mode = str(runtime_settings["mode"] or "multi_seed")
        single_seed_settings = dict(runtime_settings["single_seed"])
        multi_seed_settings = dict(runtime_settings["multi_seed"])
        target_T = build_pose_target(x_mm, y_mm, z_mm, rx_deg, ry_deg, rz_deg)

        append_terminal_line(
            (
                "Custom IK solving: "
                f"target=({x_mm:.2f}, {y_mm:.2f}, {z_mm:.2f}) mm "
                f"rpy=({rx_deg:.2f}, {ry_deg:.2f}, {rz_deg:.2f}) deg "
                f"position_only={position_only} mode={solver_mode} seed={[round(v, 2) for v in q_seed]}"
            ),
            "dim",
        )

        try:
            if solver_mode == "single_seed":
                ok, q_sol, iters, pos_err, ori_err = ik_solve(
                    T_target=target_T,
                    q0_deg=q_seed,
                    position_only=position_only,
                    joint_limits_deg=ik_config.joint_limits_deg,
                    dh_table=ik_config.dh_table,
                    max_iters=single_seed_settings["max_iters"],
                    tol_pos_mm=single_seed_settings["tol_pos_mm"],
                    tol_ori_rad=single_seed_settings["tol_ori_rad"],
                    damping=single_seed_settings["damping"],
                    step_scale=single_seed_settings["step_scale"],
                    max_dq_deg=single_seed_settings["max_dq_deg"],
                )
                solution = IKSolution(
                    ok=bool(ok),
                    q_deg=[float(value) for value in q_sol[:6]],
                    iters=int(iters),
                    pos_err_mm=float(pos_err),
                    ori_err_rad=float(ori_err),
                    seed_used=[float(value) for value in q_seed],
                    attempts=1,
                )
            else:
                ok, q_sol, iters, pos_err, ori_err, seed_used, attempts = ik_solve_multiseed(
                    T_target=target_T,
                    q0_deg=q_seed,
                    position_only=position_only,
                    joint_limits_deg=ik_config.joint_limits_deg,
                    dh_table=ik_config.dh_table,
                    max_attempts=multi_seed_settings["max_attempts"],
                    prefer_closest_to_q0=multi_seed_settings["prefer_closest_to_q0"],
                    seed_params=dict(multi_seed_settings["seed_params"]),
                    max_iters=single_seed_settings["max_iters"],
                    tol_pos_mm=single_seed_settings["tol_pos_mm"],
                    tol_ori_rad=single_seed_settings["tol_ori_rad"],
                    damping=single_seed_settings["damping"],
                    step_scale=single_seed_settings["step_scale"],
                    max_dq_deg=single_seed_settings["max_dq_deg"],
                )
                solution = IKSolution(
                    ok=bool(ok),
                    q_deg=[float(value) for value in q_sol[:6]],
                    iters=int(iters),
                    pos_err_mm=float(pos_err),
                    ori_err_rad=float(ori_err),
                    seed_used=[float(value) for value in seed_used[:6]],
                    attempts=int(attempts),
                )
        except Exception as exc:
            clear_custom_ik_display("Solver error")
            ik_log.exception("Custom IK solve crashed for pose target")
            set_pose_status("Status: custom IK error")
            append_terminal_line(f"Custom IK error: {exc}", "error")
            return

        if not solution.ok:
            clear_custom_ik_display(
                f"No solution after {int(solution.iters)} iters, attempts={int(solution.attempts)}"
            )
            ik_log.warning(
                "Custom IK failed: target=(%.3f, %.3f, %.3f, %.3f, %.3f, %.3f) position_only=%s "
                "mode=%s seed=%s pos_err_mm=%.4f ori_err_deg=%.4f iters=%s attempts=%s",
                x_mm,
                y_mm,
                z_mm,
                rx_deg,
                ry_deg,
                rz_deg,
                position_only,
                solver_mode,
                q_seed,
                float(solution.pos_err_mm),
                math.degrees(float(solution.ori_err_rad)),
                solution.iters,
                solution.attempts,
            )
            set_pose_status("Status: custom IK failed")
            append_terminal_line(
                (
                    "Custom IK failed: "
                    f"pos_err={float(solution.pos_err_mm):.3f} mm "
                    f"ori_err={math.degrees(float(solution.ori_err_rad)):.3f} deg "
                    f"iters={int(solution.iters)} attempts={int(solution.attempts)}"
                ),
                "error",
            )
            return

        try:
            solved_angles = [float(value) for value in solution.q_deg[:6]]
            solved_pose = fk_solver.get_full_pose(solved_angles)
            solved_T = fk_solver.compute(solved_angles)
            err_vec = pose_error(solved_T, target_T)
            pos_err_mm = math.sqrt(sum(float(value) ** 2 for value in err_vec[:3]))
            ori_err_deg = math.degrees(math.sqrt(sum(float(value) ** 2 for value in err_vec[3:])))
        except Exception as exc:
            clear_custom_ik_display("FK error")
            ik_log.exception("Custom IK FK verification failed")
            set_pose_status("Status: custom IK FK error")
            append_terminal_line(f"Custom IK FK verification error: {exc}", "error")
            return

        for i, value in enumerate(solved_angles):
            _set_stringvar_if_changed(custom_ik_joint_vars[i], f"{value:+.2f}")
        custom_ik_result_state["angles"] = list(solved_angles[:6])
        for i, value in enumerate(solved_pose[:3]):
            _set_stringvar_if_changed(custom_ik_tcp_pos_vars[i], f"{float(value):+.2f}")
        for i, value in enumerate(solved_pose[3:6]):
            _set_stringvar_if_changed(custom_ik_tcp_rpy_vars[i], f"{float(value):+.2f}")
        _set_stringvar_if_changed(custom_ik_pos_err_var, f"{pos_err_mm:.3f}")
        _set_stringvar_if_changed(custom_ik_ori_err_var, f"{ori_err_deg:.3f}")
        _set_stringvar_if_changed(
            custom_ik_meta_var,
            f"{solver_mode}, iters={int(solution.iters)}, attempts={int(solution.attempts)}",
        )

        ik_log.info(
            "Custom IK ok: target=(%.3f, %.3f, %.3f, %.3f, %.3f, %.3f) position_only=%s "
            "mode=%s seed=%s q=%s fk_pose=%s pos_err_mm=%.4f ori_err_deg=%.4f iters=%s attempts=%s",
            x_mm,
            y_mm,
            z_mm,
            rx_deg,
            ry_deg,
            rz_deg,
            position_only,
            solver_mode,
            q_seed,
            [round(value, 4) for value in solved_angles],
            [round(float(value), 4) for value in solved_pose],
            pos_err_mm,
            ori_err_deg,
            solution.iters,
            solution.attempts,
        )
        set_pose_status("Status: custom IK solved")
        append_terminal_line(
            (
                "Custom IK solved: "
                f"q={[round(value, 2) for value in solved_angles]} "
                f"mode={solver_mode} "
                f"fk_pos=({solved_pose[0]:.2f}, {solved_pose[1]:.2f}, {solved_pose[2]:.2f}) mm "
                f"fk_rpy=({solved_pose[3]:.2f}, {solved_pose[4]:.2f}, {solved_pose[5]:.2f}) deg "
                f"pos_err={pos_err_mm:.3f} mm ori_err={ori_err_deg:.3f} deg"
            ),
            "output",
        )

    def get_custom_ik_solved_angles():
        angles = custom_ik_result_state.get("angles")
        if not angles or len(angles) < 6:
            set_pose_status("Status: run custom IK first")
            append_terminal_line("Custom IK move unavailable: no solved joint solution yet.", "error")
            return None
        return [float(value) for value in list(angles[:6])]

    def on_custom_ik_go_sim():
        solved_angles = get_custom_ik_solved_angles()
        if solved_angles is None:
            return
        apply_sim_keyframe(solved_angles)
        set_pose_status("Status: custom IK solution sent to simulation")
        append_terminal_line(
            f"Custom IK Go Sim: moved simulation to q={[round(value, 2) for value in solved_angles]}",
            "output",
        )

    def on_custom_ik_go_robot():
        solved_angles = get_custom_ik_solved_angles()
        if solved_angles is None:
            return
        if not comm_client.is_connected:
            set_pose_status("Status: custom IK Go Robot requires CAN")
            append_terminal_line("Custom IK Go Robot failed: CAN not connected.", "error")
            return

        def task():
            move_result = execute_blocking_robot_move(
                solved_angles[:6],
                timeout_s=ROBOT_MOVE_COMPLETE_TIMEOUT_S,
            )
            move_result["target_angles"] = [float(value) for value in solved_angles[:6]]
            return move_result

        def done(result, err):
            if err:
                set_pose_status(f"Status: custom IK Go Robot error ({err})")
                append_terminal_line(f"Custom IK Go Robot failed: {err}", "error")
                return
            final_angles = list((result or {}).get("final_angles") or [])
            if final_angles:
                update_encoder_display(final_angles)
            set_pose_status("Status: custom IK solution sent to robot")
            append_terminal_line(
                f"Custom IK Go Robot: commanded q={[round(value, 2) for value in solved_angles]}",
                "output",
            )

    pose_panel.home_target_btn.configure(command=on_pose_home_target)
    pose_panel.set_target_btn.configure(command=on_pose_set_target)
    pose_panel.go_sim_btn.configure(command=on_pose_go_sim)
    pose_panel.go_robot_btn.configure(command=on_pose_go_robot)
    pose_panel.home_btn.configure(command=on_pose_home)
    pose_panel.stop_btn.configure(command=on_pose_stop)
    pose_panel.custom_ik_settings_btn.configure(command=show_custom_ik_settings)
    pose_panel.custom_ik_btn.configure(command=on_run_custom_ik)
    pose_panel.custom_ik_go_sim_btn.configure(command=on_custom_ik_go_sim)
    pose_panel.custom_ik_go_robot_btn.configure(command=on_custom_ik_go_robot)
    read_btn.configure(command=on_read_encoders)
    set_home_btn.configure(command=on_set_home)
    home_btn.configure(command=on_move_home)

    def on_add_keyframe_sim():
        joint_angles = get_sim_joint_angles()
        if joint_angles is None:
            set_keyframe_status("Keyframes: simulation joint state not ready")
            return
        append_keyframe(joint_angles, "sim")

    def on_add_keyframe_enc():
        displayed_angles = parse_displayed_encoder_angles()
        if displayed_angles is not None:
            append_keyframe(displayed_angles, "encoder")
            return
        if not comm_client.is_connected:
            set_keyframe_status("Keyframes: encoder readings unavailable")
            return
        if comm_busy["value"]:
            set_keyframe_status("Keyframes: CAN operation already in progress")
            return

        def task():
            return comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)

        def done(result, err):
            if err:
                set_keyframe_status(f"Keyframes: encoder read error ({err})")
                return
            valid_angles = []
            for value in result or []:
                if value is None:
                    set_keyframe_status("Keyframes: encoder read incomplete")
                    return
                valid_angles.append(float(value))
            if len(valid_angles) < 6:
                set_keyframe_status("Keyframes: encoder read incomplete")
                return
            update_encoder_display(valid_angles[:6])
            append_keyframe(valid_angles[:6], "encoder")

        run_comm_task(task, done, task_name="add_keyframe_encoder", timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S)

    def _set_demo_anchor_values(pose_values: list[float]):
        vars_ = [
            demo_panel.anchor_x_var,
            demo_panel.anchor_y_var,
            demo_panel.anchor_z_var,
            demo_panel.anchor_rx_var,
            demo_panel.anchor_ry_var,
            demo_panel.anchor_rz_var,
        ]
        for var, value in zip(vars_, pose_values[:6]):
            _set_stringvar_if_changed(var, f"{float(value):.2f}")

    def capture_sim_tcp_for_demo():
        joint_angles = get_sim_joint_angles()
        if joint_angles is None:
            set_demo_status("Demo: simulation joint state not ready")
            return
        pose = compute_cartesian_pose(joint_angles[:6])
        _set_demo_anchor_values(list(pose))
        set_demo_status("Demo: captured simulation TCP")

    def capture_enc_tcp_for_demo():
        displayed_angles = parse_displayed_encoder_angles()
        if displayed_angles is not None:
            pose = fk_solver.get_full_pose(displayed_angles[:6])
            _set_demo_anchor_values(list(pose))
            set_demo_status("Demo: captured encoder TCP")
            return

        if not comm_client.is_connected:
            set_demo_status("Demo: encoder readings unavailable")
            return
        if comm_busy["value"]:
            set_demo_status("Demo: CAN operation already in progress")
            return

        def task():
            return comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)

        def done(result, err):
            if err:
                set_demo_status(f"Demo: encoder read error ({err})")
                return
            valid_angles = []
            for value in result or []:
                if value is None:
                    set_demo_status("Demo: encoder read incomplete")
                    return
                valid_angles.append(float(value))
            if len(valid_angles) < 6:
                set_demo_status("Demo: encoder read incomplete")
                return
            update_encoder_display(valid_angles[:6])
            pose = fk_solver.get_full_pose(valid_angles[:6])
            _set_demo_anchor_values(list(pose))
            set_demo_status("Demo: captured encoder TCP")

        run_comm_task(task, done, task_name="demo_capture_encoder", timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S)

    def _build_demo_config():
        anchor_values = [
            parse_float_entry(demo_panel.anchor_x_var.get()),
            parse_float_entry(demo_panel.anchor_y_var.get()),
            parse_float_entry(demo_panel.anchor_z_var.get()),
            parse_float_entry(demo_panel.anchor_rx_var.get()),
            parse_float_entry(demo_panel.anchor_ry_var.get()),
            parse_float_entry(demo_panel.anchor_rz_var.get()),
        ]
        if any(value is None for value in anchor_values):
            raise ValueError("Demo anchor pose is incomplete")
        amplitude_deg = parse_float_entry(demo_panel.amplitude_var.get())
        speed_deg_s = parse_float_entry(demo_panel.speed_var.get())
        cycles = parse_int_entry(demo_panel.cycles_var.get())
        if amplitude_deg is None or amplitude_deg <= 0.0:
            raise ValueError("Demo amplitude must be > 0")
        if speed_deg_s is None or speed_deg_s <= 0.0:
            raise ValueError("Demo speed must be > 0")
        if cycles is None or cycles <= 0:
            raise ValueError("Demo cycles must be > 0")
        return SweepConfig(
            anchor_xyz_mm=[float(anchor_values[0]), float(anchor_values[1]), float(anchor_values[2])],
            center_rpy_deg=[float(anchor_values[3]), float(anchor_values[4]), float(anchor_values[5])],
            pattern=str(demo_panel.pattern_var.get() or "Cone"),
            amplitude_deg=float(amplitude_deg),
            speed_deg_s=float(speed_deg_s),
            cycles=int(cycles),
            update_rate_hz=20.0,
        )

    def _build_demo_plan_cache_key(mode: str, config: SweepConfig) -> tuple:
        return (
            str(mode),
            tuple(round(float(value), 6) for value in config.anchor_xyz_mm[:3]),
            tuple(round(float(value), 6) for value in config.center_rpy_deg[:3]),
            str(config.pattern or ""),
            round(float(config.amplitude_deg), 6),
            round(float(config.speed_deg_s), 6),
            int(config.cycles),
            round(float(config.update_rate_hz), 6),
        )

    def probe_orientation_reach():
        try:
            config = _build_demo_config()
        except Exception as exc:
            set_demo_status(f"Demo: {exc}")
            return

        set_demo_status("Demo: probing reachability...")

        def worker():
            try:
                joint_seed = get_sim_joint_angles() or [0.0] * 6
                report = probe_reachability(
                    anchor_xyz_mm=config.anchor_xyz_mm,
                    center_rpy_deg=config.center_rpy_deg,
                    amplitude_deg=config.amplitude_deg,
                    probe_grid_size=5,
                    solve_ik_fn=solve_ik_for_pose,
                    q_seed_deg=joint_seed,
                    cancel_check=lambda: False,
                )
                root.after(
                    0,
                    lambda: set_demo_status(
                        f"Demo: reachability {report.feasible_probes}/{report.total_probes} feasible"
                    ),
                )
            except Exception as exc:
                root.after(0, lambda error_text=str(exc): set_demo_status(f"Demo: probe failed ({error_text})"))

        threading.Thread(target=worker, daemon=True).start()

    def _on_sweep_complete(generation):
        if generation != demo_sweep["generation"]:
            return
        demo_sweep["active"] = False
        demo_sweep["timer_id"] = None
        if demo_panel.loop_var.get():
            start_orientation_sweep("sim")
            return
        set_demo_status("Demo: sweep complete")

    def _sweep_sim_step(generation):
        if generation != demo_sweep["generation"] or not demo_sweep["active"]:
            return
        plan = demo_sweep.get("plan")
        if plan is None:
            stop_orientation_sweep("Demo: no sweep plan")
            return
        index = int(demo_sweep.get("trajectory_index") or 0)
        if index >= len(plan.joint_trajectory_deg):
            _on_sweep_complete(generation)
            return

        apply_sim_keyframe(plan.joint_trajectory_deg[index])
        demo_sweep["trajectory_index"] = index + 1
        demo_sweep["timer_id"] = root.after(
            max(1, int(round(1000.0 / max(1.0, float(plan.update_rate_hz))))),
            lambda: _sweep_sim_step(generation),
        )

    def stop_orientation_sweep(status_text: str = "Demo: stop requested"):
        timer_id = demo_sweep.get("timer_id")
        if timer_id is not None:
            try:
                root.after_cancel(timer_id)
            except Exception:
                pass
        cancel_event = demo_sweep.get("cancel_event")
        if cancel_event is not None:
            try:
                cancel_event.set()
            except Exception:
                pass
        demo_sweep["generation"] += 1
        demo_sweep["active"] = False
        demo_sweep["plan"] = None
        demo_sweep["plan_cache_key"] = None
        demo_sweep["trajectory_index"] = 0
        demo_sweep["mode"] = None
        demo_sweep["timer_id"] = None
        demo_sweep["cancel_event"] = None
        set_demo_status(status_text)

    def start_orientation_sweep(mode):
        if str(mode) != "sim":
            set_demo_status("Demo: robot playback not implemented yet")
            return
        try:
            config = _build_demo_config()
        except Exception as exc:
            set_demo_status(f"Demo: {exc}")
            return
        plan_cache_key = _build_demo_plan_cache_key(mode, config)
        cached_plan = demo_sweep.get("plan_cache", {}).get(plan_cache_key)

        stop_orientation_sweep("Demo: starting sweep...")
        generation = demo_sweep["generation"]

        if cached_plan is not None:
            demo_sweep["active"] = True
            demo_sweep["plan"] = cached_plan
            demo_sweep["plan_cache_key"] = plan_cache_key
            demo_sweep["trajectory_index"] = 0
            demo_sweep["mode"] = "sim"
            set_demo_status("Demo: sim sweep active (cached)")
            _sweep_sim_step(generation)
            return

        set_demo_status("Demo: planning sweep...")

        def worker():
            try:
                joint_seed = get_sim_joint_angles() or [0.0] * 6
                plan = plan_orientation_sweep(
                    config=config,
                    q_seed_deg=joint_seed,
                    solve_ik_fn=solve_ik_for_pose,
                    cancel_check=lambda: generation != demo_sweep["generation"],
                    progress_cb=lambda _current, _total: None,
                )
            except Exception as exc:
                root.after(0, lambda error_text=str(exc): set_demo_status(f"Demo: planning failed ({error_text})"))
                return

            def start_plan():
                if generation != demo_sweep["generation"]:
                    return
                demo_sweep["active"] = True
                demo_sweep["plan"] = plan
                demo_sweep["plan_cache_key"] = plan_cache_key
                demo_sweep.setdefault("plan_cache", {})[plan_cache_key] = plan
                demo_sweep["trajectory_index"] = 0
                demo_sweep["mode"] = "sim"
                set_demo_status("Demo: sim sweep active")
                _sweep_sim_step(generation)

            root.after(0, start_plan)

        threading.Thread(target=worker, daemon=True).start()

    def run_sim_keyframe_move(generation: int, index: int, status_prefix: str, on_done):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        total = len(keyframes)
        if total == 0 or index < 0 or index >= total:
            stop_keyframe_playback("Keyframes: no frames to play")
            return

        record = keyframes[index]
        raw_target_angles = list(record.angles_deg[:6])
        target_angles = pybullet_ik.clamp_command_angles(raw_target_angles)
        clamp_delta_deg = max(
            [abs(float(raw) - float(clamped)) for raw, clamped in zip(raw_target_angles, target_angles)] or [0.0]
        )
        current_angles = get_sim_joint_angles() or []
        motion = resolve_keyframe_sim_motion(current_angles, target_angles)
        if motion is None:
            stop_keyframe_playback("Keyframes: simulation playback stopped")
            return

        apply_sim_keyframe(target_angles, motion_overrides=motion["overrides"])
        select_keyframe(index)
        keyframe_log.info(
            "Step sim move start generation=%d frame=%d/%d target=%s timeout_s=%.3f",
            generation,
            index + 1,
            total,
            [round(float(v), 3) for v in target_angles[:6]],
            float(motion["timeout_s"]),
        )
        set_pose_status(f"Status: {status_prefix.lower()} frame {index + 1}")
        set_keyframe_status(
            "%s %d/%d, d=%.1f deg, speed %.0f%%, accel %.0f%%%s"
            % (
                status_prefix,
                index + 1,
                total,
                motion["max_delta_deg"],
                motion["speed_pct"],
                motion["accel_pct"],
                "" if clamp_delta_deg <= 0.01 else ", clamped %.1f deg" % clamp_delta_deg,
            )
        )

        start_time_mono = time.monotonic()
        best_err_deg = {"value": float("inf")}
        last_progress_mono = {"value": start_time_mono}
        debug_state = {"poll_count": 0, "last_bucket": None}

        def monitor_sim_keyframe(stable_samples: int = 0, handoff_samples: int = 0):
            if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                return
            joint_angles = get_sim_joint_angles() or []
            now_mono = time.monotonic()
            elapsed_s = now_mono - start_time_mono
            if len(joint_angles) < 6:
                if elapsed_s >= motion["timeout_s"]:
                    stop_keyframe_playback("%s %d timed out" % (status_prefix, index + 1))
                    return
                keyframe_playback["after_id"] = root.after(
                    KEYFRAME_SIM_CHECK_MS,
                    lambda: monitor_sim_keyframe(stable_samples, handoff_samples),
                )
                return

            max_err_deg = max(abs(float(cur) - float(tgt)) for cur, tgt in zip(joint_angles[:6], target_angles[:6]))
            step_settings = resolve_keyframe_step_playback_settings()
            improving_now = best_err_deg["value"] - max_err_deg >= float(step_settings["progress_epsilon_deg"])
            debug_state["poll_count"] += 1
            if best_err_deg["value"] - max_err_deg >= KEYFRAME_SIM_PROGRESS_EPS_DEG:
                best_err_deg["value"] = max_err_deg
                last_progress_mono["value"] = now_mono
            elif max_err_deg < best_err_deg["value"]:
                best_err_deg["value"] = max_err_deg

            next_stable_samples = stable_samples + 1 if max_err_deg <= KEYFRAME_SIM_SETTLE_TOL_DEG else 0
            bucket = int(max_err_deg)
            if (
                debug_state["poll_count"] == 1
                or debug_state["poll_count"] % 10 == 0
                or debug_state["last_bucket"] != bucket
            ):
                debug_state["last_bucket"] = bucket
                keyframe_log.info(
                    "Step sim monitor generation=%d frame=%d/%d err=%.3f best=%.3f stable=%d->%d elapsed_s=%.3f",
                    generation,
                    index + 1,
                    total,
                    max_err_deg,
                    best_err_deg["value"],
                    stable_samples,
                    next_stable_samples,
                    elapsed_s,
                )
            if next_stable_samples >= KEYFRAME_SIM_STABLE_SAMPLES:
                keyframe_log.info(
                    "Step sim settle complete generation=%d frame=%d/%d err=%.3f elapsed_s=%.3f",
                    generation,
                    index + 1,
                    total,
                    max_err_deg,
                    elapsed_s,
                )
                on_done()
                return

            next_handoff_samples = 0

            stalled_s = now_mono - last_progress_mono["value"]
            if max_err_deg <= KEYFRAME_SIM_CLOSE_ENOUGH_TOL_DEG and stalled_s >= KEYFRAME_SIM_STALL_TIMEOUT_S:
                keyframe_log.info(
                    "Step sim accepted close-enough generation=%d frame=%d/%d err=%.3f stalled_s=%.3f elapsed_s=%.3f",
                    generation,
                    index + 1,
                    total,
                    max_err_deg,
                    stalled_s,
                    elapsed_s,
                )
                set_keyframe_status(
                    "%s %d/%d accepted at %.2f deg residual"
                    % (status_prefix, index + 1, total, max_err_deg)
                )
                on_done()
                return
            if elapsed_s >= motion["timeout_s"] and stalled_s >= KEYFRAME_SIM_STALL_TIMEOUT_S:
                keyframe_log.warning(
                    "Step sim timeout generation=%d frame=%d/%d err=%.3f best=%.3f stalled_s=%.3f elapsed_s=%.3f settings=%s",
                    generation,
                    index + 1,
                    total,
                    max_err_deg,
                    best_err_deg["value"],
                    stalled_s,
                    elapsed_s,
                    step_settings,
                )
                stop_keyframe_playback(
                    "%s %d timeout (err %.2f deg)" % (status_prefix, index + 1, max_err_deg)
                )
                return

            keyframe_playback["after_id"] = root.after(
                KEYFRAME_SIM_CHECK_MS,
                lambda: monitor_sim_keyframe(next_stable_samples, next_handoff_samples),
            )

        keyframe_playback["after_id"] = root.after(
            KEYFRAME_SIM_CHECK_MS,
            lambda: monitor_sim_keyframe(0, 0),
        )

    def run_robot_keyframe_start_alignment(generation: int, index: int, status_prefix: str, on_done):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        total = len(keyframes)
        if total == 0 or index < 0 or index >= total:
            stop_keyframe_playback("Keyframes: no frames to play")
            return
        if not comm_client.is_connected:
            stop_keyframe_playback("Keyframes: robot playback requires CAN connection")
            return

        record = keyframes[index]
        target_angles = list(record.angles_deg[:6])
        apply_sim_keyframe(target_angles)
        select_keyframe(index)
        set_pose_status(f"Status: {status_prefix.lower()} {index + 1}")
        set_keyframe_status(f"{status_prefix} {index + 1}/{total}...")

        def task():
            current_angles = comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)
            valid_current_angles = parse_robot_joint_angles(current_angles)
            if valid_current_angles is None:
                raise RuntimeError("Current encoder angles unavailable.")
            motion = resolve_keyframe_robot_step_motion(valid_current_angles, target_angles)
            if motion is None:
                raise RuntimeError("Invalid robot step motion settings.")
            keyframe_log.info(
                "Step robot start alignment generation=%d frame=%d/%d auto_sync=%s sync_duration_s=%.3f current=%s target=%s overrides=%s",
                generation,
                index + 1,
                total,
                bool(motion.get("auto_sync")),
                float(motion.get("sync_duration_s") or 0.0),
                [round(float(v), 3) for v in valid_current_angles[:6]],
                [round(float(v), 3) for v in target_angles[:6]],
                motion["overrides"],
            )
            move_result = execute_blocking_robot_move(
                target_angles,
                timeout_s=ROBOT_MOVE_COMPLETE_TIMEOUT_S,
                overrides=motion["overrides"],
            )
            return {"current_angles": valid_current_angles, "motion": motion, "move_result": move_result}

        def done(result, err):
            if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                return
            if err:
                stop_keyframe_playback(f"{status_prefix} error ({err})")
                set_encoder_status(f"{status_prefix} error ({err})")
                return
            motion = dict((result or {}).get("motion") or {})
            move_result = dict((result or {}).get("move_result") or {})
            final_angles = list(move_result.get("final_angles") or [])
            if final_angles:
                update_encoder_display(final_angles[:6])
            keyframe_log.info(
                "Step robot start alignment complete generation=%d frame=%d/%d final=%s details=%s",
                generation,
                index + 1,
                total,
                [round(float(v), 3) for v in final_angles[:6]],
                move_result.get("details"),
            )
            sync_suffix = ""
            if motion.get("auto_sync"):
                sync_suffix = f", sync {float(motion.get('sync_duration_s') or 0.0):.2f}s"
            set_keyframe_status(
                "%s %d/%d reached, speed %.0f%%, accel %.0f%%%s"
                % (
                    status_prefix,
                    index + 1,
                    total,
                    float(motion.get("speed_pct") or 0.0),
                    float(motion.get("accel_pct") or 0.0),
                    sync_suffix,
                )
            )
            set_encoder_status(f"{status_prefix} {index + 1}/{total} reached.")
            root.after(0, on_done)

        run_comm_task(
            task,
            done,
            task_name=f"robot_keyframe_start_align_gen{generation}_frame{index + 1}",
            timeout_s=ROBOT_MOVE_COMPLETE_TIMEOUT_S + 2.0,
            on_timeout=lambda idx=index, total_frames=total, prefix=status_prefix: (
                stop_keyframe_playback(f"{prefix} {idx + 1} timeout (start alignment)"),
                set_encoder_status(f"{prefix} {idx + 1}/{total_frames} start alignment timeout."),
            ),
        )

    def run_robot_keyframe_move(generation: int, index: int, status_prefix: str, on_done):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        total = len(keyframes)
        if total == 0 or index < 0 or index >= total:
            stop_keyframe_playback("Keyframes: no frames to play")
            return
        if not comm_client.is_connected:
            stop_keyframe_playback("Keyframes: robot playback requires CAN connection")
            return

        record = keyframes[index]
        target_angles = list(record.angles_deg[:6])
        apply_sim_keyframe(target_angles)
        select_keyframe(index)
        set_pose_status(f"Status: {status_prefix.lower()} frame {index + 1}")

        def task():
            current_angles = comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)
            valid_current_angles = parse_robot_joint_angles(current_angles)
            if valid_current_angles is None:
                raise RuntimeError("Current encoder angles unavailable.")
            motion = resolve_keyframe_robot_step_motion(valid_current_angles, target_angles)
            if motion is None:
                raise RuntimeError("Invalid robot step motion settings.")
            keyframe_log.info(
                "Step robot move start generation=%d frame=%d/%d auto_sync=%s sync_duration_s=%.3f current=%s target=%s overrides=%s",
                generation,
                index + 1,
                total,
                bool(motion.get("auto_sync")),
                float(motion.get("sync_duration_s") or 0.0),
                [round(float(v), 3) for v in valid_current_angles[:6]],
                [round(float(v), 3) for v in target_angles[:6]],
                motion["overrides"],
            )
            ok_all, details = comm_client.send_joint_targets_detailed(
                target_angles_deg=target_angles,
                current_angles_deg=valid_current_angles,
                overrides=motion["overrides"],
            )
            if not ok_all:
                failed_joint_ids = [detail.get("joint_id") for detail in details if not detail.get("ok")]
                raise RuntimeError(f"Failed to move joints {failed_joint_ids}.")
            return {"current_angles": valid_current_angles, "details": details, "motion": motion}

        def done(result, err):
            if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                return
            if err:
                stop_keyframe_playback(f"{status_prefix} error ({err})")
                set_encoder_status(f"{status_prefix} error ({err})")
                return
            motion = dict((result or {}).get("motion") or {})
            current_angles = list((result or {}).get("current_angles") or [])
            if current_angles:
                update_encoder_display(current_angles)
            sync_suffix = ""
            if motion.get("auto_sync"):
                sync_suffix = f", sync {float(motion.get('sync_duration_s') or 0.0):.2f}s"
            set_keyframe_status(
                "%s %d/%d, speed %.0f%%, accel %.0f%%%s"
                % (
                    status_prefix,
                    index + 1,
                    total,
                    float(motion.get("speed_pct") or 0.0),
                    float(motion.get("accel_pct") or 0.0),
                    sync_suffix,
                )
            )
            set_encoder_status(f"{status_prefix} {index + 1}/{total} command sent.")
            start_time_mono = time.monotonic()
            best_err_deg = {"value": float("inf")}
            last_progress_mono = {"value": start_time_mono}
            debug_state = {"poll_count": 0, "last_bucket": None, "busy_defers": 0}
            commanded_deltas_deg = [
                abs(float(tgt) - float(cur))
                for cur, tgt in zip(current_angles[:6], target_angles[:6])
            ]
            monitor_state = {
                "last_angles": None,
                "stall_counts": [0] * min(len(target_angles[:6]), len(commanded_deltas_deg)),
                "retry_attempts": 0,
                "retry_in_flight": False,
            }
            keyframe_log.info(
                "Step robot command acknowledged generation=%d frame=%d/%d details=%s",
                generation,
                index + 1,
                total,
                (result or {}).get("details"),
            )

            def monitor_robot_keyframe(stable_samples: int = 0, handoff_samples: int = 0):
                if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                    return
                if comm_busy["value"]:
                    debug_state["busy_defers"] += 1
                    if debug_state["busy_defers"] == 1 or debug_state["busy_defers"] % 10 == 0:
                        keyframe_log.info(
                            "Step robot monitor deferred generation=%d frame=%d/%d comm_busy=%s defers=%d",
                            generation,
                            index + 1,
                            total,
                            bool(comm_busy["value"]),
                            debug_state["busy_defers"],
                        )
                    keyframe_playback["after_id"] = root.after(
                        KEYFRAME_ROBOT_CHECK_MS,
                        lambda: monitor_robot_keyframe(stable_samples, handoff_samples),
                    )
                    return

                def poll_task():
                    return comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)

                def poll_done(read_result, read_err):
                    if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                        return
                    now_mono = time.monotonic()
                    elapsed_s = now_mono - start_time_mono
                    if read_err:
                        stop_keyframe_playback(f"{status_prefix} read error ({read_err})")
                        set_encoder_status(f"{status_prefix} read error ({read_err})")
                        return

                    current_angles = []
                    for value in read_result or []:
                        if value is None:
                            current_angles = []
                            break
                        current_angles.append(float(value))
                    if len(current_angles) >= 6:
                        current_angles = current_angles[:6]
                        update_encoder_display(current_angles)
                        per_joint_errors = [
                            abs(float(cur) - float(tgt))
                            for cur, tgt in zip(current_angles, target_angles)
                        ]
                        max_err_deg = max(per_joint_errors)
                        step_settings = resolve_keyframe_step_playback_settings()
                        improving_now = best_err_deg["value"] - max_err_deg >= float(
                            step_settings["progress_epsilon_deg"]
                        )
                        debug_state["poll_count"] += 1
                        if best_err_deg["value"] - max_err_deg >= KEYFRAME_ROBOT_PROGRESS_EPS_DEG:
                            best_err_deg["value"] = max_err_deg
                            last_progress_mono["value"] = now_mono
                        elif max_err_deg < best_err_deg["value"]:
                            best_err_deg["value"] = max_err_deg

                        prev_angles = monitor_state["last_angles"]
                        if prev_angles is None or len(prev_angles) != len(current_angles):
                            monitor_state["last_angles"] = list(current_angles)
                            joint_motion_since_last = [float("inf")] * len(current_angles)
                            monitor_state["stall_counts"] = [0] * len(current_angles)
                        else:
                            joint_motion_since_last = [
                                abs(float(cur) - float(prev))
                                for cur, prev in zip(current_angles, prev_angles)
                            ]
                            monitor_state["last_angles"] = list(current_angles)
                            progress_threshold = float(step_settings["progress_epsilon_deg"])
                            min_command_delta = float(step_settings["stall_retry_min_command_delta_deg"])
                            min_error_deg = float(step_settings["stall_retry_min_error_deg"])
                            updated_stall_counts = list(monitor_state["stall_counts"])
                            for joint_idx, joint_err in enumerate(per_joint_errors):
                                commanded_delta = (
                                    commanded_deltas_deg[joint_idx]
                                    if joint_idx < len(commanded_deltas_deg)
                                    else 0.0
                                )
                                moved_enough = (
                                    joint_idx < len(joint_motion_since_last)
                                    and joint_motion_since_last[joint_idx] >= progress_threshold
                                )
                                if joint_err >= min_error_deg and commanded_delta >= min_command_delta and not moved_enough:
                                    updated_stall_counts[joint_idx] += 1
                                else:
                                    updated_stall_counts[joint_idx] = 0
                            monitor_state["stall_counts"] = updated_stall_counts

                        next_stable_samples = (
                            stable_samples + 1 if max_err_deg <= KEYFRAME_ROBOT_SETTLE_TOL_DEG else 0
                        )
                        bucket = int(max_err_deg)
                        if (
                            debug_state["poll_count"] == 1
                            or debug_state["poll_count"] % 8 == 0
                            or debug_state["last_bucket"] != bucket
                        ):
                            debug_state["last_bucket"] = bucket
                            keyframe_log.info(
                                "Step robot monitor generation=%d frame=%d/%d err=%.3f best=%.3f stable=%d->%d elapsed_s=%.3f current=%s",
                                generation,
                                index + 1,
                                total,
                                max_err_deg,
                                best_err_deg["value"],
                                stable_samples,
                                next_stable_samples,
                                elapsed_s,
                                [round(float(v), 3) for v in current_angles[:6]],
                            )

                        retry_threshold = int(step_settings["stall_retry_poll_threshold"])
                        retry_max_attempts = int(step_settings["stall_retry_max_attempts"])
                        if (
                            bool(step_settings["stall_retry_enabled"])
                            and not monitor_state["retry_in_flight"]
                            and monitor_state["retry_attempts"] < retry_max_attempts
                        ):
                            stalled_joint_indices = [
                                joint_idx
                                for joint_idx, stall_count in enumerate(monitor_state["stall_counts"])
                                if stall_count >= retry_threshold
                                and joint_idx < len(per_joint_errors)
                                and per_joint_errors[joint_idx] >= float(step_settings["stall_retry_min_error_deg"])
                            ]
                            if stalled_joint_indices:
                                retry_joint_ids = [joint_idx + 1 for joint_idx in stalled_joint_indices]
                                monitor_state["retry_in_flight"] = True
                                monitor_state["retry_attempts"] += 1
                                retry_attempt = int(monitor_state["retry_attempts"])
                                keyframe_log.warning(
                                    "Step robot stall retry generation=%d frame=%d/%d attempt=%d joints=%s "
                                    "errors=%s current=%s target=%s",
                                    generation,
                                    index + 1,
                                    total,
                                    retry_attempt,
                                    retry_joint_ids,
                                    [
                                        round(float(per_joint_errors[joint_idx]), 3)
                                        for joint_idx in stalled_joint_indices
                                    ],
                                    [round(float(v), 3) for v in current_angles[:6]],
                                    [round(float(v), 3) for v in target_angles[:6]],
                                )

                                def retry_task(
                                    retry_current_angles=list(current_angles),
                                    retry_joint_ids=list(retry_joint_ids),
                                ):
                                    return comm_client.send_joint_targets_detailed(
                                        target_angles_deg=target_angles,
                                        current_angles_deg=retry_current_angles,
                                        overrides=motion["overrides"],
                                        joint_ids=retry_joint_ids,
                                    )

                                def retry_done(retry_result, retry_err, attempt=retry_attempt, joint_ids=list(retry_joint_ids)):
                                    monitor_state["retry_in_flight"] = False
                                    if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                                        return
                                    if retry_err:
                                        stop_keyframe_playback(f"{status_prefix} retry error ({retry_err})")
                                        set_encoder_status(f"{status_prefix} retry error ({retry_err})")
                                        return
                                    retry_ok_all, retry_details = retry_result or (False, [])
                                    keyframe_log.info(
                                        "Step robot retry acknowledged generation=%d frame=%d/%d attempt=%d joints=%s details=%s",
                                        generation,
                                        index + 1,
                                        total,
                                        attempt,
                                        joint_ids,
                                        retry_details,
                                    )
                                    if not retry_ok_all:
                                        failed_joint_ids = [
                                            detail.get("joint_id") for detail in retry_details if not detail.get("ok")
                                        ]
                                        stop_keyframe_playback(
                                            f"{status_prefix} retry failed ({failed_joint_ids})"
                                        )
                                        set_encoder_status(
                                            f"{status_prefix} {index + 1}/{total} retry failed."
                                        )
                                        return
                                    last_progress_mono["value"] = time.monotonic()
                                    monitor_state["last_angles"] = list(current_angles)
                                    monitor_state["stall_counts"] = [0] * len(current_angles)
                                    keyframe_playback["after_id"] = root.after(
                                        KEYFRAME_ROBOT_CHECK_MS,
                                        lambda: monitor_robot_keyframe(0, 0),
                                    )

                                run_comm_task(
                                    retry_task,
                                    retry_done,
                                    task_name=f"robot_keyframe_retry_gen{generation}_frame{index + 1}_attempt{retry_attempt}",
                                    timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S,
                                    on_timeout=lambda idx=index, total_frames=total, prefix=status_prefix: (
                                        stop_keyframe_playback(f"{prefix} {idx + 1} retry timeout"),
                                        set_encoder_status(f"{prefix} {idx + 1}/{total_frames} retry timeout."),
                                    ),
                                )
                                return

                        if next_stable_samples >= KEYFRAME_ROBOT_STABLE_SAMPLES:
                            keyframe_log.info(
                                "Step robot settle complete generation=%d frame=%d/%d err=%.3f elapsed_s=%.3f",
                                generation,
                                index + 1,
                                total,
                                max_err_deg,
                                elapsed_s,
                            )
                            set_encoder_status(f"{status_prefix} {index + 1}/{total} reached.")
                            root.after(0, on_done)
                            return

                        next_handoff_samples = 0

                        stalled_s = now_mono - last_progress_mono["value"]
                        if elapsed_s >= KEYFRAME_ROBOT_TIMEOUT_S and stalled_s >= KEYFRAME_ROBOT_STALL_TIMEOUT_S:
                            worst_joint_index, worst_joint_err = max(
                                enumerate(per_joint_errors),
                                key=lambda item: item[1],
                            )
                            per_joint_summary = ", ".join(
                                f"J{joint_index + 1}={joint_err:.3f}"
                                for joint_index, joint_err in enumerate(per_joint_errors)
                            )
                            keyframe_log.warning(
                                "Step robot timeout generation=%d frame=%d/%d err=%.3f best=%.3f "
                                "worst_joint=J%d worst_err=%.3f stalled_s=%.3f elapsed_s=%.3f "
                                "per_joint_err_deg=[%s] target=%s current=%s settings=%s",
                                generation,
                                index + 1,
                                total,
                                max_err_deg,
                                best_err_deg["value"],
                                worst_joint_index + 1,
                                worst_joint_err,
                                stalled_s,
                                elapsed_s,
                                per_joint_summary,
                                [round(float(v), 3) for v in target_angles[:6]],
                                [round(float(v), 3) for v in current_angles[:6]],
                                step_settings,
                            )
                            stop_keyframe_playback(
                                f"{status_prefix} {index + 1} timeout (err {max_err_deg:.2f} deg)"
                            )
                            set_encoder_status(
                                f"{status_prefix} {index + 1} timeout (err {max_err_deg:.2f} deg)"
                            )
                            return
                    else:
                        next_stable_samples = stable_samples
                        next_handoff_samples = handoff_samples
                        keyframe_log.info(
                            "Step robot monitor invalid-encoder generation=%d frame=%d/%d elapsed_s=%.3f read_result=%s",
                            generation,
                            index + 1,
                            total,
                            elapsed_s,
                            read_result,
                        )
                        if elapsed_s >= KEYFRAME_ROBOT_TIMEOUT_S:
                            keyframe_log.warning(
                                "Step robot timeout no-settle generation=%d frame=%d/%d elapsed_s=%.3f",
                                generation,
                                index + 1,
                                total,
                                elapsed_s,
                            )
                            stop_keyframe_playback(f"{status_prefix} {index + 1} timeout (no encoder settle)")
                            set_encoder_status(f"{status_prefix} {index + 1} timeout (no encoder settle)")
                            return

                    keyframe_playback["after_id"] = root.after(
                        KEYFRAME_ROBOT_CHECK_MS,
                        lambda: monitor_robot_keyframe(next_stable_samples, next_handoff_samples),
                    )

                run_comm_task(
                    poll_task,
                    poll_done,
                    task_name=f"robot_keyframe_poll_gen{generation}_frame{index + 1}",
                    timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S,
                    on_timeout=lambda idx=index, total_frames=total, prefix=status_prefix: (
                        stop_keyframe_playback(f"{prefix} {idx + 1} timeout (encoder poll stalled)"),
                        set_encoder_status(f"{prefix} {idx + 1}/{total_frames} encoder poll timeout."),
                    ),
                )

            keyframe_playback["after_id"] = root.after(
                KEYFRAME_ROBOT_CHECK_MS,
                lambda: monitor_robot_keyframe(0, 0),
            )

        run_comm_task(
            task,
            done,
            task_name=f"robot_keyframe_move_gen{generation}_frame{index + 1}",
            timeout_s=COMM_TASK_DEFAULT_TIMEOUT_S,
            on_timeout=lambda idx=index, total_frames=total, prefix=status_prefix: (
                stop_keyframe_playback(f"{prefix} {idx + 1} command timeout"),
                set_encoder_status(f"{prefix} {idx + 1}/{total_frames} command timeout."),
            ),
        )

    def prime_interpolated_keyframe_playback(plan, settings: dict):
        segment_end_indices = []
        last_index = 0
        for segment in plan.segments:
            last_index += max(1, int(segment.sample_count) - 1)
            segment_end_indices.append(last_index)
        keyframe_playback["execution_mode"] = "interpolated"
        keyframe_playback["plan"] = plan
        keyframe_playback["settings"] = dict(settings)
        keyframe_playback["trajectory_index"] = 1
        keyframe_playback["segment_index"] = 0
        keyframe_playback["segment_end_indices"] = segment_end_indices
        keyframe_playback["plan_estimated_duration_s"] = float(plan.estimated_duration_s)
        keyframe_playback["started_mono"] = None
        keyframe_playback["plan_ready"] = True

    def is_keyframe_pingpong_enabled() -> bool:
        return bool(keyframes_panel.pingpong_var.get())

    def is_keyframe_wrap_loop_enabled() -> bool:
        return bool(keyframes_panel.loop_var.get()) and not is_keyframe_pingpong_enabled()

    def build_interpolated_plan_cache_key(mode: str, settings: dict) -> tuple:
        keyframe_signature = tuple(
            tuple(round(float(value), 6) for value in record.angles_deg[:6])
            for record in keyframes
        )
        return (
            mode,
            round(float(settings["speed_mm_s"]), 6),
            round(float(settings["accel_mm_s2"]), 6),
            round(float(settings["update_rate_hz"]), 6),
            str(settings.get("ik_mode") or "default"),
            str(settings.get("sampling_mode") or "update_rate"),
            str(settings.get("planner_kind") or "baseline"),
            round(float(settings.get("position_tolerance_mm") or 0.0), 6),
            round(float(settings.get("orientation_tolerance_deg") or 0.0), 6),
            round(float(settings.get("max_joint_step_deg") or 10.0), 6),
            round(float(settings.get("terminal_hold_s") or 0.0), 6),
            keyframe_signature,
        )

    def reverse_interpolated_plan(plan: KeyframeInterpolationPlan) -> KeyframeInterpolationPlan:
        reversed_segments = []
        total_segments = len(plan.segments)
        total_keyframes = len(keyframes)
        for new_index, segment in enumerate(reversed(plan.segments), start=1):
            reversed_segments.append(
                InterpolatedSegment(
                    segment_index=new_index,
                    start_keyframe_index=max(1, total_keyframes - segment.end_keyframe_index + 1),
                    end_keyframe_index=max(1, total_keyframes - segment.start_keyframe_index + 1),
                    joint_trajectory_deg=[list(sample[:6]) for sample in reversed(segment.joint_trajectory_deg)],
                    sample_count=segment.sample_count,
                    duration_s=segment.duration_s,
                    max_joint_step_deg=segment.max_joint_step_deg,
                )
            )
        return KeyframeInterpolationPlan(
            segments=reversed_segments,
            full_joint_trajectory_deg=[list(sample[:6]) for sample in reversed(plan.full_joint_trajectory_deg)],
            update_rate_hz=plan.update_rate_hz,
            estimated_duration_s=plan.estimated_duration_s,
            planner_stats=plan.planner_stats,
        )

    def resolve_next_step_keyframe(index: int, direction: int, total: int) -> tuple[int | None, int]:
        next_index = index + direction
        if 0 <= next_index < total:
            return next_index, direction
        if is_keyframe_pingpong_enabled():
            bounced_direction = -direction
            bounced_index = index + bounced_direction
            if 0 <= bounced_index < total:
                return bounced_index, bounced_direction
            return None, bounced_direction
        if is_keyframe_wrap_loop_enabled():
            return 0, direction
        return None, direction

    def update_interpolated_keyframe_progress(
        generation: int,
        status_prefix: str,
        trajectory_index: int,
        measured_err_deg: float | None = None,
    ):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        plan = keyframe_playback.get("plan")
        if plan is None or not plan.full_joint_trajectory_deg:
            return
        total_segments = max(1, len(plan.segments))
        total_samples = max(1, len(plan.full_joint_trajectory_deg) - 1)
        segment_index = max(0, total_segments - 1)
        for idx, end_index in enumerate(keyframe_playback.get("segment_end_indices") or []):
            if trajectory_index <= end_index:
                segment_index = idx
                break
        keyframe_playback["trajectory_index"] = int(trajectory_index)
        keyframe_playback["segment_index"] = segment_index
        if int(keyframe_playback.get("interpolation_direction") or 1) < 0:
            selected_index = max(len(keyframes) - segment_index - 2, 0)
        else:
            selected_index = min(segment_index + 1, len(keyframes) - 1)
        select_keyframe(selected_index)
        started_mono = keyframe_playback.get("started_mono")
        elapsed_s = max(0.0, time.monotonic() - started_mono) if started_mono else 0.0
        total_duration_s = float(keyframe_playback.get("plan_estimated_duration_s") or plan.estimated_duration_s or 0.0)
        extra = f", err {float(measured_err_deg):.2f} deg" if measured_err_deg is not None else ""
        set_pose_status(
            "Status: %s segment %d/%d"
            % (status_prefix.lower(), segment_index + 1, total_segments)
        )
        set_keyframe_status(
            "%s interp %d/%d seg %d/%d t=%.1f/%.1fs%s"
            % (
                status_prefix,
                min(trajectory_index, total_samples),
                total_samples,
                segment_index + 1,
                total_segments,
                elapsed_s,
                total_duration_s,
                extra,
            ),
            echo_terminal=False,
        )
        if status_prefix == "Keyframes: robot":
            set_encoder_status(
                "Keyframes: robot interp %d/%d seg %d/%d%s"
                % (
                    min(trajectory_index, total_samples),
                    total_samples,
                    segment_index + 1,
                    total_segments,
                    extra,
                )
            )

    def restart_or_finish_interpolated_keyframe_playback(generation: int):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        mode = keyframe_playback.get("mode")
        set_keyframe_streaming_active(False)
        set_keyframe_hardware_streaming_active(False)
        if is_keyframe_pingpong_enabled():
            keyframe_playback["interpolation_direction"] = -int(keyframe_playback.get("interpolation_direction") or 1)
            keyframe_playback["trajectory_index"] = 1
            keyframe_playback["segment_index"] = 0
            keyframe_playback["started_mono"] = None
            keyframe_playback["plan_ready"] = False
            keyframe_playback["preroll_done"] = True
            start_interpolated_keyframe_playback(mode, generation, run_preroll=False)
            return
        if not is_keyframe_wrap_loop_enabled():
            if mode == "robot":
                stop_keyframe_playback("Keyframes: robot playback complete")
            else:
                stop_keyframe_playback("Keyframes: simulation playback complete")
            return
        keyframe_playback["interpolation_direction"] = 1
        keyframe_playback["trajectory_index"] = 1
        keyframe_playback["segment_index"] = 0
        keyframe_playback["started_mono"] = None
        keyframe_playback["plan_ready"] = False
        keyframe_playback["preroll_done"] = False
        start_interpolated_keyframe_playback(mode, generation, run_preroll=True)

    def maybe_start_interpolated_execution(generation: int, mode: str):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        if not keyframe_playback.get("plan_ready") or not keyframe_playback.get("preroll_done"):
            return
        keyframe_log.info(
            "Interpolated startup complete generation=%d mode=%s plan_ready=%s preroll_done=%s",
            generation,
            mode,
            keyframe_playback.get("plan_ready"),
            keyframe_playback.get("preroll_done"),
        )
        if mode == "robot":
            start_interpolated_robot_execution(generation)
        else:
            start_interpolated_sim_execution(generation)

    def on_interpolated_preroll_complete(generation: int, mode: str):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        keyframe_playback["preroll_done"] = True
        keyframe_log.info(
            "Interpolated preroll complete generation=%d mode=%s plan_ready=%s",
            generation,
            mode,
            keyframe_playback.get("plan_ready"),
        )
        maybe_start_interpolated_execution(generation, mode)

    def start_interpolated_sim_execution(generation: int):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        plan = keyframe_playback.get("plan")
        if plan is None or len(plan.full_joint_trajectory_deg) <= 1:
            restart_or_finish_interpolated_keyframe_playback(generation)
            return
        keyframe_log.info(
            "Interpolated sim execution start generation=%d samples=%d segments=%d rate_hz=%.3f estimated_duration_s=%.3f",
            generation,
            len(plan.full_joint_trajectory_deg),
            len(plan.segments),
            plan.update_rate_hz,
            plan.estimated_duration_s,
        )
        keyframe_playback["trajectory_index"] = 1
        keyframe_playback["segment_index"] = 0
        keyframe_playback["started_mono"] = time.monotonic()
        set_keyframe_streaming_active(True)

        def step():
            if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                return
            active_plan = keyframe_playback.get("plan")
            if active_plan is None:
                stop_keyframe_playback("Keyframes: missing interpolation plan")
                return
            trajectory = active_plan.full_joint_trajectory_deg
            trajectory_index = int(keyframe_playback.get("trajectory_index") or 1)
            if trajectory_index >= len(trajectory):
                keyframe_log.info(
                    "Interpolated sim execution complete generation=%d samples_sent=%d",
                    generation,
                    len(trajectory) - 1,
                )
                restart_or_finish_interpolated_keyframe_playback(generation)
                return
            apply_sim_keyframe(trajectory[trajectory_index])
            keyframe_log.info(
                "Interpolated sim sample generation=%d sample=%d/%d target_q=%s",
                generation,
                trajectory_index,
                len(trajectory) - 1,
                [round(float(value), 4) for value in trajectory[trajectory_index][:6]],
            )
            update_interpolated_keyframe_progress(generation, "Keyframes: sim", trajectory_index)
            keyframe_playback["trajectory_index"] = trajectory_index + 1
            delay_ms = max(1, int(round(1000.0 / max(active_plan.update_rate_hz, 1e-6))))
            keyframe_playback["after_id"] = root.after(delay_ms, step)

        step()

    def on_interpolated_robot_failure(generation: int, message: str):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        keyframe_log.error("Interpolated robot execution failed generation=%d message=%s", generation, message)
        set_keyframe_hardware_streaming_active(False)
        set_encoder_status(message)
        stop_keyframe_playback(message)

    def on_interpolated_robot_complete(generation: int):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        keyframe_log.info("Interpolated robot execution complete generation=%d", generation)
        set_keyframe_streaming_active(False)
        set_keyframe_hardware_streaming_active(False)
        if keyframe_playback.get("comm_reserved"):
            keyframe_playback["comm_reserved"] = False
            set_comm_busy(False)
        restart_or_finish_interpolated_keyframe_playback(generation)

    def monitor_interpolated_robot_stream(generation: int):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        if can_streamer.is_running():
            keyframe_playback["after_id"] = root.after(
                100,
                lambda: monitor_interpolated_robot_stream(generation),
            )
            return

        with can_controller.state["lock"]:
            controller_active = bool(can_controller.state.get("active"))
            finalize_in_progress = bool(can_controller.state.get("finalize_in_progress"))
            phase = str(can_controller.state.get("phase") or "UNKNOWN").upper()
            fault_reason = can_controller.state.get("fault_reason")

        if controller_active or finalize_in_progress:
            keyframe_playback["after_id"] = root.after(
                100,
                lambda: monitor_interpolated_robot_stream(generation),
            )
            return

        keyframe_playback["after_id"] = None
        if interpolated_robot_monitor_state.get("final_read_pending"):
            return
        interpolated_robot_monitor_state["final_read_pending"] = True

        def task():
            return comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)

        def done(read_result, read_err):
            interpolated_robot_monitor_state["final_read_pending"] = False
            if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                return

            with can_controller.state["lock"]:
                current_phase = str(can_controller.state.get("phase") or "UNKNOWN").upper()
                current_fault_reason = can_controller.state.get("fault_reason")

            final_angles = None
            if read_err:
                keyframe_log.warning(
                    "Interpolated robot final encoder read failed generation=%d error=%s",
                    generation,
                    read_err,
                )
            else:
                final_angles = parse_robot_joint_angles(read_result)
            if final_angles is not None:
                update_encoder_display(final_angles)
                keyframe_log.info(
                    "Interpolated robot final encoder generation=%d phase=%s final_q=%s",
                    generation,
                    current_phase,
                    [round(float(value), 4) for value in final_angles[:6]],
                )

            if current_phase == "COMPLETE":
                on_interpolated_robot_complete(generation)
                return

            if current_phase == "PAUSED":
                on_interpolated_robot_failure(generation, "Keyframes: interpolated robot playback paused")
                return

            message = "Keyframes: interpolated robot playback failed"
            if current_fault_reason:
                message = f"{message} ({current_fault_reason})"
            on_interpolated_robot_failure(generation, message)

        run_background_comm_task(task, done, task_name="interpolated_robot_final_encoder_read")

    def start_interpolated_robot_execution(generation: int):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        plan = keyframe_playback.get("plan")
        if plan is None or len(plan.full_joint_trajectory_deg) <= 1:
            restart_or_finish_interpolated_keyframe_playback(generation)
            return
        settings = dict(keyframe_playback.get("settings") or {})
        playback_settings = build_keyframe_robot_playback_settings(settings)
        keyframe_log.info(
            "Interpolated robot execution start generation=%d samples=%d segments=%d plan_rate_hz=%.3f "
            "estimated_duration_s=%.3f planner_speed_mm_s=%.3f playback_speed_mm_s=%.3f "
            "playback_accel_mm_s2=%.3f playback_rate_hz=%.3f pause_simulation=%s",
            generation,
            len(plan.full_joint_trajectory_deg),
            len(plan.segments),
            plan.update_rate_hz,
            plan.estimated_duration_s,
            float(settings.get("speed_mm_s") or 0.0),
            float(playback_settings.get("speed_mm_s") or 0.0),
            float(playback_settings.get("accel_mm_s2") or 0.0),
            float(playback_settings.get("update_rate_hz") or 0.0),
            bool(settings.get("pause_simulation_during_robot_playback", True)),
        )
        keyframe_playback["trajectory_index"] = 0
        keyframe_playback["segment_index"] = 0
        keyframe_playback["started_mono"] = time.monotonic()
        if not keyframe_playback.get("comm_reserved"):
            keyframe_playback["comm_reserved"] = True
            set_comm_busy(True, "Keyframes: interpolated robot playback active")
        set_encoder_status("Keyframes: interpolated robot playback active")
        set_keyframe_streaming_active(True)
        set_keyframe_hardware_streaming_active(
            bool(settings.get("pause_simulation_during_robot_playback", True))
        )

        def on_stream_step(step_index: int, total_steps: int, target_angles: list[float]):
            if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                return
            keyframe_log.info(
                "Interpolated robot stream sample generation=%d sample=%d/%d target_q=%s",
                generation,
                step_index,
                total_steps,
                [round(float(value), 4) for value in target_angles[:6]],
            )
            root.after(
                0,
                lambda idx=step_index: update_interpolated_keyframe_progress(
                    generation,
                    "Keyframes: robot",
                    idx,
                ),
            )

        def worker():
            started, error_text = stream_startup.start_keyframe_can_stream(
                plan=plan,
                playback_settings=playback_settings,
                set_encoder_status=set_encoder_status,
                update_encoder_display=update_encoder_display,
                step_cb=on_stream_step,
            )

            def on_started():
                if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                    return
                if not started:
                    set_keyframe_streaming_active(False)
                    set_keyframe_hardware_streaming_active(False)
                    on_interpolated_robot_failure(
                        generation,
                        f"Keyframes: interpolated robot playback start failed ({error_text})",
                    )
                    return
                monitor_interpolated_robot_stream(generation)

            root.after(0, on_started)

        threading.Thread(target=worker, daemon=True).start()

    def start_interpolated_keyframe_playback(mode: str, generation: int, run_preroll: bool = True):
        settings = resolve_keyframe_interpolation_settings(mode)
        if settings is None:
            stop_keyframe_playback("Keyframes: interpolation setup failed")
            return
        plan_settings = build_keyframe_plan_settings(mode, settings)
        runtime_settings = dict(settings.get("settings") or {})
        runtime_velocity_cfg = dict(runtime_settings.get("can_velocity_stream") or {})
        plan_cache_key = build_interpolated_plan_cache_key(mode, plan_settings)
        cached_plan = keyframe_playback.get("plan_cache", {}).get(plan_cache_key)
        keyframe_playback["plan_ready"] = False
        keyframe_playback["preroll_done"] = not run_preroll
        keyframe_log.info(
            "Interpolation requested generation=%d mode=%s keyframes=%d direction=%d preroll=%s "
            "planner_kind=%s speed_mm_s=%.3f accel_mm_s2=%.3f requested_rate_hz=%.3f effective_rate_hz=%.3f",
            generation,
            mode,
            len(keyframes),
            int(keyframe_playback.get("interpolation_direction") or 1),
            bool(run_preroll),
            str(plan_settings.get("planner_kind") or "baseline"),
            plan_settings["speed_mm_s"],
            plan_settings["accel_mm_s2"],
            plan_settings["update_rate_hz"],
            plan_settings["update_rate_hz"],
        )
        keyframe_log.info(
            "Interpolation config generation=%d mode=%s ik_mode=%s sampling_mode=%s "
            "position_tolerance_mm=%.3f orientation_tolerance_deg=%.3f max_joint_step_deg=%.3f "
            "feedback_mode=%s feedback_rate_hz=%.3f worker_joints_per_cycle=%d "
            "continuous_settle_enabled=%s capture_boost_enabled=%s capture_handoff_ready_dwell_s=%.3f "
            "capture_handoff_max_joint_error_deg=%.3f capture_handoff_max_cart_error_mm=%.3f "
            "playback_speed_mm_s=%.3f playback_accel_mm_s2=%.3f playback_rate_hz=%.3f "
            "terminal_hold_s=%.3f pause_simulation=%s",
            generation,
            mode,
            str(settings.get("ik_mode") or "default"),
            str(settings.get("sampling_mode") or "update_rate"),
            float(settings.get("position_tolerance_mm") or 0.0),
            float(settings.get("orientation_tolerance_deg") or 0.0),
            float(settings.get("max_joint_step_deg") or 10.0),
            str(runtime_velocity_cfg.get("feedback_during_stream") or "background"),
            float(parse_float_entry(runtime_velocity_cfg.get("feedback_rate_hz")) or 25.0),
            int(parse_float_entry(runtime_velocity_cfg.get("background_worker_joints_per_cycle")) or 1),
            bool(runtime_velocity_cfg.get("continuous_settle_enabled", False)),
            bool(runtime_velocity_cfg.get("capture_feedback_boost_enabled", False)),
            float(parse_float_entry(runtime_velocity_cfg.get("capture_handoff_ready_dwell_s")) or 0.0),
            float(parse_float_entry(runtime_velocity_cfg.get("capture_handoff_max_joint_error_deg")) or 0.0),
            float(parse_float_entry(runtime_velocity_cfg.get("capture_handoff_max_cart_error_mm")) or 0.0),
            float(settings.get("robot_playback_speed_mm_s") or settings["speed_mm_s"]),
            float(settings.get("robot_playback_accel_mm_s2") or settings["accel_mm_s2"]),
            float(settings.get("robot_playback_update_rate_hz") or settings["update_rate_hz"]),
            float(settings.get("terminal_hold_s") or 0.0),
            bool(settings.get("pause_simulation_during_robot_playback", True)),
        )
        if cached_plan is not None:
            active_plan = (
                cached_plan
                if int(keyframe_playback.get("interpolation_direction") or 1) > 0
                else reverse_interpolated_plan(cached_plan)
            )
            keyframe_log.info(
                "Interpolation plan cache hit generation=%d mode=%s direction=%d",
                generation,
                mode,
                int(keyframe_playback.get("interpolation_direction") or 1),
            )
            prime_interpolated_keyframe_playback(active_plan, settings)
            set_keyframe_status(
                "Keyframes: cached %d segments, %d samples, %.1fs at %.1f Hz"
                % (
                    len(active_plan.segments),
                    max(0, len(active_plan.full_joint_trajectory_deg) - 1),
                    active_plan.estimated_duration_s,
                    active_plan.update_rate_hz,
                )
            )
            maybe_start_interpolated_execution(generation, mode)
        else:
            set_keyframe_status("Keyframes: planning interpolated trajectory...")
            set_pose_status(f"Status: planning {mode} interpolation")

            keyframe_angles = [list(record.angles_deg[:6]) for record in keyframes]
            keyframe_log.info("Interpolation keyframes=%s", keyframe_angles)

            def worker():
                try:
                    stop_event_ref = keyframe_playback.get("stop_event")

                    def is_cancelled() -> bool:
                        return (
                            generation != keyframe_playback["generation"]
                            or not keyframe_playback["active"]
                            or bool(stop_event_ref is not None and stop_event_ref.is_set())
                        )

                    def on_progress(info: dict):
                        if is_cancelled():
                            return
                        segment_index = int(info.get("segment_index", 0)) + 1
                        segment_count = int(info.get("segment_count", 0))
                        sample_index = int(info.get("sample_index", 0))
                        sample_count = int(info.get("sample_count", 0))
                        elapsed_s = float(info.get("elapsed_s", 0.0))
                        root.after(
                            0,
                            lambda: set_keyframe_status(
                                "Keyframes: planning seg %d/%d sample %d/%d (%.1fs)"
                                % (
                                    segment_index,
                                    max(1, segment_count),
                                    sample_index,
                                    max(1, sample_count),
                                    elapsed_s,
                                )
                            ),
                        )

                    if mode == "sim":
                        solve_ik_fn = solve_fast_keyframe_sim_ik
                    elif str(settings.get("ik_mode") or "default").lower() == "single_seed_fallback":
                        solve_ik_fn = solve_keyframe_robot_interp_ik
                    else:
                        solve_ik_fn = solve_ik_for_pose

                    plan = plan_interpolated_keyframe_path(
                        keyframe_angles,
                        plan_settings["speed_mm_s"],
                        plan_settings["accel_mm_s2"],
                        plan_settings["update_rate_hz"],
                        solve_ik_fn=solve_ik_fn,
                        max_joint_step_deg=plan_settings.get("max_joint_step_deg", 10.0),
                        sampling_mode=plan_settings.get("sampling_mode", "update_rate"),
                        planner_kind=plan_settings.get("planner_kind", "baseline"),
                        position_tolerance_mm=plan_settings.get("position_tolerance_mm", 0.0),
                        orientation_tolerance_deg=plan_settings.get("orientation_tolerance_deg", 0.0),
                        cancel_check=is_cancelled,
                        progress_cb=on_progress,
                    )
                    error_text = None
                    planning_cancelled = False
                except InterpolationPlanningCancelled:
                    plan = None
                    error_text = None
                    planning_cancelled = True
                except Exception as exc:
                    plan = None
                    error_text = str(exc)
                    planning_cancelled = False

                def on_ready():
                    if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
                        return
                    if planning_cancelled:
                        keyframe_log.info(
                            "Interpolation planning cancelled generation=%d mode=%s",
                            generation,
                            mode,
                        )
                        return
                    if error_text is not None or plan is None:
                        keyframe_log.error(
                            "Interpolation planning failed generation=%d mode=%s error=%s",
                            generation,
                            mode,
                            error_text,
                        )
                        message = f"Keyframes: interpolation planning failed ({error_text})"
                        if mode == "robot":
                            set_encoder_status(message)
                        stop_keyframe_playback(message)
                        return
                    execution_plan = extend_plan_terminal_hold(
                        plan,
                        float(settings.get("terminal_hold_s") or 0.0),
                    )
                    keyframe_log.info(
                        "Interpolation planning succeeded generation=%d mode=%s segments=%d total_samples=%d estimated_duration_s=%.3f",
                        generation,
                        mode,
                        len(execution_plan.segments),
                        len(execution_plan.full_joint_trajectory_deg),
                        execution_plan.estimated_duration_s,
                    )
                    plan_stats = getattr(execution_plan, "planner_stats", None)
                    keyframe_log.info(
                        "Interpolation plan stats generation=%d mode=%s planner_kind=%s "
                        "requested_speed_mm_s=%s actual_speed_mm_s=%s translation_speed_limit_mm_s=%s "
                        "orientation_speed_limit_mm_s=%s ik_calls=%s ik_success=%s ik_failures=%s "
                        "failed_samples=%s rejected_joint_jumps=%s evolution_terminal_hold_s=%s "
                        "evolution_terminal_hold_samples=%s",
                        generation,
                        mode,
                        getattr(plan_stats, "planner_kind", "?"),
                        getattr(plan_stats, "requested_speed_mm_s", "?"),
                        getattr(plan_stats, "actual_speed_mm_s", "?"),
                        getattr(plan_stats, "translation_speed_limit_mm_s", "?"),
                        getattr(plan_stats, "orientation_speed_limit_mm_s", "?"),
                        getattr(plan_stats, "ik_call_count", "?"),
                        getattr(plan_stats, "ik_success_count", "?"),
                        getattr(plan_stats, "ik_failure_count", "?"),
                        getattr(plan_stats, "failed_sample_count", "?"),
                        getattr(plan_stats, "rejected_joint_jump_count", "?"),
                        getattr(plan_stats, "evolution_terminal_hold_s", 0.0),
                        getattr(plan_stats, "evolution_terminal_hold_samples", 0),
                    )
                    keyframe_playback.setdefault("plan_cache", {})[plan_cache_key] = execution_plan
                    active_plan = (
                        execution_plan
                        if int(keyframe_playback.get("interpolation_direction") or 1) > 0
                        else reverse_interpolated_plan(execution_plan)
                    )
                    prime_interpolated_keyframe_playback(active_plan, settings)
                    set_keyframe_status(
                        "Keyframes: planned %d segments, %d samples, %.1fs at %.1f Hz"
                        % (
                            len(active_plan.segments),
                            max(0, len(active_plan.full_joint_trajectory_deg) - 1),
                            active_plan.estimated_duration_s,
                            active_plan.update_rate_hz,
                        )
                    )
                    maybe_start_interpolated_execution(generation, mode)

                root.after(0, on_ready)

            threading.Thread(target=worker, daemon=True).start()
        if not run_preroll:
            return
        keyframe_log.info(
            "Interpolated preroll start generation=%d mode=%s while planning runs",
            generation,
            mode,
        )
        start_index = 0 if int(keyframe_playback.get("interpolation_direction") or 1) > 0 else len(keyframes) - 1
        if mode == "robot":
            run_robot_keyframe_start_alignment(
                generation,
                start_index,
                "Keyframes: move robot to start",
                lambda: on_interpolated_preroll_complete(generation, "robot"),
            )
        else:
            run_sim_keyframe_move(
                generation,
                start_index,
                "Keyframes: move to start",
                lambda: on_interpolated_preroll_complete(generation, "sim"),
            )

    def play_next_sim_keyframe(generation: int, index: int):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        total = len(keyframes)
        if total == 0:
            stop_keyframe_playback("Keyframes: no frames to play")
            return

        def on_move_done():
            next_index, next_direction = resolve_next_step_keyframe(
                index,
                int(keyframe_playback.get("step_direction") or 1),
                total,
            )
            keyframe_playback["step_direction"] = next_direction
            if next_index is None:
                stop_keyframe_playback("Keyframes: simulation playback complete")
                return
            continue_keyframe_step_after_delay(
                generation,
                "sim",
                index,
                lambda: play_next_sim_keyframe(generation, next_index),
            )

        run_sim_keyframe_move(
            generation,
            index,
            "Keyframes: sim",
            on_move_done,
        )

    def play_next_robot_keyframe(generation: int, index: int):
        if generation != keyframe_playback["generation"] or not keyframe_playback["active"]:
            return
        total = len(keyframes)
        if total == 0:
            stop_keyframe_playback("Keyframes: no frames to play")
            return

        def on_move_done():
            next_index, next_direction = resolve_next_step_keyframe(
                index,
                int(keyframe_playback.get("step_direction") or 1),
                total,
            )
            keyframe_playback["step_direction"] = next_direction
            if next_index is None:
                stop_keyframe_playback("Keyframes: robot playback complete")
                return
            continue_keyframe_step_after_delay(
                generation,
                "robot",
                index,
                lambda: play_next_robot_keyframe(generation, next_index),
            )

        run_robot_keyframe_move(
            generation,
            index,
            "Keyframes: robot",
            on_move_done,
        )

    def start_keyframe_playback(mode: str):
        if not keyframes:
            set_keyframe_status("Keyframes: add at least one frame first")
            return
        if keyframes_panel.interpolate_var.get() and len(keyframes) < 2:
            set_keyframe_status("Keyframes: add at least two frames for interpolation")
            return
        if keyframe_playback["active"]:
            set_keyframe_status("Keyframes: playback already active")
            return
        if mode == "robot" and not comm_client.is_connected:
            set_keyframe_status("Keyframes: robot playback requires CAN connection")
            return
        if mode == "robot" and comm_busy["value"]:
            set_keyframe_status("Keyframes: CAN operation already in progress")
            return

        keyframe_playback["active"] = True
        keyframe_playback["mode"] = mode
        keyframe_playback["after_id"] = None
        keyframe_playback["execution_mode"] = "step"
        keyframe_playback["plan"] = None
        keyframe_playback["settings"] = None
        keyframe_playback["trajectory_index"] = 0
        keyframe_playback["segment_index"] = 0
        keyframe_playback["segment_end_indices"] = []
        keyframe_playback["plan_estimated_duration_s"] = 0.0
        keyframe_playback["started_mono"] = None
        keyframe_playback["stop_event"] = threading.Event()
        keyframe_playback["comm_reserved"] = False
        keyframe_playback["plan_ready"] = False
        keyframe_playback["preroll_done"] = False
        keyframe_playback["step_direction"] = 1
        keyframe_playback["interpolation_direction"] = 1
        keyframe_playback["generation"] = int(keyframe_playback["generation"]) + 1
        generation = int(keyframe_playback["generation"])
        refresh_comm_controls()
        refresh_keyframe_controls()
        keyframe_log.info(
            "Keyframe playback start generation=%d mode=%s interpolate=%s keyframes=%d",
            generation,
            mode,
            bool(keyframes_panel.interpolate_var.get()),
            len(keyframes),
        )

        if keyframes_panel.interpolate_var.get():
            keyframe_playback["execution_mode"] = "interpolated"
            start_interpolated_keyframe_playback(mode, generation)
            return

        if mode == "sim":
            def begin_sim_sequence():
                if len(keyframes) <= 1:
                    stop_keyframe_playback("Keyframes: simulation playback complete")
                    return
                continue_keyframe_step_after_delay(
                    generation,
                    "sim",
                    0,
                    lambda: play_next_sim_keyframe(generation, 1),
                )

            run_sim_keyframe_move(
                generation,
                0,
                "Keyframes: move to start",
                begin_sim_sequence,
            )
        else:
            def begin_robot_sequence():
                if len(keyframes) <= 1:
                    stop_keyframe_playback("Keyframes: robot playback complete")
                    return
                continue_keyframe_step_after_delay(
                    generation,
                    "robot",
                    0,
                    lambda: play_next_robot_keyframe(generation, 1),
                )

            run_robot_keyframe_start_alignment(
                generation,
                0,
                "Keyframes: move robot to start",
                begin_robot_sequence,
            )

    def move_selected_keyframe(mode: str):
        index = get_selected_keyframe_index()
        if index is None:
            set_keyframe_status("Keyframes: no frame selected")
            return
        if keyframe_playback["active"]:
            set_keyframe_status("Keyframes: playback already active")
            return
        if mode == "robot" and not comm_client.is_connected:
            set_keyframe_status("Keyframes: robot playback requires CAN connection")
            return
        if mode == "robot" and comm_busy["value"]:
            set_keyframe_status("Keyframes: CAN operation already in progress")
            return

        keyframe_playback["active"] = True
        keyframe_playback["mode"] = mode
        keyframe_playback["after_id"] = None
        keyframe_playback["execution_mode"] = "step"
        keyframe_playback["plan"] = None
        keyframe_playback["settings"] = None
        keyframe_playback["trajectory_index"] = 0
        keyframe_playback["segment_index"] = 0
        keyframe_playback["segment_end_indices"] = []
        keyframe_playback["plan_estimated_duration_s"] = 0.0
        keyframe_playback["started_mono"] = None
        keyframe_playback["stop_event"] = threading.Event()
        keyframe_playback["comm_reserved"] = False
        keyframe_playback["plan_ready"] = False
        keyframe_playback["preroll_done"] = False
        keyframe_playback["step_direction"] = 1
        keyframe_playback["interpolation_direction"] = 1
        keyframe_playback["generation"] = int(keyframe_playback["generation"]) + 1
        generation = int(keyframe_playback["generation"])
        refresh_comm_controls()
        refresh_keyframe_controls()

        if mode == "sim":
            run_sim_keyframe_move(
                generation,
                index,
                "Keyframes: move sim to selected",
                lambda: stop_keyframe_playback("Keyframes: simulation move complete"),
            )
            return

        run_robot_keyframe_move(
            generation,
            index,
            "Keyframes: move robot to selected",
            lambda: stop_keyframe_playback("Keyframes: robot move complete"),
        )

    def on_remove_selected_keyframe():
        index = get_selected_keyframe_index()
        if index is None:
            set_keyframe_status("Keyframes: no frame selected")
            return
        stop_keyframe_playback()
        del keyframes[index]
        next_selection = min(index, len(keyframes) - 1) if keyframes else None
        refresh_keyframe_tree(select_index=next_selection)
        refresh_keyframe_controls()
        set_keyframe_status("Keyframes: removed selected frame")

    def on_remove_last_keyframe():
        if not keyframes:
            set_keyframe_status("Keyframes: no frames to remove")
            return
        stop_keyframe_playback()
        keyframes.pop()
        next_selection = len(keyframes) - 1 if keyframes else None
        refresh_keyframe_tree(select_index=next_selection)
        refresh_keyframe_controls()
        set_keyframe_status("Keyframes: removed last frame")

    def on_clear_keyframes():
        if not keyframes:
            set_keyframe_status("Keyframes: list already empty")
            return
        stop_keyframe_playback()
        keyframes.clear()
        refresh_keyframe_tree(select_index=None)
        refresh_keyframe_controls()
        set_keyframe_status("Keyframes: cleared")

    def on_move_keyframe_up():
        index = get_selected_keyframe_index()
        if index is None:
            set_keyframe_status("Keyframes: no frame selected")
            return
        if index <= 0:
            set_keyframe_status("Keyframes: selected frame is already at the top")
            return
        stop_keyframe_playback()
        keyframes[index - 1], keyframes[index] = keyframes[index], keyframes[index - 1]
        refresh_keyframe_tree(select_index=index - 1)
        refresh_keyframe_controls()
        set_keyframe_status(f"Keyframes: moved frame to position {index}")

    def on_move_keyframe_down():
        index = get_selected_keyframe_index()
        if index is None:
            set_keyframe_status("Keyframes: no frame selected")
            return
        if index >= len(keyframes) - 1:
            set_keyframe_status("Keyframes: selected frame is already at the bottom")
            return
        stop_keyframe_playback()
        keyframes[index], keyframes[index + 1] = keyframes[index + 1], keyframes[index]
        refresh_keyframe_tree(select_index=index + 1)
        refresh_keyframe_controls()
        set_keyframe_status(f"Keyframes: moved frame to position {index + 2}")

    def on_save_keyframes():
        if not keyframes:
            set_keyframe_status("Keyframes: nothing to save")
            return
        export_dir = get_keyframe_export_dir()
        export_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        export_path = export_dir / f"keyframes_{timestamp}.json"
        payload = build_keyframe_export_payload()
        export_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        set_keyframe_status(f"Keyframes: saved {len(keyframes)} frames to {export_path.name}")

    def on_load_keyframes():
        export_dir = get_keyframe_export_dir()
        export_dir.mkdir(parents=True, exist_ok=True)
        selected_path = filedialog.askopenfilename(
            parent=root,
            title="Load Keyframes",
            initialdir=str(export_dir),
            filetypes=[("Keyframe JSON", "*.json"), ("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not selected_path:
            set_keyframe_status("Keyframes: load cancelled")
            return

        try:
            payload = json.loads(Path(selected_path).read_text(encoding="utf-8"))
            records = parse_keyframe_payload(payload)
        except Exception as exc:
            set_keyframe_status(f"Keyframes: load failed ({exc})")
            return

        stop_keyframe_playback()
        replace_keyframes(records)
        set_keyframe_status(f"Keyframes: loaded {len(records)} frames from {Path(selected_path).name}")

    keyframe_sim_btn.configure(command=on_add_keyframe_sim)
    keyframe_enc_btn.configure(command=on_add_keyframe_enc)
    keyframes_panel.remove_selected_btn.configure(command=on_remove_selected_keyframe)
    keyframes_panel.remove_last_btn.configure(command=on_remove_last_keyframe)
    keyframes_panel.clear_all_btn.configure(command=on_clear_keyframes)
    keyframes_panel.move_up_btn.configure(command=on_move_keyframe_up)
    keyframes_panel.move_down_btn.configure(command=on_move_keyframe_down)
    keyframes_panel.save_btn.configure(command=on_save_keyframes)
    keyframes_panel.load_btn.configure(command=on_load_keyframes)
    keyframes_panel.play_sim_btn.configure(command=lambda: start_keyframe_playback("sim"))
    keyframes_panel.play_robot_btn.configure(command=lambda: start_keyframe_playback("robot"))
    keyframes_panel.stop_btn.configure(command=lambda: stop_keyframe_playback("Keyframes: stop requested"))
    keyframes_panel.move_sim_selected_btn.configure(command=lambda: move_selected_keyframe("sim"))
    keyframes_panel.move_robot_selected_btn.configure(command=lambda: move_selected_keyframe("robot"))
    demo_panel.capture_sim_btn.configure(command=capture_sim_tcp_for_demo)
    demo_panel.capture_enc_btn.configure(command=capture_enc_tcp_for_demo)
    demo_panel.probe_btn.configure(command=probe_orientation_reach)
    demo_panel.play_sim_btn.configure(command=lambda: start_orientation_sweep("sim"))
    demo_panel.play_robot_btn.configure(command=lambda: start_orientation_sweep("robot"))
    demo_panel.stop_btn.configure(command=stop_orientation_sweep)
    loop_mode_guard = {"active": False}

    def on_wrap_loop_toggle(*_args):
        if loop_mode_guard["active"] or not keyframes_panel.loop_var.get():
            return
        loop_mode_guard["active"] = True
        try:
            if keyframes_panel.pingpong_var.get():
                keyframes_panel.pingpong_var.set(False)
        finally:
            loop_mode_guard["active"] = False

    def on_pingpong_toggle(*_args):
        if loop_mode_guard["active"] or not keyframes_panel.pingpong_var.get():
            return
        loop_mode_guard["active"] = True
        try:
            if keyframes_panel.loop_var.get():
                keyframes_panel.loop_var.set(False)
        finally:
            loop_mode_guard["active"] = False

    keyframes_panel.loop_var.trace_add("write", on_wrap_loop_toggle)
    keyframes_panel.pingpong_var.trace_add("write", on_pingpong_toggle)
    keyframes_panel.show_markers_var.trace_add("write", lambda *_args: refresh_keyframe_tree())
    keyframes_panel.trace_markers_var.trace_add("write", lambda *_args: refresh_keyframe_tree())
    keyframe_tree.bind("<<TreeviewSelect>>", lambda _event: refresh_keyframe_controls())
    keyframe_tree.bind("<Double-1>", on_keyframe_tree_double_click)
    refresh_keyframe_tree()
    refresh_comm_controls()
    refresh_keyframe_controls()

    def get_manual_joint_step():
        value = parse_float_entry(manual_jog_panel.joint_step_var.get())
        if value is None or value <= 0.0:
            return None
        return value

    def get_manual_cart_step():
        value = parse_float_entry(manual_jog_panel.cart_step_var.get())
        if value is None or value <= 0.0:
            return None
        return value

    def on_manual_target_change(*_args):
        if manual_jog_panel.target_var.get() == 0:
            set_jog_status("Status: targeting simulation")
        else:
            set_jog_status("Status: targeting robot")

    manual_jog_panel.target_var.trace_add("write", on_manual_target_change)
    on_manual_target_change()

    def jog_joint_sim(joint_index: int, delta_deg: float):
        with shared_state.lock:
            targets = list(shared_state.joint_deg)
            if len(targets) < 6:
                targets = list(targets) + [0.0] * (6 - len(targets))
            targets[joint_index] = float(targets[joint_index]) + delta_deg
            shared_state.manual_joint_target_deg = targets[:6]
            shared_state.manual_joint_pending = True
            shared_state.manual_joint_motion_overrides = {}
            shared_state.ik_enabled = False
        set_jog_status(f"Status: sim jog J{joint_index + 1} {delta_deg:+.1f} deg")

    def jog_joint_robot(joint_index: int, delta_deg: float):
        if not comm_client.is_connected:
            set_jog_status("Status: robot jog requires CAN connection")
            return

        def task():
            current_angles = comm_client.read_joint_angles(timeout_s=COMM_READ_TIMEOUT_S)
            if joint_index >= len(current_angles) or current_angles[joint_index] is None:
                raise RuntimeError(f"Current angle for joint {joint_index + 1} is unknown.")
            targets = [float(angle) if angle is not None else 0.0 for angle in current_angles[:6]]
            targets[joint_index] = float(targets[joint_index]) + delta_deg
            move_result = execute_blocking_robot_move(
                targets,
                timeout_s=MANUAL_JOG_MOVE_COMPLETE_TIMEOUT_S,
            )
            move_result["current_angles"] = [float(angle) for angle in current_angles[:6]]
            return move_result

        def done(result, err):
            if err:
                set_jog_status(f"Status: robot jog error ({err})")
                return
            final_angles = list((result or {}).get("final_angles") or [])
            if final_angles:
                update_encoder_display(final_angles)
            set_jog_status(f"Status: robot jog J{joint_index + 1} {delta_deg:+.1f} deg complete")

        run_comm_task(
            task,
            done,
            busy_status="Status: robot jog running...",
            task_name=f"robot_joint_jog_{joint_index + 1}",
            timeout_s=MANUAL_JOG_MOVE_COMPLETE_TIMEOUT_S + 2.0,
        )

    def jog_cartesian_sim(axis_index: int, delta_mm: float):
        with shared_state.lock:
            target_pos_m = list(shared_state.tcp_pos_m)
            target_rpy_deg = list(shared_state.tcp_rpy_deg)
            if len(target_pos_m) < 3:
                target_pos_m = [0.0, 0.0, 0.0]
            if len(target_rpy_deg) < 3:
                target_rpy_deg = [0.0, 0.0, 0.0]
            target_pos_m[axis_index] = float(target_pos_m[axis_index]) + delta_mm * MM_TO_M
            target_orn = p.getQuaternionFromEuler([math.radians(v) for v in target_rpy_deg[:3]])
            shared_state.target_pos_m = target_pos_m[:3]
            shared_state.target_orn = target_orn
            shared_state.target_rpy_deg = target_rpy_deg[:3]
            shared_state.only_position = True
            shared_state.ik_enabled = True
        set_jog_status(f"Status: sim jog {'XYZ'[axis_index]} {delta_mm:+.1f} mm")

    def jog_cartesian_robot(axis_index: int, delta_mm: float):
        if not comm_client.is_connected:
            set_jog_status("Status: robot jog requires CAN connection")
            return

        with shared_state.lock:
            tcp_pos_m = list(shared_state.tcp_pos_m)
            tcp_rpy_deg = list(shared_state.tcp_rpy_deg)
            q_seed = list(shared_state.joint_deg)

        if len(tcp_pos_m) < 3:
            tcp_pos_m = [0.0, 0.0, 0.0]
        if len(tcp_rpy_deg) < 3:
            tcp_rpy_deg = [0.0, 0.0, 0.0]
        if len(q_seed) < 6:
            q_seed = list(q_seed) + [0.0] * (6 - len(q_seed))

        target_pos_mm = [value * M_TO_MM for value in tcp_pos_m[:3]]
        target_pos_mm[axis_index] = float(target_pos_mm[axis_index]) + delta_mm
        rx_deg, ry_deg, rz_deg = tcp_rpy_deg[:3]

        def task():
            solution = solve_ik_for_pose(
                x_mm=target_pos_mm[0],
                y_mm=target_pos_mm[1],
                z_mm=target_pos_mm[2],
                rx_deg=rx_deg,
                ry_deg=ry_deg,
                rz_deg=rz_deg,
                q0_deg=q_seed[:6],
                position_only=True,
            )
            if not solution.ok:
                raise RuntimeError(f"IK failed (pos err {solution.pos_err_mm:.2f} mm).")
            move_result = execute_blocking_robot_move(
                solution.q_deg[:6],
                timeout_s=MANUAL_JOG_MOVE_COMPLETE_TIMEOUT_S,
            )
            move_result["target_angles"] = [float(value) for value in solution.q_deg[:6]]
            return move_result

        def done(result, err):
            if err:
                set_jog_status(f"Status: robot cart jog error ({err})")
                return
            final_angles = list((result or {}).get("final_angles") or [])
            if final_angles:
                update_encoder_display(final_angles)
            target_angles = list((result or {}).get("target_angles") or [])
            if len(target_angles) >= 6:
                with shared_state.lock:
                    shared_state.manual_joint_target_deg = target_angles[:6]
                    shared_state.manual_joint_pending = True
                    shared_state.manual_joint_motion_overrides = {}
                    shared_state.only_position = True
                    shared_state.ik_enabled = False
            axis_name = "XYZ"[axis_index]
            set_jog_status(f"Status: robot jog {axis_name} {delta_mm:+.1f} mm complete")

        run_comm_task(
            task,
            done,
            busy_status="Status: robot jog running...",
            task_name=f"robot_cart_jog_{axis_index}",
            timeout_s=MANUAL_JOG_MOVE_COMPLETE_TIMEOUT_S + 2.0,
        )

    def bind_manual_jog_buttons():
        for joint_index, buttons in manual_jog_panel.joint_jog_buttons.items():
            minus_btn, plus_btn = buttons

            def make_joint_command(direction: float, idx=joint_index):
                def command():
                    step_deg = get_manual_joint_step()
                    if step_deg is None:
                        set_jog_status("Status: invalid joint jog step")
                        return
                    delta_deg = direction * step_deg
                    if manual_jog_panel.target_var.get() == 0:
                        jog_joint_sim(idx, delta_deg)
                    else:
                        jog_joint_robot(idx, delta_deg)

                return command

            minus_btn.configure(command=make_joint_command(-1.0))
            plus_btn.configure(command=make_joint_command(1.0))

        for axis_name, buttons in manual_jog_panel.cart_jog_buttons.items():
            minus_btn, plus_btn = buttons
            axis_index = "xyz".index(axis_name)

            def make_cart_command(direction: float, idx=axis_index):
                def command():
                    step_mm = get_manual_cart_step()
                    if step_mm is None:
                        set_jog_status("Status: invalid cartesian jog step")
                        return
                    delta_mm = direction * step_mm
                    if manual_jog_panel.target_var.get() == 0:
                        jog_cartesian_sim(idx, delta_mm)
                    else:
                        jog_cartesian_robot(idx, delta_mm)

                return command

            minus_btn.configure(command=make_cart_command(-1.0))
            plus_btn.configure(command=make_cart_command(1.0))

    bind_manual_jog_buttons()

    def set_path_status(state):
        def apply_update():
            state_text = str(state)
            state_key = state_text.lower()
            status_bar.path_status_var.set(f"Path: {state_text}")
            if state_key in {"running", "precheck", "active"}:
                status_bar.path_dot_label.configure(fg="#2e7d32")
            elif state_key == "error":
                status_bar.path_dot_label.configure(fg="#b71c1c")
            else:
                status_bar.path_dot_label.configure(fg="#d0d4dc")

        root.after(0, apply_update)

    def set_progress(pct):
        def apply_update():
            value = max(0.0, min(100.0, float(pct)))
            status_bar.progress_var.set(f"{value:.0f}%")
            status_bar.progress_bar.configure(value=value)

        root.after(0, apply_update)

    set_path_status("idle")
    set_progress(0.0)

    can_controller = CanStreamController(
        shared_state,
        comm_client,
        stream_log,
        set_path_status,
        update_encoder_display,
        set_encoder_status,
    )

    def finalize_can_stream(run_id: int, trigger_state: str):
        return can_controller.finalize_can_stream(run_id, trigger_state)

    def set_can_stream_status(state):
        can_controller.handle_stream_status(state, finalize_can_stream)

    can_streamer = StreamingInterpolator(
        shared_state,
        stream_settings,
        status_cb=set_can_stream_status,
        progress_cb=set_progress,
        joint_command_cb=can_controller.can_joint_command,
        segment_meta_cb=can_controller.on_can_segment_meta,
        runtime_snapshot_cb=can_controller.get_runtime_snapshot,
        ik_fallback_fn=lambda x, y, z, q0: pybullet_ik.solve_position_only(x, y, z, q0_deg=q0),
        update_shared_state=False,
    )
    can_controller.attach_streamer(can_streamer)

    stream_startup = CanStreamStartupHelper(
        shared_state=shared_state,
        comm_client=comm_client,
        pybullet_ik=pybullet_ik,
        stream_log=stream_log,
        can_controller=can_controller,
        can_streamer=can_streamer,
        solve_ik_for_pose=solve_ik_for_pose,
    )

    def on_stop_stream():
        can_controller.mark_manual_stop()
        can_streamer.stop()
        can_controller.set_can_phase("PAUSED")
        stream_log.info("Streaming stop requested")
        set_encoder_status("CAN stream: stop requested")
        run_background_comm_task(
            lambda: (
                can_controller.send_can_velocity_stop("manual_stop"),
                can_controller.cleanup_feedback_worker(),
            ),
            task_name="stop_can_stream",
            ui_callback=False,
        )

    def on_manual_stop_all():
        with shared_state.lock:
            shared_state.ik_enabled = False
        cancel_active_comm_task("manual stop all", status_text="Encoders: manual stop requested")
        on_stop_stream()
        stop_keyframe_playback("Keyframes: stop requested")
        set_jog_status("Status: stop requested")

    manual_jog_panel.stop_btn.configure(command=on_manual_stop_all)

    refresh_controller = GuiRefreshController(
        root=root,
        shared_state=shared_state,
        stop_event=stop_event,
        layout=layout,
    )

    close_modal_state = {"active": False}

    def show_closing_status():
        status_bar.path_status_var.set("Path: Closing...")
        status_bar.path_dot_label.configure(fg="#d68910")
        status_bar.progress_var.set("...")
        status_bar.progress_bar.configure(value=0)
        status_bar.ik_var.set("IK: shutting down")
        root.update_idletasks()

    def on_close():
        if close_modal_state["active"]:
            return
        close_modal_state["active"] = True
        show_closing_status()

        def finish_close():
            if not root.winfo_exists():
                return
            stop_event.set()
            cancel_active_comm_task("gui close")
            can_controller.mark_manual_stop()
            can_streamer.stop()
            stop_keyframe_playback()
            threading.Thread(
                target=lambda: (
                    can_controller.send_can_velocity_stop("gui_close"),
                    can_controller.cleanup_feedback_worker(),
                ),
                daemon=True,
            ).start()
            try:
                pybullet_ik.disconnect()
            except Exception:
                pass
            try:
                comm_client.disconnect()
            except Exception:
                pass
            stream_log.info("GUI closed; streaming stopped")
            root.destroy()

        root.after(0, finish_close)

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.after(100, refresh_controller.refresh_ui)
    root.mainloop()


def main():
    shared_state = AppSharedState()
    stop_event = threading.Event()
    sim_thread = threading.Thread(
        target=run_simulation_thread, args=(shared_state, stop_event), daemon=True
    )
    sim_thread.start()
    run_gui(shared_state, stop_event)
    sim_thread.join(timeout=2.0)


if __name__ == "__main__":
    main()




