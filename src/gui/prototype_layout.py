from __future__ import annotations

from dataclasses import dataclass

import tkinter as tk
from tkinter import ttk


BG_ROOT = "#ebebeb"
BG_PANEL = "#f7f7f7"
BG_ACCENT = "#e2e4e8"
BG_ENTRY = "#ffffff"
BG_HEADER = "#242424"
BG_SUBHEADER = "#383838"

FG_HEADER_DARK = "#d0d4dc"
FG_TITLE_DARK = "#ffffff"
FG_DIM_DARK = "#888888"

FG_SECTION = "#1a1a1a"
FG_LABEL = "#4a4a4a"
FG_VALUE = "#111111"
FG_DIM = "#888888"
FG_WHITE = "#ffffff"

BTN_PRIMARY = "#1565c0"
BTN_GO = "#2e7d32"
BTN_STOP = "#b71c1c"
BTN_NEUTRAL = "#5a5a5a"
BTN_WARN = "#e65100"

SEP_COLOR = "#cccccc"
SEP_DARK = "#484848"

FONT_LABEL = ("Helvetica", 9)
FONT_HEADER = ("Helvetica", 9, "bold")
FONT_VALUE = ("Courier", 10)
FONT_ENTRY = ("Courier", 10)
FONT_BTN = ("Helvetica", 9, "bold")
FONT_BTN_SM = ("Helvetica", 8, "bold")
FONT_TITLE = ("Helvetica", 11, "bold")
FONT_STATUS = ("Helvetica", 9)

PAD = 8
IPAD = 4
ENTRY_W = 9
ENTRY_W_SM = 7

TERM_BG = "#0a0a0a"
TERM_FG = "#00e676"
TERM_PROMPT = "#76ff03"
TERM_FONT = ("Consolas", 9)


def _lighten(hex_color, amount=30):
    hex_color = hex_color.lstrip("#")
    r, g, b = (int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    r = min(255, r + amount)
    g = min(255, g + amount)
    b = min(255, b + amount)
    return f"#{r:02x}{g:02x}{b:02x}"


def btn(parent, text, color=BTN_NEUTRAL, width=None, small=False, fg=FG_WHITE):
    kw = dict(
        text=text,
        bg=color,
        fg=fg,
        activebackground=_lighten(color, 25),
        activeforeground=FG_WHITE,
        relief="flat",
        borderwidth=0,
        padx=8 if not small else 5,
        pady=3 if not small else 2,
        font=FONT_BTN if not small else FONT_BTN_SM,
        cursor="hand2",
    )
    if width is not None:
        kw["width"] = width
    return tk.Button(parent, **kw)


def label(parent, text, fg=FG_LABEL, font=FONT_LABEL, **kw):
    bg = parent.cget("bg") if hasattr(parent, "cget") else BG_PANEL
    return tk.Label(parent, text=text, bg=bg, fg=fg, font=font, **kw)


def section_label(parent, text):
    return label(parent, text, fg=FG_SECTION, font=FONT_HEADER)


def entry(parent, default="0.00", width=ENTRY_W):
    sv = tk.StringVar(value=default)
    widget = tk.Entry(
        parent,
        textvariable=sv,
        width=width,
        bg=BG_ENTRY,
        fg=FG_VALUE,
        insertbackground=FG_VALUE,
        relief="flat",
        font=FONT_ENTRY,
        highlightthickness=1,
        highlightbackground=SEP_COLOR,
        highlightcolor=BTN_PRIMARY,
    )
    return widget, sv


def separator(parent, color=SEP_COLOR):
    return tk.Frame(parent, bg=color, height=1)


def dark_separator(parent):
    return tk.Frame(parent, bg=SEP_DARK, width=1)


def panel_frame(parent, padx=PAD, pady=PAD):
    return tk.Frame(
        parent,
        bg=BG_PANEL,
        padx=padx,
        pady=pady,
        relief="flat",
        bd=0,
        highlightthickness=1,
        highlightbackground=SEP_COLOR,
    )


def accent_frame(parent, padx=IPAD, pady=IPAD):
    return tk.Frame(parent, bg=BG_ACCENT, padx=padx, pady=pady)


def disable_widget(widget):
    widget.configure(state="disabled", cursor="arrow")


class Tooltip:
    def __init__(self, widget, text):
        self._widget = widget
        self._text = text
        self._tip = None
        widget.bind("<Enter>", self._show)
        widget.bind("<Leave>", self._hide)

    def _show(self, event=None):
        if not self._text:
            return
        self._tip = tw = tk.Toplevel(self._widget)
        tw.wm_overrideredirect(True)
        tw.configure(bg="#000000")
        label_widget = tk.Label(
            tw,
            text=self._text,
            bg="#ffffff",
            fg="#000000",
            font=("Helvetica", 8),
            padx=6,
            pady=3,
            relief="flat",
            justify="left",
            anchor="w",
        )
        label_widget.pack(padx=1, pady=1)
        tw.update_idletasks()

        x = self._widget.winfo_rootx() + self._widget.winfo_width() // 2
        y = self._widget.winfo_rooty() + self._widget.winfo_height() + 4
        tip_width = tw.winfo_reqwidth()
        tip_height = tw.winfo_reqheight()

        root = self._widget.winfo_toplevel()
        root_left = root.winfo_rootx()
        root_top = root.winfo_rooty()
        root_right = root_left + root.winfo_width()
        root_bottom = root_top + root.winfo_height()

        if x + tip_width > root_right - 8:
            x = self._widget.winfo_rootx() + self._widget.winfo_width() - tip_width
        if x < root_left + 8:
            x = root_left + 8
        if y + tip_height > root_bottom - 8:
            y = self._widget.winfo_rooty() - tip_height - 4
        if y < root_top + 8:
            y = root_top + 8

        tw.wm_geometry(f"+{x}+{y}")

    def _hide(self, event=None):
        if self._tip:
            self._tip.destroy()
            self._tip = None


@dataclass
class HeaderPanel:
    frame: tk.Frame
    can_status_var: tk.StringVar
    comm_port_var: tk.StringVar
    connect_btn: tk.Button
    read_btn: tk.Button
    set_home_btn: tk.Button
    home_btn: tk.Button
    can_dot_label: tk.Label


@dataclass
class TerminalPanel:
    frame: tk.Frame
    text: tk.Text
    command_var: tk.StringVar
    command_entry: tk.Entry
    send_btn: tk.Button


@dataclass
class PoseControlPanel:
    frame: tk.Frame
    mode_var: tk.IntVar
    x_var: tk.StringVar
    y_var: tk.StringVar
    z_var: tk.StringVar
    rx_var: tk.StringVar
    ry_var: tk.StringVar
    rz_var: tk.StringVar
    pos_step_var: tk.StringVar
    ang_step_var: tk.StringVar
    only_pos_var: tk.BooleanVar
    pos_err_var: tk.StringVar
    joint_entry_vars: list[tk.StringVar]
    axis_jog_buttons: dict[str, tuple[tk.Button, tk.Button]]
    home_target_btn: tk.Button
    set_target_btn: tk.Button
    go_sim_btn: tk.Button
    go_robot_btn: tk.Button
    home_btn: tk.Button
    stop_btn: tk.Button
    custom_ik_settings_btn: tk.Button
    custom_ik_btn: tk.Button
    custom_ik_go_sim_btn: tk.Button
    custom_ik_go_robot_btn: tk.Button
    custom_ik_joint_vars: list[tk.StringVar]
    custom_ik_tcp_pos_vars: list[tk.StringVar]
    custom_ik_tcp_rpy_vars: list[tk.StringVar]
    custom_ik_pos_err_var: tk.StringVar
    custom_ik_ori_err_var: tk.StringVar
    custom_ik_meta_var: tk.StringVar
    status_var: tk.StringVar


@dataclass
class ManualJogPanel:
    frame: tk.Frame
    target_var: tk.IntVar
    joint_step_var: tk.StringVar
    cart_step_var: tk.StringVar
    joint_step_entry: tk.Entry
    cart_step_entry: tk.Entry
    joint_current_vars: list[tk.StringVar]
    cart_current_vars: list[tk.StringVar]
    joint_jog_buttons: dict[int, tuple[tk.Button, tk.Button]]
    cart_jog_buttons: dict[str, tuple[tk.Button, tk.Button]]
    sim_btn: tk.Button
    robot_btn: tk.Button
    stop_btn: tk.Button
    busy_badge: tk.Label
    busy_progress: ttk.Progressbar
    status_var: tk.StringVar


@dataclass
class KeyframesPanel:
    frame: tk.Frame
    tree: ttk.Treeview
    speed_var: tk.StringVar
    accel_var: tk.StringVar
    auto_sync_var: tk.BooleanVar
    loop_var: tk.BooleanVar
    pingpong_var: tk.BooleanVar
    interpolate_var: tk.BooleanVar
    show_markers_var: tk.BooleanVar
    trace_markers_var: tk.BooleanVar
    status_var: tk.StringVar
    remove_selected_btn: tk.Button
    remove_last_btn: tk.Button
    clear_all_btn: tk.Button
    move_up_btn: tk.Button
    move_down_btn: tk.Button
    save_btn: tk.Button
    load_btn: tk.Button
    play_sim_btn: tk.Button
    play_robot_btn: tk.Button
    stop_btn: tk.Button
    move_robot_selected_btn: tk.Button
    move_sim_selected_btn: tk.Button


@dataclass
class DemoPanel:
    frame: tk.Frame
    anchor_x_var: tk.StringVar
    anchor_y_var: tk.StringVar
    anchor_z_var: tk.StringVar
    anchor_rx_var: tk.StringVar
    anchor_ry_var: tk.StringVar
    anchor_rz_var: tk.StringVar
    pattern_var: tk.StringVar
    amplitude_var: tk.StringVar
    speed_var: tk.StringVar
    cycles_var: tk.StringVar
    loop_var: tk.BooleanVar
    capture_sim_btn: tk.Button
    capture_enc_btn: tk.Button
    probe_btn: tk.Button
    play_sim_btn: tk.Button
    play_robot_btn: tk.Button
    stop_btn: tk.Button
    status_var: tk.StringVar


@dataclass
class RobotStatePanel:
    frame: tk.Frame
    sim_joint_vars: list[tk.StringVar]
    encoder_joint_vars: list[tk.StringVar]
    sim_tcp_pos_vars: list[tk.StringVar]
    encoder_tcp_pos_vars: list[tk.StringVar]
    sim_tcp_rpy_vars: list[tk.StringVar]
    encoder_tcp_rpy_vars: list[tk.StringVar]
    collision_frame: tk.Frame
    collision_var: tk.StringVar
    collision_value_label: tk.Label
    pos_err_var: tk.StringVar


@dataclass
class RobotControlPanel:
    frame: tk.Frame
    status_var: tk.StringVar
    keyframe_sim_btn: tk.Button
    keyframe_enc_btn: tk.Button


@dataclass
class StatusBarPanel:
    frame: tk.Frame
    path_status_var: tk.StringVar
    path_dot_label: tk.Label
    progress_var: tk.StringVar
    progress_bar: ttk.Progressbar
    ik_var: tk.StringVar


@dataclass
class MainGuiLayout:
    header: HeaderPanel
    terminal: TerminalPanel
    pose_panel: PoseControlPanel
    manual_jog_panel: ManualJogPanel
    keyframes_panel: KeyframesPanel
    demo_panel: DemoPanel
    robot_state_panel: RobotStatePanel
    robot_control_panel: RobotControlPanel
    status_bar: StatusBarPanel


def _style_notebook():
    style = ttk.Style()
    style.theme_use("default")
    style.configure(
        "Light.TNotebook",
        background=BG_PANEL,
        borderwidth=1,
        bordercolor=SEP_COLOR,
        tabmargins=[2, 2, 0, 0],
    )
    style.configure(
        "Light.TNotebook.Tab",
        background=BG_ACCENT,
        foreground=FG_LABEL,
        padding=[11, 4],
        font=FONT_HEADER,
        borderwidth=0,
    )
    style.map(
        "Light.TNotebook.Tab",
        background=[("selected", BG_PANEL), ("active", "#d0d2d6")],
        foreground=[("selected", FG_SECTION), ("active", FG_SECTION)],
    )
    style.configure(
        "V2Status.Horizontal.TProgressbar",
        troughcolor=BG_SUBHEADER,
        background=BTN_PRIMARY,
        borderwidth=0,
        thickness=8,
    )


def build_header(root, default_port: str) -> HeaderPanel:
    frame = tk.Frame(root, bg=BG_HEADER, padx=PAD, pady=7)
    frame.grid(row=0, column=0, columnspan=3, sticky="ew")
    frame.columnconfigure(9, weight=1)

    can_status_var = tk.StringVar(value="CAN: disconnected")
    can_dot_label = tk.Label(frame, text="o", bg=BG_HEADER, fg=BTN_STOP, font=("Helvetica", 13, "bold"))
    can_dot_label.grid(row=0, column=0, padx=(0, 4))
    tk.Label(frame, textvariable=can_status_var, bg=BG_HEADER, fg=FG_HEADER_DARK, font=FONT_STATUS).grid(
        row=0, column=1, padx=(0, 14)
    )
    dark_separator(frame).grid(row=0, column=2, sticky="ns", padx=8)
    tk.Label(frame, text="COM:", bg=BG_HEADER, fg=FG_DIM_DARK, font=FONT_STATUS).grid(
        row=0, column=3, padx=(0, 4)
    )
    comm_entry, comm_port_var = entry(frame, default=default_port, width=7)
    comm_entry.configure(
        bg="#3a3a3a",
        fg=FG_TITLE_DARK,
        highlightbackground=SEP_DARK,
        highlightcolor=BTN_PRIMARY,
    )
    comm_entry.grid(row=0, column=4, padx=(0, 6))
    connect_btn = btn(frame, "Connect", color=BTN_PRIMARY, width=9)
    connect_btn.grid(row=0, column=5, padx=(0, 6))
    read_btn = btn(frame, "Read Enc", color=BTN_NEUTRAL, width=9)
    set_home_btn = btn(frame, "Set Home", color=BTN_NEUTRAL, width=9)
    home_btn = btn(frame, "Go Home", color=BTN_GO, width=9)
    read_btn.grid(row=0, column=6, padx=(0, 4))
    set_home_btn.grid(row=0, column=7, padx=(0, 4))
    home_btn.grid(row=0, column=8, padx=(0, 14))
    Tooltip(read_btn, "Read current encoder positions from all joints")
    Tooltip(set_home_btn, "Save current position as the home reference")
    Tooltip(home_btn, "Move robot to the saved home position")
    tk.Label(frame, text="", bg=BG_HEADER).grid(row=0, column=9, sticky="ew")
    tk.Label(frame, text="Robot Arm Control", bg=BG_HEADER, fg=FG_TITLE_DARK, font=FONT_TITLE).grid(
        row=0, column=10
    )
    return HeaderPanel(
        frame=frame,
        can_status_var=can_status_var,
        comm_port_var=comm_port_var,
        connect_btn=connect_btn,
        read_btn=read_btn,
        set_home_btn=set_home_btn,
        home_btn=home_btn,
        can_dot_label=can_dot_label,
    )


def build_terminal(parent) -> TerminalPanel:
    frame = tk.Frame(parent, bg=BG_ROOT)
    frame.grid(row=1, column=0, sticky="nsew", padx=(PAD, PAD // 2), pady=(PAD, 0))
    frame.rowconfigure(1, weight=1)
    frame.columnconfigure(0, weight=1)

    title_strip = tk.Frame(frame, bg=BG_SUBHEADER, padx=6, pady=4)
    title_strip.grid(row=0, column=0, sticky="ew")
    tk.Label(title_strip, text="TERMINAL", bg=BG_SUBHEADER, fg=TERM_FG, font=("Consolas", 9, "bold")).pack(
        side="left"
    )
    tk.Label(title_strip, text="o", bg=BG_SUBHEADER, fg=TERM_FG, font=("Helvetica", 10, "bold")).pack(side="right")

    text_frame = tk.Frame(frame, bg=TERM_BG)
    text_frame.grid(row=1, column=0, sticky="nsew")
    text_frame.rowconfigure(0, weight=1)
    text_frame.columnconfigure(0, weight=1)
    scrollbar = tk.Scrollbar(text_frame, bg=TERM_BG, troughcolor="#1a1a1a", activebackground="#333333", relief="flat", bd=0, width=8)
    scrollbar.grid(row=0, column=1, sticky="ns")
    text = tk.Text(
        text_frame,
        bg=TERM_BG,
        fg=TERM_FG,
        font=TERM_FONT,
        width=24,
        relief="flat",
        bd=0,
        padx=6,
        pady=6,
        wrap="char",
        insertbackground=TERM_FG,
        yscrollcommand=scrollbar.set,
        state="disabled",
        cursor="arrow",
    )
    text.grid(row=0, column=0, sticky="nsew")
    scrollbar.config(command=text.yview)
    text.config(state="normal")
    text.tag_configure("prompt", foreground=TERM_PROMPT, font=("Consolas", 9, "bold"))
    text.tag_configure("output", foreground=TERM_FG)
    text.tag_configure("dim", foreground="#007a36")
    text.tag_configure("error", foreground="#ff6e6e")
    text.insert("end", "> terminal commands enabled\n", "prompt")
    text.insert("end", "Supported now: robot_window_size, pybullet_window_size\n", "dim")
    text.config(state="disabled")

    input_bar = tk.Frame(frame, bg=TERM_BG, padx=4, pady=4)
    input_bar.grid(row=2, column=0, sticky="ew")
    input_bar.columnconfigure(1, weight=1)
    tk.Label(input_bar, text=">", bg=TERM_BG, fg=TERM_PROMPT, font=("Consolas", 10, "bold")).grid(
        row=0, column=0, padx=(2, 4)
    )
    command_var = tk.StringVar()
    command_entry = tk.Entry(
        input_bar,
        textvariable=command_var,
        bg=TERM_BG,
        fg=TERM_FG,
        insertbackground=TERM_FG,
        relief="flat",
        font=TERM_FONT,
        highlightthickness=1,
        highlightbackground="#1f4f2a",
        highlightcolor=TERM_PROMPT,
    )
    command_entry.grid(row=0, column=1, sticky="ew", padx=(0, 4))
    send_btn = tk.Button(
        input_bar,
        text="Send",
        bg="#004d20",
        fg=TERM_PROMPT,
        activebackground="#007a36",
        activeforeground=TERM_FG,
        relief="flat",
        borderwidth=0,
        font=("Consolas", 9, "bold"),
        padx=6,
        pady=2,
        cursor="hand2",
    )
    send_btn.grid(row=0, column=2)
    return TerminalPanel(
        frame=frame,
        text=text,
        command_var=command_var,
        command_entry=command_entry,
        send_btn=send_btn,
    )


def _apply_pose_mode_styles(mode_var, pose_wrap, joint_wrap, pose_btn, joint_btn):
    if mode_var.get() == 0:
        pose_btn.configure(bg=BTN_PRIMARY, fg=FG_WHITE, activebackground=_lighten(BTN_PRIMARY))
        joint_btn.configure(bg="#dde0e6", fg=FG_LABEL, activebackground="#c8ccd4", activeforeground=FG_SECTION)
        pose_wrap.configure(highlightbackground=BTN_PRIMARY)
        joint_wrap.configure(highlightbackground=SEP_COLOR)
    else:
        pose_btn.configure(bg="#dde0e6", fg=FG_LABEL, activebackground="#c8ccd4", activeforeground=FG_SECTION)
        joint_btn.configure(bg=BTN_PRIMARY, fg=FG_WHITE, activebackground=_lighten(BTN_PRIMARY))
        pose_wrap.configure(highlightbackground=SEP_COLOR)
        joint_wrap.configure(highlightbackground=BTN_PRIMARY)


def _build_pose_control_tab(tab) -> PoseControlPanel:
    tab.configure(bg=BG_PANEL)
    tab.columnconfigure(0, weight=1)
    tab.columnconfigure(1, weight=1)
    tab.columnconfigure(2, weight=1)

    mode_var = tk.IntVar(value=0)
    toggle_frame = tk.Frame(tab, bg=BG_PANEL)
    toggle_frame.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
    label(toggle_frame, "Mode:", fg=FG_LABEL, font=FONT_HEADER).grid(row=0, column=0, padx=(0, 10))
    pill = tk.Frame(toggle_frame, bg="#b0b4bc", padx=1, pady=1)
    pill.grid(row=0, column=1)
    pose_mode_btn = tk.Button(pill, text="Target Pose", relief="flat", bd=0, padx=14, pady=5, font=FONT_HEADER)
    joint_mode_btn = tk.Button(pill, text="Joint Angles", relief="flat", bd=0, padx=14, pady=5, font=FONT_HEADER)
    pose_mode_btn.grid(row=0, column=0, padx=(0, 1))
    joint_mode_btn.grid(row=0, column=1)
    separator(tab).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 8))

    pose_wrap = tk.Frame(tab, bg=BG_PANEL, highlightthickness=2, highlightbackground=BTN_PRIMARY)
    pose_wrap.grid(row=2, column=0, sticky="nsew", padx=(0, 4))
    pose_inner = accent_frame(pose_wrap, padx=8, pady=8)
    pose_inner.pack(fill="both", expand=True)
    section_label(pose_inner, "Target Pose").grid(row=0, column=0, columnspan=5, sticky="w", pady=(0, 6))

    axis_jog_buttons = {}
    axis_vars = {}
    axis_defs = [
        ("X", "0.00", "mm"),
        ("Y", "0.00", "mm"),
        ("Z", "200.00", "mm"),
        ("Rx", "0.00", "deg"),
        ("Ry", "0.00", "deg"),
        ("Rz", "0.00", "deg"),
    ]
    for row_index, (name, default, unit) in enumerate(axis_defs, start=1):
        label(pose_inner, name, fg=FG_LABEL).grid(row=row_index, column=0, sticky="w", pady=2, padx=(0, 4))
        axis_entry, axis_var = entry(pose_inner, default=default, width=ENTRY_W)
        axis_entry.grid(row=row_index, column=1, padx=4, pady=2)
        label(pose_inner, unit, fg=FG_DIM).grid(row=row_index, column=2, sticky="w", padx=(0, 6))
        minus_btn = btn(pose_inner, "-", color=BTN_NEUTRAL, small=True)
        plus_btn = btn(pose_inner, "+", color=BTN_PRIMARY, small=True)
        minus_btn.grid(row=row_index, column=3, padx=(0, 2), pady=2)
        plus_btn.grid(row=row_index, column=4, pady=2)
        axis_vars[name.lower()] = axis_var
        axis_jog_buttons[name.lower()] = (minus_btn, plus_btn)

    separator(pose_inner).grid(row=7, column=0, columnspan=5, sticky="ew", pady=(6, 4))
    step_row = tk.Frame(pose_inner, bg=BG_ACCENT)
    step_row.grid(row=8, column=0, columnspan=5, sticky="w")
    label(step_row, "Step:", fg=FG_LABEL).grid(row=0, column=0, padx=(0, 4))
    label(step_row, "pos", fg=FG_DIM).grid(row=0, column=1, padx=(0, 2))
    pos_entry, pos_step_var = entry(step_row, default="5.0", width=5)
    pos_entry.grid(row=0, column=2, padx=(0, 2))
    label(step_row, "mm", fg=FG_DIM).grid(row=0, column=3, padx=(0, 8))
    label(step_row, "ang", fg=FG_DIM).grid(row=0, column=4, padx=(0, 2))
    ang_entry, ang_step_var = entry(step_row, default="10", width=5)
    ang_entry.grid(row=0, column=5, padx=(0, 2))
    label(step_row, "deg", fg=FG_DIM).grid(row=0, column=6)

    only_pos_var = tk.BooleanVar(value=False)
    pos_err_var = tk.StringVar(value="-- mm")
    options_row = tk.Frame(pose_inner, bg=BG_ACCENT)
    options_row.grid(row=9, column=0, columnspan=5, sticky="w", pady=(4, 0))
    tk.Checkbutton(
        options_row,
        text="Position Only",
        variable=only_pos_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        activebackground=BG_ACCENT,
        activeforeground=FG_SECTION,
        selectcolor=BG_ENTRY,
        font=FONT_LABEL,
    ).grid(row=0, column=0, sticky="w")
    label(options_row, "Err:", fg=FG_DIM).grid(row=0, column=1, sticky="w", padx=(14, 3))
    tk.Label(
        options_row,
        textvariable=pos_err_var,
        bg=BG_ACCENT,
        fg=FG_VALUE,
        font=FONT_VALUE,
        width=9,
        anchor="e",
    ).grid(row=0, column=2, sticky="w")

    joint_wrap = tk.Frame(tab, bg=BG_PANEL, highlightthickness=2, highlightbackground=SEP_COLOR)
    joint_wrap.grid(row=2, column=1, sticky="nsew", padx=(4, 0))
    joint_inner = accent_frame(joint_wrap, padx=8, pady=8)
    joint_inner.pack(fill="both", expand=True)
    section_label(joint_inner, "Joint Angles").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 6))
    joint_entry_vars = []
    for i in range(6):
        label(joint_inner, f"J{i + 1}", fg=FG_LABEL).grid(row=i + 1, column=0, sticky="w", pady=3)
        joint_entry, joint_var = entry(joint_inner, default="0.00", width=ENTRY_W)
        joint_entry.grid(row=i + 1, column=1, padx=8, pady=3)
        label(joint_inner, "deg", fg=FG_DIM).grid(row=i + 1, column=2, sticky="w")
        joint_entry_vars.append(joint_var)

    action_frame = tk.Frame(tab, bg=BG_PANEL)
    action_frame.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 4))
    home_target_btn = btn(action_frame, "Home Target", color=BTN_NEUTRAL)
    set_target_btn = btn(action_frame, "Set Target", color=BTN_NEUTRAL)
    go_sim_btn = btn(action_frame, "Go Sim", color=BTN_NEUTRAL)
    go_robot_btn = btn(action_frame, "Go Robot", color=BTN_GO)
    home_btn = btn(action_frame, "Home Sim", color=BTN_NEUTRAL)
    stop_btn = btn(action_frame, "Stop", color=BTN_STOP)
    action_buttons = [home_target_btn, set_target_btn, go_sim_btn, go_robot_btn, home_btn, stop_btn]
    for index, button in enumerate(action_buttons):
        button.grid(row=0, column=index, padx=(0, 4) if index < len(action_buttons) - 1 else 0)
    Tooltip(home_target_btn, "Set the target pose to the robot home configuration (all joints at 0 deg)")
    Tooltip(set_target_btn, "Apply entered values as the current target")
    Tooltip(go_sim_btn, "Move simulation using existing behavior")
    Tooltip(go_robot_btn, "Uses the existing robot move behavior only")
    Tooltip(home_btn, "Move simulation to the home position")
    Tooltip(stop_btn, "Stop the currently active simulation behavior")

    status_var = tk.StringVar(value="Status: idle")
    tk.Label(tab, textvariable=status_var, bg=BG_PANEL, fg=FG_DIM, font=FONT_STATUS).grid(
        row=4, column=0, columnspan=2, sticky="w"
    )

    custom_ik_frame = accent_frame(tab, padx=10, pady=8)
    custom_ik_frame.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    custom_ik_frame.columnconfigure(0, weight=1)
    custom_ik_frame.columnconfigure(1, weight=1)
    custom_ik_frame.columnconfigure(2, weight=1)

    header_row = tk.Frame(custom_ik_frame, bg=BG_ACCENT)
    header_row.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 6))
    header_row.columnconfigure(0, weight=1)
    section_label(header_row, "Custom IK").grid(row=0, column=0, sticky="w")
    settings_btn = btn(header_row, "⚙", color=BTN_PRIMARY, width=2)
    settings_btn.grid(row=0, column=1, sticky="e", padx=(0, 4))
    Tooltip(settings_btn, "Open Custom IK solver settings")
    action_btn_frame = tk.Frame(header_row, bg=BG_ACCENT)
    action_btn_frame.grid(row=0, column=2, sticky="e")
    custom_ik_btn = btn(action_btn_frame, "Run Custom IK", color=BTN_PRIMARY)
    custom_ik_go_sim_btn = btn(action_btn_frame, "Go Sim", color=BTN_NEUTRAL)
    custom_ik_go_robot_btn = btn(action_btn_frame, "Go Robot", color=BTN_GO)
    custom_ik_btn.grid(row=0, column=0, padx=(0, 4))
    custom_ik_go_sim_btn.grid(row=0, column=1, padx=(0, 4))
    custom_ik_go_robot_btn.grid(row=0, column=2)
    Tooltip(custom_ik_btn, "Solve using the custom IK solver from the Target Pose fields")
    Tooltip(custom_ik_go_sim_btn, "Move simulation to the most recent Custom IK joint solution")
    Tooltip(custom_ik_go_robot_btn, "Move robot to the most recent Custom IK joint solution")

    solved_frame = tk.Frame(custom_ik_frame, bg=BG_ACCENT)
    solved_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 6))
    section_label(solved_frame, "Solved Joints").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))
    custom_ik_joint_vars = []
    for i in range(6):
        label(solved_frame, f"J{i + 1}", fg=FG_LABEL).grid(row=i + 1, column=0, sticky="w", pady=2)
        value_var = tk.StringVar(value="--")
        label(solved_frame, "", textvariable=value_var, fg=FG_VALUE, font=FONT_VALUE, width=8, anchor="e").grid(
            row=i + 1, column=1, sticky="e", padx=(8, 4), pady=2
        )
        label(solved_frame, "deg", fg=FG_DIM).grid(row=i + 1, column=2, sticky="w", pady=2)
        custom_ik_joint_vars.append(value_var)

    tcp_frame = tk.Frame(custom_ik_frame, bg=BG_ACCENT)
    tcp_frame.grid(row=1, column=1, sticky="nsew", padx=(0, 6))
    section_label(tcp_frame, "FK TCP Pose").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))
    custom_ik_tcp_pos_vars = []
    custom_ik_tcp_rpy_vars = []
    for row_index, axis in enumerate(["X", "Y", "Z"], start=1):
        label(tcp_frame, axis, fg=FG_LABEL).grid(row=row_index, column=0, sticky="w", pady=2)
        value_var = tk.StringVar(value="--")
        label(tcp_frame, "", textvariable=value_var, fg=FG_VALUE, font=FONT_VALUE, width=8, anchor="e").grid(
            row=row_index, column=1, sticky="e", padx=(8, 4), pady=2
        )
        label(tcp_frame, "mm", fg=FG_DIM).grid(row=row_index, column=2, sticky="w", pady=2)
        custom_ik_tcp_pos_vars.append(value_var)
    for row_index, axis in enumerate(["Rx", "Ry", "Rz"], start=4):
        label(tcp_frame, axis, fg=FG_LABEL).grid(row=row_index, column=0, sticky="w", pady=2)
        value_var = tk.StringVar(value="--")
        label(tcp_frame, "", textvariable=value_var, fg=FG_VALUE, font=FONT_VALUE, width=8, anchor="e").grid(
            row=row_index, column=1, sticky="e", padx=(8, 4), pady=2
        )
        label(tcp_frame, "deg", fg=FG_DIM).grid(row=row_index, column=2, sticky="w", pady=2)
        custom_ik_tcp_rpy_vars.append(value_var)

    error_frame = tk.Frame(custom_ik_frame, bg=BG_ACCENT)
    error_frame.grid(row=1, column=2, sticky="nsew")
    section_label(error_frame, "Target vs FK Error").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))
    custom_ik_pos_err_var = tk.StringVar(value="--")
    custom_ik_ori_err_var = tk.StringVar(value="--")
    custom_ik_meta_var = tk.StringVar(value="Idle")
    label(error_frame, "Position", fg=FG_LABEL).grid(row=1, column=0, sticky="w", pady=2)
    label(error_frame, "", textvariable=custom_ik_pos_err_var, fg=FG_VALUE, font=FONT_VALUE, width=8, anchor="e").grid(
        row=1, column=1, sticky="e", padx=(8, 4), pady=2
    )
    label(error_frame, "mm", fg=FG_DIM).grid(row=1, column=2, sticky="w", pady=2)
    label(error_frame, "Orientation", fg=FG_LABEL).grid(row=2, column=0, sticky="w", pady=2)
    label(error_frame, "", textvariable=custom_ik_ori_err_var, fg=FG_VALUE, font=FONT_VALUE, width=8, anchor="e").grid(
        row=2, column=1, sticky="e", padx=(8, 4), pady=2
    )
    label(error_frame, "deg", fg=FG_DIM).grid(row=2, column=2, sticky="w", pady=2)
    label(error_frame, "Solver", fg=FG_LABEL).grid(row=3, column=0, sticky="w", pady=(6, 2))
    tk.Label(
        error_frame,
        textvariable=custom_ik_meta_var,
        bg=BG_ACCENT,
        fg=FG_DIM,
        font=FONT_LABEL,
        justify="left",
        anchor="w",
    ).grid(row=4, column=0, columnspan=3, sticky="w")

    pose_mode_btn.configure(
        command=lambda: (mode_var.set(0), _apply_pose_mode_styles(mode_var, pose_wrap, joint_wrap, pose_mode_btn, joint_mode_btn))
    )
    joint_mode_btn.configure(
        command=lambda: (mode_var.set(1), _apply_pose_mode_styles(mode_var, pose_wrap, joint_wrap, pose_mode_btn, joint_mode_btn))
    )
    _apply_pose_mode_styles(mode_var, pose_wrap, joint_wrap, pose_mode_btn, joint_mode_btn)

    return PoseControlPanel(
        frame=tab,
        mode_var=mode_var,
        x_var=axis_vars["x"],
        y_var=axis_vars["y"],
        z_var=axis_vars["z"],
        rx_var=axis_vars["rx"],
        ry_var=axis_vars["ry"],
        rz_var=axis_vars["rz"],
        pos_step_var=pos_step_var,
        ang_step_var=ang_step_var,
        only_pos_var=only_pos_var,
        pos_err_var=pos_err_var,
        joint_entry_vars=joint_entry_vars,
        axis_jog_buttons=axis_jog_buttons,
        home_target_btn=home_target_btn,
        set_target_btn=set_target_btn,
        go_sim_btn=go_sim_btn,
        go_robot_btn=go_robot_btn,
        home_btn=home_btn,
        stop_btn=stop_btn,
        custom_ik_settings_btn=settings_btn,
        custom_ik_btn=custom_ik_btn,
        custom_ik_go_sim_btn=custom_ik_go_sim_btn,
        custom_ik_go_robot_btn=custom_ik_go_robot_btn,
        custom_ik_joint_vars=custom_ik_joint_vars,
        custom_ik_tcp_pos_vars=custom_ik_tcp_pos_vars,
        custom_ik_tcp_rpy_vars=custom_ik_tcp_rpy_vars,
        custom_ik_pos_err_var=custom_ik_pos_err_var,
        custom_ik_ori_err_var=custom_ik_ori_err_var,
        custom_ik_meta_var=custom_ik_meta_var,
        status_var=status_var,
    )


def _build_disabled_tab(tab, title, message, stop_label=None):
    tab.configure(bg=BG_PANEL)
    tab.columnconfigure(0, weight=1)
    frame = accent_frame(tab, padx=10, pady=8)
    frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
    section_label(frame, title).grid(row=0, column=0, sticky="w", pady=(0, 4))
    tk.Label(frame, text=message, bg=BG_ACCENT, fg=FG_LABEL, font=FONT_LABEL, justify="left").grid(
        row=1, column=0, sticky="w"
    )
    if stop_label:
        footer = tk.Frame(tab, bg=BG_PANEL)
        footer.grid(row=1, column=0, sticky="w")
        button = btn(footer, stop_label, color=BTN_STOP)
        button.grid(row=0, column=0)
        disable_widget(button)


def _apply_jog_target_styles(target_var, sim_btn, robot_btn):
    if target_var.get() == 0:
        sim_btn.configure(bg=BTN_PRIMARY, fg=FG_WHITE, activebackground=_lighten(BTN_PRIMARY))
        robot_btn.configure(bg="#dde0e6", fg=FG_LABEL, activebackground="#c8ccd4", activeforeground=FG_SECTION)
    else:
        sim_btn.configure(bg="#dde0e6", fg=FG_LABEL, activebackground="#c8ccd4", activeforeground=FG_SECTION)
        robot_btn.configure(bg=BTN_PRIMARY, fg=FG_WHITE, activebackground=_lighten(BTN_PRIMARY))


def _build_jog_tab(tab) -> ManualJogPanel:
    tab.configure(bg=BG_PANEL)
    tab.columnconfigure(0, weight=1)

    target_var = tk.IntVar(value=1)

    target_bar = tk.Frame(tab, bg=BG_PANEL)
    target_bar.grid(row=0, column=0, sticky="w", pady=(0, 8))
    label(target_bar, "Jog target:", fg=FG_LABEL, font=FONT_HEADER).grid(row=0, column=0, padx=(0, 10))

    jog_pill = tk.Frame(target_bar, bg="#b0b4bc", padx=1, pady=1)
    jog_pill.grid(row=0, column=1)
    sim_btn = tk.Button(jog_pill, text="Sim", relief="flat", bd=0, padx=14, pady=4, font=FONT_HEADER)
    robot_btn = tk.Button(jog_pill, text="Robot", relief="flat", bd=0, padx=14, pady=4, font=FONT_HEADER)
    sim_btn.grid(row=0, column=0, padx=(0, 1))
    robot_btn.grid(row=0, column=1)

    cols_frame = tk.Frame(tab, bg=BG_PANEL)
    cols_frame.grid(row=1, column=0, sticky="nsew")
    cols_frame.columnconfigure(0, weight=1)
    cols_frame.columnconfigure(1, weight=1)

    joint_frame = accent_frame(cols_frame, padx=10, pady=8)
    joint_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
    section_label(joint_frame, "Joint Jog").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 2))

    step_j_row = tk.Frame(joint_frame, bg=BG_ACCENT)
    step_j_row.grid(row=1, column=0, columnspan=3, sticky="w", pady=(0, 8))
    label(step_j_row, "Step:", fg=FG_LABEL).grid(row=0, column=0, padx=(0, 4))
    joint_step_entry, joint_step_var = entry(step_j_row, default="5.0", width=6)
    joint_step_entry.grid(row=0, column=1, padx=(0, 3))
    label(step_j_row, "deg", fg=FG_DIM).grid(row=0, column=2)

    separator(joint_frame).grid(row=2, column=0, columnspan=3, sticky="ew", pady=(0, 6))

    joint_current_vars = []
    joint_jog_buttons = {}
    for i in range(6):
        row = i + 3
        label(joint_frame, f"J{i + 1}", fg=FG_LABEL, font=FONT_HEADER).grid(row=row, column=0, sticky="w", pady=3)
        current_var = tk.StringVar(value="--")
        minus_btn = btn(joint_frame, "-", color=BTN_NEUTRAL, width=3)
        plus_btn = btn(joint_frame, "+", color=BTN_PRIMARY, width=3)
        minus_btn.grid(row=row, column=1, padx=(8, 3), pady=3)
        plus_btn.grid(row=row, column=2, pady=3)
        joint_current_vars.append(current_var)
        joint_jog_buttons[i] = (minus_btn, plus_btn)

    cart_frame = accent_frame(cols_frame, padx=10, pady=8)
    cart_frame.grid(row=0, column=1, sticky="nsew")
    section_label(cart_frame, "Cartesian Jog").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 2))

    step_c_row = tk.Frame(cart_frame, bg=BG_ACCENT)
    step_c_row.grid(row=1, column=0, columnspan=3, sticky="w", pady=(0, 8))
    label(step_c_row, "Step:", fg=FG_LABEL).grid(row=0, column=0, padx=(0, 4))
    cart_step_entry, cart_step_var = entry(step_c_row, default="5.0", width=6)
    cart_step_entry.grid(row=0, column=1, padx=(0, 3))
    label(step_c_row, "mm", fg=FG_DIM).grid(row=0, column=2)

    separator(cart_frame).grid(row=2, column=0, columnspan=3, sticky="ew", pady=(0, 6))

    cart_current_vars = []
    cart_jog_buttons = {}
    for i, axis in enumerate(["X", "Y", "Z"]):
        row = i + 3
        label(cart_frame, axis, fg=FG_LABEL, font=FONT_HEADER).grid(row=row, column=0, sticky="w", pady=4)
        current_var = tk.StringVar(value="--")
        minus_btn = btn(cart_frame, "-", color=BTN_NEUTRAL, width=3)
        plus_btn = btn(cart_frame, "+", color=BTN_PRIMARY, width=3)
        minus_btn.grid(row=row, column=1, padx=(8, 3), pady=4)
        plus_btn.grid(row=row, column=2, pady=4)
        cart_current_vars.append(current_var)
        cart_jog_buttons[axis.lower()] = (minus_btn, plus_btn)

    foot = tk.Frame(tab, bg=BG_PANEL)
    foot.grid(row=2, column=0, sticky="ew", pady=(8, 0))
    foot.columnconfigure(2, weight=1)
    stop_btn = btn(foot, "Stop All", color=BTN_STOP)
    stop_btn.grid(row=0, column=0)
    busy_badge = tk.Label(
        foot,
        text="MOVING",
        bg="#fff3e0",
        fg=BTN_WARN,
        font=FONT_HEADER,
        padx=8,
        pady=3,
    )
    busy_badge.grid(row=0, column=1, padx=(10, 6))
    busy_badge.grid_remove()
    busy_progress = ttk.Progressbar(
        foot,
        mode="indeterminate",
        length=120,
        style="V2Status.Horizontal.TProgressbar",
    )
    busy_progress.grid(row=0, column=2, sticky="w")
    busy_progress.grid_remove()
    status_var = tk.StringVar(value="Status: idle")
    tk.Label(foot, textvariable=status_var, bg=BG_PANEL, fg=FG_DIM, font=FONT_STATUS).grid(
        row=1, column=0, columnspan=3, sticky="w", pady=(8, 0)
    )

    sim_btn.configure(command=lambda: (target_var.set(0), _apply_jog_target_styles(target_var, sim_btn, robot_btn)))
    robot_btn.configure(command=lambda: (target_var.set(1), _apply_jog_target_styles(target_var, sim_btn, robot_btn)))
    _apply_jog_target_styles(target_var, sim_btn, robot_btn)

    return ManualJogPanel(
        frame=tab,
        target_var=target_var,
        joint_step_var=joint_step_var,
        cart_step_var=cart_step_var,
        joint_step_entry=joint_step_entry,
        cart_step_entry=cart_step_entry,
        joint_current_vars=joint_current_vars,
        cart_current_vars=cart_current_vars,
        joint_jog_buttons=joint_jog_buttons,
        cart_jog_buttons=cart_jog_buttons,
        sim_btn=sim_btn,
        robot_btn=robot_btn,
        stop_btn=stop_btn,
        busy_badge=busy_badge,
        busy_progress=busy_progress,
        status_var=status_var,
    )


def _build_keyframes_tab(tab) -> KeyframesPanel:
    tab.configure(bg=BG_PANEL)
    tab.columnconfigure(0, weight=1)
    tab.rowconfigure(0, weight=1)

    tree_style = ttk.Style()
    tree_style.configure(
        "KF.Treeview",
        background=BG_PANEL,
        foreground=FG_VALUE,
        fieldbackground=BG_PANEL,
        rowheight=24,
        font=FONT_VALUE,
        borderwidth=0,
    )
    tree_style.configure(
        "KF.Treeview.Heading",
        background="#d6d8dc",
        foreground=FG_SECTION,
        font=("Helvetica", 8, "bold"),
        relief="flat",
    )
    tree_style.map(
        "KF.Treeview",
        background=[("selected", BTN_PRIMARY)],
        foreground=[("selected", FG_WHITE)],
    )
    tree_style.map(
        "KF.Treeview.Heading",
        background=[("active", "#c4c8ce")],
    )

    cols = ("idx", "j1", "j2", "j3", "j4", "j5", "j6", "delay")
    col_widths = {"idx": 28, "j1": 58, "j2": 58, "j3": 58, "j4": 58, "j5": 58, "j6": 58, "delay": 68}
    col_labels = {
        "idx": "#",
        "j1": "J1",
        "j2": "J2",
        "j3": "J3",
        "j4": "J4",
        "j5": "J5",
        "j6": "J6",
        "delay": "Delay",
    }

    tree_frame = tk.Frame(
        tab,
        bg=BG_PANEL,
        highlightthickness=1,
        highlightbackground=SEP_COLOR,
    )
    tree_frame.grid(row=0, column=0, sticky="nsew", pady=(0, 8))
    tree_frame.columnconfigure(0, weight=1)
    tree_frame.rowconfigure(0, weight=1)

    scrollbar = ttk.Scrollbar(tree_frame, orient="vertical")
    scrollbar.grid(row=0, column=1, sticky="ns")

    tree = ttk.Treeview(
        tree_frame,
        columns=cols,
        show="headings",
        style="KF.Treeview",
        yscrollcommand=scrollbar.set,
        selectmode="browse",
        height=6,
    )
    tree.grid(row=0, column=0, sticky="nsew")
    scrollbar.configure(command=tree.yview)
    for col in cols:
        tree.heading(col, text=col_labels[col], anchor="center")
        tree.column(col, width=col_widths[col], minwidth=col_widths[col], anchor="center", stretch=False)

    tree.tag_configure("odd", background=BG_PANEL)
    tree.tag_configure("even", background="#f0f1f3")

    list_btns = tk.Frame(tab, bg=BG_PANEL)
    list_btns.grid(row=1, column=0, sticky="w", pady=(0, 8))
    remove_selected_btn = btn(list_btns, "Remove Selected", color=BTN_NEUTRAL, small=True)
    remove_last_btn = btn(list_btns, "Remove Last", color=BTN_NEUTRAL, small=True)
    clear_all_btn = btn(list_btns, "Clear All", color=BTN_STOP, small=True)
    move_up_btn = btn(list_btns, "Move Up", color=BTN_NEUTRAL, small=True)
    move_down_btn = btn(list_btns, "Move Down", color=BTN_NEUTRAL, small=True)
    save_btn = btn(list_btns, "Save", color=BTN_PRIMARY, small=True)
    load_btn = btn(list_btns, "Load", color=BTN_NEUTRAL, small=True)
    remove_selected_btn.grid(row=0, column=0, padx=(0, 4))
    remove_last_btn.grid(row=0, column=1, padx=(0, 4))
    clear_all_btn.grid(row=0, column=2, padx=(0, 10))
    move_up_btn.grid(row=0, column=3, padx=(0, 4))
    move_down_btn.grid(row=0, column=4, padx=(0, 10))
    save_btn.grid(row=0, column=5, padx=(0, 4))
    load_btn.grid(row=0, column=6)

    play_frame = accent_frame(tab, padx=10, pady=8)
    play_frame.grid(row=2, column=0, sticky="ew")
    play_frame.columnconfigure(0, weight=1)

    settings_row = tk.Frame(play_frame, bg=BG_ACCENT)
    settings_row.grid(row=0, column=0, sticky="w", pady=(0, 8))

    speed_entry, speed_var = entry(settings_row, default="100", width=ENTRY_W_SM)
    accel_entry, accel_var = entry(settings_row, default="100", width=ENTRY_W_SM)
    auto_sync_var = tk.BooleanVar(value=True)
    label(settings_row, "Speed %", fg=FG_LABEL).grid(row=0, column=0, padx=(0, 4))
    speed_entry.grid(row=0, column=1, padx=(0, 2))
    label(settings_row, "Accel %", fg=FG_LABEL).grid(row=0, column=3, padx=(12, 4))
    accel_entry.grid(row=0, column=4, padx=(0, 2))
    settings_btn = btn(settings_row, "⚙", color=BTN_NEUTRAL, small=True, width=2)
    settings_btn.grid(row=0, column=7, padx=(12, 0), sticky="e")

    chk_frame = tk.Frame(play_frame, bg=BG_ACCENT)
    chk_frame.grid(row=1, column=0, sticky="w", pady=(0, 8))
    settings_btn.grid_remove()
    loop_var = tk.BooleanVar(value=False)
    pingpong_var = tk.BooleanVar(value=False)
    interpolate_var = tk.BooleanVar(value=False)
    show_markers_var = tk.BooleanVar(value=False)
    trace_markers_var = tk.BooleanVar(value=False)
    loop_chk = tk.Checkbutton(
        chk_frame,
        text="Loop",
        variable=loop_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        selectcolor=BG_PANEL,
        activebackground=BG_ACCENT,
        font=FONT_HEADER,
        cursor="hand2",
    )
    pingpong_chk = tk.Checkbutton(
        chk_frame,
        text="Pin-Pong",
        variable=pingpong_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        selectcolor=BG_PANEL,
        activebackground=BG_ACCENT,
        font=FONT_HEADER,
        cursor="hand2",
    )
    auto_sync_chk = tk.Checkbutton(
        chk_frame,
        text="Auto Sync Arrival",
        variable=auto_sync_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        selectcolor=BG_PANEL,
        activebackground=BG_ACCENT,
        font=FONT_HEADER,
        cursor="hand2",
    )
    interpolate_chk = tk.Checkbutton(
        chk_frame,
        text="Interpolate",
        variable=interpolate_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        selectcolor=BG_PANEL,
        activebackground=BG_ACCENT,
        font=FONT_HEADER,
        cursor="hand2",
    )
    show_markers_chk = tk.Checkbutton(
        chk_frame,
        text="Show Keyframes",
        variable=show_markers_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        selectcolor=BG_PANEL,
        activebackground=BG_ACCENT,
        font=FONT_HEADER,
        cursor="hand2",
    )
    trace_markers_chk = tk.Checkbutton(
        chk_frame,
        text="Trace Keyframes",
        variable=trace_markers_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        selectcolor=BG_PANEL,
        activebackground=BG_ACCENT,
        font=FONT_HEADER,
        cursor="hand2",
    )
    loop_chk.grid(row=0, column=0, padx=(0, 12), sticky="w")
    pingpong_chk.grid(row=0, column=1, sticky="w")
    auto_sync_chk.grid(row=0, column=2, padx=(12, 0), sticky="w")
    interpolate_chk.grid(row=1, column=0, padx=(0, 12), pady=(4, 0), sticky="w")
    show_markers_chk.grid(row=1, column=1, pady=(4, 0), sticky="w")
    trace_markers_chk.grid(row=1, column=2, padx=(12, 0), pady=(4, 0), sticky="w")
    Tooltip(auto_sync_chk, "For non-interpolated robot keyframes, slow each joint so they reach the target together")
    Tooltip(interpolate_chk, "Interpolate continuously between keyframes using Cartesian path planning")

    play_row = tk.Frame(play_frame, bg=BG_ACCENT)
    play_row.grid(row=2, column=0, sticky="w")
    play_sim_btn = btn(play_row, "Play Sim", color=BTN_NEUTRAL)
    play_robot_btn = btn(play_row, "Play Robot", color=BTN_GO)
    stop_btn = btn(play_row, "Stop", color=BTN_STOP)
    move_robot_selected_btn = btn(play_row, "Move Robot to Selected", color=BTN_GO)
    move_sim_selected_btn = btn(play_row, "Move Sim to Selected", color=BTN_NEUTRAL)
    play_sim_btn.grid(row=0, column=0, padx=(0, 4))
    play_robot_btn.grid(row=0, column=1, padx=(0, 4))
    stop_btn.grid(row=0, column=2, padx=(0, 4))
    move_robot_selected_btn.grid(row=0, column=3, padx=(0, 4))
    move_sim_selected_btn.grid(row=0, column=4)

    status_var = tk.StringVar(value="")

    return KeyframesPanel(
        frame=tab,
        tree=tree,
        speed_var=speed_var,
        accel_var=accel_var,
        auto_sync_var=auto_sync_var,
        loop_var=loop_var,
        pingpong_var=pingpong_var,
        interpolate_var=interpolate_var,
        show_markers_var=show_markers_var,
        trace_markers_var=trace_markers_var,
        status_var=status_var,
        remove_selected_btn=remove_selected_btn,
        remove_last_btn=remove_last_btn,
        clear_all_btn=clear_all_btn,
        move_up_btn=move_up_btn,
        move_down_btn=move_down_btn,
        save_btn=save_btn,
        load_btn=load_btn,
        play_sim_btn=play_sim_btn,
        play_robot_btn=play_robot_btn,
        stop_btn=stop_btn,
        move_robot_selected_btn=move_robot_selected_btn,
        move_sim_selected_btn=move_sim_selected_btn,
    )


def _build_demo_tab(tab) -> DemoPanel:
    tab.configure(bg=BG_PANEL)
    tab.columnconfigure(0, weight=1)

    demo_help_text = (
        "Patterns:\n"
        "Cone: sweeps Rx and Ry around the center orientation while keeping Rz fixed.\n"
        "Axis Wobble: oscillates one orientation axis at a time around the center pose.\n"
        "Raster: scans a grid-like patch in Rx/Ry around the center orientation.\n\n"
        "Parameters:\n"
        "Amplitude: maximum angular offset from the center orientation, in degrees.\n"
        "Speed: orientation sweep speed in degrees per second.\n"
        "Cycles: how many times the selected pattern repeats.\n"
        "Loop: automatically restarts the sim sweep after completion.\n\n"
        "Actions:\n"
        "Capture TCP: copies the current simulation TCP pose into the demo anchor fields.\n"
        "Capture Enc TCP: copies the latest encoder TCP pose into the demo anchor fields.\n"
        "Probe Reach: checks a 5x5 sample of orientations at the fixed XYZ anchor.\n"
        "Play Sim: plans the sweep and plays it in simulation only.\n"
        "Play Robot: reserved for future hardware playback.\n"
        "Stop: halts the current sweep."
    )

    anchor_frame = accent_frame(tab, padx=10, pady=8)
    anchor_frame.grid(row=0, column=0, sticky="ew", pady=(0, 8))
    section_label(anchor_frame, "Anchor Pose").grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 6))

    anchor_vars = []
    axes = ("X", "Y", "Z", "Rx", "Ry", "Rz")
    defaults = ("0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    for idx, (axis, default) in enumerate(zip(axes, defaults)):
        row = 1 + (idx // 3)
        col = (idx % 3) * 2
        label(anchor_frame, axis, fg=FG_LABEL).grid(row=row, column=col, padx=(0, 4), pady=2, sticky="w")
        anchor_entry, anchor_var = entry(anchor_frame, default=default, width=ENTRY_W_SM)
        anchor_entry.grid(row=row, column=col + 1, padx=(0, 10), pady=2, sticky="w")
        anchor_entry.configure(state="readonly")
        anchor_vars.append(anchor_var)

    capture_row = tk.Frame(anchor_frame, bg=BG_ACCENT)
    capture_row.grid(row=3, column=0, columnspan=6, sticky="w", pady=(8, 0))
    capture_sim_btn = btn(capture_row, "Capture TCP", color=BTN_NEUTRAL)
    capture_enc_btn = btn(capture_row, "Capture Enc TCP", color=BTN_PRIMARY)
    capture_sim_btn.grid(row=0, column=0, padx=(0, 4))
    capture_enc_btn.grid(row=0, column=1)

    pattern_frame = accent_frame(tab, padx=10, pady=8)
    pattern_frame.grid(row=1, column=0, sticky="ew", pady=(0, 8))
    section_label(pattern_frame, "Pattern").grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 6))
    info_btn = btn(pattern_frame, "i", color=BTN_PRIMARY, small=True, width=2)
    info_btn.grid(row=0, column=5, sticky="e", pady=(0, 6))
    Tooltip(info_btn, demo_help_text)
    pattern_var = tk.StringVar(value="Cone")
    amplitude_entry, amplitude_var = entry(pattern_frame, default="15", width=ENTRY_W_SM)
    speed_entry, speed_var = entry(pattern_frame, default="30", width=ENTRY_W_SM)
    cycles_entry, cycles_var = entry(pattern_frame, default="2", width=ENTRY_W_SM)
    loop_var = tk.BooleanVar(value=False)

    label(pattern_frame, "Pattern", fg=FG_LABEL).grid(row=1, column=0, padx=(0, 4), pady=2, sticky="w")
    tk.OptionMenu(pattern_frame, pattern_var, "Cone", "Axis Wobble", "Raster").grid(row=1, column=1, padx=(0, 10), pady=2, sticky="w")
    label(pattern_frame, "Amplitude", fg=FG_LABEL).grid(row=1, column=2, padx=(0, 4), pady=2, sticky="w")
    amplitude_entry.grid(row=1, column=3, padx=(0, 10), pady=2, sticky="w")
    label(pattern_frame, "Speed", fg=FG_LABEL).grid(row=2, column=0, padx=(0, 4), pady=2, sticky="w")
    speed_entry.grid(row=2, column=1, padx=(0, 10), pady=2, sticky="w")
    label(pattern_frame, "Cycles", fg=FG_LABEL).grid(row=2, column=2, padx=(0, 4), pady=2, sticky="w")
    cycles_entry.grid(row=2, column=3, padx=(0, 10), pady=2, sticky="w")
    tk.Checkbutton(
        pattern_frame,
        text="Loop",
        variable=loop_var,
        bg=BG_ACCENT,
        fg=FG_LABEL,
        selectcolor=BG_PANEL,
        activebackground=BG_ACCENT,
        font=FONT_HEADER,
        cursor="hand2",
    ).grid(row=3, column=0, sticky="w", pady=(6, 0))

    action_row = tk.Frame(tab, bg=BG_PANEL)
    action_row.grid(row=2, column=0, sticky="w")
    probe_btn = btn(action_row, "Probe Reach", color=BTN_NEUTRAL)
    play_sim_btn = btn(action_row, "Play Sim", color=BTN_NEUTRAL)
    play_robot_btn = btn(action_row, "Play Robot", color=BTN_GO)
    stop_btn = btn(action_row, "Stop", color=BTN_STOP)
    probe_btn.grid(row=0, column=0, padx=(0, 4))
    play_sim_btn.grid(row=0, column=1, padx=(0, 4))
    play_robot_btn.grid(row=0, column=2, padx=(0, 4))
    stop_btn.grid(row=0, column=3)

    status_var = tk.StringVar(value="Demo: idle")
    tk.Label(
        tab,
        textvariable=status_var,
        bg=BG_PANEL,
        fg=FG_DIM,
        font=FONT_STATUS,
        anchor="w",
        justify="left",
        wraplength=420,
    ).grid(row=3, column=0, sticky="ew", pady=(8, 0))

    return DemoPanel(
        frame=tab,
        anchor_x_var=anchor_vars[0],
        anchor_y_var=anchor_vars[1],
        anchor_z_var=anchor_vars[2],
        anchor_rx_var=anchor_vars[3],
        anchor_ry_var=anchor_vars[4],
        anchor_rz_var=anchor_vars[5],
        pattern_var=pattern_var,
        amplitude_var=amplitude_var,
        speed_var=speed_var,
        cycles_var=cycles_var,
        loop_var=loop_var,
        capture_sim_btn=capture_sim_btn,
        capture_enc_btn=capture_enc_btn,
        probe_btn=probe_btn,
        play_sim_btn=play_sim_btn,
        play_robot_btn=play_robot_btn,
        stop_btn=stop_btn,
        status_var=status_var,
    )


def build_center_column(parent) -> tuple[PoseControlPanel, ManualJogPanel, KeyframesPanel, DemoPanel]:
    outer = panel_frame(parent)
    outer.grid(row=1, column=1, sticky="nsew", padx=PAD // 2, pady=(PAD, 0))
    outer.columnconfigure(0, weight=1)
    outer.rowconfigure(2, weight=1)
    section_label(outer, "MOTION CONTROL").grid(row=0, column=0, sticky="w", pady=(0, 6))
    separator(outer).grid(row=1, column=0, sticky="ew", pady=(0, 8))
    _style_notebook()
    notebook = ttk.Notebook(outer, style="Light.TNotebook")
    notebook.grid(row=2, column=0, sticky="nsew")

    pose_tab = tk.Frame(notebook, bg=BG_PANEL, padx=PAD, pady=PAD)
    jog_tab = tk.Frame(notebook, bg=BG_PANEL, padx=PAD, pady=PAD)
    keyframes_tab = tk.Frame(notebook, bg=BG_PANEL, padx=PAD, pady=PAD)
    demo_tab = tk.Frame(notebook, bg=BG_PANEL, padx=PAD, pady=PAD)
    notebook.add(pose_tab, text="  Pose Control  ")
    notebook.add(jog_tab, text="  Manual Jog  ")
    notebook.add(keyframes_tab, text="  Keyframes  ")
    notebook.add(demo_tab, text="  Demo  ")

    pose_panel = _build_pose_control_tab(pose_tab)
    manual_jog_panel = _build_jog_tab(jog_tab)
    keyframes_panel = _build_keyframes_tab(keyframes_tab)
    demo_panel = _build_demo_tab(demo_tab)
    return pose_panel, manual_jog_panel, keyframes_panel, demo_panel


def _section_row(table, row, text, colspan=3):
    frame = tk.Frame(table, bg=BG_ACCENT)
    frame.grid(row=row, column=0, columnspan=colspan, sticky="ew", pady=(4, 0))
    tk.Label(frame, text=text, bg=BG_ACCENT, fg=FG_SECTION, font=("Helvetica", 8, "bold"), anchor="w", padx=6, pady=2).pack(fill="x")


def _data_row(table, row, row_label, sim_var, enc_var, even=True):
    bg = BG_PANEL if even else "#f0f1f3"
    tk.Label(table, text=row_label, bg=bg, fg=FG_LABEL, font=FONT_HEADER, width=5, anchor="w", padx=6).grid(
        row=row, column=0, sticky="ew", ipady=2
    )
    tk.Label(table, textvariable=sim_var, bg=bg, fg=FG_VALUE, font=FONT_VALUE, width=9, anchor="e").grid(
        row=row, column=1, sticky="ew", padx=(0, 1), ipady=2
    )
    tk.Label(table, textvariable=enc_var, bg=bg, fg=FG_VALUE, font=FONT_VALUE, width=9, anchor="e").grid(
        row=row, column=2, sticky="ew", ipady=2
    )


def build_right_column(parent) -> tuple[RobotStatePanel, RobotControlPanel]:
    outer = tk.Frame(parent, bg=BG_ROOT)
    outer.grid(row=1, column=2, sticky="nsew", padx=(PAD // 2, PAD), pady=(PAD, 0))
    outer.columnconfigure(0, weight=1)
    outer.rowconfigure(0, weight=1)

    state_frame = panel_frame(outer, padx=PAD, pady=PAD)
    state_frame.grid(row=0, column=0, sticky="nsew")
    section_label(state_frame, "ROBOT STATE").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 6))
    separator(state_frame).grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 6))

    table = tk.Frame(state_frame, bg=BG_PANEL)
    table.grid(row=2, column=0, columnspan=3, sticky="ew")
    table.columnconfigure(1, weight=1)
    table.columnconfigure(2, weight=1)
    header_bg = "#d6d8dc"
    for col, (text, anchor) in enumerate([("", "w"), ("Sim", "e"), ("Encoder", "e")]):
        tk.Label(
            table,
            text=text,
            bg=header_bg,
            fg=FG_SECTION,
            font=("Helvetica", 8, "bold"),
            width=9 if col > 0 else 5,
            anchor=anchor,
            padx=6,
            pady=3,
        ).grid(row=0, column=col, sticky="ew", padx=(0, 1) if col < 2 else 0)

    sim_joint_vars = [tk.StringVar(value="--") for _ in range(6)]
    encoder_joint_vars = [tk.StringVar(value="--") for _ in range(6)]
    _section_row(table, 1, "Joint Angles (deg)")
    for i in range(6):
        _data_row(table, i + 2, f"J{i + 1}", sim_joint_vars[i], encoder_joint_vars[i], even=i % 2 == 0)

    sim_tcp_pos_vars = [tk.StringVar(value="--") for _ in range(3)]
    encoder_tcp_pos_vars = [tk.StringVar(value="--") for _ in range(3)]
    _section_row(table, 8, "TCP Position (mm)")
    for i, axis in enumerate(["X", "Y", "Z"]):
        _data_row(table, i + 9, axis, sim_tcp_pos_vars[i], encoder_tcp_pos_vars[i], even=i % 2 == 0)

    sim_tcp_rpy_vars = [tk.StringVar(value="--") for _ in range(3)]
    encoder_tcp_rpy_vars = [tk.StringVar(value="--") for _ in range(3)]
    _section_row(table, 12, "TCP Orientation (deg)")
    for i, axis in enumerate(["Rx", "Ry", "Rz"]):
        _data_row(table, i + 13, axis, sim_tcp_rpy_vars[i], encoder_tcp_rpy_vars[i], even=i % 2 == 0)

    collision_var = tk.StringVar(value="No")
    collision_frame = tk.Frame(state_frame, bg="#dcedd7", padx=10, pady=8, highlightthickness=1, highlightbackground=SEP_COLOR)
    collision_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(8, 0))
    collision_frame.columnconfigure(0, weight=1)
    collision_value_label = tk.Label(
        collision_frame,
        textvariable=collision_var,
        text="Collision Detected: No",
        bg="#dcedd7",
        fg=FG_SECTION,
        font=FONT_HEADER,
        anchor="w",
        padx=2,
    )
    collision_value_label.grid(row=0, column=0, sticky="ew")

    pos_err_var = tk.StringVar(value="-- mm")

    control_frame = panel_frame(outer, padx=PAD, pady=PAD)
    control_frame.grid(row=1, column=0, sticky="ew", pady=(PAD // 2, 0))
    control_frame.columnconfigure(0, weight=1)
    section_label(control_frame, "ADD KEYFRAMES").grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 6))
    separator(control_frame).grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 8))
    keyframe_frame = tk.Frame(control_frame, bg=BG_PANEL)
    keyframe_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(0, 6))
    keyframe_frame.columnconfigure(0, weight=1)
    keyframe_frame.columnconfigure(1, weight=1)
    keyframe_sim_btn = btn(keyframe_frame, "+ KF Sim", color=BTN_WARN, width=12)
    keyframe_enc_btn = btn(keyframe_frame, "+ KF Enc", color=BTN_PRIMARY, width=12)
    keyframe_sim_btn.grid(row=0, column=0, sticky="ew", padx=(0, 2))
    keyframe_enc_btn.grid(row=0, column=1, sticky="ew", padx=(2, 0))
    Tooltip(keyframe_sim_btn, "Add keyframe from current simulation joint angles")
    Tooltip(keyframe_enc_btn, "Add keyframe from current encoder readings")

    separator(control_frame).grid(row=3, column=0, columnspan=3, sticky="ew", pady=(0, 6))
    status_var = tk.StringVar(value="Encoders: idle")
    tk.Label(
        control_frame,
        textvariable=status_var,
        bg=BG_PANEL,
        fg=FG_DIM,
        font=FONT_STATUS,
        anchor="w",
        justify="left",
        wraplength=220,
    ).grid(row=4, column=0, columnspan=3, sticky="ew")

    return (
        RobotStatePanel(
            frame=state_frame,
            sim_joint_vars=sim_joint_vars,
            encoder_joint_vars=encoder_joint_vars,
            sim_tcp_pos_vars=sim_tcp_pos_vars,
            encoder_tcp_pos_vars=encoder_tcp_pos_vars,
            sim_tcp_rpy_vars=sim_tcp_rpy_vars,
            encoder_tcp_rpy_vars=encoder_tcp_rpy_vars,
            collision_frame=collision_frame,
            collision_var=collision_var,
            collision_value_label=collision_value_label,
            pos_err_var=pos_err_var,
        ),
        RobotControlPanel(
            frame=control_frame,
            status_var=status_var,
            keyframe_sim_btn=keyframe_sim_btn,
            keyframe_enc_btn=keyframe_enc_btn,
        ),
    )


def build_status_bar(root) -> StatusBarPanel:
    frame = tk.Frame(root, bg=BG_HEADER, padx=PAD, pady=5)
    frame.grid(row=2, column=0, columnspan=3, sticky="ew")
    frame.columnconfigure(9, weight=1)
    path_status_var = tk.StringVar(value="Path: idle")
    progress_var = tk.StringVar(value="0%")
    ik_var = tk.StringVar(value="IK: idle")
    path_dot_label = tk.Label(frame, text="o", bg=BG_HEADER, fg=BTN_STOP, font=("Helvetica", 11, "bold"))
    path_dot_label.grid(row=0, column=0, padx=(0, 3))
    tk.Label(frame, textvariable=path_status_var, bg=BG_HEADER, fg=FG_HEADER_DARK, font=FONT_STATUS).grid(
        row=0, column=1, padx=(0, 14)
    )
    dark_separator(frame).grid(row=0, column=2, sticky="ns", padx=8)
    tk.Label(frame, text="Progress:", bg=BG_HEADER, fg=FG_DIM_DARK, font=FONT_STATUS).grid(
        row=0, column=3, padx=(0, 4)
    )
    progress_bar = ttk.Progressbar(
        frame,
        orient="horizontal",
        length=140,
        maximum=100,
        value=0,
        style="V2Status.Horizontal.TProgressbar",
    )
    progress_bar.grid(row=0, column=4, padx=(0, 4))
    tk.Label(frame, textvariable=progress_var, bg=BG_HEADER, fg=FG_HEADER_DARK, font=FONT_STATUS).grid(
        row=0, column=5, padx=(0, 14)
    )
    dark_separator(frame).grid(row=0, column=6, sticky="ns", padx=8)
    tk.Label(frame, text="", bg=BG_HEADER).grid(row=0, column=7, sticky="ew")
    tk.Label(frame, textvariable=ik_var, bg=BG_HEADER, fg=FG_HEADER_DARK, font=FONT_STATUS).grid(
        row=0, column=8
    )
    return StatusBarPanel(
        frame=frame,
        path_status_var=path_status_var,
        path_dot_label=path_dot_label,
        progress_var=progress_var,
        progress_bar=progress_bar,
        ik_var=ik_var,
    )


def build_main_layout(root, default_port: str) -> MainGuiLayout:
    root.title("Robot Arm Control")
    root.geometry("1150x825")
    root.minsize(1000, 600)
    root.configure(bg=BG_ROOT)
    root.resizable(True, True)
    root.columnconfigure(0, weight=0, minsize=220)
    root.columnconfigure(1, weight=1, minsize=420)
    root.columnconfigure(2, weight=0, minsize=260)
    root.rowconfigure(0, weight=0)
    root.rowconfigure(1, weight=1)
    root.rowconfigure(2, weight=0)

    header = build_header(root, default_port)
    terminal = build_terminal(root)
    pose_panel, manual_jog_panel, keyframes_panel, demo_panel = build_center_column(root)
    robot_state_panel, robot_control_panel = build_right_column(root)
    status_bar = build_status_bar(root)
    return MainGuiLayout(
        header=header,
        terminal=terminal,
        pose_panel=pose_panel,
        manual_jog_panel=manual_jog_panel,
        keyframes_panel=keyframes_panel,
        demo_panel=demo_panel,
        robot_state_panel=robot_state_panel,
        robot_control_panel=robot_control_panel,
        status_bar=status_bar,
    )
