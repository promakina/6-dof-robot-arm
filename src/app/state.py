import threading
from dataclasses import dataclass, field


@dataclass
class SharedState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    target_pos_m: list = field(default_factory=lambda: [0.0, 0.0, 0.0])
    target_orn: tuple = field(default_factory=lambda: (0.0, 0.0, 0.0, 1.0))
    target_rpy_deg: list = field(default_factory=lambda: [0.0, 0.0, 0.0])
    ik_enabled: bool = False
    only_position: bool = False
    joint_deg: list = field(default_factory=lambda: [0.0] * 6)
    manual_joint_target_deg: list = field(default_factory=lambda: [0.0] * 6)
    manual_joint_pending: bool = False
    manual_joint_motion_overrides: dict = field(default_factory=dict)
    tcp_pos_m: list = field(default_factory=lambda: [0.0, 0.0, 0.0])
    tcp_rpy_deg: list = field(default_factory=lambda: [0.0, 0.0, 0.0])
    point_a_m: list = field(default_factory=lambda: [0.0, 0.0, 0.0])
    point_a_valid: bool = False
    point_b_m: list = field(default_factory=lambda: [0.0, 0.0, 0.0])
    point_b_valid: bool = False
    state_hz: float = 0.0
    pos_err_m: float = 0.0
    collision_detected: bool = False
    pybullet_window_size: list = field(default_factory=lambda: [0, 0])
    streaming_active: bool = False
    hardware_streaming_active: bool = False
    trajectory_final_target: list = field(default_factory=lambda: [0.0] * 6)
    keyframe_markers_visible: bool = False
    keyframe_trace_visible: bool = False
    keyframe_marker_joint_sets_deg: list = field(default_factory=list)
    keyframe_markers_dirty: bool = False
