import json
import math
import os
import time
import ctypes
from ctypes import wintypes
from pathlib import Path

import pybullet as p
import pybullet_data

from app.logging_utils import get_stream_logger
from app_paths import get_settings_path
from ik.adapter import IKSolution
from ik.config import get_config as get_ik_config


TARGET_AXIS_LENGTH = 0.05
MM_TO_M = 0.001
M_TO_MM = 1000.0
MAX_JOINT_VELOCITY = 0.5
PYBULLET_WINDOW_WIDTH = 740
PYBULLET_WINDOW_HEIGHT = 720
_MODULE_LOG = get_stream_logger("streaming")
ROBOT_URDF_FLAGS = p.URDF_USE_INERTIA_FROM_FILE | p.URDF_USE_SELF_COLLISION


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_simulation_tuning():
    tuning_path = get_settings_path("simulation_tuning.json")
    if not tuning_path.exists():
        return {}
    try:
        return json.loads(tuning_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _MODULE_LOG.warning("Failed to load simulation_tuning.json: %s", exc)
        return {}


def get_tuning_value(section, key, default):
    if section is None:
        return default
    value = section.get(key, default)
    return default if value is None else value


def parse_kinematic_alpha(value):
    if value is None:
        return None
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        _MODULE_LOG.warning(
            "Invalid kinematic_mode.interpolation_alpha=%r; expected null or a numeric value in [0.0, 1.0]",
            value,
        )
        return None


def build_joint_type_names():
    type_names = {
        p.JOINT_REVOLUTE: "REVOLUTE",
        p.JOINT_PRISMATIC: "PRISMATIC",
        p.JOINT_SPHERICAL: "SPHERICAL",
        p.JOINT_PLANAR: "PLANAR",
        p.JOINT_FIXED: "FIXED",
    }
    joint_continuous = getattr(p, "JOINT_CONTINUOUS", None)
    if joint_continuous is not None:
        type_names[joint_continuous] = "CONTINUOUS"
    return type_names


def quat_multiply(q1, q2):
    x1, y1, z1, w1 = q1
    x2, y2, z2, w2 = q2
    return (
        w1 * x2 + x1 * w2 + y1 * z2 - z1 * y2,
        w1 * y2 - x1 * z2 + y1 * w2 + z1 * x2,
        w1 * z2 + x1 * y2 - y1 * x2 + z1 * w2,
        w1 * w2 - x1 * x2 - y1 * y2 - z1 * z2,
    )


def quat_inverse(q):
    x, y, z, w = q
    return (-x, -y, -z, w)


class PyBulletDirectIK:
    """Headless PyBullet client for IK fallback (independent from GUI client)."""

    def __init__(self):
        self._log = get_stream_logger("pybullet_direct_ik")
        self._cid = p.connect(p.DIRECT)
        urdf_path = _project_root() / "Assy_6axisArm_v3_export.urdf"
        self._robot_id = p.loadURDF(
            str(urdf_path),
            [0, 0, 0],
            p.getQuaternionFromEuler([0, 0, 0]),
            useFixedBase=True,
            flags=ROBOT_URDF_FLAGS,
            physicsClientId=self._cid,
        )

        joint_continuous = getattr(p, "JOINT_CONTINUOUS", None)
        num_joints = p.getNumJoints(self._robot_id, physicsClientId=self._cid)
        self._command_joint_indices = []
        self._lower_limits = []
        self._upper_limits = []
        self._joint_ranges = []
        self._rest_poses = []

        for i in range(num_joints):
            info = p.getJointInfo(self._robot_id, i, physicsClientId=self._cid)
            joint_type = info[2]
            lower = info[8]
            upper = info[9]
            current = p.getJointState(self._robot_id, i, physicsClientId=self._cid)[0]
            self._rest_poses.append(current)

            if joint_type == p.JOINT_FIXED:
                self._lower_limits.append(0.0)
                self._upper_limits.append(0.0)
                self._joint_ranges.append(0.0)
                continue

            if lower >= upper:
                revolute_like = {p.JOINT_REVOLUTE}
                if joint_continuous is not None:
                    revolute_like.add(joint_continuous)
                if joint_type in revolute_like:
                    lower, upper = -math.pi, math.pi
                elif joint_type == p.JOINT_PRISMATIC:
                    lower, upper = -0.1, 0.1
                else:
                    lower, upper = -math.pi, math.pi

            self._lower_limits.append(lower)
            self._upper_limits.append(upper)
            self._joint_ranges.append(upper - lower)
            if joint_type == p.JOINT_REVOLUTE and i <= 5:
                self._command_joint_indices.append(i)

        self._ee_index = None
        for i in range(num_joints):
            link_name = p.getJointInfo(self._robot_id, i, physicsClientId=self._cid)[12].decode("utf-8")
            if link_name == "tcp":
                self._ee_index = i
                break
        if self._ee_index is None:
            raise RuntimeError("PyBulletDirectIK: TCP link 'tcp' not found in URDF")

        self._log.info(
            "PyBulletDirectIK initialized: robot_id=%d ee_index=%d joints=%s",
            self._robot_id,
            self._ee_index,
            self._command_joint_indices,
        )

    def solve_position_only(self, x_mm, y_mm, z_mm, q0_deg=None):
        """Solve IK for position only, returns IKSolution."""
        target_pos = [x_mm * MM_TO_M, y_mm * MM_TO_M, z_mm * MM_TO_M]

        if q0_deg is not None:
            self._log.debug(
                "solve_position_only: target=(%.1f, %.1f, %.1f) seed=%s",
                x_mm,
                y_mm,
                z_mm,
                [f"{v:.2f}" for v in q0_deg],
            )
        else:
            self._log.debug(
                "solve_position_only: target=(%.1f, %.1f, %.1f) seed=None",
                x_mm,
                y_mm,
                z_mm,
            )

        if q0_deg is not None:
            for idx, ji in enumerate(self._command_joint_indices):
                if idx < len(q0_deg):
                    p.resetJointState(
                        self._robot_id,
                        ji,
                        math.radians(q0_deg[idx]),
                        physicsClientId=self._cid,
                    )

        p.stepSimulation(physicsClientId=self._cid)

        if q0_deg is not None and len(q0_deg) >= len(self._command_joint_indices):
            rest = list(self._rest_poses)
            for idx, ji in enumerate(self._command_joint_indices):
                if idx < len(q0_deg):
                    rest[ji] = math.radians(q0_deg[idx])
        else:
            rest = self._rest_poses

        joint_positions = p.calculateInverseKinematics(
            self._robot_id,
            self._ee_index,
            target_pos,
            lowerLimits=self._lower_limits,
            upperLimits=self._upper_limits,
            jointRanges=self._joint_ranges,
            restPoses=rest,
            maxNumIterations=200,
            residualThreshold=1e-4,
            physicsClientId=self._cid,
        )

        q_deg = [math.degrees(joint_positions[ji]) for ji in self._command_joint_indices]

        config = get_ik_config()
        limits = config.joint_limits_deg
        limits_ok = True
        for i, angle in enumerate(q_deg):
            if i < len(limits):
                lo, hi = limits[i]
                if angle < lo - 0.5 or angle > hi + 0.5:
                    self._log.warning(
                        "PyBullet IK joint %d = %.2f deg outside limits [%.1f, %.1f]",
                        i,
                        angle,
                        lo,
                        hi,
                    )
                    limits_ok = False

        for idx, ji in enumerate(self._command_joint_indices):
            if idx < len(q_deg):
                p.resetJointState(
                    self._robot_id,
                    ji,
                    math.radians(q_deg[idx]),
                    physicsClientId=self._cid,
                )
        link_state = p.getLinkState(
            self._robot_id,
            self._ee_index,
            computeForwardKinematics=True,
            physicsClientId=self._cid,
        )
        actual_pos = link_state[4]
        pos_err_mm = math.sqrt(sum((actual_pos[j] - target_pos[j]) ** 2 for j in range(3))) * M_TO_MM

        ok = limits_ok and pos_err_mm < 5.0

        self._log.info(
            "PyBullet IK result: ok=%s q=%s pos_err=%.3f mm limits_ok=%s",
            ok,
            [f"{v:.2f}" for v in q_deg],
            pos_err_mm,
            limits_ok,
        )

        return IKSolution(
            ok=ok,
            q_deg=q_deg,
            iters=200,
            pos_err_mm=pos_err_mm,
            ori_err_rad=0.0,
            seed_used=list(q0_deg[:6]) if q0_deg else [0.0] * 6,
            attempts=1,
        )

    def clamp_command_angles(self, q_deg):
        clamped = []
        for idx, ji in enumerate(self._command_joint_indices):
            if idx >= len(q_deg):
                break
            target_rad = math.radians(float(q_deg[idx]))
            lower = self._lower_limits[ji] if ji < len(self._lower_limits) else -math.pi
            upper = self._upper_limits[ji] if ji < len(self._upper_limits) else math.pi
            if lower < upper:
                target_rad = max(min(target_rad, upper), lower)
            clamped.append(math.degrees(target_rad))
        return clamped

    def disconnect(self):
        """Disconnect the DIRECT physics client."""
        try:
            p.disconnect(self._cid)
            self._log.info("PyBulletDirectIK disconnected")
        except Exception:
            pass


def find_tcp_link_index(robot_id, tcp_link_name="tcp"):
    num_joints = p.getNumJoints(robot_id)
    for i in range(num_joints):
        link_name = p.getJointInfo(robot_id, i)[12].decode("utf-8")
        if link_name == tcp_link_name:
            return i

    type_names = build_joint_type_names()
    _MODULE_LOG.error("TCP link not found. Available joints/links:")
    _MODULE_LOG.error("jointIndex | jointName | jointType | linkName | parentLinkName")
    for i in range(num_joints):
        info = p.getJointInfo(robot_id, i)
        joint_name = info[1].decode("utf-8")
        joint_type = type_names.get(info[2], str(info[2]))
        link_name = info[12].decode("utf-8")
        parent_index = info[16]
        if parent_index == -1:
            parent_link_name = "base"
        else:
            parent_link_name = p.getJointInfo(robot_id, parent_index)[12].decode("utf-8")
        _MODULE_LOG.error(
            "%9d | %s | %s | %s | %s",
            i,
            joint_name,
            joint_type,
            link_name,
            parent_link_name,
        )

    raise RuntimeError(f"TCP link '{tcp_link_name}' not found.")


def update_link_crosshair(robot_id, link_index, marker_item_ids, color):
    link_state = p.getLinkState(robot_id, link_index, computeForwardKinematics=True)
    link_pos = link_state[4]
    update_link_crosshair_from_pos(link_pos, marker_item_ids, color)


def update_link_crosshair_from_pos(link_pos, marker_item_ids, color):
    s = 0.01
    start_end = [
        ([link_pos[0] - s, link_pos[1], link_pos[2]], [link_pos[0] + s, link_pos[1], link_pos[2]]),
        ([link_pos[0], link_pos[1] - s, link_pos[2]], [link_pos[0], link_pos[1] + s, link_pos[2]]),
        ([link_pos[0], link_pos[1], link_pos[2] - s], [link_pos[0], link_pos[1], link_pos[2] + s]),
    ]

    if len(marker_item_ids) != 3:
        marker_item_ids.clear()
        for start, end in start_end:
            marker_item_ids.append(p.addUserDebugLine(start, end, color, lineWidth=2))
        return

    for idx, (start, end) in enumerate(start_end):
        marker_item_ids[idx] = p.addUserDebugLine(
            start,
            end,
            color,
            lineWidth=2,
            replaceItemUniqueId=marker_item_ids[idx],
        )


def clear_debug_items(item_ids):
    if not item_ids:
        return
    for item_id in list(item_ids):
        try:
            p.removeUserDebugItem(item_id)
        except Exception:
            pass
    item_ids.clear()


def update_text_markers(marker_positions, marker_item_ids, text="x", color=(1.0, 0.75, 0.2), text_size=1.4):
    if len(marker_item_ids) > len(marker_positions):
        for item_id in marker_item_ids[len(marker_positions) :]:
            try:
                p.removeUserDebugItem(item_id)
            except Exception:
                pass
        del marker_item_ids[len(marker_positions) :]

    for idx, marker_pos in enumerate(marker_positions):
        replace_item_id = marker_item_ids[idx] if idx < len(marker_item_ids) else -1
        item_id = p.addUserDebugText(
            text,
            [float(marker_pos[0]), float(marker_pos[1]), float(marker_pos[2])],
            textColorRGB=list(color),
            textSize=text_size,
            replaceItemUniqueId=replace_item_id,
        )
        if idx < len(marker_item_ids):
            marker_item_ids[idx] = item_id
        else:
            marker_item_ids.append(item_id)


def update_crosshair_markers(marker_positions, marker_item_ids, color=(1.0, 0.75, 0.2), size=0.01, line_width=2):
    required_item_count = len(marker_positions) * 3
    if len(marker_item_ids) > required_item_count:
        for item_id in marker_item_ids[required_item_count:]:
            try:
                p.removeUserDebugItem(item_id)
            except Exception:
                pass
        del marker_item_ids[required_item_count:]

    for marker_index, marker_pos in enumerate(marker_positions):
        start_end = [
            ([marker_pos[0] - size, marker_pos[1], marker_pos[2]], [marker_pos[0] + size, marker_pos[1], marker_pos[2]]),
            ([marker_pos[0], marker_pos[1] - size, marker_pos[2]], [marker_pos[0], marker_pos[1] + size, marker_pos[2]]),
            ([marker_pos[0], marker_pos[1], marker_pos[2] - size], [marker_pos[0], marker_pos[1], marker_pos[2] + size]),
        ]
        for axis_index, (start, end) in enumerate(start_end):
            item_index = marker_index * 3 + axis_index
            replace_item_id = marker_item_ids[item_index] if item_index < len(marker_item_ids) else -1
            item_id = p.addUserDebugLine(
                [float(value) for value in start],
                [float(value) for value in end],
                color,
                lineWidth=line_width,
                replaceItemUniqueId=replace_item_id,
            )
            if item_index < len(marker_item_ids):
                marker_item_ids[item_index] = item_id
            else:
                marker_item_ids.append(item_id)


def update_polyline(marker_positions, marker_item_ids, color=(1.0, 0.0, 0.0), line_width=2):
    segment_count = max(0, len(marker_positions) - 1)
    if len(marker_item_ids) > segment_count:
        for item_id in marker_item_ids[segment_count:]:
            try:
                p.removeUserDebugItem(item_id)
            except Exception:
                pass
        del marker_item_ids[segment_count:]

    for idx in range(segment_count):
        start = [float(value) for value in marker_positions[idx]]
        end = [float(value) for value in marker_positions[idx + 1]]
        replace_item_id = marker_item_ids[idx] if idx < len(marker_item_ids) else -1
        item_id = p.addUserDebugLine(
            start,
            end,
            color,
            lineWidth=line_width,
            replaceItemUniqueId=replace_item_id,
        )
        if idx < len(marker_item_ids):
            marker_item_ids[idx] = item_id
        else:
            marker_item_ids.append(item_id)


def compute_tcp_positions_for_keyframes(robot_id, tcp_link_index, command_joint_indices, keyframe_joint_sets_deg):
    if not keyframe_joint_sets_deg:
        return []

    original_joint_states = {}
    for joint_index in command_joint_indices:
        joint_state = p.getJointState(robot_id, joint_index)
        original_joint_states[joint_index] = (joint_state[0], joint_state[1])

    marker_positions = []
    try:
        for joint_set_deg in keyframe_joint_sets_deg:
            for idx, joint_index in enumerate(command_joint_indices):
                if idx >= len(joint_set_deg):
                    break
                p.resetJointState(robot_id, joint_index, math.radians(float(joint_set_deg[idx])))
            link_state = p.getLinkState(robot_id, tcp_link_index, computeForwardKinematics=True)
            marker_positions.append(list(link_state[4]))
    finally:
        for joint_index in command_joint_indices:
            position, velocity = original_joint_states.get(joint_index, (0.0, 0.0))
            p.resetJointState(robot_id, joint_index, position, targetVelocity=velocity)

    return marker_positions


def detect_robot_collision(robot_id):
    return bool(p.getContactPoints(bodyA=robot_id))


def update_target_gizmo(target_pos, target_orn, gizmo_item_ids):
    m = p.getMatrixFromQuaternion(target_orn)
    x_axis = [m[0], m[3], m[6]]
    y_axis = [m[1], m[4], m[7]]
    z_axis = [m[2], m[5], m[8]]

    def axis_end(axis):
        return [
            target_pos[0] + axis[0] * TARGET_AXIS_LENGTH,
            target_pos[1] + axis[1] * TARGET_AXIS_LENGTH,
            target_pos[2] + axis[2] * TARGET_AXIS_LENGTH,
        ]

    axes = [
        (axis_end(x_axis), [1, 0, 0]),
        (axis_end(y_axis), [0, 1, 0]),
        (axis_end(z_axis), [0, 0, 1]),
    ]

    if len(gizmo_item_ids) != 3:
        gizmo_item_ids.clear()
        for end, color in axes:
            gizmo_item_ids.append(p.addUserDebugLine(target_pos, end, color, lineWidth=3))
        return

    for idx, (end, color) in enumerate(axes):
        gizmo_item_ids[idx] = p.addUserDebugLine(
            target_pos,
            end,
            color,
            lineWidth=3,
            replaceItemUniqueId=gizmo_item_ids[idx],
        )


def fit_robot_in_view(robot_id, distance_scale=2.5, min_distance=0.2):
    num_joints = p.getNumJoints(robot_id)
    link_indices = [-1] + list(range(num_joints))
    aabb_min = [float("inf"), float("inf"), float("inf")]
    aabb_max = [float("-inf"), float("-inf"), float("-inf")]

    for link_index in link_indices:
        try:
            aabb = p.getAABB(robot_id, link_index)
        except Exception:
            continue
        if not aabb:
            continue
        mn, mx = aabb
        for i in range(3):
            aabb_min[i] = min(aabb_min[i], mn[i])
            aabb_max[i] = max(aabb_max[i], mx[i])

    if any(val == float("inf") for val in aabb_min):
        return

    center = [(aabb_min[i] + aabb_max[i]) * 0.5 for i in range(3)]
    size = [aabb_max[i] - aabb_min[i] for i in range(3)]
    max_extent = max(size) if size else 0.0
    distance = max(min_distance, max_extent * distance_scale)

    cam = p.getDebugVisualizerCamera()
    yaw = cam[8] if len(cam) > 8 else 50.0
    pitch = cam[9] if len(cam) > 9 else -35.0
    p.resetDebugVisualizerCamera(distance, yaw, pitch, center)


def dump_simulation_settings(robot_id, connection_mode, realtime_enabled=None):
    params = p.getPhysicsEngineParameters()
    _MODULE_LOG.info("=== Simulation Settings ===")
    _MODULE_LOG.info("Connection mode: %s", connection_mode)
    if hasattr(p, "getRealTimeSimulation"):
        rt_value = p.getRealTimeSimulation()
    else:
        rt_value = realtime_enabled if realtime_enabled is not None else "unknown"
    _MODULE_LOG.info("Real-time simulation enabled: %s", rt_value)
    _MODULE_LOG.info("Physics time step: %s", params.get("fixedTimeStep"))
    _MODULE_LOG.info("Solver iterations: %s", params.get("numSolverIterations"))
    _MODULE_LOG.info("ERP: %s", params.get("erp"))
    _MODULE_LOG.info("CFM: %s", params.get("globalCFM"))
    _MODULE_LOG.info("Num substeps: %s", params.get("numSubSteps"))
    _MODULE_LOG.info("Physics engine parameters: %s", params)

    _MODULE_LOG.info("=== Scene Complexity ===")
    num_bodies = p.getNumBodies()
    _MODULE_LOG.info("Num bodies: %d", num_bodies)
    get_body_uid = getattr(p, "getBodyUniqueId", None)
    for i in range(num_bodies):
        body_id = get_body_uid(i) if get_body_uid is not None else i
        base_name, body_name = p.getBodyInfo(body_id)
        body_name_str = body_name.decode("utf-8")
        base_name_str = base_name.decode("utf-8")
        num_joints = p.getNumJoints(body_id)
        num_links = num_joints
        _MODULE_LOG.info(
            "Body %s: name='%s', base='%s', joints=%s, links=%s",
            body_id,
            body_name_str,
            base_name_str,
            num_joints,
            num_links,
        )

    _MODULE_LOG.info("=== Robot Joints ===")
    type_names = build_joint_type_names()
    _MODULE_LOG.info("jointIndex | jointName | jointType | linkName | fixed/revolute")
    for i in range(p.getNumJoints(robot_id)):
        info = p.getJointInfo(robot_id, i)
        joint_name = info[1].decode("utf-8")
        joint_type = type_names.get(info[2], str(info[2]))
        link_name = info[12].decode("utf-8")
        fixed_or_revolute = "fixed" if info[2] == p.JOINT_FIXED else "revolute"
        _MODULE_LOG.info(
            "%9d | %s | %s | %s | %s",
            i,
            joint_name,
            joint_type,
            link_name,
            fixed_or_revolute,
        )


def _get_windows_work_area():
    if os.name != "nt":
        return None
    rect = wintypes.RECT()
    ok = ctypes.windll.user32.SystemParametersInfoW(0x0030, 0, ctypes.byref(rect), 0)
    if not ok:
        return None
    return rect.left, rect.top, rect.right, rect.bottom


def _find_window_by_title_fragment(title_fragments):
    if os.name != "nt":
        return None

    found = []
    user32 = ctypes.windll.user32

    @ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)
    def enum_proc(hwnd, _lparam):
        if not user32.IsWindowVisible(hwnd):
            return True
        length = user32.GetWindowTextLengthW(hwnd)
        if length <= 0:
            return True
        buffer = ctypes.create_unicode_buffer(length + 1)
        user32.GetWindowTextW(hwnd, buffer, length + 1)
        title = buffer.value.strip()
        lowered = title.lower()
        if any(fragment.lower() in lowered for fragment in title_fragments):
            found.append(hwnd)
            return False
        return True

    user32.EnumWindows(enum_proc, 0)
    return found[0] if found else None


def _position_pybullet_window():
    if os.name != "nt":
        return

    work_area = _get_windows_work_area()
    if work_area is None:
        return
    left, top, right, bottom = work_area
    y = top + max(0, ((bottom - top) - PYBULLET_WINDOW_HEIGHT) // 2)

    hwnd = None
    for _ in range(40):
        hwnd = _find_window_by_title_fragment(("physics server", "pybullet"))
        if hwnd:
            break
        time.sleep(0.1)

    if hwnd:
        ctypes.windll.user32.MoveWindow(
            hwnd,
            int(left),
            int(y),
            int(PYBULLET_WINDOW_WIDTH),
            int(PYBULLET_WINDOW_HEIGHT),
            True,
        )


def run_simulation(shared_state, stop_event):
    p.connect(p.GUI, options=f"--width={PYBULLET_WINDOW_WIDTH} --height={PYBULLET_WINDOW_HEIGHT}")
    connection_mode = "GUI"
    root = _project_root()
    tuning = load_simulation_tuning()
    motor_tuning = tuning.get("joint_motor_control", {})
    ik_tuning = tuning.get("ik_pybullet", {})
    physics_tuning = tuning.get("physics_engine", {})
    runtime_tuning = tuning.get("runtime_loop", {})

    tuning_path = get_settings_path("simulation_tuning.json")
    tuning_last_mtime = tuning_path.stat().st_mtime if tuning_path.exists() else None
    tuning_check_period = 1.0
    tuning_last_check_time = 0.0

    p.configureDebugVisualizer(p.COV_ENABLE_GUI, 0)
    p.configureDebugVisualizer(p.COV_ENABLE_RGB_BUFFER_PREVIEW, 0)
    p.configureDebugVisualizer(p.COV_ENABLE_DEPTH_BUFFER_PREVIEW, 0)
    p.configureDebugVisualizer(p.COV_ENABLE_SEGMENTATION_MARK_PREVIEW, 0)
    _position_pybullet_window()

    gravity = tuning.get("gravity_m_s2", [0.0, 0.0, -9.81])
    p.setGravity(gravity[0], gravity[1], gravity[2])
    if physics_tuning:
        physics_kwargs = {}
        fixed_time_step = physics_tuning.get("fixedTimeStep")
        num_solver_iterations = physics_tuning.get("numSolverIterations")
        num_substeps = physics_tuning.get("numSubSteps")
        erp = physics_tuning.get("erp")
        cfm = physics_tuning.get("cfm")
        if fixed_time_step is not None:
            physics_kwargs["fixedTimeStep"] = fixed_time_step
        if num_solver_iterations is not None:
            physics_kwargs["numSolverIterations"] = num_solver_iterations
        if num_substeps is not None:
            physics_kwargs["numSubSteps"] = num_substeps
        if erp is not None:
            physics_kwargs["erp"] = erp
        if cfm is not None:
            physics_kwargs["globalCFM"] = cfm
        if physics_kwargs:
            p.setPhysicsEngineParameter(**physics_kwargs)
    plane_path = Path(pybullet_data.getDataPath()) / "plane.urdf"
    p.loadURDF(str(plane_path))

    urdf_path = root / "Assy_6axisArm_v3_export.urdf"
    if not urdf_path.exists():
        raise FileNotFoundError(f"URDF not found: {urdf_path}")

    robot_id = p.loadURDF(
        str(urdf_path),
        [0, 0, 0],
        p.getQuaternionFromEuler([0, 0, 0]),
        useFixedBase=True,
        flags=ROBOT_URDF_FLAGS,
    )
    visualizer_tuning = tuning.get("visualizer", {})
    distance_scale = get_tuning_value(visualizer_tuning, "fit_distance_scale", 2.5)
    min_distance = get_tuning_value(visualizer_tuning, "min_distance", 0.2)
    fit_robot_in_view(robot_id, distance_scale=distance_scale, min_distance=min_distance)

    joint_continuous = getattr(p, "JOINT_CONTINUOUS", None)
    type_names = {k: v.lower() for k, v in build_joint_type_names().items()}

    num_joints = p.getNumJoints(robot_id)
    _MODULE_LOG.info("Joints:")
    sliders = {}
    command_joint_indices = []
    lower_limits = []
    upper_limits = []
    joint_ranges = []
    rest_poses = []
    motor_force = get_tuning_value(motor_tuning, "force", 500)
    motor_max_velocity = motor_tuning.get("maxVelocity_rad_s", MAX_JOINT_VELOCITY)
    motor_position_gain = motor_tuning.get("positionGain")
    motor_velocity_gain = motor_tuning.get("velocityGain")
    motor_kwargs = {"force": motor_force}
    if motor_max_velocity is not None:
        motor_kwargs["maxVelocity"] = motor_max_velocity
    if motor_position_gain is not None:
        motor_kwargs["positionGain"] = motor_position_gain
    if motor_velocity_gain is not None:
        motor_kwargs["velocityGain"] = motor_velocity_gain
    motor_kwargs_unclamped = dict(motor_kwargs)
    motor_kwargs_unclamped.pop("maxVelocity", None)
    joint_damping = motor_tuning.get("jointDamping", 0.0)

    for i in range(num_joints):
        info = p.getJointInfo(robot_id, i)
        name = info[1].decode("utf-8")
        joint_type = info[2]
        lower = info[8]
        upper = info[9]

        _MODULE_LOG.info("%d: %s (%s)", i, name, type_names.get(joint_type, joint_type))

        current = p.getJointState(robot_id, i)[0]
        rest_poses.append(current)

        if joint_type == p.JOINT_FIXED:
            lower_limits.append(0.0)
            upper_limits.append(0.0)
            joint_ranges.append(0.0)
            continue

        if lower >= upper:
            revolute_like = {p.JOINT_REVOLUTE}
            if joint_continuous is not None:
                revolute_like.add(joint_continuous)
            if joint_type in revolute_like:
                lower, upper = -math.pi, math.pi
            elif joint_type == p.JOINT_PRISMATIC:
                lower, upper = -0.1, 0.1
            else:
                lower, upper = -math.pi, math.pi

        lower_limits.append(lower)
        upper_limits.append(upper)
        joint_ranges.append(upper - lower)
        if joint_type == p.JOINT_REVOLUTE and i <= 5:
            command_joint_indices.append(i)

        param_id = p.addUserDebugParameter(f"{i}:{name}", lower, upper, current)
        sliders[i] = param_id

        if i in command_joint_indices:
            p.setJointMotorControl2(
                robot_id,
                i,
                p.POSITION_CONTROL,
                targetPosition=current,
                **motor_kwargs,
            )

    if not command_joint_indices:
        raise RuntimeError("No revolute joints found in indices 0..5; cannot command IK.")

    if joint_damping > 0.0:
        for joint_index in command_joint_indices:
            p.changeDynamics(robot_id, joint_index, jointDamping=joint_damping)
        _MODULE_LOG.info("Applied jointDamping=%.3f to %d joints", joint_damping, len(command_joint_indices))

    tcp_link_index = find_tcp_link_index(robot_id, tcp_link_name="tcp")
    end_effector_index = tcp_link_index
    if p.getNumJoints(robot_id) <= 5:
        raise RuntimeError(
            "Expected joint index 5 for J6 link pose, but robot has fewer than 6 joints."
        )
    j6_link_index = 5
    ee_state = p.getLinkState(robot_id, end_effector_index, computeForwardKinematics=True)
    ee_pos = ee_state[4]
    ee_orn = ee_state[5]
    ee_rpy = p.getEulerFromQuaternion(ee_orn)

    tcp_marker_item_ids = []
    j6_marker_item_ids = []
    target_gizmo_item_ids = []
    point_a_marker_item_ids = []
    point_b_marker_item_ids = []
    keyframe_marker_item_ids = []
    keyframe_trace_item_ids = []
    keyframe_marker_positions = []
    keyframe_marker_visibility = False
    keyframe_trace_visibility = False

    p.setRealTimeSimulation(0)
    realtime_enabled = False
    dump_simulation_settings(robot_id, connection_mode, realtime_enabled=realtime_enabled)

    with shared_state.lock:
        shared_state.target_pos_m = list(ee_pos)
        shared_state.target_orn = ee_orn
        shared_state.target_rpy_deg = [
            math.degrees(ee_rpy[0]),
            math.degrees(ee_rpy[1]),
            math.degrees(ee_rpy[2]),
        ]
        shared_state.ik_enabled = False
        shared_state.only_position = False

    try:
        debug_update_hz = get_tuning_value(runtime_tuning, "debug_update_hz", 20)
        state_update_hz = get_tuning_value(runtime_tuning, "state_update_hz", 15)
        ik_update_hz = get_tuning_value(runtime_tuning, "ik_update_hz", 30)
        debug_update_period = 1.0 / max(debug_update_hz, 1e-6)
        state_update_period = 1.0 / max(state_update_hz, 1e-6)
        ik_update_period = 1.0 / max(ik_update_hz, 1e-6)
        sim_sleep_s = get_tuning_value(runtime_tuning, "sleep_seconds", 1.0 / 240.0)
        ik_max_iters = int(get_tuning_value(ik_tuning, "maxNumIterations", 200))
        ik_residual = float(get_tuning_value(ik_tuning, "residualThreshold", 1e-4))
        last_debug_update_time = 0.0
        last_state_update_time = 0.0
        last_ik_update_time = 0.0
        last_target_pos = list(ee_pos)
        last_target_orn = ee_orn
        cached_ik_angles = list(rest_poses)
        kinematic_enabled = bool(tuning.get("kinematic_mode", {}).get("enabled", False))
        kinematic_alpha = parse_kinematic_alpha(
            tuning.get("kinematic_mode", {}).get("interpolation_alpha", None)
        )
        kinematic_angles = list(rest_poses)
        last_ik_enabled = False
        last_hardware_streaming_active = False

        def resync_kinematic_angles_from_robot():
            if not kinematic_enabled:
                return
            for joint_index in command_joint_indices:
                kinematic_angles[joint_index] = p.getJointState(robot_id, joint_index)[0]

        def update_kinematic_targets(target_angles, use_interpolation):
            if not kinematic_enabled:
                return
            if not use_interpolation or kinematic_alpha is None:
                for joint_index in command_joint_indices:
                    kinematic_angles[joint_index] = target_angles[joint_index]
                return
            for joint_index in command_joint_indices:
                current = kinematic_angles[joint_index]
                target = target_angles[joint_index]
                kinematic_angles[joint_index] = current + kinematic_alpha * (target - current)

        def apply_joint_targets(target_angles, active_motor_kwargs, use_kinematic_interpolation=True):
            if kinematic_enabled:
                update_kinematic_targets(target_angles, use_kinematic_interpolation)
            for joint_index in command_joint_indices:
                if kinematic_enabled:
                    p.resetJointState(robot_id, joint_index, kinematic_angles[joint_index])
                else:
                    p.setJointMotorControl2(
                        robot_id,
                        joint_index,
                        p.POSITION_CONTROL,
                        targetPosition=target_angles[joint_index],
                        **active_motor_kwargs,
                    )

        while True:
            if stop_event.is_set():
                break
            now = time.monotonic()

            if (now - tuning_last_check_time) >= tuning_check_period:
                tuning_last_check_time = now
                if tuning_path.exists():
                    try:
                        current_mtime = tuning_path.stat().st_mtime
                        if tuning_last_mtime is None or current_mtime != tuning_last_mtime:
                            tuning_last_mtime = current_mtime
                            tuning = load_simulation_tuning()
                            motor_tuning = tuning.get("joint_motor_control", {})
                            ik_tuning = tuning.get("ik_pybullet", {})
                            runtime_tuning = tuning.get("runtime_loop", {})

                            motor_force = get_tuning_value(motor_tuning, "force", 500)
                            motor_max_velocity = motor_tuning.get("maxVelocity_rad_s", MAX_JOINT_VELOCITY)
                            motor_position_gain = motor_tuning.get("positionGain")
                            motor_velocity_gain = motor_tuning.get("velocityGain")
                            motor_kwargs = {"force": motor_force}
                            if motor_max_velocity is not None:
                                motor_kwargs["maxVelocity"] = motor_max_velocity
                            if motor_position_gain is not None:
                                motor_kwargs["positionGain"] = motor_position_gain
                            if motor_velocity_gain is not None:
                                motor_kwargs["velocityGain"] = motor_velocity_gain
                            motor_kwargs_unclamped = dict(motor_kwargs)
                            motor_kwargs_unclamped.pop("maxVelocity", None)
                            was_kinematic_enabled = kinematic_enabled
                            kinematic_tuning = tuning.get("kinematic_mode", {})
                            kinematic_enabled = bool(kinematic_tuning.get("enabled", False))
                            kinematic_alpha = parse_kinematic_alpha(
                                kinematic_tuning.get("interpolation_alpha", None)
                            )
                            if kinematic_enabled and not was_kinematic_enabled:
                                resync_kinematic_angles_from_robot()

                            new_damping = motor_tuning.get("jointDamping", 0.0)
                            if new_damping != joint_damping:
                                joint_damping = new_damping
                                for joint_index in command_joint_indices:
                                    p.changeDynamics(robot_id, joint_index, jointDamping=joint_damping)
                                _MODULE_LOG.info("Updated jointDamping=%.3f", joint_damping)

                            debug_update_hz = get_tuning_value(runtime_tuning, "debug_update_hz", 20)
                            state_update_hz = get_tuning_value(runtime_tuning, "state_update_hz", 15)
                            ik_update_hz = get_tuning_value(runtime_tuning, "ik_update_hz", 30)
                            debug_update_period = 1.0 / max(debug_update_hz, 1e-6)
                            state_update_period = 1.0 / max(state_update_hz, 1e-6)
                            ik_update_period = 1.0 / max(ik_update_hz, 1e-6)
                            sim_sleep_s = get_tuning_value(runtime_tuning, "sleep_seconds", 1.0 / 240.0)

                            ik_max_iters = int(get_tuning_value(ik_tuning, "maxNumIterations", 200))
                            ik_residual = float(get_tuning_value(ik_tuning, "residualThreshold", 1e-4))

                            _MODULE_LOG.info("Hot-reloaded simulation_tuning.json")
                    except Exception as exc:
                        _MODULE_LOG.warning("Failed to hot-reload simulation_tuning.json: %s", exc)

            with shared_state.lock:
                target_pos = list(shared_state.target_pos_m)
                target_orn = shared_state.target_orn
                ik_enabled = shared_state.ik_enabled
                only_position = shared_state.only_position
                manual_pending = shared_state.manual_joint_pending
                manual_joint_deg = list(shared_state.manual_joint_target_deg)
                manual_joint_motion_overrides = dict(getattr(shared_state, "manual_joint_motion_overrides", {}) or {})
                streaming_active = shared_state.streaming_active
                hardware_streaming_active = bool(getattr(shared_state, "hardware_streaming_active", False))
                point_a_valid = shared_state.point_a_valid
                point_b_valid = shared_state.point_b_valid
                point_a_m = list(shared_state.point_a_m) if point_a_valid else None
                point_b_m = list(shared_state.point_b_m) if point_b_valid else None
                keyframe_markers_visible = bool(getattr(shared_state, "keyframe_markers_visible", False))
                keyframe_trace_visible = bool(getattr(shared_state, "keyframe_trace_visible", False))
                keyframe_marker_joint_sets_deg = list(getattr(shared_state, "keyframe_marker_joint_sets_deg", []) or [])
                keyframe_markers_dirty = bool(getattr(shared_state, "keyframe_markers_dirty", False))

            if hardware_streaming_active:
                if not last_hardware_streaming_active:
                    _MODULE_LOG.info("Pausing simulation updates during hardware streaming")
                    last_hardware_streaming_active = True
                time.sleep(max(sim_sleep_s, 0.02))
                continue

            if last_hardware_streaming_active:
                _MODULE_LOG.info("Resuming simulation updates after hardware streaming")
                resync_kinematic_angles_from_robot()
                last_debug_update_time = now
                last_state_update_time = now
                last_ik_update_time = now
                last_hardware_streaming_active = False

            if keyframe_markers_dirty:
                keyframe_marker_positions = compute_tcp_positions_for_keyframes(
                    robot_id,
                    tcp_link_index,
                    command_joint_indices,
                    keyframe_marker_joint_sets_deg,
                )
                with shared_state.lock:
                    shared_state.keyframe_markers_dirty = False

            if keyframe_markers_visible != keyframe_marker_visibility:
                keyframe_marker_visibility = keyframe_markers_visible
            if keyframe_trace_visible != keyframe_trace_visibility:
                keyframe_trace_visibility = keyframe_trace_visible

            if manual_pending:
                cached_ik_angles = list(rest_poses)
                resync_kinematic_angles_from_robot()
                active_motor_kwargs = motor_kwargs_unclamped if streaming_active else dict(motor_kwargs)
                if manual_joint_motion_overrides and not streaming_active:
                    for key in ("force", "maxVelocity", "positionGain", "velocityGain"):
                        value = manual_joint_motion_overrides.get(key)
                        if value is not None:
                            active_motor_kwargs[key] = value
                for idx, joint_index in enumerate(command_joint_indices):
                    if idx >= len(manual_joint_deg):
                        break
                    target_rad = math.radians(manual_joint_deg[idx])
                    lower = lower_limits[joint_index]
                    upper = upper_limits[joint_index]
                    if lower < upper and not streaming_active:
                        target_rad = max(min(target_rad, upper), lower)
                    cached_ik_angles[joint_index] = target_rad
                apply_joint_targets(
                    cached_ik_angles,
                    active_motor_kwargs,
                    use_kinematic_interpolation=not streaming_active,
                )
                with shared_state.lock:
                    shared_state.manual_joint_pending = False
                    shared_state.manual_joint_motion_overrides = {}
                    shared_state.ik_enabled = False
                ik_enabled = False
                last_ik_enabled = False

            debug_update_due = (now - last_debug_update_time) >= debug_update_period
            state_update_due = (now - last_state_update_time) >= state_update_period
            tcp_state = None

            if ik_enabled:
                dx = target_pos[0] - last_target_pos[0]
                dy = target_pos[1] - last_target_pos[1]
                dz = target_pos[2] - last_target_pos[2]
                pos_delta = math.sqrt(dx * dx + dy * dy + dz * dz)
                q_delta = quat_multiply(target_orn, quat_inverse(last_target_orn))
                w = max(-1.0, min(1.0, abs(q_delta[3])))
                ori_delta = 2.0 * math.acos(w)

                ik_due = (
                    not last_ik_enabled
                    or (
                        (pos_delta > 0.0005 or ori_delta > math.radians(0.5))
                        and (now - last_ik_update_time) >= ik_update_period
                    )
                )
                if ik_due:
                    if only_position:
                        cached_ik_angles = p.calculateInverseKinematics(
                            robot_id,
                            end_effector_index,
                            target_pos,
                            lowerLimits=lower_limits,
                            upperLimits=upper_limits,
                            jointRanges=joint_ranges,
                            restPoses=rest_poses,
                            maxNumIterations=ik_max_iters,
                            residualThreshold=ik_residual,
                        )
                    else:
                        cached_ik_angles = p.calculateInverseKinematics(
                            robot_id,
                            end_effector_index,
                            target_pos,
                            targetOrientation=target_orn,
                            lowerLimits=lower_limits,
                            upperLimits=upper_limits,
                            jointRanges=joint_ranges,
                            restPoses=rest_poses,
                            maxNumIterations=ik_max_iters,
                            residualThreshold=ik_residual,
                        )
                    last_target_pos = list(target_pos)
                    last_target_orn = target_orn
                    last_ik_update_time = now

                apply_joint_targets(
                    cached_ik_angles,
                    motor_kwargs,
                    use_kinematic_interpolation=not streaming_active,
                )
            else:
                apply_joint_targets(
                    cached_ik_angles,
                    motor_kwargs,
                    use_kinematic_interpolation=not streaming_active,
                )
            last_ik_enabled = ik_enabled

            if debug_update_due or state_update_due:
                tcp_state = p.getLinkState(robot_id, tcp_link_index, computeForwardKinematics=True)

            if debug_update_due:
                j6_state = p.getLinkState(robot_id, j6_link_index, computeForwardKinematics=True)
                update_link_crosshair_from_pos(tcp_state[4], tcp_marker_item_ids, [0, 1, 0])
                update_link_crosshair_from_pos(j6_state[4], j6_marker_item_ids, [0.2, 0.4, 1.0])
                update_target_gizmo(target_pos, target_orn, target_gizmo_item_ids)
                if point_a_m is not None:
                    update_link_crosshair_from_pos(point_a_m, point_a_marker_item_ids, [1.0, 0.2, 0.2])
                else:
                    clear_debug_items(point_a_marker_item_ids)
                if point_b_m is not None:
                    update_link_crosshair_from_pos(point_b_m, point_b_marker_item_ids, [0.2, 0.8, 1.0])
                else:
                    clear_debug_items(point_b_marker_item_ids)
                if keyframe_marker_visibility and keyframe_marker_positions:
                    update_crosshair_markers(keyframe_marker_positions, keyframe_marker_item_ids)
                else:
                    clear_debug_items(keyframe_marker_item_ids)
                if keyframe_trace_visibility and len(keyframe_marker_positions) >= 2:
                    update_polyline(keyframe_marker_positions, keyframe_trace_item_ids, color=(1.0, 0.0, 0.0), line_width=2)
                else:
                    clear_debug_items(keyframe_trace_item_ids)

                ee_pos = tcp_state[4]
                ee_orn = tcp_state[5]
                dx = target_pos[0] - ee_pos[0]
                dy = target_pos[1] - ee_pos[1]
                dz = target_pos[2] - ee_pos[2]
                pos_err = math.sqrt(dx * dx + dy * dy + dz * dz)

                q_err = quat_multiply(target_orn, quat_inverse(ee_orn))
                w = max(-1.0, min(1.0, abs(q_err[3])))
                ori_err = 2.0 * math.acos(w)

                with shared_state.lock:
                    shared_state.pos_err_m = pos_err
                last_debug_update_time = now

            if state_update_due:
                if tcp_state is None:
                    tcp_state = p.getLinkState(robot_id, tcp_link_index, computeForwardKinematics=True)
                joint_deg = []
                for joint_index in command_joint_indices:
                    joint_deg.append(math.degrees(p.getJointState(robot_id, joint_index)[0]))
                tcp_pos = tcp_state[4]
                tcp_rpy = p.getEulerFromQuaternion(tcp_state[5])
                tcp_rpy_deg = [
                    math.degrees(tcp_rpy[0]),
                    math.degrees(tcp_rpy[1]),
                    math.degrees(tcp_rpy[2]),
                ]
                collision_detected = detect_robot_collision(robot_id)
                cam = p.getDebugVisualizerCamera()
                pybullet_window_size = [0, 0]
                if cam and len(cam) >= 2:
                    try:
                        pybullet_window_size = [int(cam[0]), int(cam[1])]
                    except Exception:
                        pybullet_window_size = [0, 0]
                state_hz = 1.0 / (now - last_state_update_time) if last_state_update_time > 0 else 0.0
                with shared_state.lock:
                    shared_state.joint_deg = joint_deg
                    shared_state.tcp_pos_m = list(tcp_pos)
                    shared_state.tcp_rpy_deg = tcp_rpy_deg
                    shared_state.collision_detected = collision_detected
                    shared_state.pybullet_window_size = pybullet_window_size
                    shared_state.state_hz = state_hz
                last_state_update_time = now
            p.stepSimulation()
            time.sleep(sim_sleep_s)
    except KeyboardInterrupt:
        pass
    finally:
        clear_debug_items(keyframe_marker_item_ids)
        clear_debug_items(keyframe_trace_item_ids)
        p.disconnect()
