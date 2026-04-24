MM_TO_M = 0.001
M_TO_MM = 1000.0


class GuiRefreshController:
    def __init__(self, root, shared_state, stop_event, layout):
        self.root = root
        self.shared_state = shared_state
        self.stop_event = stop_event
        self.layout = layout
        self.initialized = {"target": False, "joints": False}

    @staticmethod
    def _set_if_changed(var, text: str):
        if var.get() != text:
            var.set(text)

    def refresh_ui(self):
        with self.shared_state.lock:
            joint_deg = list(self.shared_state.joint_deg)
            tcp_pos_m = list(self.shared_state.tcp_pos_m)
            tcp_rpy_deg = list(self.shared_state.tcp_rpy_deg)
            target_pos_m = list(self.shared_state.target_pos_m)
            target_rpy_deg = list(self.shared_state.target_rpy_deg)
            only_position = bool(self.shared_state.only_position)
            pos_err_m = float(self.shared_state.pos_err_m)
            collision_detected = bool(getattr(self.shared_state, "collision_detected", False))
            ik_enabled = bool(self.shared_state.ik_enabled)

        pose_panel = self.layout.pose_panel
        manual_jog_panel = self.layout.manual_jog_panel
        state_panel = self.layout.robot_state_panel
        status_bar = self.layout.status_bar

        if not self.initialized["target"]:
            has_initial_target = any(abs(value) > 1e-9 for value in target_pos_m + target_rpy_deg)
            has_live_tcp = any(abs(value) > 1e-9 for value in tcp_pos_m + tcp_rpy_deg)
            if has_live_tcp or has_initial_target:
                init_pos_m = tcp_pos_m if has_live_tcp else target_pos_m
                init_rpy_deg = tcp_rpy_deg if has_live_tcp else target_rpy_deg
                self._set_if_changed(pose_panel.x_var, f"{init_pos_m[0] * M_TO_MM:.2f}")
                self._set_if_changed(pose_panel.y_var, f"{init_pos_m[1] * M_TO_MM:.2f}")
                self._set_if_changed(pose_panel.z_var, f"{init_pos_m[2] * M_TO_MM:.2f}")
                self._set_if_changed(pose_panel.rx_var, f"{init_rpy_deg[0]:.2f}")
                self._set_if_changed(pose_panel.ry_var, f"{init_rpy_deg[1]:.2f}")
                self._set_if_changed(pose_panel.rz_var, f"{init_rpy_deg[2]:.2f}")
                self.initialized["target"] = True

        if not self.initialized["joints"] and len(joint_deg) >= 6:
            for i, var in enumerate(pose_panel.joint_entry_vars):
                self._set_if_changed(var, f"{joint_deg[i]:.2f}")
            self.initialized["joints"] = True

        if bool(pose_panel.only_pos_var.get()) != only_position:
            pose_panel.only_pos_var.set(only_position)

        for i, var in enumerate(state_panel.sim_joint_vars):
            self._set_if_changed(var, f"{joint_deg[i]:+.2f}" if i < len(joint_deg) else "--")

        for i, value in enumerate([tcp_pos_m[0] * M_TO_MM, tcp_pos_m[1] * M_TO_MM, tcp_pos_m[2] * M_TO_MM]):
            self._set_if_changed(state_panel.sim_tcp_pos_vars[i], f"{value:+.2f}")

        for i, value in enumerate(tcp_rpy_deg[:3]):
            self._set_if_changed(state_panel.sim_tcp_rpy_vars[i], f"{value:+.2f}")

        for var in state_panel.encoder_tcp_pos_vars + state_panel.encoder_tcp_rpy_vars:
            if not var.get():
                var.set("--")

        if manual_jog_panel.target_var.get() == 0:
            joint_sources = [f"{value:+7.1f} deg" for value in joint_deg[:6]]
            cart_sources = [f"{value:+8.2f} mm" for value in [tcp_pos_m[0] * M_TO_MM, tcp_pos_m[1] * M_TO_MM, tcp_pos_m[2] * M_TO_MM]]
        else:
            joint_sources = [var.get() or "--" for var in state_panel.encoder_joint_vars]
            cart_sources = [var.get() or "--" for var in state_panel.encoder_tcp_pos_vars]
            if all(value == "--" for value in cart_sources):
                cart_sources = [
                    f"{value:+8.2f} mm"
                    for value in [tcp_pos_m[0] * M_TO_MM, tcp_pos_m[1] * M_TO_MM, tcp_pos_m[2] * M_TO_MM]
                ]

        for i, var in enumerate(manual_jog_panel.joint_current_vars):
            self._set_if_changed(var, joint_sources[i] if i < len(joint_sources) else "--")

        for i, var in enumerate(manual_jog_panel.cart_current_vars):
            self._set_if_changed(var, cart_sources[i] if i < len(cart_sources) else "--")

        err_text = f"{pos_err_m * M_TO_MM:.2f} mm"
        self._set_if_changed(pose_panel.pos_err_var, err_text)
        self._set_if_changed(state_panel.pos_err_var, err_text)
        collision_text = "Yes" if collision_detected else "No"
        collision_bg = "#f4c7c3" if collision_detected else "#dcedd7"
        self._set_if_changed(state_panel.collision_var, f"Collision Detected: {collision_text}")
        state_panel.collision_frame.configure(bg=collision_bg)
        state_panel.collision_value_label.configure(bg=collision_bg, fg="#1a1a1a")

        if ik_enabled:
            mode_text = "position-only" if only_position else "full pose"
            self._set_if_changed(status_bar.ik_var, f"IK: active ({mode_text})")
        else:
            self._set_if_changed(status_bar.ik_var, "IK: idle")

        if not self.stop_event.is_set():
            self.root.after(100, self.refresh_ui)
