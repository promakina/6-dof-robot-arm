# Getting Started

This guide is for first-time setup of the Robot Arm PyBullet app.

## 1. Prerequisites

- Python 3 installed
- Tkinter available in your Python installation
- Windows recommended for the current CAN and GUI workflow
- Optional: CAN adapter and local drivers if you want to control hardware

## 2. Install Python dependencies

From this folder, run:

```powershell
python -m pip install -r requirements.txt
```

This installs:

- `pybullet` for simulation
- `numpy` for IK and interpolation
- `python-can` for CAN communication features

## 3. Verify the simulator

Run the smoke test:

```powershell
python hello_bullet.py
```

Expected result:

- A PyBullet window opens
- The robot model loads
- No missing mesh or URDF path errors appear

## 4. Launch the full application

Run:

```powershell
python src/main_robot.py
```

Expected result:

- The Tk control panel opens
- The PyBullet simulation window opens
- The robot appears in the scene

## 5. Keep these files together

Do not separate these items:

- `src/`
- `meshes/`
- `Assy_6axisArm_v3_export.urdf`
- `docs/terminal_commands.md`

The application depends on their current relative paths.

## 6. Optional: Use CAN hardware control

If you want to use robot communication features:

1. Connect your CAN adapter.
2. Make sure the adapter drivers are installed.
3. Check `src/settings/robot_comm_settings.json`.
4. Confirm the configured interface, port, and bitrate match your hardware.

If `python-can` is installed but the adapter configuration is wrong, the GUI can still open while CAN connection attempts fail.

## 7. Runtime-generated folders

The app creates or uses these as needed:

- `src/logs/` for log files
- `keyframes/` for exported keyframe JSON files

You do not need to pre-populate them.

## 8. Common problems

### Robot does not load

Check that:

- `meshes/` exists at the project root
- `Assy_6axisArm_v3_export.urdf` is at the project root
- You launched the app from this folder

### PyBullet opens but the robot is missing or broken

Check for:

- missing STL files in `meshes/`
- renamed or moved URDF or mesh paths

### GUI does not open

Possible causes:

- Tkinter is not available in your Python installation
- Python dependencies were not installed

### CAN does not connect

Check:

- CAN adapter drivers
- port and interface settings in `src/settings/robot_comm_settings.json`
- bitrate configuration
- physical connection to the adapter

## 9. Next steps

- Use `docs/terminal_commands.md` for terminal command help inside the GUI
- Use `hello_bullet.py` when you want a quick environment smoke test
- Use `src/main_robot.py` for the full simulator and control workflow
