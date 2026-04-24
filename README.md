# Robot Arm PyBullet Share Bundle

This folder contains the files needed to run and share the Robot Arm PyBullet simulator.

## Included

- `src/` application code and JSON settings
- `meshes/` STL meshes used by the URDF
- `Assy_6axisArm_v3_export.urdf` robot model loaded by the simulator
- `hello_bullet.py` minimal smoke test
- `docs/terminal_commands.md` terminal command reference used by the GUI

## Requirements

- Python 3
- Tkinter available in your Python installation
- CAN features require `python-can` and compatible local CAN adapter drivers

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Run

Smoke test:

```powershell
python hello_bullet.py
```

Main GUI:

```powershell
python src/main_robot.py
```

## Notes

- The application creates runtime files such as logs and exported keyframes as needed.
- Keep the relative layout of `src/`, `meshes/`, and the URDF file unchanged.
