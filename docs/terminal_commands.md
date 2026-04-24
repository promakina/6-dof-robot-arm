# Terminal Commands

This file documents the terminal commands available in the Robot Arm Control GUI.

## Available Commands

### `robot_window_size`

Returns the current size of the Robot Arm Control Tk window.

Example output:

```text
Robot Arm Control window size: 1150 x 720
```

### `pybullet_window_size`

Returns the current size reported by the active PyBullet visualizer window.

Example output:

```text
PyBullet window size: 1280 x 720
```

## Notes

- Commands are case-insensitive.
- Spaces and hyphens are normalized, so `robot window size` and `robot-window-size` resolve to `robot_window_size`.
- Unknown commands should be treated as unsupported and the terminal should point users back to this file.
