"""Compatibility shim for legacy imports.

Prefer importing ``get_ik_logger`` from ``app.logging_utils``.
"""

from app.logging_utils import get_ik_logger as get_logger

__all__ = ["get_logger"]
