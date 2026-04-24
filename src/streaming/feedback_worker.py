import threading
import time


class MultiJointFeedbackWorker:
    """
    Background thread that polls all joint encoders at fixed rate.
    Provides thread-safe non-blocking access to latest encoder values.
    """

    def __init__(
        self,
        comm_client,
        joint_ids: list,
        poll_rate_hz: float = 25.0,
        timeout_s: float = 0.02,
        joints_per_cycle: int = 1,
        inter_joint_gap_s: float = 0.0,
        slow_cycle_warn_s: float = 0.06,
        cycle_log_interval_s: float = 2.0,
        logger=None,
    ):
        self._comm_client = comm_client
        self._joint_ids = list(joint_ids)
        self._poll_interval_s = 1.0 / max(1.0, float(poll_rate_hz))
        self._timeout_s = max(0.001, float(timeout_s))
        self._joints_per_cycle = max(1, int(joints_per_cycle))
        self._inter_joint_gap_s = max(0.0, float(inter_joint_gap_s))
        self._slow_cycle_warn_s = max(0.001, float(slow_cycle_warn_s))
        self._cycle_log_interval_s = max(0.2, float(cycle_log_interval_s))
        self._log = logger

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = None

        self._latest_angles = {}
        self._latest_timestamp = {}
        self._total_samples = {}
        self._total_misses = {}
        self._consecutive_misses = {}

        self._round_robin_index = 0
        self._total_cycles = 0
        self._slow_cycles = 0
        self._last_cycle_elapsed_s = 0.0
        self._last_cycle_timestamp = None
        self._last_cycle_joint_ids = []
        self._last_cycle_valid_count = 0
        self._last_cycle_target_reads = 0
        self._last_cycle_sleep_s = 0.0
        self._last_diag_log_mono = None

    def seed(self, angles_deg):
        """Pre-populate feedback cache with known-good angles."""
        now = time.monotonic()
        with self._lock:
            for joint_id, angle in angles_deg.items():
                self._latest_angles[joint_id] = float(angle)
                self._latest_timestamp[joint_id] = now
                self._total_samples[joint_id] = self._total_samples.get(joint_id, 0) + 1
        if self._log is not None:
            self._log.info(
                "FeedbackWorker seeded with %d joints: %s",
                len(angles_deg),
                {k: f"{v:.2f}" for k, v in angles_deg.items()},
            )

    def start(self):
        """Start background polling thread."""
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        if self._log is not None:
            self._log.info(
                "FeedbackWorker started: poll_rate=%.1f Hz timeout=%.3f s joints=%s "
                "reads_per_cycle=%d inter_joint_gap=%.1f ms slow_warn=%.1f ms",
                1.0 / self._poll_interval_s,
                self._timeout_s,
                self._joint_ids,
                self._joints_per_cycle,
                self._inter_joint_gap_s * 1000.0,
                self._slow_cycle_warn_s * 1000.0,
            )

    def stop(self):
        """Stop background thread gracefully."""
        if self._log is not None:
            self._log.info("FeedbackWorker stopping...")
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                if self._log is not None:
                    self._log.warning("FeedbackWorker thread did not stop within 1s")
            else:
                if self._log is not None:
                    self._log.info("FeedbackWorker stopped cleanly")
        self._thread = None

    def _next_cycle_joint_ids(self):
        with self._lock:
            if not self._joint_ids:
                return []
            read_count = min(self._joints_per_cycle, len(self._joint_ids))
            start_idx = self._round_robin_index % len(self._joint_ids)
            selected = [
                self._joint_ids[(start_idx + idx) % len(self._joint_ids)]
                for idx in range(read_count)
            ]
            self._round_robin_index = (start_idx + read_count) % len(self._joint_ids)
            return selected

    def update_schedule(
        self,
        *,
        poll_rate_hz=None,
        timeout_s=None,
        joints_per_cycle=None,
        inter_joint_gap_s=None,
    ):
        with self._lock:
            if poll_rate_hz is not None:
                self._poll_interval_s = 1.0 / max(1.0, float(poll_rate_hz))
            if timeout_s is not None:
                self._timeout_s = max(0.001, float(timeout_s))
            if joints_per_cycle is not None:
                self._joints_per_cycle = max(1, int(joints_per_cycle))
            if inter_joint_gap_s is not None:
                self._inter_joint_gap_s = max(0.0, float(inter_joint_gap_s))
            poll_rate = 1.0 / self._poll_interval_s
            timeout_now = self._timeout_s
            joints_now = self._joints_per_cycle
            gap_now = self._inter_joint_gap_s
        if self._log is not None:
            self._log.info(
                "FeedbackWorker schedule updated: poll_rate=%.1f Hz timeout=%.3f s "
                "reads_per_cycle=%d inter_joint_gap=%.1f ms",
                poll_rate,
                timeout_now,
                joints_now,
                gap_now * 1000.0,
            )

    def _worker(self):
        """
        Background thread: polls encoder values at fixed rate.
        Reads a tunable subset of joints each cycle in round-robin order to
        reduce CAN contention during 6-joint streaming.
        Uses monotonic timing to maintain precise polling cadence.
        """
        t_next = time.perf_counter()
        while not self._stop_event.is_set():
            poll_start = time.perf_counter()
            cycle_joint_ids = self._next_cycle_joint_ids()
            joint_times_ms = {}
            valid_count = 0

            for idx, joint_id in enumerate(cycle_joint_ids):
                if self._stop_event.is_set():
                    break

                angle = None
                try:
                    joint_start = time.perf_counter()
                    angle = self._comm_client.read_encoder(joint_id, timeout_s=self._timeout_s)
                    joint_elapsed = time.perf_counter() - joint_start
                    joint_times_ms[joint_id] = joint_elapsed * 1000.0
                except Exception as exc:
                    joint_times_ms[joint_id] = (time.perf_counter() - joint_start) * 1000.0
                    if self._log is not None:
                        self._log.warning(
                            "[FeedbackWorker] encoder read exception on J%d: %s",
                            joint_id,
                            exc,
                        )

                now_mono = time.monotonic()
                with self._lock:
                    if angle is None:
                        self._total_misses[joint_id] = self._total_misses.get(joint_id, 0) + 1
                        self._consecutive_misses[joint_id] = self._consecutive_misses.get(joint_id, 0) + 1
                    else:
                        valid_count += 1
                        self._latest_angles[joint_id] = angle
                        self._latest_timestamp[joint_id] = now_mono
                        self._total_samples[joint_id] = self._total_samples.get(joint_id, 0) + 1
                        self._consecutive_misses[joint_id] = 0

                if (
                    self._inter_joint_gap_s > 0.0
                    and idx < (len(cycle_joint_ids) - 1)
                    and not self._stop_event.is_set()
                ):
                    time.sleep(self._inter_joint_gap_s)

            poll_elapsed = time.perf_counter() - poll_start
            t_next += self._poll_interval_s
            sleep_s = t_next - time.perf_counter()
            cycle_mono = time.monotonic()

            with self._lock:
                self._total_cycles += 1
                self._last_cycle_elapsed_s = poll_elapsed
                self._last_cycle_timestamp = cycle_mono
                self._last_cycle_joint_ids = list(cycle_joint_ids)
                self._last_cycle_valid_count = int(valid_count)
                self._last_cycle_target_reads = len(cycle_joint_ids)
                self._last_cycle_sleep_s = max(0.0, sleep_s)
                if poll_elapsed >= self._slow_cycle_warn_s:
                    self._slow_cycles += 1

            if self._log is not None and poll_elapsed >= self._slow_cycle_warn_s:
                slowest_joint = None
                if joint_times_ms:
                    slowest_joint = max(joint_times_ms.items(), key=lambda item: item[1])
                if slowest_joint is not None:
                    self._log.warning(
                        "[FeedbackWorker] Slow cycle: elapsed=%.1f ms reads=%d valid=%d "
                        "slowest=J%d %.1f ms per_joint_ms=%s",
                        poll_elapsed * 1000.0,
                        len(cycle_joint_ids),
                        valid_count,
                        slowest_joint[0],
                        slowest_joint[1],
                        {f'J{jid}': f'{ms:.1f}' for jid, ms in joint_times_ms.items()},
                    )
                else:
                    self._log.warning(
                        "[FeedbackWorker] Slow cycle: elapsed=%.1f ms reads=%d valid=%d",
                        poll_elapsed * 1000.0,
                        len(cycle_joint_ids),
                        valid_count,
                    )

            if self._log is not None:
                should_log_diag = False
                with self._lock:
                    if (
                        self._last_diag_log_mono is None
                        or (cycle_mono - float(self._last_diag_log_mono)) >= self._cycle_log_interval_s
                    ):
                        self._last_diag_log_mono = cycle_mono
                        should_log_diag = True
                    total_cycles = int(self._total_cycles)
                    slow_cycles = int(self._slow_cycles)
                if should_log_diag:
                    slow_ratio = (100.0 * slow_cycles / total_cycles) if total_cycles > 0 else 0.0
                    self._log.info(
                        "[FeedbackWorker] Stats: cycles=%d slow_cycles=%d (%.1f%%) "
                        "last_elapsed=%.1f ms reads=%d valid=%d sleep=%.1f ms",
                        total_cycles,
                        slow_cycles,
                        slow_ratio,
                        poll_elapsed * 1000.0,
                        len(cycle_joint_ids),
                        valid_count,
                        max(0.0, sleep_s) * 1000.0,
                    )

            if sleep_s > 0:
                time.sleep(sleep_s)
            else:
                t_next = time.perf_counter()

    def get_snapshot(self, joint_id: int):
        """Get latest feedback for a joint (non-blocking)."""
        with self._lock:
            angle = self._latest_angles.get(joint_id)
            timestamp = self._latest_timestamp.get(joint_id)

            has_sample = angle is not None and timestamp is not None
            sample_age_s = None
            if has_sample and timestamp is not None:
                sample_age_s = max(0.0, time.monotonic() - timestamp)

            return {
                "angle_deg": angle,
                "sample_age_s": sample_age_s,
                "sample_mono_s": timestamp,
                "has_sample": has_sample,
                "total_samples": self._total_samples.get(joint_id, 0),
                "total_misses": self._total_misses.get(joint_id, 0),
                "consecutive_misses": self._consecutive_misses.get(joint_id, 0),
            }

    def get_vector_snapshot(self, joint_ids=None):
        """Return a non-blocking vector snapshot for multiple joints."""
        if joint_ids is None:
            selected_joint_ids = list(self._joint_ids)
        else:
            selected_joint_ids = [int(jid) for jid in joint_ids]

        now_mono = time.monotonic()
        with self._lock:
            angles_deg = []
            sample_ages_s = []
            sample_mono_s = []
            has_sample = []
            total_samples = []
            total_misses = []
            consecutive_misses = []

            sample_timestamps = []
            sample_ages_only = []
            valid_count = 0

            for joint_id in selected_joint_ids:
                angle = self._latest_angles.get(joint_id)
                timestamp = self._latest_timestamp.get(joint_id)
                sampled = angle is not None and timestamp is not None
                age_s = None
                if sampled and timestamp is not None:
                    age_s = max(0.0, now_mono - timestamp)
                    sample_timestamps.append(float(timestamp))
                    sample_ages_only.append(float(age_s))
                    valid_count += 1

                angles_deg.append(angle)
                sample_ages_s.append(age_s)
                sample_mono_s.append(timestamp)
                has_sample.append(sampled)
                total_samples.append(int(self._total_samples.get(joint_id, 0)))
                total_misses.append(int(self._total_misses.get(joint_id, 0)))
                consecutive_misses.append(int(self._consecutive_misses.get(joint_id, 0)))

            coherence_window_s = None
            if len(sample_timestamps) >= 2:
                coherence_window_s = max(sample_timestamps) - min(sample_timestamps)
            elif len(sample_timestamps) == 1:
                coherence_window_s = 0.0

            max_age_s = max(sample_ages_only) if sample_ages_only else None
            min_age_s = min(sample_ages_only) if sample_ages_only else None

            return {
                "joint_ids": selected_joint_ids,
                "angles_deg": angles_deg,
                "sample_ages_s": sample_ages_s,
                "sample_mono_s": sample_mono_s,
                "has_sample": has_sample,
                "total_samples": total_samples,
                "total_misses": total_misses,
                "consecutive_misses": consecutive_misses,
                "valid_count": valid_count,
                "max_age_s": max_age_s,
                "min_age_s": min_age_s,
                "coherence_window_s": coherence_window_s,
                "total_cycles": int(self._total_cycles),
                "slow_cycles": int(self._slow_cycles),
                "last_cycle_elapsed_s": float(self._last_cycle_elapsed_s),
                "last_cycle_timestamp_s": self._last_cycle_timestamp,
                "last_cycle_joint_ids": list(self._last_cycle_joint_ids),
                "last_cycle_valid_count": int(self._last_cycle_valid_count),
                "last_cycle_target_reads": int(self._last_cycle_target_reads),
                "last_cycle_sleep_s": float(self._last_cycle_sleep_s),
            }
