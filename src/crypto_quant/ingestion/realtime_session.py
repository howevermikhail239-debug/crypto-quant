"""Bounded Realtime Envelope Queues and Session Lifecycle Management (Phase 1C Item 7B).

Implements producer-consumer architecture for WebSocket market data streams with:
- Configurable bounded queues with explicit telemetry (high-watermark, wait duration, lag).
- Zero silent drops: explicit backpressure & auditable data conservation.
- Formal Session Lifecycle state machine (CREATED -> CONNECTING -> ACTIVE -> DRAINING -> CLOSED / FAILED).
- Graceful drain with configurable timeout.
- Task cancellation & disk write failure safety.
- Per-stream session isolation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

from ..time import utc_now
from .realtime_envelope import (
    RawWsEnvelope,
    RawWsSegmentWriter,
    WsSessionInfo,
    normalize_ws_envelope_to_aggregate_trades,
    normalize_ws_envelope_to_individual_trades,
)

logger = logging.getLogger(__name__)


class RealtimeSessionState(StrEnum):
    CREATED = "CREATED"
    CONNECTING = "CONNECTING"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


VALID_TRANSITIONS: dict[RealtimeSessionState, set[RealtimeSessionState]] = {
    RealtimeSessionState.CREATED: {RealtimeSessionState.CONNECTING, RealtimeSessionState.FAILED},
    RealtimeSessionState.CONNECTING: {RealtimeSessionState.ACTIVE, RealtimeSessionState.FAILED},
    RealtimeSessionState.ACTIVE: {RealtimeSessionState.DRAINING, RealtimeSessionState.FAILED},
    RealtimeSessionState.DRAINING: {RealtimeSessionState.CLOSED, RealtimeSessionState.FAILED},
    RealtimeSessionState.CLOSED: set(),
    RealtimeSessionState.FAILED: set(),
}


@dataclass
class BoundedQueueMetrics:
    capacity: int
    current_size: int = 0
    utilization: float = 0.0
    high_watermark: int = 0
    messages_enqueued: int = 0
    messages_dequeued: int = 0
    producer_wait_count: int = 0
    producer_wait_duration_sec: float = 0.0
    writer_lag_sec: float = 0.0


class BoundedWsEnvelopeQueue:
    """Thread/Async-safe Bounded Queue for RawWsEnvelopes with Telemetry."""

    def __init__(self, maxsize: int = 10_000) -> None:
        if maxsize <= 0:
            raise ValueError("Queue maxsize must be > 0")
        self._queue: asyncio.Queue[RawWsEnvelope] = asyncio.Queue(maxsize=maxsize)
        self.capacity = maxsize
        self.high_watermark = 0
        self.messages_enqueued = 0
        self.messages_dequeued = 0
        self.producer_wait_count = 0
        self.producer_wait_duration_sec = 0.0
        self._last_dequeued_received_at: datetime | None = None

    async def put(self, envelope: RawWsEnvelope, timeout: float | None = None) -> None:
        start_t = time.monotonic()
        is_full = self._queue.full()
        if is_full:
            self.producer_wait_count += 1

        if timeout is None:
            await self._queue.put(envelope)
        else:
            await asyncio.wait_for(self._queue.put(envelope), timeout=timeout)

        wait_duration = time.monotonic() - start_t
        if is_full:
            self.producer_wait_duration_sec += wait_duration

        self.messages_enqueued += 1
        qsize = self._queue.qsize()
        if qsize > self.high_watermark:
            self.high_watermark = qsize

    async def get(self) -> RawWsEnvelope:
        envelope = await self._queue.get()
        self.messages_dequeued += 1
        self._last_dequeued_received_at = envelope.received_at
        return envelope

    def task_done(self) -> None:
        self._queue.task_done()

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    def get_metrics(self) -> BoundedQueueMetrics:
        sz = self.qsize
        util = sz / self.capacity if self.capacity > 0 else 0.0
        lag = 0.0
        if self._last_dequeued_received_at is not None:
            lag = max(0.0, (utc_now() - self._last_dequeued_received_at).total_seconds())
        return BoundedQueueMetrics(
            capacity=self.capacity,
            current_size=sz,
            utilization=util,
            high_watermark=self.high_watermark,
            messages_enqueued=self.messages_enqueued,
            messages_dequeued=self.messages_dequeued,
            producer_wait_count=self.producer_wait_count,
            producer_wait_duration_sec=self.producer_wait_duration_sec,
            writer_lag_sec=lag,
        )


@dataclass
class RealtimeSessionMetadata:
    session_id: str
    connection_id: str
    exchange: str
    market_type: str
    symbol: str
    stream_topic: str
    state: RealtimeSessionState
    created_at: datetime
    connected_at: datetime | None = None
    draining_at: datetime | None = None
    closed_at: datetime | None = None
    envelopes_received: int = 0
    envelopes_enqueued: int = 0
    envelopes_written: int = 0
    normalized_records: int = 0
    queue_high_watermark: int = 0
    shutdown_reason: str | None = None
    failure_reason: str | None = None


class RealtimeSessionLifecycle:
    """Session State Machine and Worker Runner for 7B."""

    def __init__(
        self,
        *,
        session_info: WsSessionInfo,
        symbol: str,
        root: Path,
        queue_maxsize: int = 10_000,
        drain_timeout_sec: float = 5.0,
    ) -> None:
        self.session_info = session_info
        self.symbol = symbol
        self.root = root
        self.drain_timeout_sec = drain_timeout_sec

        self.queue = BoundedWsEnvelopeQueue(maxsize=queue_maxsize)
        self.writer = RawWsSegmentWriter(root, session_info.exchange, session_info.market_type, symbol)

        now = utc_now()
        self.meta = RealtimeSessionMetadata(
            session_id=session_info.session_id,
            connection_id=session_info.connection_id,
            exchange=session_info.exchange,
            market_type=session_info.market_type,
            symbol=symbol,
            stream_topic=session_info.stream_topic,
            state=RealtimeSessionState.CREATED,
            created_at=now,
        )
        self._consumer_task: asyncio.Task | None = None
        self._stop_requested = False

    def transition_to(self, target: RealtimeSessionState, reason: str | None = None) -> None:
        current = self.meta.state
        valid_targets = VALID_TRANSITIONS.get(current, set())
        if target not in valid_targets:
            raise ValueError(
                f"Invalid session state transition: {current.value} -> {target.value}. "
                f"Allowed transitions from {current.value}: {[t.value for t in valid_targets]}"
            )

        now = utc_now()
        self.meta.state = target
        if target == RealtimeSessionState.CONNECTING:
            pass
        elif target == RealtimeSessionState.ACTIVE:
            self.meta.connected_at = now
        elif target == RealtimeSessionState.DRAINING:
            self.meta.draining_at = now
        elif target in (RealtimeSessionState.CLOSED, RealtimeSessionState.FAILED):
            self.meta.closed_at = now

        if reason:
            if target == RealtimeSessionState.FAILED:
                self.meta.failure_reason = reason
            else:
                self.meta.shutdown_reason = reason
        logger.info(f"Session {self.meta.session_id} state transition: {current.value} -> {target.value} ({reason or 'normal'})")

    async def push_envelope(self, envelope: RawWsEnvelope) -> None:
        """Producer interface: enqueues raw envelope with backpressure."""
        if self.meta.state not in (RealtimeSessionState.CONNECTING, RealtimeSessionState.ACTIVE):
            raise RuntimeError(f"Cannot push envelope in session state {self.meta.state.value}")

        self.meta.envelopes_received += 1
        await self.queue.put(envelope)
        self.meta.envelopes_enqueued += 1
        self.meta.queue_high_watermark = max(self.meta.queue_high_watermark, self.queue.high_watermark)

    async def _consumer_loop(self) -> None:
        """Consumer worker loop: dequeues envelopes, writes segment, normalizes."""
        while True:
            try:
                if self._stop_requested and self.queue.empty:
                    break

                try:
                    envelope = await asyncio.wait_for(self.queue.get(), timeout=0.2)
                except TimeoutError:
                    continue

                try:
                    self.writer.write_envelope(envelope)
                    self.meta.envelopes_written += 1

                    # Normalize based on topic
                    if "aggTrade" in envelope.stream_topic:
                        rb = normalize_ws_envelope_to_aggregate_trades(envelope)
                    else:
                        rb = normalize_ws_envelope_to_individual_trades(envelope)

                    self.meta.normalized_records += rb.num_rows
                except Exception as exc:
                    logger.error(f"Consumer exception in session {self.meta.session_id}: {exc}", exc_info=True)
                    self.transition_to(RealtimeSessionState.FAILED, reason=f"Consumer error: {exc}")
                    raise
                finally:
                    self.queue.task_done()

            except asyncio.CancelledError:
                logger.warning(f"Consumer task cancelled for session {self.meta.session_id}")
                raise

    def start_consumer(self) -> None:
        if self._consumer_task is not None and not self._consumer_task.done():
            return
        self._consumer_task = asyncio.create_task(self._consumer_loop())

    async def close_session(self, reason: str = "normal shutdown") -> Path | None:
        """Graceful session close: transitions ACTIVE -> DRAINING -> CLOSED."""
        self._stop_requested = True
        if self.meta.state in (RealtimeSessionState.CONNECTING, RealtimeSessionState.ACTIVE):
            self.transition_to(RealtimeSessionState.DRAINING, reason=reason)

        if self._consumer_task is not None:
            try:
                await asyncio.wait_for(self._consumer_task, timeout=self.drain_timeout_sec)
            except TimeoutError:
                logger.error(f"Drain timeout ({self.drain_timeout_sec}s) exceeded for session {self.meta.session_id}")
                self._consumer_task.cancel()
                self.transition_to(RealtimeSessionState.FAILED, reason=f"Drain timeout ({self.drain_timeout_sec}s)")
                return self.writer.seal()

        sealed_file = self.writer.seal()

        if self.meta.state == RealtimeSessionState.DRAINING:
            self.transition_to(RealtimeSessionState.CLOSED, reason=reason)

        return sealed_file
