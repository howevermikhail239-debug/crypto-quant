"""Realtime Connection Supervisor, Reconnect Manager, and Gap Automation (Phase 1C Item 7C).

Coordinates WebSocket stream reconnection, zombie connection prevention, gap detection,
and REST recovery invocation.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ..time import utc_now
from .gap_registry import GapRecord, GapRegistry, GapType
from .realtime_envelope import WsSessionInfo
from .realtime_recovery import perform_gap_recovery
from .realtime_session import RealtimeSessionLifecycle
from .reconnect import ReconnectConfig, compute_reconnect_delay

logger = logging.getLogger(__name__)


class RealtimeStreamSupervisor:
    """Supervises a single WebSocket stream with automatic reconnect, gap registry, and recovery."""

    def __init__(
        self,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        stream_topic: str,
        dataset_class: str,
        root: Path,
        reconnect_config: ReconnectConfig | None = None,
        queue_maxsize: int = 10_000,
    ) -> None:
        self.exchange = exchange
        self.market_type = market_type
        self.symbol = symbol
        self.stream_topic = stream_topic
        self.dataset_class = dataset_class
        self.root = root
        self.reconnect_config = reconnect_config or ReconnectConfig()
        self.queue_maxsize = queue_maxsize

        self.registry = GapRegistry(root)
        self.active_session: RealtimeSessionLifecycle | None = None
        self.last_event_time: datetime | None = None
        self.reconnect_count = 0
        self.last_gap: GapRecord | None = None

    def create_new_session(self) -> RealtimeSessionLifecycle:
        """Creates a new RealtimeSessionLifecycle with unique session_id and connection_id."""
        self.reconnect_count += 1
        sess_id = f"sess_{uuid.uuid4().hex[:12]}"
        conn_id = f"conn_{uuid.uuid4().hex[:8]}"

        session_info = WsSessionInfo(
            session_id=sess_id,
            connection_id=conn_id,
            exchange=self.exchange,
            market_type=self.market_type,
            stream_topic=self.stream_topic,
            connected_at=utc_now(),
        )

        runner = RealtimeSessionLifecycle(
            session_info=session_info,
            symbol=self.symbol,
            root=self.root,
            queue_maxsize=self.queue_maxsize,
        )
        return runner

    async def handle_disconnect(self, disconnect_reason: str = "socket disconnected") -> tuple[GapRecord | None, float]:
        """Handles session disconnect:
        1. Gracefully closes old session (zombie protection).
        2. Calculates candidate gap interval.
        3. Registers candidate gap in GapRegistry.
        4. Calculates reconnect delay with exponential backoff & jitter.
        """
        old_sess_id = None
        gap_start = self.last_event_time or utc_now() - timedelta(seconds=10)

        if self.active_session is not None:
            old_sess_id = self.active_session.meta.session_id
            await self.active_session.close_session(reason=disconnect_reason)
            self.active_session = None

        gap_end = utc_now()
        delay_sec = compute_reconnect_delay(self.reconnect_count + 1, self.reconnect_config)

        gap_record = self.registry.register_gap(
            exchange=self.exchange,
            market_type=self.market_type,
            instrument_id=f"ins_{self.exchange}_{self.market_type}_{self.symbol}",
            dataset_class=self.dataset_class,
            source_stream=self.stream_topic,
            gap_start=gap_start,
            gap_end=gap_end,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
            session_before=old_sess_id,
            notes=f"Disconnect reason: {disconnect_reason}. Reconnect delay: {delay_sec:.2f}s",
        )
        self.last_gap = gap_record

        return gap_record, delay_sec

    async def trigger_gap_recovery(
        self,
        gap: GapRecord,
        mock_items: list[dict[str, Any]] | None = None,
    ) -> GapRecord:
        """Triggers recovery for candidate gap asynchronously without blocking live stream."""
        updated_gap = await asyncio.to_thread(
            perform_gap_recovery, gap, self.root, mock_fetched_items=mock_items
        )
        self.last_gap = updated_gap
        return updated_gap
