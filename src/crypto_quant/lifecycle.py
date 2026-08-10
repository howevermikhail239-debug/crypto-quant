"""Semantic validation for the future lifecycle interface; not an execution engine."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .time import require_utc

TERMINAL_STATES = frozenset({"INVALIDATED", "CLOSED", "EXPIRED"})
TARGET_RANK = {"NONE": 0, "TP1": 1, "TP2": 2, "TP3": 3}


def _states(*values: str) -> frozenset[str]:
    return frozenset(values)


ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "NEW": _states("ACTIVE", "INVALIDATED", "EXPIRED"),
    "ACTIVE": _states(
        "ACTIVE", "STRENGTHENED", "WEAKENED", "TP1_REACHED", "TP2_REACHED",
        "TP3_REACHED", "TRAILING", "INVALIDATED", "EXIT_RECOMMENDED", "CLOSED", "EXPIRED",
    ),
    "STRENGTHENED": _states(
        "ACTIVE", "STRENGTHENED", "WEAKENED", "TP1_REACHED", "TP2_REACHED",
        "TP3_REACHED", "TRAILING", "INVALIDATED", "EXIT_RECOMMENDED", "CLOSED", "EXPIRED",
    ),
    "WEAKENED": _states(
        "ACTIVE", "WEAKENED", "TP1_REACHED", "TP2_REACHED", "TP3_REACHED", "TRAILING",
        "INVALIDATED", "EXIT_RECOMMENDED", "CLOSED", "EXPIRED",
    ),
    "TP1_REACHED": _states(
        "ACTIVE", "STRENGTHENED", "WEAKENED", "TP2_REACHED", "TP3_REACHED", "TRAILING",
        "INVALIDATED", "EXIT_RECOMMENDED", "CLOSED", "EXPIRED",
    ),
    "TP2_REACHED": _states(
        "ACTIVE", "STRENGTHENED", "WEAKENED", "TP3_REACHED", "TRAILING", "INVALIDATED",
        "EXIT_RECOMMENDED", "CLOSED", "EXPIRED",
    ),
    "TP3_REACHED": _states("TRAILING", "EXIT_RECOMMENDED", "CLOSED", "EXPIRED"),
    "TRAILING": _states(
        "TRAILING", "TP2_REACHED", "TP3_REACHED", "EXIT_RECOMMENDED", "CLOSED",
        "INVALIDATED", "EXPIRED",
    ),
    "EXIT_RECOMMENDED": _states("EXIT_RECOMMENDED", "CLOSED", "INVALIDATED", "EXPIRED"),
}


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from error
    return require_utc(parsed)


def validate_revision_chain(revisions: Sequence[Mapping[str, Any]]) -> None:
    """Validate immutable lineage and conservative lifecycle safety invariants."""
    if not revisions:
        raise ValueError("revision chain must not be empty")
    signal_id: object | None = None
    created_at: object | None = None
    seen_ids: set[str] = set()
    previous: Mapping[str, Any] | None = None
    for expected_number, revision in enumerate(revisions, start=1):
        current_signal_id = revision.get("signal_id")
        revision_id = revision.get("revision_id")
        if not isinstance(current_signal_id, str) or not isinstance(revision_id, str):
            raise ValueError("signal_id and revision_id are required")
        signal_id = current_signal_id if signal_id is None else signal_id
        if current_signal_id != signal_id or revision_id in seen_ids:
            raise ValueError("revisions must have one signal_id and unique revision_ids")
        seen_ids.add(revision_id)
        if revision.get("revision_number") != expected_number:
            raise ValueError("revision_number must be contiguous and start at 1")
        parent = revision.get("parent_revision_id")
        if expected_number == 1:
            if parent is not None or revision.get("scenario_state") != "NEW":
                raise ValueError("first revision must have null parent and NEW state")
        elif previous is not None and parent != previous.get("revision_id"):
            raise ValueError("revision parent must be the immediately preceding revision")

        revision_time = _timestamp(revision.get("revision_timestamp"), "revision_timestamp")
        knowledge_time = _timestamp(revision.get("knowledge_time"), "knowledge_time")
        if knowledge_time > revision_time:
            raise ValueError("knowledge_time cannot be after revision_timestamp")
        context = revision.get("time_context")
        if not isinstance(context, Mapping):
            raise ValueError("time_context is required")
        current_created_at = context.get("signal_created_at")
        created_at = current_created_at if created_at is None else created_at
        if current_created_at != created_at:
            raise ValueError("signal_created_at is immutable across revisions")
        expiry = _timestamp(context.get("scenario_expiry_at"), "scenario_expiry_at")
        if expiry < _timestamp(created_at, "signal_created_at"):
            raise ValueError("scenario_expiry_at cannot precede signal_created_at")
        next_review = context.get("next_review_at")
        state = revision.get("scenario_state")
        if state in TERMINAL_STATES and next_review is not None:
            raise ValueError("terminal state must have null next_review_at")
        if next_review is not None:
            review_time = _timestamp(next_review, "next_review_at")
            if review_time <= revision_time or review_time > expiry:
                raise ValueError("next_review_at must be after revision and not after expiry")

        if previous is not None:
            previous_time = _timestamp(previous.get("revision_timestamp"), "revision_timestamp")
            previous_knowledge = _timestamp(previous.get("knowledge_time"), "knowledge_time")
            if revision_time <= previous_time or knowledge_time < previous_knowledge:
                raise ValueError("revision time must increase and knowledge_time cannot regress")
            old_state = str(previous.get("scenario_state"))
            if old_state in TERMINAL_STATES:
                raise ValueError("terminal revision cannot have a child")
            if state not in ALLOWED_TRANSITIONS.get(old_state, frozenset()):
                raise ValueError(f"illegal lifecycle transition: {old_state} -> {state}")
            if TARGET_RANK.get(str(revision.get("target_progress")), -1) < TARGET_RANK.get(
                str(previous.get("target_progress")), -1
            ):
                raise ValueError("target_progress cannot regress")
            _validate_stop_not_widened(previous, revision)
        previous = revision


def _validate_stop_not_widened(previous: Mapping[str, Any], current: Mapping[str, Any]) -> None:
    decision = current.get("decision")
    if decision not in {"LONG", "SHORT"} or previous.get("decision") != decision:
        return
    old_stop, new_stop = previous.get("stop_loss"), current.get("stop_loss")
    if not isinstance(old_stop, Mapping) or not isinstance(new_stop, Mapping):
        return
    if new_stop.get("action") == "CANCEL_STOP_AND_EXIT":
        return
    try:
        old_price = Decimal(str(old_stop["price"]))
        new_price = Decimal(str(new_stop["price"]))
    except (InvalidOperation, KeyError, TypeError):
        return
    if decision == "LONG" and new_price < old_price:
        raise ValueError("long stop cannot be widened downward")
    if decision == "SHORT" and new_price > old_price:
        raise ValueError("short stop cannot be widened upward")
