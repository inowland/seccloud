from __future__ import annotations

import hashlib
import uuid
from typing import Any


def _new_uuid() -> uuid.UUID:
    uuid7 = getattr(uuid, "uuid7", None)
    if callable(uuid7):
        return uuid7()
    return uuid.uuid4()


def new_prefixed_id(prefix: str) -> str:
    return f"{prefix}_{_new_uuid()}"


def stable_digest_key(prefix: str, *parts: Any) -> str:
    serialized = "|".join("" if part is None else str(part) for part in parts)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def event_key(source: str, source_event_id: str, integration_id: str | None = None) -> str:
    return stable_digest_key("evk", source, integration_id or "", source_event_id)


def entity_key(
    *,
    entity_kind: str,
    source: str,
    native_id: str,
    provider: str | None = None,
) -> str:
    return stable_digest_key("enk", entity_kind, source, provider or "", native_id)
