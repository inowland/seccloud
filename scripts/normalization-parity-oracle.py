from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from seccloud.pipeline import RawEventValidationError, normalize_raw_event, validate_raw_event
from seccloud.storage import Workspace


def _canonicalize_event(event: dict[str, object]) -> dict[str, object]:
    payload = json.loads(json.dumps(event))
    payload["event_id"] = f"event::{payload['event_key']}"
    payload["principal"]["entity_id"] = f"entity::{payload['principal']['entity_key']}"
    payload["resource"]["entity_id"] = f"entity::{payload['resource']['entity_key']}"
    return payload


def build_oracle_result(fixture_path: Path) -> dict[str, object]:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    raw_event = fixture["raw_event"]

    with tempfile.TemporaryDirectory(prefix="seccloud-normalization-") as tmpdir:
        workspace = Workspace(Path(tmpdir))
        workspace.bootstrap()
        try:
            validate_raw_event(raw_event)
        except RawEventValidationError as exc:
            return {"kind": "dead_letter", "reason": str(exc)}

        object_key = workspace.raw_object_key(raw_event["source"], raw_event)
        event = normalize_raw_event(workspace, raw_event, object_key)
        return {"kind": "normalized", "event": _canonicalize_event(event.to_dict())}


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: normalization-parity-oracle.py <fixture-path>", file=sys.stderr)
        return 2
    fixture_path = Path(sys.argv[1])
    result = build_oracle_result(fixture_path)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
