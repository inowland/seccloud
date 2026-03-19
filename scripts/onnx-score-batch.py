from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from seccloud.onnx_export import ONNXActionTower, ONNXContextTower


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    metadata = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
    request = json.loads(Path(args.input).read_text(encoding="utf-8"))
    bundle_root = Path(args.metadata).resolve().parent
    context_tower = ONNXContextTower(bundle_root / metadata["context_tower"]["path"])
    action_towers = {
        source: ONNXActionTower(bundle_root / tower["path"])
        for source, tower in metadata.get("action_towers", {}).items()
    }

    items: list[dict[str, object]] = []
    for item in request.get("items", []):
        source = str(item["source"])
        action_tower = action_towers.get(source)
        if action_tower is None:
            continue
        action_embedding = action_tower(
            np.asarray([item["action_indices"]], dtype=np.int64),
            np.asarray([item["action_weights"]], dtype=np.float32),
            np.asarray([item["action_mask"]], dtype=bool),
        )[0]
        context_embedding = context_tower(
            hist_window_indices=np.asarray([item["hist_window_indices"]], dtype=np.int64),
            hist_window_mask=np.asarray([item["hist_window_mask"]], dtype=bool),
            hist_num_windows=np.asarray([item["hist_num_windows"]], dtype=np.int64),
            peer_indices=np.asarray([item["peer_indices"]], dtype=np.int64),
            peer_weights=np.asarray([item["peer_weights"]], dtype=np.float32),
            peer_mask=np.asarray([item["peer_mask"]], dtype=bool),
            role=np.asarray([item["role"]], dtype=np.int64),
            location=np.asarray([item["location"]], dtype=np.int64),
            duration=np.asarray([item["duration"]], dtype=np.int64),
            privilege=np.asarray([item["privilege"]], dtype=np.int64),
        )[0]
        distance = 1.0 - float(np.dot(action_embedding, context_embedding))
        items.append(
            {
                "event_id": item["event_id"],
                "model_score": max(0.0, min(0.99, distance)),
            }
        )

    Path(args.output).write_text(json.dumps({"items": items}, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
