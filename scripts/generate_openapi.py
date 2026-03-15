from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from seccloud.api import create_app


def main() -> None:
    schema = create_app().openapi()
    target = Path("web/openapi.json")
    target.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
