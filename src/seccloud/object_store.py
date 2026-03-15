from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Protocol


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


class ObjectStore(Protocol):
    def put_json(self, object_key: str, payload: Any) -> str: ...

    def get_json(self, object_key: str, default: Any = None) -> Any: ...

    def put_bytes(self, object_key: str, payload: bytes, *, content_type: str | None = None) -> str: ...

    def get_bytes(self, object_key: str, default: bytes | None = None) -> bytes | None: ...

    def delete(self, object_key: str) -> None: ...

    def list_json(self, prefix: str = "") -> list[tuple[str, Any]]: ...


class LocalObjectStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def put_json(self, object_key: str, payload: Any) -> str:
        path = self.root / object_key
        _write_json(path, payload)
        return object_key

    def get_json(self, object_key: str, default: Any = None) -> Any:
        return _read_json(self.root / object_key, default)

    def put_bytes(self, object_key: str, payload: bytes, *, content_type: str | None = None) -> str:
        path = self.root / object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return object_key

    def get_bytes(self, object_key: str, default: bytes | None = None) -> bytes | None:
        path = self.root / object_key
        if not path.exists():
            return default
        return path.read_bytes()

    def delete(self, object_key: str) -> None:
        path = self.root / object_key
        if path.exists():
            path.unlink()

    def list_json(self, prefix: str = "") -> list[tuple[str, Any]]:
        base = self.root / prefix
        if not base.exists():
            return []
        items: list[tuple[str, Any]] = []
        for path in sorted(base.rglob("*.json")):
            payload = _read_json(path)
            if payload is not None:
                items.append((str(path.relative_to(self.root)), payload))
        return items


class S3ObjectStore:
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        endpoint_url: str | None = None,
        region_name: str | None = None,
    ):
        try:
            import boto3
        except ModuleNotFoundError as exc:
            raise RuntimeError("S3 object store backend requires boto3 to be installed.") from exc

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

    def _full_key(self, object_key: str) -> str:
        if not self.prefix:
            return object_key
        return f"{self.prefix}/{object_key}"

    def _relative_key(self, object_key: str) -> str:
        if self.prefix and object_key.startswith(f"{self.prefix}/"):
            return object_key[len(self.prefix) + 1 :]
        return object_key

    def put_json(self, object_key: str, payload: Any) -> str:
        self._client.put_object(
            Bucket=self.bucket,
            Key=self._full_key(object_key),
            Body=(json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
            ContentType="application/json",
        )
        return object_key

    def get_json(self, object_key: str, default: Any = None) -> Any:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=self._full_key(object_key))
        except Exception as exc:  # pragma: no cover - depends on optional boto3 runtime
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code in {"NoSuchKey", "404"}:
                return default
            raise
        return json.loads(response["Body"].read().decode("utf-8"))

    def put_bytes(self, object_key: str, payload: bytes, *, content_type: str | None = None) -> str:
        self._client.put_object(
            Bucket=self.bucket,
            Key=self._full_key(object_key),
            Body=payload,
            ContentType=content_type or "application/octet-stream",
        )
        return object_key

    def get_bytes(self, object_key: str, default: bytes | None = None) -> bytes | None:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=self._full_key(object_key))
        except Exception as exc:  # pragma: no cover - depends on optional boto3 runtime
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code in {"NoSuchKey", "404"}:
                return default
            raise
        return response["Body"].read()

    def delete(self, object_key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=self._full_key(object_key))

    def list_json(self, prefix: str = "") -> list[tuple[str, Any]]:
        paginator = self._client.get_paginator("list_objects_v2")
        items: list[tuple[str, Any]] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._full_key(prefix)):
            for obj in page.get("Contents", []):
                relative_key = self._relative_key(obj["Key"])
                if not relative_key.endswith(".json"):
                    continue
                payload = self.get_json(relative_key)
                if payload is not None:
                    items.append((relative_key, payload))
        items.sort(key=lambda item: item[0])
        return items


def build_object_store(root: str | Path) -> ObjectStore:
    backend = os.environ.get("SECCLOUD_OBJECT_STORE_BACKEND", "local").lower()
    if backend == "local":
        return LocalObjectStore(root)
    if backend == "s3":
        bucket = os.environ.get("SECCLOUD_OBJECT_STORE_BUCKET")
        if not bucket:
            raise RuntimeError("SECCLOUD_OBJECT_STORE_BUCKET is required for the s3 object store backend.")
        return S3ObjectStore(
            bucket=bucket,
            prefix=os.environ.get("SECCLOUD_OBJECT_STORE_PREFIX", ""),
            endpoint_url=os.environ.get("SECCLOUD_OBJECT_STORE_ENDPOINT_URL"),
            region_name=os.environ.get("SECCLOUD_OBJECT_STORE_REGION"),
        )
    raise RuntimeError(f"Unsupported object store backend: {backend}")
