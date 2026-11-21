"""Tiny Schema Registry client with a cache. Schema ids are immutable, so caching is safe."""
from __future__ import annotations

import json
import urllib.error
import urllib.request


class SchemaNotFound(Exception):
    """The registry says this schema id does not exist (HTTP 404). Permanent, not transient."""


class SchemaRegistry:
    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self._cache: dict[int, str] = {}

    def writer_schema(self, schema_id: int) -> str:
        """Avro schema JSON that was used to write messages carrying this id."""
        if schema_id not in self._cache:
            try:
                with urllib.request.urlopen(f"{self.url}/schemas/ids/{schema_id}", timeout=10) as r:
                    self._cache[schema_id] = json.load(r)["schema"]
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    raise SchemaNotFound(schema_id) from e
                raise  # 5xx etc. are transient: let the batch fail and retry, don't quarantine
        return self._cache[schema_id]
