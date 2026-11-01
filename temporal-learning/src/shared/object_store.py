"""A folder pretending to be S3. Same idea, no cloud account needed."""

import hashlib
import json
from pathlib import Path

from shared.payloads import DatasetRef


class ObjectStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put_rows(self, key: str, rows: list[dict]) -> DatasetRef:
        body = json.dumps(rows).encode()
        (self.root / key).parent.mkdir(parents=True, exist_ok=True)
        (self.root / key).write_bytes(body)
        return DatasetRef(key, len(rows), len(body), hashlib.sha256(body).hexdigest())

    def get_rows(self, ref: DatasetRef) -> list[dict]:
        body = (self.root / ref.uri).read_bytes()
        if hashlib.sha256(body).hexdigest() != ref.sha256:
            raise ValueError(f"{ref.uri} changed since it was written")
        return json.loads(body)

    def delete(self, key: str) -> None:
        (self.root / key).unlink(missing_ok=True)

    def exists(self, key: str) -> bool:
        return (self.root / key).exists()
