"""In-memory stand-in for the few MongoDB collection calls leases.py makes.

Supports equality, `$lt`, `$gte` and `$or` filters, `$set` updates with
dotted paths, and upserts. Each call reads and writes one document without
awaiting in between, which is the per-document atomicity MongoDB gives
find_one_and_update and update_one, so concurrent workers can be simulated
with asyncio.gather.
"""

from __future__ import annotations

import copy
from typing import Any
from unittest.mock import MagicMock


class FakeCursor:
    def __init__(self, docs) -> None:
        self._docs = list(docs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._docs:
            raise StopAsyncIteration
        return self._docs.pop(0)


class FakeCollection:
    def __init__(self, docs: list[dict] = ()) -> None:
        self.docs: dict[Any, dict] = {d["_id"]: d for d in docs}

    @staticmethod
    def _get(doc: dict, path: str):
        for part in path.split("."):
            if not isinstance(doc, dict):
                return None
            doc = doc.get(part)
        return doc

    def _matches(self, doc: dict, flt: dict) -> bool:
        for key, want in flt.items():
            if key == "$or":
                if not any(self._matches(doc, sub) for sub in want):
                    return False
                continue
            have = self._get(doc, key)
            if isinstance(want, dict) and "$lt" in want:
                if have is None or not have < want["$lt"]:
                    return False
            elif isinstance(want, dict) and "$gte" in want:
                if have is None or not have >= want["$gte"]:
                    return False
            elif have != want:
                return False
        return True

    @staticmethod
    def _apply(doc: dict, update: dict) -> None:
        for path, value in update["$set"].items():
            *parents, last = path.split(".")
            target = doc
            for part in parents:
                target = target[part]
            target[last] = value

    def _one(self, flt: dict) -> dict | None:
        doc = self.docs.get(flt["_id"])
        return doc if doc is not None and self._matches(doc, flt) else None

    def find(self, flt: dict) -> FakeCursor:
        return FakeCursor(copy.deepcopy(d) for d in self.docs.values() if self._matches(d, flt))

    async def count_documents(self, flt: dict) -> int:
        return sum(1 for d in self.docs.values() if self._matches(d, flt))

    async def find_one_and_update(self, filter: dict, update: dict):
        doc = self._one(filter)
        if doc is None:
            return None
        before = copy.deepcopy(doc)
        self._apply(doc, update)
        return before

    async def update_one(self, filter: dict, update: dict, upsert: bool = False):
        doc = self._one(filter)
        if doc is None and upsert:
            doc = self.docs[filter["_id"]] = {"_id": filter["_id"]}
        if doc is not None:
            self._apply(doc, update)
        return MagicMock(modified_count=int(doc is not None))

    async def delete_one(self, filter: dict):
        doc = self._one(filter)
        if doc is not None:
            del self.docs[filter["_id"]]
        return MagicMock(deleted_count=int(doc is not None))


def fake_db(*api_configs: dict) -> MagicMock:
    db = MagicMock()
    db.api_configs = FakeCollection(list(api_configs))
    db.workers = FakeCollection()
    return db
