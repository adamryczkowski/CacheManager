import datetime as dt
from pathlib import Path
from typing import Optional, Any, Iterator

import numpy as np
from EntityHash import EntityHash
from humanize import naturaldelta, naturalsize
from overrides import overrides
from pydantic import BaseModel, TypeAdapter

from .abstract_cache_manager import AbstractCacheManager
from .ifaces import (
    ModelCacheManagerConfig,
    I_CacheStorageModify,
    I_StorageKeyGenerator,
    I_AbstractItemID,
    ItemID,
)
from .object_cache import ObjectCache, I_MockItemProducer
from .sqlite_settings_manager import SQLitePersistentDB


class MockObject(BaseModel):
    size: int
    hash: EntityHash

    def __len__(self) -> int:
        return self.size


class MockCacheStorage_Path(I_CacheStorageModify):
    """This mock stores only hashes and fake "lengths" of items. Otherwise, it is fine."""

    _stored_objects: dict[Path, MockObject]
    _total_space: float

    def __init__(self, total_space: float) -> None:
        """
        :param total_space: Total space allocated for the storage in bytes.
        """
        self._stored_objects = {}
        self._total_space = total_space

    @property
    def stored_objects(self) -> Iterator[MockObject]:
        for obj in self._stored_objects.values():
            yield obj

    @overrides
    def remove_item(self, item_storage_key: ItemID) -> bool:
        if item_storage_key in self._stored_objects:
            del self._stored_objects[item_storage_key]
            return True
        else:
            return False

    @overrides
    def load_item(self, item_storage_key: ItemID) -> bytes:
        # We are mocking the storage by returning the key itself with appended "data" string

        assert item_storage_key in self._stored_objects

        # noinspection PyTypeChecker
        return self._stored_objects[item_storage_key]

    @overrides
    def save_item(self, item: bytes, item_storage_key: ItemID):
        assert isinstance(item_storage_key, Path)
        assert isinstance(item, bytes)
        assert item_storage_key not in self._stored_objects
        ans = TypeAdapter(MockObject).validate_json(item.decode())
        self._stored_objects[item_storage_key] = ans

    @property
    @overrides
    def free_space(self) -> float:
        return self._total_space - sum(
            [len(obj) for obj in self._stored_objects.values()]
        )

    @property
    @overrides
    def storage_id(self) -> str:
        return "Mock storage"

    @overrides
    def calculate_hash(self, item_storage_key: ItemID) -> Optional[EntityHash]:
        assert isinstance(item_storage_key, Path)
        return self._stored_objects[item_storage_key].hash

    @overrides
    def does_item_exists(self, item_storage_key: ItemID) -> bool:
        return item_storage_key in self._stored_objects

    @overrides
    def close(self):
        pass

    @overrides
    def make_absolute_item_storage_key(self, item_storage_key: ItemID) -> ItemID:
        return item_storage_key

    @overrides
    def item_size(self, item_storage_key: ItemID) -> int:
        return self._stored_objects[item_storage_key].size


class MockStorageKeyGenerator_Path(BaseModel, I_StorageKeyGenerator):
    prefix: Path = Path()

    def generate_item_storage_key(self, item_key: EntityHash) -> Path:
        return self.prefix / Path(f"mock_{item_key.as_base64[0:6]}.bin")


class MockItemProducer(I_MockItemProducer):
    _compute_time: dt.timedelta
    _result_size: float

    def __init__(
        self, compute_time: dt.timedelta = None, result_size: float = None
    ) -> None:
        if compute_time is None:
            compute_time = dt.timedelta(
                seconds=np.random.exponential(size=1, scale=10.0)
            )

        if result_size is None:
            result_size = np.random.exponential(size=1, scale=1000000.0)

        self._compute_time = compute_time
        self._result_size = result_size

    @overrides
    def get_item_key(self) -> EntityHash:
        objstr = (
            f"{naturalsize(self._result_size)} and {naturaldelta(self._compute_time)}"
        )
        hash = EntityHash.HashBytes(objstr.encode(), "sha256")
        return hash

    @overrides
    def compute_item(self) -> Any:
        ans = MockObject(size=int(self._result_size), hash=self.get_item_key())
        # sleep(self._compute_time.total_seconds())
        return ans

    @overrides
    def instantiate_item(self, data: bytes) -> Any:
        ans = TypeAdapter(MockObject).validate_json(data.decode())
        assert isinstance(ans, MockObject)
        return ans

    @overrides
    def serialize_item(self, item: Any) -> bytes:
        assert isinstance(item, MockObject)
        return item.model_dump_json().encode()

    @property
    @overrides
    def compute_time(self) -> dt.timedelta:
        return self._compute_time

    @overrides
    def propose_item_storage_key(self) -> Optional[Path | I_AbstractItemID]:
        return None


def produce_mock_result(
    compute_time: dt.timedelta, result_size: float
) -> MockItemProducer:
    item = MockItemProducer(compute_time=compute_time, result_size=result_size)
    return item


def generate_mock_cache_Path(
    db_filename: Path,
    total_space: float,
    initial_config: ModelCacheManagerConfig = None,
) -> tuple[ObjectCache, MockCacheStorage_Path]:
    db = SQLitePersistentDB(db_filename, initial_config=initial_config)
    storage = MockCacheStorage_Path(total_space=total_space)

    abs_cache = AbstractCacheManager(db, storage)
    storage_key_generator = MockStorageKeyGenerator_Path()
    cache = ObjectCache(
        storage=storage,
        cache_manager=abs_cache,
        storage_key_generator=storage_key_generator,
        calculate_hash=True,
    )

    return cache, storage


def generate_mock_cache_view(
    file_cache: ObjectCache, file_prefix: Path = Path()
) -> ObjectCache:
    storage_key_generator = MockStorageKeyGenerator_Path(prefix=file_prefix)

    cache = ObjectCache(
        storage=file_cache._storage,
        cache_manager=file_cache._cache_manager,
        storage_key_generator=storage_key_generator,
        calculate_hash=file_cache.calculate_hash,
    )

    return cache
