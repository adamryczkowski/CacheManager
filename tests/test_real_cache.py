from typing import Any, Optional

from EntityHash import EntityHash

from CacheManager import (
    generate_file_cache,
    StorageKeyGenerator_Path,
    I_ItemProducer,
    I_AbstractItemID,
    StoredItemID,
    ItemUtility,
)
from pathlib import Path
import datetime as dt
from tempfile import TemporaryDirectory
import os
import math
from overrides import overrides
import random
import time
import pytest

from CacheManager.ifaces import I_CacheStorageModify


def bytes_array(arr_length: int) -> bytes:
    return bytes([0] * arr_length)


class ItemPromise(I_ItemProducer):
    item_key: EntityHash
    compute_time: dt.timedelta
    result_size: float

    def __init__(
        self,
        item_key: EntityHash,
        compute_time: dt.timedelta,
        result_size: float,
    ) -> None:
        assert isinstance(compute_time, dt.timedelta)
        assert isinstance(result_size, float | int)
        assert result_size > 0
        assert isinstance(item_key, EntityHash)
        self.compute_time = compute_time
        self.result_size = float(result_size)
        self.item_key = item_key

    @overrides
    def get_item_key(self) -> EntityHash:
        return self.item_key

    @overrides
    def get_item_serialization_class(self) -> str:
        return ""

    @overrides
    def get_files_storing_state(
        self, storage: I_CacheStorageModify
    ) -> dict[str, StoredItemID]:
        return {}

    @overrides
    def protect_item(self):
        raise NotImplementedError()

    @overrides
    def compute_item(self) -> Any:
        time.sleep(self.compute_time.total_seconds())
        return f"Computed item of size {int(self.result_size)} bytes"

    @overrides
    def instantiate_item(
        self, data: bytes, extra_files: dict[str, StoredItemID] | None = None
    ) -> Any:
        assert extra_files is None
        return f"Computed item of size {len(data)} bytes"

    @overrides
    def serialize_item(self, item: Any) -> bytes:
        assert isinstance(item, str)
        assert item.startswith("Computed item of size ")
        itemsize = int(item[22:-6])
        return bytes_array(itemsize)

    @overrides
    def propose_item_storage_key(self) -> Optional[Path | I_AbstractItemID]:
        return None


def mock_result_promise(compute_time: dt.timedelta, result_size: int) -> ItemPromise:
    random_hash = EntityHash.HashBytes(
        random.randint(0, 2**32).to_bytes(length=8), "sha256"
    )
    promise = ItemPromise(
        item_key=random_hash, compute_time=compute_time, result_size=result_size
    )
    return promise


@pytest.fixture
def cache():
    db_path = Path(__file__).parent / "test_real.db"
    storage_path = TemporaryDirectory()

    if db_path.exists():
        db_path.unlink()

    storage_file_naming_settings = StorageKeyGenerator_Path(file_prefix="model_")
    utility_gen = ItemUtility(
        reserved_free_space=1024 * 1024 * 1024,
    )
    cache = generate_file_cache(
        cached_dir=Path(storage_path.name),
        utility_gen=utility_gen,
        storage_key_generator=storage_file_naming_settings,
        db_filename=str(db_path),
        calculate_hash=True,
    )
    yield cache
    cache.close()
    db_path.unlink()


def test1(cache):
    cache = cache
    storage_path = cache.storage.storage_id
    actual_free_space = (
        os.statvfs(storage_path).f_bavail * os.statvfs(storage_path).f_bsize
    )
    assert (
        math.fabs(int(cache.free_space) - (actual_free_space - 1024 * 1024 * 1024))
        < 4096
    )  # Should match within 4kB

    object_promise = mock_result_promise(
        compute_time=dt.timedelta(seconds=5), result_size=128
    )

    cache.get_object(object_promise)
    cache_item = cache.get_object_info(object_promise.get_item_key())
    assert cache_item.utility > 0
    assert cache_item.exists
    assert cache.storage.does_item_exists(cache_item.item_storage_key)

    mock_object_promise = mock_result_promise(
        compute_time=dt.timedelta(seconds=0), result_size=500 * 1024
    )
    cache.print_contents()
    cache.get_object(mock_object_promise)
    cache_item = cache.get_object_info(mock_object_promise.get_item_key())
    assert cache_item.utility < 0
    assert not cache.storage.does_item_exists(cache_item.item_storage_key)
    assert not cache_item.exists
