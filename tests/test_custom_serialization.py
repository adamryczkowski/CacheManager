import datetime as dt
import math
import os
import pickle
import time
import zlib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional

import numpy as np
import pytest
from EntityHash import calc_hash, EntityHash
from overrides import overrides

from CacheManager import (
    generate_file_cache,
    StorageKeyGenerator_Path,
    I_ItemProducer,
    ObjectCache,
    I_AbstractItemID,
    StoredItemID,
    ItemUtility,
)
from CacheManager.ifaces import I_CacheStorageModify


class SomeHeavyResult:
    _data: bytes

    def __init__(self, object_size: int):
        # Array of random bytes of size 'object_size'
        self._data = np.random.bytes(object_size)
        assert isinstance(self._data, bytes)


class SomeHeavyComputation(I_ItemProducer):
    compute_arguments: dict
    item_serialization_performance_class: str = ""

    def __init__(self, item_serialization_performance_class="", **kwargs):
        self.compute_arguments = kwargs
        self.item_serialization_performance_class = item_serialization_performance_class

    @overrides
    def get_item_serialization_class(self) -> str:
        return self.item_serialization_performance_class

    @overrides
    def get_files_storing_state(
        self, storage: I_CacheStorageModify
    ) -> dict[str, StoredItemID]:
        return {}

    def protect_item(self):
        raise NotImplementedError

    # @overrides
    def get_item_key(self) -> EntityHash:
        return calc_hash(self.compute_arguments)

    # @overrides
    def compute_item(self) -> Any:
        return self.some_heavy_computation(**self.compute_arguments)

    @staticmethod
    def some_heavy_computation(
        arg1_important_arg: str, arg2_compute_time: dt.timedelta, arg3_result_size: int
    ) -> SomeHeavyResult:
        time.sleep(arg2_compute_time.total_seconds())
        return SomeHeavyResult(arg3_result_size)

    # @overrides
    def instantiate_item(
        self, data: bytes, extra_files: dict[str, StoredItemID] | None = None
    ) -> Any:
        assert extra_files is None
        uncompressed_data = zlib.decompress(data)
        item = pickle.loads(uncompressed_data)
        return item

    # @overrides
    def serialize_item(self, item: Any) -> bytes:
        bytes = pickle.dumps(item)
        compressed_bytes = zlib.compress(bytes)
        return compressed_bytes

    # @overrides
    def propose_item_storage_key(self) -> Optional[Path | I_AbstractItemID]:
        return None


@pytest.fixture
def cache():
    db_path = Path(__file__).parent / "test_real.db"
    storage_path = TemporaryDirectory()

    if db_path.exists():
        db_path.unlink()

    storage_file_naming_settings = StorageKeyGenerator_Path(
        file_prefix="model_", file_extension="json"
    )
    caching_utility = ItemUtility(
        reserved_free_space=1024 * 1024 * 1024,
        cost_of_minute_compute_rel_to_cost_of_1GB=1000,
    )
    cache = generate_file_cache(
        cached_dir=Path(storage_path.name),
        utility_gen=caching_utility,
        storage_key_generator=storage_file_naming_settings,
        db_filename=str(db_path),
        calculate_hash=True,
    )
    yield cache
    cache.remove_all_cached_items()
    cache.close()
    db_path.unlink()


def test1(cache: ObjectCache):
    cache = cache
    storage_path = cache.storage.storage_id
    actual_free_space = (
        os.statvfs(storage_path).f_bavail * os.statvfs(storage_path).f_bsize
    )
    assert (
        math.fabs(int(cache.free_space) - (actual_free_space - 1024 * 1024 * 1024))
        < 4096
    )  # Should match within 4kB

    object_promise = SomeHeavyComputation(
        arg1_important_arg="test1",
        arg2_compute_time=dt.timedelta(seconds=5),
        arg3_result_size=128,
    )

    cache.get_object(object_promise)
    cache_item = cache.get_object_info(object_promise.get_item_key())
    assert cache_item is not None
    assert cache_item.utility > 0
    assert cache_item.exists
    assert cache.storage.does_item_exists(cache_item.main_item_storage_key)

    mock_object_promise = SomeHeavyComputation(
        arg1_important_arg="test2",
        arg2_compute_time=dt.timedelta(seconds=0),
        arg3_result_size=1024 * 1024,
    )
    cache.print_contents()
    cache.get_object(mock_object_promise)
    cache_item = cache.get_object_info(mock_object_promise.get_item_key())
    assert cache_item is not None
    assert cache_item.utility < 0
    assert not cache.storage.does_item_exists(cache_item.main_item_storage_key)
    assert not cache_item.exists
