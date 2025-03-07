import datetime as dt
import math
import os
import numpy as np
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from EntityHash import calc_hash

from CacheManager import (
    generate_file_cache,
    ModelCacheManagerConfig,
    StorageKeyGenerator_Path,
    pickle_wrap_promise,
    I_ItemProducer,
)
import time


class SomeHeavyResult:
    _data: bytes

    def __init__(self, object_size: int):
        # Array of random bytes of size 'object_size'
        self._data = np.random.bytes(object_size)
        assert isinstance(self._data, bytes)


def some_heavy_computation(
    arg1_important_arg: str, arg2_compute_time: dt.timedelta, arg3_result_size: int
) -> SomeHeavyResult:
    time.sleep(arg2_compute_time.total_seconds())
    return SomeHeavyResult(arg3_result_size)


def wrapped_heavy_computation(
    arg1_important_arg: str, arg2_compute_time: dt.timedelta, arg3_result_size: int
) -> I_ItemProducer:
    args = {
        "arg1_important_arg": arg1_important_arg,
        "arg2_compute_time": arg2_compute_time,
        "arg3_result_size": arg3_result_size,
    }
    item_key = calc_hash(args)
    # noinspection PyTypeChecker
    return pickle_wrap_promise(item_key, some_heavy_computation, **args)


@pytest.fixture
def cache():
    db_path = Path(__file__).parent / "test_real.db"
    storage_path = TemporaryDirectory()

    if db_path.exists():
        db_path.unlink()

    storage_file_naming_settings = StorageKeyGenerator_Path(
        file_prefix="model_", file_extension="pickle"
    )
    initial_config = ModelCacheManagerConfig()
    initial_config.reserved_free_space = 1024 * 1024 * 1024  # 1 GB set aside
    initial_config.cost_of_minute_compute_rel_to_cost_of_1GB = 1000
    cache = generate_file_cache(
        cached_dir=Path(storage_path.name),
        initial_config=initial_config,
        storage_key_generator=storage_file_naming_settings,
        db_filename=str(db_path),
        calculate_hash=True,
    )
    yield cache
    cache.remove_all_cached_items()
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

    object_promise = wrapped_heavy_computation(
        arg1_important_arg="test1",
        arg2_compute_time=dt.timedelta(seconds=5),
        arg3_result_size=128,
    )

    cache.get_object(object_promise)
    cache_item = cache.get_object_info(object_promise.get_item_key())
    assert cache_item.utility > 0
    assert cache_item.exists
    assert cache.storage.does_item_exists(cache_item.item_storage_key)

    mock_object_promise = wrapped_heavy_computation(
        arg1_important_arg="test2",
        arg2_compute_time=dt.timedelta(seconds=0),
        arg3_result_size=1024 * 1024,
    )
    cache.print_contents()
    cache.get_object(mock_object_promise)
    cache_item = cache.get_object_info(mock_object_promise.get_item_key())
    assert cache_item.utility < 0
    assert not cache.storage.does_item_exists(cache_item.item_storage_key)
    assert not cache_item.exists
