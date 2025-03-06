from CacheManager import (
    generate_mock_cache_Path,
    ModelCacheManagerConfig,
    produce_mock_result,
)
from pathlib import Path
import datetime as dt
import numpy as np


def bytes_array(arr_length: int) -> bytes:
    return bytes([0] * arr_length)


def test1():
    db_path = Path(__file__).parent / "test1.db"

    if db_path.exists():
        db_path.unlink()

    initial_config = ModelCacheManagerConfig()
    cache, storage = generate_mock_cache_Path(
        db_path, 10**8, initial_config=initial_config
    )
    assert cache.free_space == 10**8

    mock_object_promise = produce_mock_result(
        compute_time=dt.timedelta(seconds=5), result_size=500 * 1024
    )
    cache.get_object(mock_object_promise)
    cache_item = cache.get_object_info(mock_object_promise.get_item_key())
    assert cache_item.utility > 0
    assert cache_item.exists
    assert storage.does_item_exists(cache_item.item_storage_key)

    mock_object_promise = produce_mock_result(
        compute_time=dt.timedelta(seconds=0), result_size=500 * 1024 * 1024 * 1024
    )
    cache.print_contents()
    cache.get_object(mock_object_promise)
    cache_item = cache.get_object_info(mock_object_promise.get_item_key())
    assert cache_item.utility < 0
    assert not storage.does_item_exists(cache_item.item_storage_key)
    assert not cache_item.exists

    cache.close()
    db_path.unlink()


def test2():
    db_path = Path(__file__).parent / "test2.db"

    if db_path.exists():
        db_path.unlink()
    initial_config = ModelCacheManagerConfig()
    cache, storage = generate_mock_cache_Path(
        db_path, 10**8, initial_config=initial_config
    )

    # Set seed=123
    np.random.seed(123)
    # Generate 1000 random pairs of object size and time - both from exponential distribution
    for i in range(1001):
        size = np.random.exponential(10 * 1024 * 1024)
        time = np.random.exponential(0.5 * 60)
        object_promise = produce_mock_result(
            compute_time=dt.timedelta(seconds=time), result_size=size
        )

        # object = bytes_array(int(i))
        # obj_path = Path(__file__)
        item_was_stored: bool = False
        item = cache.get_object_info(object_promise.get_item_key())
        if item is None:
            cache.get_object(object_promise, verbose=True)
            item = cache.get_object_info(object_promise.get_item_key())
            print(repr(cache))
            if item.exists:
                item_was_stored = True

        if item_was_stored:
            cache.prune_cache(verbose=True)
        if i % 100 == 0:
            cache.print_contents()

    print("Stored objects:")
    for item in storage.stored_objects:
        item_key = item.hash
        cached_item = cache.get_object_info(item_key)
        print(
            f"Object of size {cached_item.pretty_size} and time {cached_item.pretty_compute_time}"
        )


if __name__ == "__main__":
    test1()
    print("test1() passed")
    test2()
    print("test2() passed")
