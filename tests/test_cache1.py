from cachemanager import ObjectCache
from pathlib import Path
import os
from math import isclose
from entityhash import calc_hash

# from random import seed, random
import numpy as np


def bytes_array(arr_length: int) -> bytes:
    return bytes([0] * arr_length)


def test1():
    if os.path.exists(Path(__file__).parent / ".metadata.sqlite"):
        os.remove(Path(__file__).parent / ".metadata.sqlite")
    cache = ObjectCache.MockCache(0.1, Path(__file__).parent, reserved_free_space=0)
    assert cache.free_space == 0.1
    object_size = 0.005
    # obj = int(42).to_bytes()
    obj = Path(__file__)
    item = cache.store_object(
        obj,
        obj_hash=calc_hash(int(42).to_bytes()),
        compute_time=0.5,
        object_size=object_size,
    )
    cache._impl.store_file(obj, None, object_size)
    print(f"util={item.utility}")
    util = cache.calculate_items_utility(item, item_exists=True)
    print(f"util={util}")
    assert isclose(util, 3.9472922, abs_tol=0.001)


def test2():
    if os.path.exists(Path(__file__).parent / ".metadata.sqlite"):
        os.remove(Path(__file__).parent / ".metadata.sqlite")
    cache = ObjectCache.MockCache(0.1, Path(__file__).parent, reserved_free_space=0)
    assert cache.free_space == 0.1

    # Set seed=123
    np.random.seed(123)
    # Generate 1000 random pairs of object size and time - both from exponential distribution
    for i in range(1000):
        size = np.random.exponential(0.01)
        time = np.random.exponential(0.1)
        object = bytes_array(int(i))
        obj_path = Path(__file__)
        item = cache.store_object(obj_path, calc_hash(object), time, object_size=size)
        if item.utility >= 0:
            cache._impl.store_file(Path(calc_hash(object).as_hex), None, size)
            cache.prune_cache(remove_metadata=True, verbose=True)
        else:
            print(
                f"Failed to store object of size {item.pretty_size} and time {item.pretty_compute_time}"
            )
            continue
        print(
            f"Stored object of size {item.pretty_size} and time {item.pretty_compute_time}. Free space: {cache.pretty_free_space}"
        )

    print("Stored objects:")
    for item in cache.cached_objects:
        print(f"Object of size {item.pretty_size} and time {item.pretty_compute_time}")


if __name__ == "__main__":
    test1()
    print("test1() passed")
    test2()
    print("test2() passed")
