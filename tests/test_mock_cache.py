from CacheManager import (
    generate_mock_cache_Path,
    ModelCacheManagerConfig,
    produce_mock_result,
)
from pathlib import Path
import datetime as dt


def bytes_array(arr_length: int) -> bytes:
    return bytes([0] * arr_length)


def test1():
    db_path = Path(__file__).parent / "test.db"

    if db_path.exists():
        db_path.unlink()

    initial_config = ModelCacheManagerConfig()
    cache = generate_mock_cache_Path(db_path, 10**8, initial_config=initial_config)
    assert cache.free_space == 10**8

    mock_object_promise = produce_mock_result(
        compute_time=dt.timedelta(seconds=5), result_size=500 * 1024
    )
    ans = cache.get_object(mock_object_promise)
    print(ans)
    cache_item = cache.get_object_info(mock_object_promise.get_item_key())
    print(cache_item)


# def test2():
#     if os.path.exists(Path(__file__).parent / ".metadata.sqlite"):
#         os.remove(Path(__file__).parent / ".metadata.sqlite")
#     cache = ObjectCache.MockCache(0.1, Path(__file__).parent, reserved_free_space=0)
#     assert cache.free_space == 0.1
#
#     # Set seed=123
#     np.random.seed(123)
#     # Generate 1000 random pairs of object size and time - both from exponential distribution
#     for i in range(1000):
#         size = np.random.exponential(0.01)
#         time = np.random.exponential(0.1)
#         object = bytes_array(int(i))
#         obj_path = Path(__file__)
#         item = cache.store_object(
#             item_filename=obj_path,
#             obj_hash=calc_hash(object),
#             compute_time=time,
#             object_size=size,
#         )
#         if item.utility >= 0:
#             cache._impl.store_file(
#                 item_filename=Path(calc_hash(object).as_hex), obj_hash=None
#             )
#             cache.prune_cache(remove_history=True, verbose=True)
#         else:
#             print(
#                 f"Failed to store object of size {item.pretty_size} and time {item.pretty_compute_time}"
#             )
#             continue
#         print(
#             f"Stored object of size {item.pretty_size} and time {item.pretty_compute_time}. Free space: {cache.pretty_free_space}"
#         )
#
#     print("Stored objects:")
#     for item in cache.cached_objects:
#         print(f"Object of size {item.pretty_size} and time {item.pretty_compute_time}")
#

if __name__ == "__main__":
    test1()
    print("test1() passed")
    # test2()
    # print("test2() passed")
