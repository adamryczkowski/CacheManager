from CacheManager import SQLitePersistentDB, DC_CacheItem
from pathlib import Path
from EntityHash import calc_hash
import datetime as dt


def test_connection():
    db_path = Path(__file__).parent / "test_mock_cache.db"

    if db_path.exists():
        db_path.unlink()

    man = SQLitePersistentDB[Path](db_path)
    assert man.is_ItemID_Path()
    man.clear_items()
    some_hash = calc_hash("some item content")
    cacheitem = DC_CacheItem[Path](
        hash=some_hash,
        item_key=some_hash,
        item_storage_key=Path("my_item.bin"),
        compute_time=dt.timedelta(seconds=123),
        filesize=0.01,
        weight=1,
    )
    man.add_item(cacheitem)
    man.add_access_to_item(cacheitem.item_key, dt.datetime.now())
    man.commit()
    cacheitem2 = man.get_item_by_key(some_hash)
    assert cacheitem2 is not None
    assert cacheitem2.item_key == some_hash
    assert cacheitem2.item_storage_key.name == Path("my_item.bin").name

    assert man.config.cost_of_minute_compute_rel_to_cost_of_1GB == 0.1
    assert man.config.reserved_free_space == 0.0
    config = man.config
    config.reserved_free_space = 1.0
    man.store_config(config)
    man.commit()
    assert man.config.reserved_free_space == 1.0
    man.close()


if __name__ == "__main__":
    test_connection()
    print("test passed")
