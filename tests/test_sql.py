from CacheManager import SettingsManager, DC_CacheItem
from pathlib import Path
from EntityHash import calc_hash
import datetime as dt


def test_connection():
    man = SettingsManager[Path](Path(__file__).parent / "test.db")
    assert man.is_ItemID_Path()
    man.clear_cacheitems()
    some_hash = calc_hash("some item content")
    cacheitem = DC_CacheItem[Path](
        hash=some_hash,
        filename=Path("my_item.bin"),
        compute_time=dt.timedelta(seconds=123),
        filesize=0.01,
        weight=1,
    )
    man.add_object(cacheitem)
    man.add_access_to_object(cacheitem.hash, dt.datetime.now())
    man.commit()
    cacheitem2 = man.get_object_by_hash(some_hash)
    assert cacheitem2 is not None
    assert cacheitem2.hash == some_hash
    assert cacheitem2.filename.name == Path("my_item.bin").name

    assert man.config.cost_of_minute_compute_rel_to_cost_of_1GB == 0.1
    assert man.config.reserved_free_space == 0.0
    config = man.config
    config.reserved_free_space = 1.0
    man.store_config(config)
    man.commit()
    assert man.config.reserved_free_space == 1.0


if __name__ == "__main__":
    test_connection()
    print("test passed")
