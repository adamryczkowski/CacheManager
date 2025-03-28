from EntityHash import calc_hash, EntityHash

from .abstract_cache_manager import AbstractCacheManager
from .file_cache import (
    generate_file_cache_view,
    generate_file_cache,
    StorageKeyGenerator_Path,
)
from .ifaces import (
    I_AbstractItemID,
    DC_CacheItem,
    I_CacheStorageRead,
    I_StorageKeyGenerator,
    I_PersistentDB,
    ModelCacheOptionName,
    StoredItemID,
    I_UtilityOfStoredItem,
)
from .mock_cache import (
    generate_mock_cache_Path,
    generate_mock_cache_view,
    MockItemProducer,
    produce_mock_result,
)
from .object_cache import ObjectCache, I_ItemProducer  # , ItemProducer
from .abstract_cache_manager import CacheItem
from .pretty_path import shorten_path
from .serialization_json import I_JSONItemPromise, json_wrap_promise
from .serialization_pickle import I_PickledItemPromise, pickle_wrap_promise
from .sqlite_settings_manager import SQLitePersistentDB
from .item_utility import ItemUtility

__all__ = [
    "shorten_path",
    "I_AbstractItemID",
    "DC_CacheItem",
    "I_CacheStorageRead",
    "I_StorageKeyGenerator",
    "I_PersistentDB",
    "ModelCacheOptionName",
    "SQLitePersistentDB",
    "AbstractCacheManager",
    "I_ItemProducer",
    "MockItemProducer",
    "ObjectCache",
    "produce_mock_result",
    "generate_file_cache_view",
    "generate_file_cache",
    "generate_mock_cache_Path",
    "generate_mock_cache_view",
    "StorageKeyGenerator_Path",
    "I_JSONItemPromise",
    "json_wrap_promise",
    "I_PickledItemPromise",
    "pickle_wrap_promise",
    "calc_hash",
    "EntityHash",
    "CacheItem",
    "StoredItemID",
    "I_UtilityOfStoredItem",
    "ItemUtility",
]
