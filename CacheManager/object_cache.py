from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Protocol, Any

from EntityHash import EntityHash

from .abstract_cache_manager import AbstractCacheManager
from .ifaces import I_AbstractItemID, I_CacheStorageModify, I_StorageKeyGenerator


class ItemProducer(Protocol):
    def get_item_key(self) -> EntityHash: ...

    def compute_item(self) -> Any: ...

    def instantiate_item(self, data: bytes) -> Any: ...

    @staticmethod
    def serialize_item(item: Any) -> bytes: ...


class ObjectCache[ItemID: (Path, I_AbstractItemID)]:
    """
    This cache object is ready for use. It only requires a user-friendly factory function that will create this object
    """

    _cache_manager: AbstractCacheManager[ItemID]
    _storage: I_CacheStorageModify[ItemID]
    _storage_key_generator: I_StorageKeyGenerator
    _calculate_hash: bool

    def __init__(
        self,
        storage: I_CacheStorageModify[ItemID],
        cache_manager: AbstractCacheManager[ItemID],
        storage_key_generator: I_StorageKeyGenerator,
        calculate_hash: bool,
    ):
        assert isinstance(cache_manager, AbstractCacheManager)
        assert isinstance(storage, I_CacheStorageModify)
        assert isinstance(storage_key_generator, I_StorageKeyGenerator)
        assert isinstance(calculate_hash, bool)
        self._cache_manager = cache_manager
        self._storage = storage
        self._storage_key_generator = storage_key_generator
        self._calculate_hash = calculate_hash

    @property
    def calculate_hash(self) -> bool:
        return self._calculate_hash

    @calculate_hash.setter
    def calculate_hash(self, value: bool):
        self._calculate_hash = value

    def __call__(self, object_factory: ItemProducer) -> Any:
        """
        Returns what would be a result of `compute_item` - either from calling it, or from cache
        :param object_factory:
        :return:
        """
        return self.get_object(object_factory)

    def get_object(
        self, object_factory: ItemProducer, weight: float = 1.0, verify: bool = False
    ) -> Any:
        item_key = object_factory.get_item_key()
        item = self._cache_manager.get_item_by_key(item_key)
        if item is not None:
            if item.exists:
                if verify:
                    if not item.compute_item():
                        raise ValueError("Item verification failed.")
                self._cache_manager.add_access_to_item(item.item_key)
                object_bytes = self._storage.load_item(item.item_storage_key)
                return object_factory.instantiate_item(object_bytes)

        time1 = dt.datetime.now()
        object = object_factory.compute_item()
        time2 = dt.datetime.now()  # The end of not-predictable part of the computation

        storage_key = self._storage_key_generator.generate_item_storage_key(item_key)
        if item is not None:
            new_item = self._cache_manager.make_Item(
                item_key=item_key,
                item_storage_key=storage_key,
                hash=item.hash,
                compute_time=time2 - time1,
                filesize=item.filesize,
                weight=weight,
            )
            if new_item.utility < 0:
                self._cache_manager.update_item(new_item)
                return object  # No point in storing it
        else:
            new_item = None

        object_bytes = object_factory.serialize_item(object)

        if item is None:
            new_item = self._cache_manager.make_Item(
                item_key=item_key,
                item_storage_key=storage_key,
                hash=None,
                compute_time=time2 - time1,
                filesize=len(object_bytes),
                weight=weight,
            )
            if new_item.utility < 0:
                self._cache_manager.add_item_unconditionally(new_item)
                return object

        self._storage.save_item(object_bytes, item_storage_key=storage_key)
        if new_item.hash is None and self._calculate_hash:
            new_item.hash = self._storage.calculate_hash(storage_key)

        if item is None:
            self._cache_manager.add_item_unconditionally(new_item)
        else:
            self._cache_manager.update_item(new_item)

        return object

    def prune_cache(self, remove_history: bool = False, verbose: bool = False):
        for item in self._cache_manager.prunning_iterator():
            assert item.exists
            if verbose:
                print(f"Removing item {item.item_key} from cache")
            if not self._storage.remove_item(item.item_storage_key):
                raise ResourceWarning(
                    f"Cannot remove item {item.item_storage_key} from cache"
                )
            self._cache_manager.remove_item(
                item.item_key, remove_history=remove_history
            )
