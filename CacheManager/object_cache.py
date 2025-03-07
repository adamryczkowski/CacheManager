from __future__ import annotations

import datetime as dt
import heapq
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

from EntityHash import EntityHash
from humanize import naturalsize

from .abstract_cache_manager import AbstractCacheManager
from .abstract_cache_manager import CacheItem
from .ifaces import I_AbstractItemID, I_CacheStorageModify, I_StorageKeyGenerator

# class ItemProducer(Protocol):
#     def get_item_key(self) -> EntityHash: ...
#
#     def compute_item(self) -> Any: ...
#
#     def instantiate_item(self, data: bytes) -> Any: ...
#
#     @staticmethod
#     def serialize_item(item: Any) -> bytes: ...


class I_ItemProducer(ABC):
    """
    Object that is intended as a wrapper around a function that produces an object of interest.
    It is expected that user will provide an initializer that takes all the data required to generate the result.

    This object is treated as ephemeral and is not stored anywhere by the cache system. It is expected to be tied to a single call to the cache system.
    """

    @abstractmethod
    def get_item_key(self) -> EntityHash: ...

    @abstractmethod
    def compute_item(self) -> Any: ...

    @abstractmethod
    def instantiate_item(self, data: bytes) -> Any: ...

    @abstractmethod
    def serialize_item(item: Any) -> bytes: ...


class I_MockItemProducer(I_ItemProducer):
    @property
    @abstractmethod
    def compute_time(self) -> dt.timedelta: ...


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

    @property
    def free_space(self) -> float:
        return self._storage.free_space - self._cache_manager.config.reserved_free_space

    def __call__(self, object_factory: I_ItemProducer) -> Any:
        """
        Returns what would be a result of `compute_item` - either from calling it, or from cache
        :param object_factory:
        :return:
        """
        return self.get_object(object_factory)

    def get_object(
        self,
        object_factory: I_ItemProducer,
        weight: float = 1.0,
        verify: bool = False,
        verbose: bool = False,
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
        if isinstance(object_factory, I_MockItemProducer):
            time2 = time1 + object_factory.compute_time
        else:
            time2 = (
                dt.datetime.now()
            )  # The end of not-predictable part of the computation

        storage_key = self._storage_key_generator.generate_item_storage_key(item_key)
        if item is not None:
            # the cache has seen this item before. It can either be in the cache, or the item has been pruned.
            # Anyway, there is no point in calculating the hash again.
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
                if verbose:
                    print(
                        f"Item {new_item.pretty_description} existed before, and has negative utility, not storing it."
                    )
                return object  # Negative utility, no point in storing it.
        else:
            new_item = None

        object_bytes = object_factory.serialize_item(object)
        if isinstance(object_factory, I_MockItemProducer):
            object_size = len(object)
        else:
            object_size = len(object_bytes)

        if new_item is None:
            # the cache has not seen this item before.
            new_item = self._cache_manager.make_Item(
                item_key=item_key,
                item_storage_key=storage_key,
                hash=None,
                compute_time=time2 - time1,
                filesize=object_size,
                weight=weight,
            )
            if (
                new_item.utility < 0
            ):  # We are not going to store this item, and we are not going to bother with the hash.
                # We will store the entry so we can track its usage.
                self._cache_manager.add_item_unconditionally(new_item)
                if verbose:
                    print(
                        f"Item {new_item.pretty_description} has negative utility, not bothering doing anything with it."
                    )
                return object

        # At this point we don't care about the old item anymore. We are going to store or update the new one.
        self._storage.save_item(
            object_bytes,
            item_storage_key=self._storage.make_absolute_item_storage_key(storage_key),
        )
        if new_item.hash is None and self._calculate_hash:
            new_item.hash = self._storage.calculate_hash(storage_key)

        if item is None:
            self._cache_manager.add_item_unconditionally(new_item)
        else:
            self._cache_manager.update_item(new_item)

        if verbose:
            print(
                f"Item {new_item.pretty_description} has positive utility and was stored in cache"
            )
        return object

    def get_object_info(self, item_key: EntityHash) -> Optional[CacheItem]:
        return self._cache_manager.get_item_by_key(item_key)

    def remove_all_cached_items(self, remove_history: bool = False):
        for item in self._cache_manager.iterate_cache_items():
            assert item.exists
            if not self._storage.remove_item(item.item_storage_key):
                raise ResourceWarning(
                    f"Cannot remove item {item.item_storage_key} from cache"
                )
            self._cache_manager.remove_item(
                item.item_key, remove_history=remove_history
            )

    def prune_cache(self, remove_history: bool = False, verbose: bool = False):
        for item in self._cache_manager.prunning_iterator():
            assert item.exists
            if verbose:
                print(f"Removing item {item.pretty_description} from cache")
            if not self._storage.remove_item(item.item_storage_key):
                raise ResourceWarning(
                    f"Cannot remove item {item.item_storage_key} from cache"
                )
            self._cache_manager.remove_item(
                item.item_key, remove_history=remove_history
            )

    def print_contents(self):
        item_list: list[CacheItem[ItemID]] = []
        item: CacheItem[ItemID]
        for item in self._cache_manager.iterate_cache_items():
            heapq.heappush(item_list, item)

        for i in range(len(item_list)):
            item = heapq.heappop(item_list)
            print(f"Util={item.utility:.3f}, {item.pretty_description}")

    def size_of_all_elements(
        self, count_existing: Optional[bool] = None
    ) -> tuple[float, int]:
        size = 0
        count = 0
        item: CacheItem[ItemID]
        for item in self._cache_manager.iterate_cache_items(False):
            if count_existing is None or (count_existing == item.exists):
                size += int(item.filesize)
                count += 1

        return float(size), count

    def __repr__(self) -> str:
        size_of_all, count_all = self.size_of_all_elements()
        size_of_incache, count_incache = self.size_of_all_elements(True)
        size_of_rejected, count_rejected = self.size_of_all_elements(False)
        ans = f"Cache of size {naturalsize(self.free_space + size_of_incache)}, {size_of_incache/(self.free_space + size_of_incache):.2%} full. {count_incache} elements stored out of {count_all} total items seen. {naturalsize(size_of_rejected)} of items were rejected to cache."
        return ans

    @property
    def storage(self) -> I_CacheStorageModify[ItemID]:
        return self._storage

    def close(self):
        self._cache_manager.close()
        self._storage.close()
