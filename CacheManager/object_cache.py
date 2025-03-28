from __future__ import annotations

import datetime as dt
import heapq
from typing import Any, Optional

from EntityHash import EntityHash, calc_hash
from humanize import naturalsize

from .abstract_cache_manager import AbstractCacheManager
from .abstract_cache_manager import CacheItem
from .ifaces import (
    StoredItemID,
    I_CacheStorageModify,
    I_StorageKeyGenerator,
    DC_StoredItem,
    I_ItemProducer,
    I_MockItemProducer,
)


class ObjectCache:
    """
    This cache object is ready for use. It only requires a user-friendly factory function that will create this object
    """

    _cache_manager: AbstractCacheManager
    _storage: I_CacheStorageModify
    _storage_key_generator: I_StorageKeyGenerator
    _calculate_hash: bool

    def __init__(
        self,
        storage: I_CacheStorageModify,
        cache_manager: AbstractCacheManager,
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
        return self._storage.free_space

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
        reuse_past_hash: bool = True,
        verbose: bool = False,
    ) -> Any:
        # The performance constraints:
        # 1. Computed item needs to be serialized to get the hash.
        #    Serialization takes time and should be avoided if past hash is available (by default `reuse_past_hash` is true)
        #
        # The algorithm:
        # 1. If the item is in the cache - return it from there directly after updating the access statistics and verifying it (if `verify` is set).
        # 2. Compute the item, clock the time.
        # 3. Calculate the hash: if the item _was_ in the cache or `reuse_past_hash` is off - retrieve and use the past hash information.
        #    Make sure all the extra external files storing state are accounted for.
        # 4. Calculate the utility of the item.
        # 5. Update the db about the item - if the item was in the cache, update just the access statistics, if not - add the item to metadata.
        # 6.
        item_key = object_factory.get_item_key()
        item = self._cache_manager.get_item_by_key(item_key)
        if item is not None:
            if item.exists:
                if verify:
                    actual_hash = item.calculate_hash(self._storage)
                    if not item.item_hash == actual_hash:
                        raise ValueError("Item verification failed.")
                self._cache_manager.add_access_to_item(item.item_key)
                object_bytes = self._storage.load_item(item.main_item_storage_key)
                return object_factory.instantiate_item(
                    object_bytes, item.non_main_stored_item_keys
                )

        # 2. Compute the item, clock the time.
        time1 = dt.datetime.now()
        object = object_factory.compute_item()

        if isinstance(object_factory, I_MockItemProducer):
            time2 = time1 + object_factory.compute_time
            main_item_hash = object.hash
        else:
            time2 = (
                dt.datetime.now()
            )  # The end of not-predictable part of the computation
            main_item_hash = calc_hash(object)

        # 3. Calculate the hash: if the item _was_ in the cache or `reuse_past_hash` is off - retrieve and use the past hash information
        if (storage_key := object_factory.propose_item_storage_key()) is None:
            storage_key = self._storage_key_generator.generate_item_storage_key(
                item_key
            )
        main_object = DC_StoredItem(
            filesize=len(object),
            item_store_key=storage_key,
            hash=main_item_hash,
            tag="",
        )

        try:
            extra_item_keys = object_factory.get_files_storing_state(self._storage)
        except NotImplementedError:
            extra_item_keys = {}

        extra_items: dict[StoredItemID, DC_StoredItem] = {}
        for tag, stored_item in extra_item_keys.items():
            extra_items[stored_item] = DC_StoredItem(
                item_store_key=stored_item,
                filesize=self._storage.item_size(stored_item),
                hash=self._storage.calculate_hash(stored_item),
                tag=tag,
            )
        extra_items[storage_key] = main_object

        if item is not None and not reuse_past_hash:
            # the cache has seen this item before. The item is sure to be missing from cache,
            # but it may still be worthwhile to store it this time.
            # If `reuse_past_hash` is False, we still have something to verify here.

            if set(list(extra_items.keys())) != set(list(item.stored_items.keys())):
                raise ValueError(
                    "The list of extra stored items have changed. This indicates that the cache generating function is not truly functional with respect to the extra files."
                )

            for key in extra_items.keys():
                if extra_items[key].hash != item.stored_items[key].hash:
                    raise ValueError(
                        f"The content of {key} has changed since the last time cache has seen it."
                    )
            new_item = self._cache_manager.make_Item(
                item_key=item_key,
                main_item_storage_key=storage_key,
                stored_items=extra_items,
                compute_time=time2 - time1,
                weight=weight,
                serialization_performance_class=object_factory.get_item_serialization_class(),
            )

            # 4. Calculate the utility of the item.
            if new_item.utility < 0:
                # We are going to deny caching this item, but we still need to update the access statistics for the item.
                self._cache_manager.update_item(
                    new_item
                )  # TODO: The item may contain completely bogus file keys. We need to check if this update is ok.
                if verbose:
                    print(
                        f"Item {new_item.pretty_description} existed before, and has negative utility, not storing it."
                    )
                return object  # Negative utility, no point in storing it.
        else:
            new_item = None

        # From this point on we are commited to storing the item.
        object_bytes = object_factory.serialize_item(object)

        if new_item is None:
            # the cache has not seen this item before.
            new_item = self._cache_manager.make_Item(
                item_key=item_key,
                compute_time=time2 - time1,
                weight=weight,
                serialization_performance_class=object_factory.get_item_serialization_class(),
                main_item_storage_key=storage_key,
                stored_items=extra_items,
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
        if len(extra_items) > 1:
            object_factory.protect_item()  # Don't delete the side files! We are going to refer to them in the cache!
        self._storage.save_item(
            object_bytes,
            item_storage_key=self._storage.make_absolute_item_storage_key(storage_key),
        )

        if item is None:
            self._cache_manager.add_item_unconditionally(new_item)
        else:
            self._cache_manager.update_item(new_item)

        if verbose:
            print(
                f"Item {new_item.pretty_description} has positive utility and was stored in cache"
            )
        actual_hash = new_item.calculate_hash(self._storage)

        return object

    def get_object_info(self, item_key: EntityHash) -> Optional[CacheItem]:
        return self._cache_manager.get_item_by_key(item_key)

    def remove_all_cached_items(self, remove_history: bool = False):
        for item in self._cache_manager.iterate_cache_items():
            assert item.exists
            for stored_item_key, stored_item in item.stored_items.items():
                if not self._storage.remove_item(stored_item_key):
                    raise ResourceWarning(
                        f"Cannot remove item {stored_item_key} from cache"
                    )
            self._cache_manager.remove_item(
                item.item_key, remove_history=remove_history
            )

    def prune_cache(self, remove_history: bool = False, verbose: bool = False):
        for item in self._cache_manager.prunning_iterator():
            assert item.exists
            if verbose:
                print(f"Removing item {item.pretty_description} from cache")
            for stored_item_key, stored_item in item.stored_items.items():
                if not self._storage.remove_item(stored_item_key):
                    raise ResourceWarning(
                        f"Cannot remove item {stored_item_key} from cache"
                    )
            self._cache_manager.remove_item(
                item.item_key, remove_history=remove_history
            )

    def print_contents(self):
        item_list: list[CacheItem] = []
        item: CacheItem
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
        item: CacheItem
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
    def storage(self) -> I_CacheStorageModify:
        return self._storage

    def close(self):
        self._cache_manager.close()
        self._storage.close()
