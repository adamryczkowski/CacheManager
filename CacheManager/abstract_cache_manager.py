from __future__ import annotations

import datetime as dt
import heapq
from typing import Iterator, Optional

from EntityHash import EntityHash
from pydantic import PositiveFloat

from .ifaces import (
    ModelCacheManagerConfig,
    I_PersistentDB,
    I_CacheStorageRead,
    DC_CacheItem,
    DC_StoredItem,
    ItemID,
)


class CacheItem(DC_CacheItem):
    _cache_manager: AbstractCacheManager

    def __init__(
        self,
        *,
        item_key: EntityHash,
        compute_time: dt.timedelta,
        weight: PositiveFloat,
        stored_items: dict[ItemID, DC_StoredItem],
        cache_manager: AbstractCacheManager,
    ):
        assert isinstance(cache_manager, AbstractCacheManager)
        super().__init__(
            item_key=item_key,
            compute_time=compute_time,
            weight=weight,
            stored_items=stored_items,
        )
        self._cache_manager = cache_manager

    @property
    def utility(self) -> float:
        return self._cache_manager.calculate_net_utility_of_item(self, self.exists)

    def verify_hash(self) -> bool:
        # noinspection PyBroadException
        stored_item: DC_StoredItem
        for stored_item in self.stored_items.values():
            if not self._cache_manager.storage.does_item_exists(
                stored_item.item_store_key
            ):
                raise ValueError(
                    f"Item {stored_item.pretty_store_key} does not exist in the cache."
                )
            if item_store_hash := stored_item.hash is not None:
                try:
                    hash_on_disk = self._cache_manager.storage.calculate_hash(
                        stored_item.item_store_key
                    )
                    if item_store_hash != hash_on_disk:
                        return False
                except Exception as _:  # pylint: disable=broad-exception-caught
                    return False
        return True

    @property
    def exists(self) -> bool:
        key: ItemID
        for key in self.stored_items.keys():
            if not self._cache_manager.storage.does_item_exists(key):
                return False
        return True

    def add_access_to_object(self, when: dt.datetime = dt.datetime.now()):
        self._cache_manager.add_access_to_item(self.item_key, when)

    def __lt__(self, other: CacheItem) -> bool:
        return self.utility < other.utility

    @property
    def age(self) -> float:
        """Age of the last access in hours"""
        if (last_access_time := self.last_access_time) is None:
            raise RuntimeError(f"Item {self.pretty_key} has never been accessed")
        return (dt.datetime.now() - last_access_time).total_seconds() / 60 / 60

    def get_history_of_accesses(self) -> list[dt.datetime]:
        return self._cache_manager.metadata_database.get_accesses(self.item_key)

    @property
    def last_access_time(self) -> Optional[dt.datetime]:
        return self._cache_manager.metadata_database.get_last_access(self.item_key)


class AbstractCacheManager:
    """
    Class that manages persistence of items in the abstract cache.

    The class is aware of the time it takes for the computation, and caches only if the cost of storing the object is less than the cost of re-computing it.

    There's an option to set a custom cost multiplier for the task in order to retain the specific cache items for longer or shorter periods of time.

    The class does not know about the actual backing storage. All it has is an abstract "filename" concept, which is a unique identifier
    to the cached entity in the storage (presumably a file path).

    Cost of the disk space is computed relatively to the free space on the disk using an exponential decay function with a custom coefficient.

    Warning: cache item utility function can be modified, but the current prunning algorithm requires
    that utility of each item does not decrease with increase of net free space available.
    I.e. removing one item has only positive impact on all the other items stored in the cache.
    Otherwise, prunning algorithm will be of sudoku complexity.
    """

    _db: I_PersistentDB
    _storage: I_CacheStorageRead

    def __init__(self, db: I_PersistentDB, storage: I_CacheStorageRead):
        assert db is not None
        assert isinstance(db, I_PersistentDB)
        assert storage is not None
        assert isinstance(storage, I_CacheStorageRead)

        self._db = db
        self._storage = storage

    @property
    def metadata_database(self) -> I_PersistentDB:
        return self._db

    @property
    def free_space(self) -> float:
        """Returns free space in GB"""
        return self._storage.free_space

    @property
    def config(self) -> ModelCacheManagerConfig:
        return self._db.config

    @property
    def storage(self) -> I_CacheStorageRead:
        return self._storage

    def _calculate_disk_cost_of_new_item(
        self, size: float, existing: bool = False
    ) -> float:
        """
        Calculate the cost of storing the object in the cache.
        """
        size /= 1024 * 1024 * 1024  # Convert to GB
        free_space = self.free_space - self.config.reserved_free_space
        free_space /= 1024 * 1024 * 1024  # Convert to GB
        if existing:
            if free_space < 0:
                return -float("inf")
            utility_before = self._calculate_utility_of_free_space(free_space + size)
            utility_after = self._calculate_utility_of_free_space(free_space)
        else:
            if free_space < size:
                return -float("inf")
            utility_before = self._calculate_utility_of_free_space(free_space)
            utility_after = self._calculate_utility_of_free_space(free_space - size)
        return utility_before - utility_after

    def _calculate_utility_of_free_space(self, free_space: float) -> float:
        """
        Calculate the utility of the free space (measured in GB).
        """
        return self.config.utility_of_1GB_free_space * free_space ** (
            -self.config.marginal_relative_utility_at_1GB
        )

    def _calculate_decay_weight(self, age: float) -> float:
        """
        Calculate the weight of the object based on the age of the object in hours.
        """
        return 2 ** (-age / self.config.half_life_of_cache)

    def calculate_net_utility_of_item(
        self, item: DC_CacheItem, existing: bool = False
    ) -> float:
        """
        :param item: item to calculate utility for
        :param existing: True - calculate utility in the context of prunning. False - calculate utility in the context of storing new item.
        Calculate the net utility of storing the object in the cache.
        Size is in GB, time in minutes.
        If the utility is negative, the object should not be stored in the cache.

        If item does not exist, it would make sense to estimate the expected utility of the decay.
        """
        last_access_time = self._db.get_last_access(item.item_key)
        if last_access_time is None:
            last_access_time = dt.datetime.now()
        item_age = (dt.datetime.now() - last_access_time).total_seconds() / 60.0

        positive_utility = (
            item.compute_time.total_seconds()
            / 60
            / self.config.cost_of_minute_compute_rel_to_cost_of_1GB
            * item.weight
            * self._calculate_decay_weight(item_age)
        )
        negative_cost = self._calculate_disk_cost_of_new_item(
            item.filesize, existing=existing
        )
        utility = positive_utility + negative_cost
        return utility

    def iterate_cache_items(self, OnlyExisting: bool = True) -> Iterator[CacheItem]:
        for db_item in self._db.iterate_items():
            item = self._enrich_cacheitem(db_item)
            if item.exists or not OnlyExisting:
                yield item

    def prunning_iterator(self, remove_metadata: bool = False) -> Iterator[CacheItem]:
        """
        Iterates over all the item that needs to be removed.
        :param remove_metadata: removes item's metadata, incl. access times.
        :return:
        """
        to_delete: list[CacheItem] = []
        for item in self.iterate_cache_items():
            if item.exists and item.utility < 0:
                heapq.heappush(to_delete, item)

        if len(to_delete) == 0:
            return

        while len(to_delete) > 0:
            item = heapq.heappop(to_delete)
            if item.utility >= 0:
                break
            yield item
            if remove_metadata:
                self._db.remove_item(item.item_key)

    def update_item(self, new_item: DC_CacheItem):
        """Replace already existing cache item. Basically ensure the properties hold"""
        old_item = self.get_item_by_key(new_item.item_key)
        if old_item is None:
            raise KeyError(f"Item {new_item.item_key} does not exist")

        if new_item.stored_items != old_item.stored_items:
            raise ValueError(
                f"Two cache items have different storage keys: {old_item.pretty_storage_keys} vs {new_item.pretty_storage_keys}. Cannot update in this case out of caution."
            )

        if old_item.item_hash != new_item.item_hash:
            raise ValueError(f"Item {new_item.item_key} has different hash")

        compute_time = max(
            old_item.compute_time, new_item.compute_time
        )  # The most pessimistic measure
        weight = new_item.weight
        self._db.remove_item(old_item.item_key, remove_history=False)
        item = DC_CacheItem(
            item_key=old_item.item_key,
            stored_items=old_item.stored_items,
            compute_time=compute_time,
            weight=weight,
        )
        self._db.add_item(item)
        self._db.add_access_to_item(item.item_key, dt.datetime.now())
        self._db.commit()

    def add_item_unconditionally(self, item: DC_CacheItem) -> CacheItem:
        """
        Store the object in the cache without questioning its utility.
        """
        item_key = item.item_key
        if self._db.get_item_by_key(item_key) is not None:
            raise ValueError(
                f"Object with hash {item_key} is already in the cache. Remove the old one first."
            )

        # Put the object into db
        self._db.add_item(item)

        # Serialize the object
        self._db.add_access_to_item(item_key, dt.datetime.now())

        self._db.commit()

        return self._enrich_cacheitem(item)

    def add_access_to_item(
        self, item_key: EntityHash, when: dt.datetime = dt.datetime.now()
    ):
        self._db.add_access_to_item(item_key, when)
        self._db.commit()

    def _enrich_cacheitem(self, item: DC_CacheItem) -> CacheItem:
        ans_item = CacheItem(
            item_key=item.item_key,
            stored_items=item.stored_items,
            compute_time=item.compute_time,
            weight=item.weight,
            cache_manager=self,
        )
        return ans_item

    def get_item_by_key(self, item_key: EntityHash) -> Optional[CacheItem]:
        item = self._db.get_item_by_key(item_key)
        if item is None:
            return None
        return self._enrich_cacheitem(item)

    def remove_item(self, item_key: EntityHash, remove_history: bool) -> bool:
        return self._db.remove_item(item_key, remove_history=remove_history)

    def make_Item(
        self,
        *,
        item_key: EntityHash,
        compute_time: dt.timedelta,
        weight: PositiveFloat,
        stored_items: dict[ItemID, DC_StoredItem],
    ) -> CacheItem:
        return CacheItem(
            cache_manager=self,
            item_key=item_key,
            stored_items=stored_items,
            compute_time=compute_time,
            weight=weight,
        )

    def close(self):
        self._db.close()
        self._storage.close()
