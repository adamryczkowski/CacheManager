from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterator

from entityhash import EntityHash
from humanize import naturalsize

from .cache_item import CacheItem
from .cache_manager import ModelCacheManagerImpl, MockModelCacheManagerImpl


class ObjectCache:
    _impl: ModelCacheManagerImpl

    @staticmethod
    def MockCache(
        free_space: float,
        cache_dir: Path,
        half_life_of_cache: float = 24.0,  # in hours
        utility_of_1GB_free_space: float = 2.0,
        marginal_relative_utility_at_1GB: float = 1.0,
        cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1,
        reserved_free_space: float = 1.0,
        object_file_extension: str = "bin",
    ) -> ObjectCache:
        impl = MockModelCacheManagerImpl(
            free_space=free_space,
            db_directory=cache_dir,
            half_life_of_cache=half_life_of_cache,
            utility_of_1GB_free_space=utility_of_1GB_free_space,
            marginal_relative_utility_at_1GB=marginal_relative_utility_at_1GB,
            cost_of_minute_compute_rel_to_cost_of_1GB=cost_of_minute_compute_rel_to_cost_of_1GB,
            reserved_free_space=reserved_free_space,
            object_file_extension=object_file_extension,
        )
        return ObjectCache(impl)

    @staticmethod
    def InitCache(
        cache_dir: Path,
        half_life_of_cache: float = 24.0,  # in hours
        utility_of_1GB_free_space: float = 2.0,
        marginal_relative_utility_at_1GB: float = 1.0,
        cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1,
        reserved_free_space: float = 1.0,
        object_file_extension: str = "bin",
    ) -> ObjectCache:
        impl = ModelCacheManagerImpl(
            cache_dir=cache_dir,
            half_life_of_cache=half_life_of_cache,
            utility_of_1GB_free_space=utility_of_1GB_free_space,
            marginal_relative_utility_at_1GB=marginal_relative_utility_at_1GB,
            cost_of_minute_compute_rel_to_cost_of_1GB=cost_of_minute_compute_rel_to_cost_of_1GB,
            reserved_free_space=reserved_free_space,
            object_file_extension=object_file_extension,
        )
        return ObjectCache(impl)

    def __init__(self, impl: ModelCacheManagerImpl):
        assert isinstance(impl, ModelCacheManagerImpl)
        self._impl = impl

    def is_object_in_cache(self, obj_hash: EntityHash) -> bool:
        return self._impl.is_object_in_cache(obj_hash)

    def get_item_filename(self, obj_hash: EntityHash) -> Path:
        return CacheItem.FilenameFromHash(
            obj_hash,
            self._impl.cache_dir,
            self._impl._settings.object_file_extension,
            self._impl._settings.filename_prefix,
        )

    def get_object_by_hash(self, obj_hash: EntityHash) -> Optional[CacheItem]:
        item = self._impl.get_object_by_hash(obj_hash)

        if item is None:
            return None

        self._impl.add_access_to_object(obj_hash)

        return item

    def remove_object(self, obj_hash: EntityHash, remove_access_history: bool = True):
        self._impl.remove_object(
            obj_hash=obj_hash, remove_access_history=remove_access_history
        )

    def prune_cache(self, remove_metadata: bool = False, verbose: bool = False):
        self._impl.prune_cache(remove_metadata=remove_metadata, verbose=verbose)

    def store_object(
        self,
        object: bytes,
        obj_hash: EntityHash,
        compute_time: float,
        weight: float = 1.0,
        object_size: float = None,
        force_store: bool = False,
    ) -> CacheItem:
        exists = self._impl.verify_object(obj_hash)
        item_filename = CacheItem.FilenameFromHash(
            obj_hash, self._impl.cache_dir, self._impl._settings.object_file_extension
        )
        if object_size is None:
            object_size = len(object) / 1024 / 1024 / 1024
        item = CacheItem(
            obj_hash, item_filename, compute_time, weight, size=object_size
        )

        self._impl.calculate_net_utility_of_object(item, existing=exists)
        if item.utility < 0 and not force_store:
            if exists:
                self._impl.remove_object(obj_hash, remove_access_history=False)
            return item
        else:
            if not exists:
                self._impl.store_object_unconditionally(object, item)
            return item

    def calculate_items_utility(
        self, item: CacheItem, item_exists: bool = None
    ) -> float:
        if item_exists is None:
            item_exists = self._impl.does_object_exist(item)
        self._impl.calculate_net_utility_of_object(item, existing=item_exists)
        return item.utility

    @property
    def free_space(self) -> float:
        return self._impl.free_space

    @property
    def pretty_free_space(self) -> str:
        return f"{naturalsize(self.free_space * 1024 * 1024 * 1024)}"

    @property
    def cached_objects(self) -> Iterator[CacheItem]:
        return self._impl.iterate_cache_items()
