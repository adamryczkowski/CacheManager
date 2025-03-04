from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterator

import EntityHash
from humanize import naturalsize

from .cache_item_old import CacheItem
from .cache_manager_old import ModelCacheManagerImpl, MockModelCacheManagerImpl


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
    ) -> ObjectCache:
        impl = MockModelCacheManagerImpl(
            free_space=free_space,
            db_directory=cache_dir,
            half_life_of_cache=half_life_of_cache,
            utility_of_1GB_free_space=utility_of_1GB_free_space,
            marginal_relative_utility_at_1GB=marginal_relative_utility_at_1GB,
            cost_of_minute_compute_rel_to_cost_of_1GB=cost_of_minute_compute_rel_to_cost_of_1GB,
            reserved_free_space=reserved_free_space,
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
    ) -> ObjectCache:
        impl = ModelCacheManagerImpl(
            cache_dir=cache_dir,
            half_life_of_cache=half_life_of_cache,
            utility_of_1GB_free_space=utility_of_1GB_free_space,
            marginal_relative_utility_at_1GB=marginal_relative_utility_at_1GB,
            cost_of_minute_compute_rel_to_cost_of_1GB=cost_of_minute_compute_rel_to_cost_of_1GB,
            reserved_free_space=reserved_free_space,
        )
        return ObjectCache(impl)

    @property
    def parent_dir(self) -> Path:
        return self._impl.cache_dir

    def __init__(self, impl: ModelCacheManagerImpl):
        assert isinstance(impl, ModelCacheManagerImpl)
        self._impl = impl

    def get_item_filename(
        self,
        obj_hash: EntityHash,
        subfolder: Path,
        file_prefix: str,
        file_extension: str,
    ) -> Path:
        return CacheItem.FilenameFromHash(
            obj_hash,
            self._impl.cache_dir,
            file_extension=file_extension,
            subfolder=subfolder,
            file_prefix=file_prefix,
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

    def generate_object_filename(
        self,
        obj_hash: EntityHash,
        file_extension: str,
        subfolder: Path,
        file_prefix: str,
    ) -> Path:
        return CacheItem.FilenameFromHash(
            obj_hash,
            self._impl.cache_dir,
            file_extension=file_extension,
            subfolder=subfolder,
            file_prefix=file_prefix,
        )

    def store_object(
        self,
        item_filename: Path,
        compute_time: float,
        obj_hash: EntityHash = None,
        weight: float = 1.0,
        object_size: float = None,
        force_store: bool = False,
    ) -> CacheItem:
        assert item_filename.exists()
        check_hash = True
        if obj_hash is None:
            obj_hash = EntityHash.EntityHash.FromDiskFile(item_filename, "sha256")
            check_hash = False
        item = self._impl.get_object_by_hash(obj_hash)
        # exists = item is not None
        if item is None:
            if object_size is None:
                object_size = item_filename.stat().st_size / (1024 * 1024 * 1024)
            item = CacheItem(
                obj_hash, item_filename, compute_time, weight, size=object_size
            )
        else:
            if not check_hash or item.verify_hash():
                return item
        self._impl.store_object_unconditionally(item)
        return item

        # self._impl.calculate_net_utility_of_object(item, existing=exists)
        # if item.utility < 0 and not force_store:
        #     if exists:
        #         self._impl.remove_object(obj_hash, remove_access_history=False)
        #     return item
        # else:
        #     if not exists:
        #         self._impl.store_object_unconditionally(item)
        #     return item

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


class ObjectCacheView:
    _cache: ObjectCache
    _subfolder: Path
    _file_prefix: str
    _file_extension: str

    def __init__(
        self,
        cache: ObjectCache,
        subfolder: Path = None,
        file_prefix: str = "",
        file_extension: str = ".bin",
    ):
        assert isinstance(cache, ObjectCache)
        if subfolder is None:
            subfolder = Path()
        self._subfolder = subfolder
        self._file_prefix = file_prefix
        self._file_extension = file_extension

    def generate_object_filename(self, obj_hash: EntityHash) -> Path:
        return self._cache.generate_object_filename(
            obj_hash,
            file_extension=self._file_extension,
            subfolder=self._subfolder,
            file_prefix=self._file_prefix,
        )

    def is_object_in_cache(self, obj_hash: EntityHash) -> bool:
        item = self._cache.get_object_by_hash(obj_hash)
        is_in_db = item is not None
        filename = self.generate_object_filename(obj_hash)
        is_file = filename.exists()
        if is_file:
            # Check if size > 0
            file_size = filename.stat().st_size
            if file_size == 0:
                is_file = False
        if is_in_db and is_file:
            return True
        if not is_file:
            return False
        if not is_in_db and is_file:
            raise ResourceWarning(
                f"File {filename} for some reason is not in the cache database. Possible cache corruption."
            )
        return False

    def get_object_by_hash(self, obj_hash: EntityHash) -> Optional[CacheItem]:
        return self._cache.get_object_by_hash(obj_hash)

    def remove_object(self, obj_hash: EntityHash, remove_access_history: bool = True):
        return self._cache.remove_object(
            obj_hash, remove_access_history=remove_access_history
        )

    def store_object(
        self,
        item_filename: Path,
        compute_time: float,
        obj_hash: EntityHash = None,
        weight: float = 1.0,
        object_size: float = None,
        force_store: bool = False,
    ) -> CacheItem:
        return self._cache.store_object(
            item_filename, compute_time, obj_hash, weight, object_size, force_store
        )

    def calculate_items_utility(
        self, item: CacheItem, item_exists: bool = None
    ) -> float:
        return self._cache.calculate_items_utility(item, item_exists=item_exists)
