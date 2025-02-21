from __future__ import annotations

import datetime as dt
import heapq
import os
from pathlib import Path
from typing import Iterator, Optional

from entityhash import EntityHash
from humanize import naturalsize

from .cache_config import ModelCacheManagerOptions
from .cache_item import CacheItem
from .settings_manager import SettingsManager


class ModelCacheManagerImpl:
    """
    Class that manages persistance of
    * compiled models
    * computed inferences.

    The class is aware of the time it takes for the computation, and caches only if the cost of storing the object is less than the cost of re-computing it.

    There's an option to set a custom cost multiplier for the task in order to retain the specific cache items for longer or shorter periods of time.

    The class manages a single cache directory.

    Cost of the disk space is computed relatively to the free space on the disk using an exponential decay function with a custom coefficient.

    On top of that, the manager maintains a database of metadata as SQLite database using three tables:
    # Objects
    .* hash <primary key>
    .* filename
    .* compute_cost
    .* weight
    # Accesses
    .* hash <foreign key>
    .* timestamp
    # Settings
    .* key <primary key> - name of the setting of the class
    .* value

    The database is stored in .metadata.sqlite file stored in the cache directory.

    """

    _metadata_manager: SettingsManager
    _settings: ModelCacheManagerOptions

    def __init__(
        self,
        cache_dir: Path,
        half_life_of_cache: float = 24.0,  # in hours
        utility_of_1GB_free_space: float = 2.0,
        marginal_relative_utility_at_1GB: float = 1.0,
        cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1,
        reserved_free_space: float = 1.0,
        object_file_extension: str = "bin",
    ):
        self._metadata_manager = SettingsManager(cache_dir)
        self._settings = ModelCacheManagerOptions(
            cache_dir=cache_dir,
            half_life_of_cache=half_life_of_cache,
            utility_of_1GB_free_space=utility_of_1GB_free_space,
            marginal_relative_utility_at_1GB=marginal_relative_utility_at_1GB,
            cost_of_minute_compute_rel_to_cost_of_1GB=cost_of_minute_compute_rel_to_cost_of_1GB,
            reserved_free_space=reserved_free_space,
            object_file_extension=object_file_extension,
        )

    @property
    def free_space(self) -> float:
        """Returns free space in GB"""
        return (
            os.statvfs(self._settings.cache_dir).f_bsize
            * os.statvfs(self._settings.cache_dir).f_bavail
            / 1024
            / 1024
            / 1024
        )

    def calculate_disk_cost_of_new_object(
        self, size: float, existing: bool = False
    ) -> float:
        """
        Calculate the cost of storing the object in the cache.
        """
        free_space = self.free_space - self._settings.reserved_free_space
        if existing:
            if free_space < 0:
                return -float("inf")
            utility_before = self.calculate_utility_of_free_space(free_space + size)
            utility_after = self.calculate_utility_of_free_space(free_space)
        else:
            if free_space < size:
                return -float("inf")
            utility_before = self.calculate_utility_of_free_space(free_space)
            utility_after = self.calculate_utility_of_free_space(free_space - size)
        return utility_before - utility_after

    def calculate_utility_of_free_space(self, free_space: float) -> float:
        """
        Calculate the utility of the free space (measured in GB).
        """
        return self._settings.utility_of_1GB_free_space * free_space ** (
            -self._settings.marginal_relative_utility_at_1GB
        )

    def calculate_decay_weight(self, age: float) -> float:
        """
        Calculate the weight of the object based on the age of the object in hours.
        """
        return 2 ** (-age / self._settings.half_life_of_cache)

    def calculate_net_utility_of_object(self, item: CacheItem, existing: bool = False):
        """
        Calculate the net utility of storing the object in the cache.
        Size is in GB, time in minutes.
        If the utility is negative, the object should not be stored in the cache.
        """
        positive_utility = (
            item.compute_time
            / self._settings.cost_of_minute_compute_rel_to_cost_of_1GB
            * item.weight
            * self.calculate_decay_weight(item.age)
        )
        negative_cost = self.calculate_disk_cost_of_new_object(
            item.size, existing=existing
        )
        item.utility = positive_utility + negative_cost

    def iterate_cache_items(self, calc_utility: bool = False) -> Iterator[CacheItem]:
        for item in self._metadata_manager.iterate_objects():
            if not self.does_object_exist(item):
                continue
            if calc_utility:
                self.calculate_net_utility_of_object(item)
            yield item

    def store_file(self, filename: Path, data: bytes, size: float):
        # Serialize the object
        with open(filename, "wb") as f:
            f.write(data)

    def remove_file(self, filename: Path):
        os.remove(filename)

    def prune_cache(self, remove_metadata: bool = False, verbose: bool = False):
        """
        Prune the cache by removing the items that are no longer worth being stored
        """
        to_delete: list[CacheItem] = []
        for item in self.iterate_cache_items(True):
            if item.utility < 0:
                heapq.heappush(to_delete, item)

        if len(to_delete) == 0:
            return

        while len(to_delete) > 0:
            item = heapq.heappop(to_delete)
            if item.utility >= 0:
                break
            self.remove_file(item.filename)
            if remove_metadata:
                self._metadata_manager.remove_object(item.hash)
            if verbose:
                print(f"Removed object {item} from the cache.")

    def does_object_exist(self, item: CacheItem) -> bool:
        """
        Check if the object with the hash exists in the cache.
        """
        file_path = self._settings.cache_dir / item.filename
        return file_path.exists()

    def verify_object(self, object_hash: EntityHash) -> bool:
        """
        Verify if the object is in the cache and is valid.
        """
        item = self._metadata_manager.get_object_by_hash(object_hash)
        item_filename = CacheItem.FilenameFromHash(
            object_hash, self._settings.cache_dir, self._settings.object_file_extension
        )
        exists_db = item is not None
        exists_file = item_filename.exists()
        if exists_db != exists_file:
            if exists_db:
                return False  # Does not exist in the file system.
            else:
                raise ValueError(
                    f"Object with hash {object_hash} is in the database but not in the file system. Because there is no metadata associated with it, it has to be removed."
                )
        return exists_db

    def store_object(self, object: bytes, item: CacheItem):
        """
        Store the object in the cache without questioning its utility.
        """
        obj_hash = item.hash
        if (
            self._metadata_manager.get_object_compute_time_and_weight(obj_hash)
            is not None
        ):
            raise ValueError(f"Object with hash {obj_hash} is already in the cache.")

        # Put the object into db
        self._metadata_manager.put_object(item)

        # Serialize the object
        self.store_file(item.filename, object, item.size)
        self.add_access_to_object(obj_hash)

        return item

    def is_object_in_cache(self, obj_hash: EntityHash) -> bool:
        item = self._metadata_manager.get_object_by_hash(obj_hash)
        if item is None:
            return False
        return (self._settings.cache_dir / item.filename).exists()

    def add_access_to_object(self, obj_hash: EntityHash):
        self._metadata_manager.store_access(
            obj_hash, int(dt.datetime.now().timestamp())
        )

    def get_object_by_hash(self, obj_hash: EntityHash) -> Optional[CacheItem]:
        ans = self._metadata_manager.get_object_by_hash(obj_hash)
        if ans is not None:
            if (self._settings.cache_dir / ans.filename).exists():
                self.add_access_to_object(obj_hash)
                return ans

        return None

    def remove_object(self, obj_hash: EntityHash, remove_access_history: bool) -> bool:
        if item := self._metadata_manager.get_object_by_hash(obj_hash) is not None:
            if self._settings.cache_dir / item.filename.exists():
                self.remove_file(item.filename)
                if remove_access_history:
                    self._metadata_manager.remove_object(obj_hash)
                return True
            if remove_access_history:
                self._metadata_manager.remove_object(obj_hash)
            return False
        return False

    @property
    def cache_dir(self) -> Path:
        return self._settings.cache_dir


class MockModelCacheManagerImpl(ModelCacheManagerImpl):
    """Class that tests the cache manager without writing anything to disk.
    It still uses the on-disk sqlite database.
    """

    _base_free_space: float
    _files: dict[str, float]  # List of "stored" files: filename -> size in GB

    def __init__(
        self,
        free_space: float,
        db_directory: Path,
        half_life_of_cache: float = 24.0,  # in hours
        utility_of_1GB_free_space: float = 2.0,
        marginal_relative_utility_at_1GB: float = 1.0,
        cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1,
        reserved_free_space: float = 1.0,
        object_file_extension: str = "bin",
    ):
        super().__init__(
            cache_dir=db_directory,
            half_life_of_cache=half_life_of_cache,
            utility_of_1GB_free_space=utility_of_1GB_free_space,
            marginal_relative_utility_at_1GB=marginal_relative_utility_at_1GB,
            cost_of_minute_compute_rel_to_cost_of_1GB=cost_of_minute_compute_rel_to_cost_of_1GB,
            reserved_free_space=reserved_free_space,
            object_file_extension=object_file_extension,
        )
        self._base_free_space = free_space
        self._files = {}

        self._metadata_manager.clear_objects()

    @property
    def free_space(self) -> float:
        """Returns free space in GB"""
        sum_space = sum(self._files.values())
        return self._base_free_space - sum_space

    def store_file(self, filename: Path, data: bytes, size):
        # Serialize the object
        self._files[str(filename)] = size

    def remove_file(self, filename: Path):
        del self._files[str(filename)]

    def does_object_exist(self, item: CacheItem) -> bool:
        """
        Check if the object with the hash exists in the cache.
        """
        return str(item.filename) in self._files


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

    def get_object_by_hash(self, obj_hash: EntityHash) -> Optional[bytes]:
        item = self._impl.get_object_by_hash(obj_hash)

        if item is None:
            return None

        self._impl.add_access_to_object(obj_hash)

        with open(item.filename, "rb") as f:
            return f.read()

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
        if item.utility < 0:
            if exists:
                self._impl.remove_object(obj_hash, remove_access_history=False)
            return item
        else:
            if not exists:
                self._impl.store_object(object, item)
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
