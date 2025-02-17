from pydantic import BaseModel
from .settings_manager import SettingsManager
from pathlib import Path
import os


class ModelCacheManager(BaseModel):
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

    def __init__(
        self,
        cache_dir: Path,
        half_life_of_cache: float = 24.0,
        utility_of_1GB_free_space: float = 2.0,
        marginal_relative_utility_at_1GB: float = 1.0,
        cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1,
        reserved_free_space: float = 1.0,
        object_file_extension: str = "bin",
    ):
        self._metadata_manager = SettingsManager(cache_dir)
        super().__init__(
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
            os.statvfs(self.cache_dir).f_bsize
            * os.statvfs(self.cache_dir).f_bavail
            / 1024
            / 1024
            / 1024
        )

    def calculate_disk_cost_of_new_object(self, size: float) -> float:
        """
        Calculate the cost of storing the object in the cache.
        """
        free_space = self.free_space
        if free_space < size:
            return float("inf")
        utility_before = self.calculate_utility_of_free_space(free_space)
        utility_after = self.calculate_utility_of_free_space(free_space - size)
        return utility_before - utility_after

    def calculate_utility_of_free_space(self, free_space: float) -> float:
        """
        Calculate the utility of the free space (measured in GB).
        """
        return self.utility_of_1GB_free_space * free_space ** (
            -self.marginal_relative_utility_at_1GB
        )

    def calculate_net_utility_of_new_object(self, size: float, time: float) -> float:
        """
        Calculate the net utility of storing the object in the cache.
        Size is in GB, time in minutes.
        If the utility is negative, the object should not be stored in the cache.
        """
        return (
            time / self.cost_of_minute_compute_rel_to_cost_of_1GB
            - self.calculate_disk_cost_of_new_object(size)
        )

    def prune_cache(self):
        """
        Prune the cache by removing the items that are no longer worth being stored
        """
        pass


class DiskCostFunction(BaseModel):
    free_space_that_costs_1: float
    free_space_with: float  # It is relative to the cost of 1GB of free space.
    # 2.0 means that around 1GB of free space the additional cost of adding 1kB of data is 2.0 times the 1kB/1GB ratio.
