from enum import Enum
from pathlib import Path

from pydantic import BaseModel


class ModelCacheOptionName(Enum):
    """
    Enum that stores the names of the options for the ModelCacheManager class."""

    cost_of_minute_compute_rel_to_cost_of_1GB = (
        "cost_of_minute_compute_rel_to_cost_of_1GB"
    )
    reserved_free_space = "reserved_free_space"
    half_life_of_cache = "half_life_of_cache"
    utility_of_1GB_free_space = "utility_of_1GB_free_space"
    marginal_relative_utility_at_1GB = "marginal_relative_utility_at_1GB"
    cache_dir = "cache_dir"
    object_file_extension = "object_file_extension"


class ModelCacheManagerOptions(BaseModel):
    """
    Class that manages stores the options for the ModelCacheManager class."""

    cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1  # e.g. Value of 10.0 means that 10 minute of compute time costs the same as holding 1GB of storage.
    reserved_free_space: float  # Reserved free space in GB excluded from the cache. E.g. 1.0 means the cache will not leave system with less than 1GB of free space.
    half_life_of_cache: float = 24.0  # Cache prunning strategy. The half-life of the value of cached items cache in hours. E.g. 24.0 means that the value of each cache item is halved every 24 hours.
    utility_of_1GB_free_space: float = 2  # The amount of free space that is considered as a cost of storing the cache item. E.g. 0.9 means that 10% of the free space is considered as a cost of storing the cache item.
    marginal_relative_utility_at_1GB: float = 1  # Shape parameter, equal to minus the derivative of the utility function at 1GB of free space divided by the utility at 1GB of free space. E.g. 2.0 means that the cost of storing the cache item at 1GB free space is rising 2 times faster than the cost of storing the cache item at 1GB of free space.
    cache_dir: Path
    object_file_extension: str
