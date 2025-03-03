from __future__ import annotations
import datetime as dt
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, Iterator

from EntityHash import EntityHash
from humanize import naturalsize, naturaldelta
from pydantic import BaseModel, PositiveFloat

from .pretty_path import shorten_path


class I_AbstractItemID(ABC, BaseModel):
    @abstractmethod
    def pretty_shorten(self, max_len: int) -> str: ...

    @abstractmethod
    def serialize(self) -> str: ...

    @staticmethod
    @abstractmethod
    def Unserialize(string: str) -> I_AbstractItemID: ...


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


class ModelCacheManagerConfig(BaseModel):
    """
    Class that manages stores the options for the ModelCacheManager class."""

    cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1  # e.g. Value of 10.0 means that 10 minute of compute time costs the same as holding 1GB of storage.
    reserved_free_space: float = 0.0  # Reserved free space in GB excluded from the cache. E.g. 1.0 means the cache will not leave system with less than 1GB of free space.
    half_life_of_cache: float = 24.0  # Cache prunning strategy. The half-life of the value of cached items cache in hours. E.g. 24.0 means that the value of each cache item is halved every 24 hours.
    utility_of_1GB_free_space: float = 2  # The amount of free space that is considered as a cost of storing the cache item. E.g. 0.9 means that 10% of the free space is considered as a cost of storing the cache item.
    marginal_relative_utility_at_1GB: float = 1  # Shape parameter, equal to minus the derivative of the utility function at 1GB of free space divided by the utility at 1GB of free space. E.g. 2.0 means that the cost of storing the cache item at 1GB free space is rising 2 times faster than the cost of storing the cache item at 1GB of free space.


class DC_CacheItem[ItemID: (Path, I_AbstractItemID)](
    BaseModel, ABC
):  # DC stands for DataClass - a glorified struct
    hash: EntityHash
    filename: ItemID  # Path or any other type of item ID in future.
    compute_time: dt.timedelta  # in minutes
    filesize: PositiveFloat  # in GB
    weight: PositiveFloat

    # last_access_time: dt.datetime

    def __str__(self):
        ans = ""
        if isinstance(self.filename, Path):
            file_len = len(self.filename.name)
            ans += f"{shorten_path(self.filename.absolute(), 30 + file_len)}:\n"
        else:
            ans += f"{self.filename.pretty_shorten(50)}:\n"
        ans += f" object hash={self.hash}\n"
        ans += f" object size={self.pretty_size}\n"
        ans += f" compute time={self.pretty_compute_time}\n"
        # ans += f" last accessed {naturaldelta(dt.datetime.now() - self.last_access_time, months=False, minimum_unit="seconds")} ago\n"
        if self.weight != 1.0:
            ans += f" weight={self.weight}\n"

        return ans

    def __repr__(self):
        return self.__str__()

    @property
    def serialized_filename(self) -> str:
        if isinstance(self.filename, Path):
            return str(self.filename.absolute())
        else:
            return self.filename.serialize()

    # @property
    # def age(self) -> float:
    #     """Age of the last access in hours"""
    #     return (dt.datetime.now() - self.last_access_time).total_seconds() / 60 / 60

    @property
    def pretty_size(self) -> str:
        return naturalsize(self.filesize * 1024 * 1024 * 1024)

    @property
    def pretty_compute_time(self) -> str:
        return naturaldelta(
            self.compute_time, months=False, minimum_unit="microseconds"
        )


class I_SettingsManager[ItemID: (Path, I_AbstractItemID)](ABC):
    """Class that abstracts away storage of persistent settings."""

    def is_ItemID_Path(self) -> bool:
        from pathlib import Path

        # noinspection PyUnresolvedReferences
        return issubclass(self.__orig_class__.__args__[0], Path)

    @abstractmethod
    def add_object(self, object: DC_CacheItem): ...

    @abstractmethod
    def add_access_to_object(self, objectID: EntityHash, timestamp: dt.datetime): ...

    @abstractmethod
    def commit(self): ...

    @abstractmethod
    def get_object_by_hash(self, hash: EntityHash) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_object_by_filename(self, filename: ItemID) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_accesses(self, hash: EntityHash) -> list[dt.datetime]: ...

    @abstractmethod
    def get_last_access(self, hash: EntityHash) -> Optional[dt.datetime]: ...

    @abstractmethod
    def remove_object(self, hash: EntityHash): ...

    @property
    @abstractmethod
    def config(self) -> ModelCacheManagerConfig: ...

    @abstractmethod
    def store_config(self, options: ModelCacheManagerConfig): ...

    @abstractmethod
    def iterate_cacheitems(self) -> Iterator[DC_CacheItem]: ...

    @abstractmethod
    def clear_cacheitems(self): ...


class I_CacheStorage[ItemID: (Path, I_AbstractItemID)](ABC):
    @property
    @abstractmethod
    def free_space(self) -> float: ...

    @property
    @abstractmethod
    def storage_id(self) -> str: ...

    @abstractmethod
    def get_settings_manager(self) -> I_SettingsManager[ItemID]: ...


class I_CacheStorageView[ItemID: (Path, I_AbstractItemID)](ABC):
    """
    Class that is responsible for naming new cache items.
    """

    @abstractmethod
    def generate_object_filename(self, obj_hash: EntityHash) -> ItemID: ...
