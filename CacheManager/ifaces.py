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
    item_key: EntityHash  # A key with which it will be acquired from the cache
    hash: Optional[EntityHash]  # A hash of the item to check for consistency
    item_storage_key: ItemID  # Path or any other type of item ID in future.
    compute_time: dt.timedelta  # in minutes
    filesize: PositiveFloat  # in bytes
    weight: PositiveFloat

    # last_access_time: dt.datetime

    def __str__(self):
        ans = ""
        if isinstance(self.item_storage_key, Path):
            file_len = len(self.item_storage_key.name)
            ans += f"{shorten_path(self.item_storage_key.absolute(), 30 + file_len)}:\n"
        else:
            ans += f"storage key={self.item_storage_key.pretty_shorten(50)}:\n"
        ans += f" item_key={self.pretty_key}\n"
        ans += f" hash={self.hash}\n"
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
        if isinstance(self.item_storage_key, Path):
            return str(self.item_storage_key)
        else:
            return self.item_storage_key.serialize()

    @property
    def pretty_size(self) -> str:
        return naturalsize(self.filesize)

    @property
    def pretty_compute_time(self) -> str:
        return naturaldelta(
            self.compute_time, months=False, minimum_unit="microseconds"
        )

    @property
    def pretty_description(self) -> str:
        return f"{self.pretty_key}: {self.pretty_size} and {self.pretty_compute_time}"

    @property
    def pretty_key(self) -> str:
        return self.item_key.as_base64[:10]

    def __eq__(self, other: DC_CacheItem[ItemID]) -> bool:
        ans = self.item_key == other.item_key
        if __debug__:
            if self.hash is not None and other.hash is not None:
                assert (self.hash == other.hash) == ans
        return ans


class I_PersistentDB[ItemID: (Path, I_AbstractItemID)](ABC):
    """Class that abstracts away storage of persistent settings."""

    def is_ItemID_Path(self) -> bool:
        # # noinspection PyUnresolvedReferences
        # return issubclass(self.__orig_class__.__args__[0], Path)
        return True  # The code above relies on undefined behaviour and sometimes fails.
        # This is a reason not to use generics in Python anymore.

    @abstractmethod
    def add_item(self, item: DC_CacheItem): ...

    @abstractmethod
    def add_access_to_item(self, item_key: EntityHash, timestamp: dt.datetime): ...

    @abstractmethod
    def commit(self): ...

    @abstractmethod
    def get_item_by_key(self, item_key: EntityHash) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_item_by_storage_key(
        self, storage_key: ItemID
    ) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_accesses(self, item_key: EntityHash) -> list[dt.datetime]: ...

    @abstractmethod
    def get_last_access(self, item_key: EntityHash) -> Optional[dt.datetime]: ...

    @abstractmethod
    def remove_item(self, item_key: EntityHash, remove_history: bool = True): ...

    @property
    @abstractmethod
    def config(self) -> ModelCacheManagerConfig: ...

    @abstractmethod
    def store_config(self, options: ModelCacheManagerConfig): ...

    @abstractmethod
    def iterate_items(self) -> Iterator[DC_CacheItem]: ...

    @abstractmethod
    def clear_items(self): ...

    @abstractmethod
    def close(self): ...


class I_CacheStorageRead[ItemID: (Path, I_AbstractItemID)](ABC):
    @property
    @abstractmethod
    def free_space(self) -> float: ...

    @property
    @abstractmethod
    def storage_id(self) -> str: ...

    @abstractmethod
    def calculate_hash(self, item_storage_key: ItemID) -> Optional[EntityHash]:
        """None if hash calculation is not possible in principle for the given domain. In such case object verification will always pass.
        Throw if hash calculation failed for some reason. In this case object verification will fail.
        """
        ...

    @abstractmethod
    def remove_item(self, item_storage_key: ItemID) -> bool:
        """True if removal succeeded, False otherwise."""
        ...

    @abstractmethod
    def does_item_exists(self, item_storage_key: ItemID) -> bool: ...

    @abstractmethod
    def close(self): ...


class I_CacheStorageModify[ItemID: (Path, I_AbstractItemID)](
    I_CacheStorageRead[ItemID]
):
    @abstractmethod
    def remove_item(self, item_storage_key: ItemID) -> bool:
        """True if removal succeeded, False otherwise."""
        ...

    @abstractmethod
    def load_item(self, item_storage_key: ItemID) -> bytes: ...

    @abstractmethod
    def save_item(self, object: bytes, item_storage_key: ItemID): ...


class I_StorageKeyGenerator[ItemID: (Path, I_AbstractItemID)](ABC):
    """
    Class that is responsible for naming new cache items.
    """

    @abstractmethod
    def generate_item_storage_key(self, item_key: EntityHash) -> ItemID: ...
