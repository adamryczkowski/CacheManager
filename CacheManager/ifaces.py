from __future__ import annotations
import datetime as dt
import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, Iterator, Protocol, Any, Union

from EntityHash import EntityHash
from humanize import naturalsize, naturaldelta
from pydantic import BaseModel, PositiveInt, PositiveFloat

from .pretty_path import shorten_path


class ProducerCallback(Protocol):
    def __call__(self, *args, **kwargs) -> Any: ...


class I_AbstractItemID(ABC, BaseModel):
    @abstractmethod
    def pretty_shorten(self, max_len: int) -> str: ...

    @abstractmethod
    def serialize(self) -> str: ...

    @staticmethod
    @abstractmethod
    def Unserialize(string: str) -> I_AbstractItemID: ...


ItemID = Union[Path, I_AbstractItemID]


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


class DC_StoredItem(BaseModel, ABC):
    filesize: PositiveInt  # in bytes
    item_store_key: ItemID
    hash: EntityHash

    def __init__(
        self,
        filesize: PositiveInt,
        item_store_key: ItemID,
        hash: EntityHash,
    ):
        super().__init__(filesize=filesize, item_store_key=item_store_key, hash=hash)

    @property
    def serialized_filename(self) -> str:
        if isinstance(self.item_store_key, Path):
            return str(self.item_store_key)
        else:
            return self.item_store_key.serialize()

    @property
    def pretty_size(self) -> str:
        return naturalsize(self.filesize)

    @property
    def pretty_store_key(self) -> str:
        if isinstance(self.item_store_key, Path):
            return shorten_path(self.item_store_key, 30 + len(self.item_store_key.name))
        else:
            return self.item_store_key.pretty_shorten(50)


class DC_CacheItem(BaseModel, ABC):  # DC stands for DataClass - a glorified struct
    item_key: EntityHash  # A key with which it will be acquired from the cache
    compute_time: dt.timedelta  # in minutes
    weight: PositiveFloat
    stored_items: dict[ItemID, DC_StoredItem]

    def __str__(self):
        ans = ""
        ans += f" item_key={self.pretty_key}\n"
        ans += f" object size={self.pretty_size}\n"
        ans += f" compute time={self.pretty_compute_time}\n"
        # ans += f" last accessed {naturaldelta(dt.datetime.now() - self.last_access_time, months=False, minimum_unit="seconds")} ago\n"
        if self.weight != 1.0:
            ans += f" weight={self.weight}\n"

        return ans

    def __repr__(self):
        return self.__str__()

    @property
    def pretty_size(self) -> str:
        return naturalsize(self.filesize)

    @property
    def filesize(self) -> int:
        filesize = 0
        for item in self.stored_items.values():
            filesize += item.filesize
        return filesize

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

    @property
    def pretty_storage_keys(self) -> str:
        ans = []
        for item in self.stored_items.values():
            ans.append(item.pretty_store_key)
        return ", ".join(ans)

    @property
    def item_hash(self) -> EntityHash:
        keys = list(self.stored_items.keys())
        keys.sort()
        sha256 = hashlib.sha256()
        for key in keys:
            storage_item = self.stored_items[key]
            sha256.update(storage_item.hash.as_bytes)
        return EntityHash.FromHashlib(sha256)

    def __eq__(self, other: Any) -> bool:
        assert isinstance(other, DC_CacheItem)
        ans = self.item_key == other.item_key
        if __debug__:
            assert (self.item_hash == other.item_hash) == ans
        return ans


class I_PersistentDB(ABC):
    """Class that abstracts away storage of persistent settings."""

    @abstractmethod
    def add_item(self, item: DC_CacheItem): ...

    @abstractmethod
    def add_file_to_item(self, item_key: EntityHash, storage_key: ItemID): ...

    @abstractmethod
    def add_access_to_item(self, item_key: EntityHash, timestamp: dt.datetime): ...

    @abstractmethod
    def commit(self): ...

    @abstractmethod
    def get_item_by_key(self, item_key: EntityHash) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_stored_items(self, item_key: EntityHash) -> dict[ItemID, DC_StoredItem]: ...

    @abstractmethod
    def get_item_by_storage_key(
        self, storage_key: ItemID
    ) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_accesses(self, item_key: EntityHash) -> list[dt.datetime]: ...

    @abstractmethod
    def get_last_access(self, item_key: EntityHash) -> Optional[dt.datetime]: ...

    @abstractmethod
    def remove_item(self, item_key: EntityHash, remove_history: bool = True) -> bool:
        """Returns True if operation was successful, False otherwise."""
        ...

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


class I_CacheStorageRead(ABC):
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

    @abstractmethod
    def item_size(self, item_storage_key: ItemID) -> int:
        """Returns size of the item in bytes"""
        ...


class I_CacheStorageModify(I_CacheStorageRead):
    @abstractmethod
    def remove_item(self, item_storage_key: ItemID) -> bool:
        """True if removal succeeded, False otherwise."""
        ...

    @abstractmethod
    def load_item(self, item_storage_key: ItemID) -> bytes: ...

    @abstractmethod
    def save_item(self, item: bytes, item_storage_key: ItemID): ...

    @abstractmethod
    def make_absolute_item_storage_key(self, item_storage_key: ItemID) -> ItemID: ...


class I_StorageKeyGenerator(ABC):
    """
    Class that is responsible for naming new cache items.
    """

    @abstractmethod
    def generate_item_storage_key(self, item_key: EntityHash) -> ItemID: ...
