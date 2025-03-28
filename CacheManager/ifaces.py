from __future__ import annotations
import datetime as dt
import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, Iterator, Protocol, Any, Union

from EntityHash import EntityHash
from humanize import naturalsize, naturaldelta
from pydantic import BaseModel, PositiveInt, PositiveFloat, constr
from ValueWithError import ValueWithError

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

    @abstractmethod
    def __hash__(self) -> int: ...


StoredItemID = Union[Path, I_AbstractItemID]


class I_ItemProducer(ABC):
    """
    Object that is intended as a wrapper around a function that produces an object of interest.
    It is expected that user will provide an initializer that takes all the data required to generate the result.

    This object is treated as ephemeral and is not stored anywhere by the cache system. It is expected to be tied to a single call to the cache system.
    """

    @abstractmethod
    def get_item_key(self) -> EntityHash: ...

    @abstractmethod
    def get_item_serialization_class(self) -> str:
        """
        :return: A string that identifies a type of serialization performance of the item.
        Items with the same serialization class and the same size are assumed to take the same time to serialize and deserialize.
        """

    @abstractmethod
    def compute_item(self) -> Any: ...

    @abstractmethod
    def instantiate_item(
        self, data: bytes, extra_files: dict[str, StoredItemID] | None = None
    ) -> Any: ...

    @abstractmethod
    def get_files_storing_state(
        self, storage: I_CacheStorageModify
    ) -> dict[str, StoredItemID]:
        """
        This method should return a list of additional items (files) that should be stored in the cache
        along with the main item.

        Don't override this method if the computed item result is completely contained in its own serialization stream.

        You may need it if the computation involves creation of additional items (files) that contain additional state,
        like e.g. compiled object code/executables of the C++ components.
        """
        return {}

    @abstractmethod
    def protect_item(self):
        """Called by the cache when the item was computed and was deemed to be worthy of being stored in the cache,
        as a signal to not remove the additional files from disk in object's destructor.

        The method will not be called, if `get_files_storing_state()` returned empty list, or was not implemented.
        """
        raise NotImplementedError

    @abstractmethod
    def serialize_item(self, item: Any) -> bytes: ...

    @abstractmethod
    def propose_item_storage_key(self) -> Optional[StoredItemID]: ...


class I_MockItemProducer(I_ItemProducer):
    @property
    @abstractmethod
    def compute_time(self) -> dt.timedelta: ...


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


class I_UtilityOfStoredItem(BaseModel, ABC):
    """
    Class that is capable of computing the utility of the stored item.
    """

    @abstractmethod
    def utility(
        self,
        item: DC_CacheItem,
        free_space: int,
        meta_db: I_PersistentDB,
        existing: bool = False,
        last_access_time: dt.datetime | None = None,
    ) -> float:
        """
        :param item: item to calculate utility for
        :param existing: True - calculate utility in the context of prunning. False - calculate utility in the context of storing new item.
        Calculate the net utility of storing the object in the cache.
        :param last_access_time: time of the last access to the item, or None to use the current time.
        :param free_space: free space in bytes on the storage
        :param meta_db: metadata database. Used to get the list of accesses to the item.


        If the utility is negative, the object should not be stored in the cache.

        If item does not exist, it would make sense to estimate the expected utility of the decay.
        """
        ...


class AdditionalItemStorageKeys:
    """
    This is a class that is intended to be returned by the `compute_item` method of an `I_ItemProducer` object.

    This class is relevant if the `comupte_item` produces not only an object, but has a side-effect of also storing some data directly in the cache,
    (for example, a large file that is created by a call to a 3-rd party)
    It is a tuple of two elements: the result of the computation, and the storage keys that were used to store the result.
    """

    additional_item_storage_keys: dict[str, StoredItemID]  # tag -> file pointer

    def __init__(self, extra_files: dict[str, StoredItemID] | None = None):
        if extra_files is None:
            extra_files = {}
        self.additional_item_storage_keys = extra_files

    def add_stored_item(self, tag: str, storage_key: StoredItemID):
        self.additional_item_storage_keys[tag] = storage_key


class DC_StoredItem(BaseModel, ABC):
    filesize: PositiveInt  # in bytes
    item_store_key: StoredItemID
    tag: constr(max_length=10)
    hash: EntityHash

    def __init__(
        self,
        filesize: PositiveInt,
        item_store_key: StoredItemID,
        hash: EntityHash,
        tag: str,
    ):
        super().__init__(
            filesize=filesize, item_store_key=item_store_key, hash=hash, tag=tag
        )

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
    main_item_storage_key: StoredItemID
    stored_items: dict[StoredItemID, DC_StoredItem]
    serialization_performance_class: str  # Holds a tag that identify a type of serialization performance of the item.

    # It is used to determine the serialization performance of the item.

    def __str__(self):
        ans = ""
        ans += f" item_key={self.pretty_key}\n"
        ans += f" main storage key={self.pretty_main_storage_key}\n"
        ans += f" object size={self.pretty_size}\n"
        ans += f" compute time={self.pretty_compute_time}\n"
        ans += f" serialization class={self.serialization_performance_class}\n"
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
    def pretty_main_storage_key(self) -> str:
        item = self.stored_items[self.main_item_storage_key]
        return item.pretty_store_key

    @property
    def item_hash(self) -> EntityHash:
        keys = list(self.stored_items.keys())
        keys.sort()  # pyright: ignore [reportCallIssue]
        sha256 = hashlib.sha256()
        for key in keys:
            storage_item = self.stored_items[key]
            sha256.update(storage_item.hash.as_bytes)
        return EntityHash.FromHashlib(sha256)

    def calculate_hash(self, storage: I_CacheStorageRead) -> EntityHash:
        keys = list(self.stored_items.keys())
        keys.sort()  # pyright: ignore [reportCallIssue]
        sha256 = hashlib.sha256()
        for key in keys:
            hash = storage.calculate_hash(key)
            sha256.update(hash.as_bytes)
        return EntityHash.FromHashlib(sha256)

    @property
    def non_main_stored_item_keys(self) -> dict[str, StoredItemID] | None:
        ans = {
            item.tag: key
            for key, item in self.stored_items.items()
            if key != self.main_item_storage_key
        }
        if len(ans) == 0:
            return None
        return ans

    def __eq__(self, other: Any) -> bool:
        assert isinstance(other, DC_CacheItem)
        ans = self.item_key == other.item_key
        if __debug__:
            assert (self.item_hash == other.item_hash) == ans
        return ans


class I_SerializationPerformanceModel(ABC):
    @property
    @abstractmethod
    def serialization_time(self) -> ValueWithError: ...

    @property
    @abstractmethod
    def deserialization_time(self) -> ValueWithError: ...

    @property
    @abstractmethod
    def model_age(self) -> dt.datetime:
        """
        Timestamp of the model creation. Used to determine whether to refresh the model or not.
        """
        ...

    @property
    @abstractmethod
    def sample_count(self) -> int:
        """
        Number of samples used to calculate the model. Used to determine whether to refresh the model or not.
        """


class I_PersistentDB(ABC):
    """Class that abstracts away storage of persistent settings."""

    @abstractmethod
    def add_item(self, item: DC_CacheItem): ...

    @abstractmethod
    def add_file_to_item(
        self,
        item_key: EntityHash,
        storage_key: StoredItemID,
        tag: str,
        item_hash: EntityHash,
        filesize: int,
    ): ...

    @abstractmethod
    def add_access_to_item(self, item_key: EntityHash, timestamp: dt.datetime): ...

    @abstractmethod
    def add_serialization_time(
        self,
        serialization_class: str,
        serialization_time: dt.timedelta,
        deserialization_time: dt.timedelta,
        serialized_size: int,
        object_size: int | None = None,
    ): ...

    @abstractmethod
    def get_serialization_statistics(
        self,
        serialization_class: str,
        last_n: int | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        min_time: dt.timedelta | None = None,
    ) -> I_SerializationPerformanceModel: ...

    @abstractmethod
    def commit(self): ...

    @abstractmethod
    def get_item_by_key(self, item_key: EntityHash) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_stored_items(
        self, item_key: EntityHash
    ) -> dict[StoredItemID, DC_StoredItem]: ...

    @abstractmethod
    def get_item_by_storage_key(
        self, storage_key: StoredItemID
    ) -> Optional[DC_CacheItem]: ...

    @abstractmethod
    def get_accesses(self, item_key: EntityHash) -> list[dt.datetime]: ...

    @abstractmethod
    def get_last_access(self, item_key: EntityHash) -> Optional[dt.datetime]: ...

    @abstractmethod
    def remove_item(self, item_key: EntityHash, remove_history: bool = True) -> bool:
        """Returns True if operation was successful, False otherwise."""
        ...

    @abstractmethod
    def iterate_items(self) -> Iterator[DC_CacheItem]: ...

    @abstractmethod
    def clear_items(self): ...

    @abstractmethod
    def close(self): ...


class I_CacheStorageRead(ABC):
    @property
    @abstractmethod
    def free_space(self) -> int: ...

    @property
    @abstractmethod
    def storage_id(self) -> str: ...

    @abstractmethod
    def calculate_hash(self, item_storage_key: StoredItemID) -> EntityHash:
        """None if hash calculation is not possible in principle for the given domain. In such case object verification will always pass.
        Throw if hash calculation failed for some reason. In this case object verification will fail.
        """
        ...

    @abstractmethod
    def remove_item(self, item_storage_key: StoredItemID) -> bool:
        """True if removal succeeded, False otherwise."""
        ...

    @abstractmethod
    def does_item_exists(self, item_storage_key: StoredItemID) -> bool: ...

    @abstractmethod
    def close(self): ...

    @abstractmethod
    def item_size(self, item_storage_key: StoredItemID) -> int:
        """Returns size of the item in bytes"""
        ...


class I_CacheStorageModify(I_CacheStorageRead):
    @abstractmethod
    def remove_item(self, item_storage_key: StoredItemID) -> bool:
        """True if removal succeeded, False otherwise."""
        ...

    @abstractmethod
    def load_item(self, item_storage_key: StoredItemID) -> bytes: ...

    @abstractmethod
    def save_item(self, item: bytes, item_storage_key: StoredItemID): ...

    @abstractmethod
    def make_absolute_item_storage_key(
        self, item_storage_key: StoredItemID
    ) -> StoredItemID: ...


class I_StorageKeyGenerator(ABC):
    """
    Class that is responsible for naming new cache items.
    """

    @abstractmethod
    def generate_item_storage_key(self, item_key: EntityHash) -> StoredItemID: ...
