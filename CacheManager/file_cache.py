import os
from EntityHash import EntityHash
from overrides import overrides
from pathlib import Path
from pydantic import BaseModel

from .abstract_cache_manager import AbstractCacheManager
from .ifaces import (
    I_CacheStorageModify,
    I_StorageKeyGenerator,
    StoredItemID,
    I_UtilityOfStoredItem,
)
from .object_cache import ObjectCache
from .sqlite_settings_manager import SQLitePersistentDB


class FileCacheStorage(I_CacheStorageModify):
    _cache_root_path: Path
    _reserved_free_space: int

    def __init__(self, cache_root_path: Path, reserved_free_space: int):
        assert cache_root_path.is_dir()
        self._cache_root_path = cache_root_path
        self._reserved_free_space = reserved_free_space

    @overrides
    def remove_item(self, item_storage_key: StoredItemID) -> bool:
        item_storage_key = self.make_absolute_item_storage_key(item_storage_key)
        assert isinstance(item_storage_key, Path)
        try:
            item_storage_key.unlink()
        except Exception as _:  # pylint: disable=broad-exception-caught
            return False
        return True

    @overrides
    def load_item(self, item_storage_key: StoredItemID) -> bytes:
        item_storage_key = self.make_absolute_item_storage_key(item_storage_key)
        assert isinstance(item_storage_key, Path)
        if not item_storage_key.exists():
            raise FileExistsError(f"File {item_storage_key} does not exist")
        with open(item_storage_key, "rb") as f:
            return f.read()

    @overrides
    def save_item(self, item: bytes, item_storage_key: StoredItemID):
        item_storage_key = self.make_absolute_item_storage_key(item_storage_key)
        assert isinstance(item_storage_key, Path)
        if item_storage_key.exists():
            raise FileExistsError(
                f"File {item_storage_key} already exists. Cannot silently overwrite."
            )
        with open(item_storage_key, "wb") as f:
            f.write(item)

    @property
    @overrides
    def free_space(self) -> int:
        return (
            os.statvfs(self._cache_root_path).f_bavail
            * os.statvfs(self._cache_root_path).f_bsize
        ) - self._reserved_free_space

    @overrides
    def make_absolute_item_storage_key(
        self, item_storage_key: StoredItemID
    ) -> StoredItemID:
        assert isinstance(item_storage_key, Path)
        return self._cache_root_path / item_storage_key

    @property
    @overrides
    def storage_id(self) -> str:
        return str(self._cache_root_path)

    @overrides
    def calculate_hash(self, item_storage_key: StoredItemID) -> EntityHash:
        assert isinstance(item_storage_key, Path)
        item_storage_key = self.make_absolute_item_storage_key(item_storage_key)
        assert isinstance(item_storage_key, Path)
        return EntityHash.HashDiskFile(item_storage_key, "sha256")

    @overrides
    def does_item_exists(self, item_storage_key: StoredItemID) -> bool:
        assert isinstance(item_storage_key, Path)
        item_storage_key = self.make_absolute_item_storage_key(item_storage_key)
        assert isinstance(item_storage_key, Path)
        return item_storage_key.exists()

    @overrides
    def close(self):
        pass

    @overrides
    def item_size(self, item_storage_key: StoredItemID) -> int:
        assert isinstance(item_storage_key, Path)
        return item_storage_key.stat().st_size


class StorageKeyGenerator_Path(BaseModel, I_StorageKeyGenerator):
    subfolder: Path = Path()
    file_prefix: str = ""
    file_extension: str = "bin"
    hash_len: int = 8

    def generate_item_storage_key(self, item_key: EntityHash) -> StoredItemID:
        safe_base64 = item_key.as_base64[: self.hash_len].replace("/", "_")
        return self.subfolder / f"{self.file_prefix}{safe_base64}.{self.file_extension}"


def generate_file_cache(
    cached_dir: Path,
    utility_gen: I_UtilityOfStoredItem,
    storage_key_generator: StorageKeyGenerator_Path = StorageKeyGenerator_Path(),
    db_filename: str | Path = ".metadata.sqlite",
    calculate_hash: bool = True,
) -> ObjectCache:
    if isinstance(db_filename, str):
        db_filename = Path(db_filename)
    if not db_filename.is_absolute():
        db_filename = cached_dir / db_filename
    db = SQLitePersistentDB(cached_dir / db_filename)
    storage = FileCacheStorage(cached_dir, reserved_free_space=100 * 1024 * 1024)

    cache_man = AbstractCacheManager(db=db, storage=storage, utility_gen=utility_gen)

    cache = ObjectCache(
        storage=storage,
        cache_manager=cache_man,
        storage_key_generator=storage_key_generator,
        calculate_hash=calculate_hash,
    )

    return cache


def generate_file_cache_view(
    file_cache: ObjectCache,
    subfolder: Path = Path(),
    file_prefix: str = "",
    file_extension: str = "bin",
    hash_len: int = 8,
) -> ObjectCache:
    storage_key_generator = StorageKeyGenerator_Path(
        subfolder=subfolder,
        file_prefix=file_prefix,
        file_extension=file_extension,
        hash_len=hash_len,
    )
    cache = ObjectCache(
        storage=file_cache.storage,
        cache_manager=file_cache._cache_manager,  # pylint: disable=protected-access
        storage_key_generator=storage_key_generator,
        calculate_hash=file_cache.calculate_hash,
    )

    return cache
