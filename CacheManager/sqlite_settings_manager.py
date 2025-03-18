# Manages the sqlite instance that holds the settings and properties of the cache

import datetime as dt
import sqlite3
from builtins import issubclass
from pathlib import Path
from typing import Optional, Iterator

from EntityHash import EntityHash
from overrides import overrides
from pip._internal.utils.filesystem import file_size

from .ifaces import (
    ModelCacheManagerConfig,
    I_PersistentDB,
    DC_CacheItem,
    DC_StoredItem,
    ItemID,
)


# from .cache_item import CacheItem


class SQLitePersistentDB(I_PersistentDB):
    """
    The manager maintains a database of metadata as SQLite database using three tables:
    # Objects
    .* hash <primary key>, as 256bit hash
    .* filename
    .* compute_cost
    .* weight
    # Accesses
    .* hash <foreign key>
    .* timestamp <sorted index>
    # Settings
    .* key <primary key> - name of the setting of the class
    .* value

    The database is stored in .metadata.sqlite file stored in the cache directory.
    """

    database_path: Path
    connection: sqlite3.Connection
    itemID_type: type[ItemID]

    def __init__(
        self,
        database_path: Path,
        initial_config: ModelCacheManagerConfig = None,
        itemID_type: type[ItemID] = Path,
    ):
        self.database_path = database_path
        self._make_sure_db_exists()
        if initial_config is None:
            initial_config = ModelCacheManagerConfig()
        self._ensure_tables(initial_config)
        self.itemID_type = itemID_type

    def _make_sure_db_exists(self):
        if self.database_path.exists():
            if self.database_path.is_dir():
                self.database_path /= ".metadata.sqlite"
        else:
            if self.database_path.suffix == "":
                self.database_path /= ".metadata.sqlite"

            self.database_path.parent.mkdir(exist_ok=True)

        self.connection = sqlite3.connect(self.database_path)

    def __del__(self):
        self.connection.close()

    def _ensure_tables(self, initial_config: ModelCacheManagerConfig = None):
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Objects (
            item_key TEXT PRIMARY KEY,
            compute_time REAL,
            weight REAL
        )
        """)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_filename ON Objects (hash)"
        )
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS StoredItems (
            item_storage_key TEXT PRIMARY KEY,
            item_key TEXT,
            hash TEXT,
            filesize REAL
        )
        """)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS item_key ON Objects (item_key, tag)"
        )
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Accesses (
            item_key TEXT,
            timestamp REAL,
            FOREIGN KEY (item_key) REFERENCES Objects (item_key)
        )
        """)
        # Add index to the timestamp column
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_Accesses_timestamp ON Accesses (timestamp)"
        )

        # Check if Settings table exists:
        populate_settings = (
            self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='Settings'"
            ).fetchone()
            is None
        )

        if not populate_settings:
            populate_settings = (
                self.connection.execute("SELECT key FROM Settings").fetchone() is None
            )

        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        if populate_settings:
            self.store_config(initial_config, table_init=True)
        self.connection.commit()

    def _put_settings(self, settings: dict[str, str]):
        for key, value in settings.items():
            self.connection.execute(
                "UPDATE Settings SET value = ? WHERE key = ?", (value, key)
            )

    def _new_settings(self, settings: dict[str, str]):
        for key, value in settings.items():
            self.connection.execute(
                "INSERT INTO Settings (key, value) VALUES (?, ?)", (key, value)
            )

    def _get_settings(self) -> dict[str, str]:
        cursor = self.connection.execute("SELECT key, value FROM Settings")
        return dict(cursor.fetchall())

    @overrides
    def add_item(self, item: DC_CacheItem):
        self.connection.execute(
            "INSERT INTO Objects (item_key,compute_time, weight) VALUES (?, ?, ?)",
            (
                item.item_key.as_base64,
                item.compute_time.total_seconds(),
                item.weight,
            ),
        )

        for stored_item in item.stored_items.values():
            if issubclass(self.itemID_type, Path):
                item_storage_key_str = str(stored_item.item_store_key)
            else:
                item_storage_key_str = stored_item.item_store_key.serialize()
            self.connection.execute(
                "INSERT INTO StoredItems (item_key, item_storage_key, hash, filesize) VALUES (?, ?, ?, ?)",
                (
                    item.item_key.as_base64,
                    item_storage_key_str,
                    stored_item.hash.as_base64,
                    stored_item.filesize,
                ),
            )

    @overrides
    def get_stored_items(self, item_key: EntityHash) -> dict[ItemID, DC_StoredItem]:
        cursor = self.connection.execute(
            "SELECT item_storage_key, hash, filesize FROM StoredItems WHERE item_key=?",
            (item_key.as_base64,),
        )
        ans = {}
        for row in cursor.fetchall():
            item_storage_key_str, hash, filesize = row
            if issubclass(self.itemID_type, Path):
                item_storage_key: ItemID = Path(item_storage_key_str)
            else:
                item_storage_key: ItemID = self.itemID_type.Unserialize(
                    item_storage_key_str
                )
            item = DC_StoredItem(
                filesize=file_size, item_store_key=item_storage_key, hash=hash
            )

            ans[item_storage_key] = item

        return ans

    @overrides
    def get_item_by_key(self, item_key: EntityHash) -> Optional[DC_CacheItem]:
        cursor = self.connection.execute(
            "SELECT compute_time, weight FROM Objects WHERE item_key=?",
            (item_key.as_base64,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        compute_time, weight = row

        stored_items = self.get_stored_items(item_key)

        return DC_CacheItem(
            item_key=item_key,
            stored_items=stored_items,
            compute_time=dt.timedelta(seconds=compute_time),
            weight=weight,
        )

    @overrides
    def get_item_by_storage_key(self, storage_key: ItemID) -> Optional[DC_CacheItem]:
        assert storage_key is self.itemID_type
        if isinstance(storage_key, Path):
            filename_str = str(storage_key)
        else:
            filename_str = storage_key.serialize()
        cursor = self.connection.execute(
            "SELECT item_key FROM StoredItems WHERE item_storage_key=?",
            (filename_str,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        item_key_str = row[0]
        item_key = EntityHash.FromBase64(item_key_str)
        return self.get_item_by_key(item_key)

    @overrides
    def add_access_to_item(self, item_key: EntityHash, timestamp: dt.datetime):
        self.connection.execute(
            "INSERT INTO Accesses (item_key, timestamp) VALUES (?, ?)",
            (item_key.as_base64, timestamp.timestamp()),
        )

    @overrides
    def get_accesses(self, item_key: EntityHash) -> list[dt.datetime]:
        cursor = self.connection.execute(
            "SELECT timestamp FROM Accesses WHERE item_key=?", (item_key.as_base64,)
        )
        return [dt.datetime.fromtimestamp(row[0]) for row in cursor.fetchall()]

    @overrides
    def get_last_access(self, item_key: EntityHash) -> Optional[dt.datetime]:
        cursor = self.connection.execute(
            "SELECT timestamp FROM Accesses WHERE item_key=? ORDER BY timestamp DESC LIMIT 1",
            (item_key.as_base64,),
        )
        if (row := cursor.fetchone()) is None:
            return None
        else:
            return dt.datetime.fromtimestamp(row[0])

    @overrides
    def remove_item(self, item_key: EntityHash, remove_history: bool = True) -> True:
        self.connection.execute(
            "DELETE FROM Objects WHERE item_key=?", (item_key.as_base64,)
        )

        self.connection.execute(
            "DELETE FROM StoredItems WHERE item_key=?", (item_key.as_base64,)
        )

        if remove_history:
            self.connection.execute(
                "DELETE FROM Accesses WHERE item_key=?", (item_key.as_base64,)
            )
        return True

    @overrides
    def commit(self):
        self.connection.commit()

    @property
    @overrides
    def config(self) -> ModelCacheManagerConfig:
        settings = self._get_settings()
        return ModelCacheManagerConfig(
            cost_of_minute_compute_rel_to_cost_of_1GB=float(
                settings.get("cost_of_minute_compute_rel_to_cost_of_1GB", 10.0)
            ),
            reserved_free_space=float(settings.get("reserved_free_space", 1.0)),
            half_life_of_cache=float(settings.get("half_life_of_cache", 24.0)),
            utility_of_1GB_free_space=float(
                settings.get("utility_of_1GB_free_space", 2.0)
            ),
            marginal_relative_utility_at_1GB=float(
                settings.get("marginal_relative_utility_at_1GB", 1.0)
            ),
        )

    @overrides
    def store_config(self, options: ModelCacheManagerConfig, table_init: bool = False):
        settings_dict = {
            "cost_of_minute_compute_rel_to_cost_of_1GB": str(
                options.cost_of_minute_compute_rel_to_cost_of_1GB
            ),
            "reserved_free_space": str(options.reserved_free_space),
            "half_life_of_cache": str(options.half_life_of_cache),
            "utility_of_1GB_free_space": str(options.utility_of_1GB_free_space),
            "marginal_relative_utility_at_1GB": str(
                options.marginal_relative_utility_at_1GB
            ),
        }
        if table_init:
            self._new_settings(settings_dict)
        else:
            self._put_settings(settings_dict)

    @overrides
    def iterate_items(self) -> Iterator[DC_CacheItem]:
        cursor = self.connection.execute(
            "SELECT item_key, compute_time, weight FROM Objects"
        )
        for item_key, compute_time, weight in cursor:
            item_key = EntityHash.FromBase64(item_key)
            ans = DC_CacheItem(
                item_key=item_key,
                compute_time=dt.timedelta(seconds=compute_time),
                weight=weight,
                stored_items=self.get_stored_items(item_key),
            )
            yield ans

    # noinspection SqlWithoutWhere
    @overrides
    def clear_items(self):
        self.connection.execute("DELETE FROM Objects")
        self.connection.execute("DELETE FROM Accesses")
        self.connection.commit()

    @overrides
    def close(self):
        if self.connection is not None:
            self.connection.close()

    @overrides
    def add_file_to_item(self, item_key: EntityHash, storage_key: ItemID):
        # Adds item to the StoredItems table
        assert item_key is not None
        assert storage_key is not None

        if isinstance(storage_key, Path):
            storage_key = str(storage_key)
        else:
            storage_key = storage_key.serialize()

        self.connection.execute(
            "INSERT INTO StoredItems (item_key, item_storage_key) VALUES (?, ?)",
            (item_key.as_base64, storage_key),
        )
