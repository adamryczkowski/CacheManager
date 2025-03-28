# Manages the sqlite instance that holds the settings and properties of the cache

import datetime as dt
import sqlite3
from builtins import issubclass
from pathlib import Path
from typing import Optional, Iterator

import numpy as np
from EntityHash import EntityHash
from ValueWithError import ValueWithError, make_ValueWithError_from_vector
from overrides import overrides
from pydantic import BaseModel

from .ifaces import (
    I_PersistentDB,
    DC_CacheItem,
    DC_StoredItem,
    StoredItemID,
    I_SerializationPerformanceModel,
    I_AbstractItemID,
)


class SerializationPerformance(BaseModel, I_SerializationPerformanceModel):
    serialization_time: ValueWithError  # pyright: ignore [reportIncompatibleMethodOverride]
    deserialization_time: ValueWithError  # pyright: ignore [reportIncompatibleMethodOverride]
    model_age: dt.datetime  # pyright: ignore [reportIncompatibleMethodOverride]
    sample_count: int  # pyright: ignore [reportIncompatibleMethodOverride]


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
    itemID_type: type[StoredItemID]

    def __init__(
        self,
        database_path: Path,
        itemID_type: type[StoredItemID] = Path,
    ):
        self.database_path = database_path
        self._make_sure_db_exists()
        self._ensure_tables()
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

    def _ensure_tables(self):
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Objects (
            item_key TEXT PRIMARY KEY,
            compute_time REAL,
            main_item_storage_key TEXT,
            weight REAL,
            serialization_class TEXT
        )
        """)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_main_item_storage_key ON Objects (main_item_storage_key)"
        )
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS StoredItems (
            item_storage_key TEXT PRIMARY KEY,
            tag VARCHAR(32),
            item_key TEXT,
            hash TEXT,
            filesize REAL
        )
        """)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS item_key ON StoredItems (item_key, tag)"
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

        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS SerializationStats (
            serialization_class VARCHAR(32),
            timestamp REAL,
            serialization_time REAL,
            deserialization_time REAL,
            serialized_size REAL,
            deserialized_size REAL,
            PRIMARY KEY (serialization_class, timestamp))
        """)

        # # Check if Settings table exists:
        # populate_settings = (
        #     self.connection.execute(
        #         "SELECT name FROM sqlite_master WHERE type='table' AND name='Settings'"
        #     ).fetchone()
        #     is None
        # )
        #
        # if not populate_settings:
        #     populate_settings = (
        #         self.connection.execute("SELECT key FROM Settings").fetchone() is None
        #     )
        #
        # self.connection.execute("""
        # CREATE TABLE IF NOT EXISTS Settings (
        #     key TEXT PRIMARY KEY,
        #     value TEXT
        # )
        # """)
        # if populate_settings:
        #     self.store_config(initial_config, table_init=True)
        self.connection.commit()

    # def _put_settings(self, settings: dict[str, str]):
    #     for key, value in settings.items():
    #         self.connection.execute(
    #             "UPDATE Settings SET value = ? WHERE key = ?", (value, key)
    #         )
    #
    # def _new_settings(self, settings: dict[str, str]):
    #     for key, value in settings.items():
    #         self.connection.execute(
    #             "INSERT INTO Settings (key, value) VALUES (?, ?)", (key, value)
    #         )

    @overrides
    def add_serialization_time(
        self,
        serialization_class: str,
        serialization_time: dt.timedelta,
        deserialization_time: dt.timedelta,
        serialized_size: int,
        object_size: int | None = None,
    ):
        if object_size is None:
            object_size = serialized_size
        self.connection.execute(
            "INSERT INTO SerializationStats (serialization_class, timestamp, serialization_time, deserialization_time, serialized_size, deserialized_size) VALUES (?, ?, ?, ?, ?, ?)",
            (
                serialization_class,
                dt.datetime.now().timestamp(),
                serialization_time.total_seconds(),
                deserialization_time.total_seconds(),
                serialized_size,
                object_size,
            ),
        )

    @overrides
    def get_serialization_statistics(
        self,
        serialization_class: str,
        last_n: int | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        min_time: dt.timedelta | None = None,
    ) -> I_SerializationPerformanceModel:
        cursor = self.connection.execute(
            "SELECT serialization_time, deserialization_time, serialized_size, deserialized_size, timestamp FROM SerializationStats WHERE serialization_class=?",
            (serialization_class,),
        )
        last_timestamp: dt.datetime = dt.datetime.now()
        deserialization_times_arr: list[float] = []
        serialization_times_arr: list[float] = []
        for rec in cursor.fetchall():
            (
                serialization_time,
                deserialization_time,
                serialized_size,
                deserialized_size,
                timestamp,
            ) = rec
            if last_timestamp > timestamp:
                last_timestamp = timestamp
            deserialization_times_arr.append(deserialization_time)
            serialization_times_arr.append(serialization_time)

        return SerializationPerformance(
            serialization_time=make_ValueWithError_from_vector(
                np.asarray(serialization_times_arr)
            ).get_ValueWithError(),
            deserialization_time=make_ValueWithError_from_vector(
                np.asarray(deserialization_times_arr)
            ).get_ValueWithError(),
            model_age=last_timestamp,
            sample_count=len(serialization_times_arr),
        )

    # def _get_settings(self) -> dict[str, str]:
    #     cursor = self.connection.execute("SELECT key, value FROM Settings")
    #     return dict(cursor.fetchall())

    @overrides
    def add_item(self, item: DC_CacheItem):
        if isinstance(main_item_storage_key := item.main_item_storage_key, Path):
            main_item_storage_key = str(main_item_storage_key)
        else:
            main_item_storage_key = main_item_storage_key.serialize()

        self.connection.execute(
            "INSERT INTO Objects (item_key, main_item_storage_key, compute_time, weight, serialization_class) VALUES (?, ?, ?, ?, ?)",
            (
                item.item_key.as_base64,
                main_item_storage_key,
                item.compute_time.total_seconds(),
                item.weight,
                item.serialization_performance_class,
            ),
        )

        for stored_item_key, stored_item in item.stored_items.items():
            if isinstance(stored_item_key, Path):
                item_storage_key_str = str(stored_item_key)
            else:
                assert isinstance(stored_item_key, I_AbstractItemID)
                item_storage_key_str = stored_item_key.serialize()
            self.connection.execute(
                "INSERT INTO StoredItems (item_key, tag, item_storage_key, hash, filesize) VALUES (?, ?, ?, ?, ?)",
                (
                    item.item_key.as_base64,
                    stored_item.tag,
                    item_storage_key_str,
                    stored_item.hash.as_base64,
                    stored_item.filesize,
                ),
            )

    @overrides
    def get_stored_items(
        self, item_key: EntityHash
    ) -> dict[StoredItemID, DC_StoredItem]:
        cursor = self.connection.execute(
            "SELECT item_storage_key, hash, filesize, tag FROM StoredItems WHERE item_key=?",
            (item_key.as_base64,),
        )
        ans = {}
        for row in cursor.fetchall():
            item_storage_key_str, hash_str, filesize, tag = row
            if issubclass(self.itemID_type, Path):
                item_storage_key: StoredItemID = Path(item_storage_key_str)
            else:
                item_storage_key: StoredItemID = self.itemID_type.Unserialize(
                    item_storage_key_str
                )
            hash_obj = EntityHash.FromBase64(hash_str)
            item = DC_StoredItem(
                filesize=filesize,
                item_store_key=item_storage_key,
                hash=hash_obj,
                tag=tag,
            )

            ans[item_storage_key] = item

        return ans

    @overrides
    def get_item_by_key(self, item_key: EntityHash) -> Optional[DC_CacheItem]:
        cursor = self.connection.execute(
            "SELECT compute_time, weight,main_item_storage_key, serialization_class FROM Objects WHERE item_key=?",
            (item_key.as_base64,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        compute_time, weight, main_item_storage_key, serialization_performance_class = (
            row
        )

        stored_items = self.get_stored_items(item_key)

        return DC_CacheItem(
            item_key=item_key,
            stored_items=stored_items,
            compute_time=dt.timedelta(seconds=compute_time),
            weight=weight,
            main_item_storage_key=main_item_storage_key,
            serialization_performance_class=serialization_performance_class,
        )

    @overrides
    def get_item_by_storage_key(
        self, storage_key: StoredItemID
    ) -> Optional[DC_CacheItem]:
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
    def remove_item(self, item_key: EntityHash, remove_history: bool = True) -> bool:
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

    @overrides
    def iterate_items(self) -> Iterator[DC_CacheItem]:
        cursor = self.connection.execute(
            "SELECT item_key, compute_time, weight, main_item_storage_key, serialization_class FROM Objects"
        )
        for (
            item_key,
            compute_time,
            weight,
            main_item_storage_key,
            serialization_performance_class,
        ) in cursor:
            item_key = EntityHash.FromBase64(item_key)
            ans = DC_CacheItem(
                item_key=item_key,
                compute_time=dt.timedelta(seconds=compute_time),
                weight=weight,
                stored_items=self.get_stored_items(item_key),
                main_item_storage_key=main_item_storage_key,
                serialization_performance_class=serialization_performance_class,
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
    def add_file_to_item(
        self,
        item_key: EntityHash,
        storage_key: StoredItemID,
        tag: str,
        item_hash: EntityHash,
        filesize: int,
    ):
        # Adds item to the StoredItems table
        if isinstance(storage_key, Path):
            storage_key_str = str(storage_key)
        else:
            storage_key_str = storage_key.serialize()

        self.connection.execute(
            "INSERT INTO StoredItems (item_storage_key, tag, item_key, hash, filesize) VALUES (?, ?, ?, ?, ?)",
            (storage_key_str, tag, item_key.as_base64, hash, filesize),
        )
