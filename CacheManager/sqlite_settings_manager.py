# Manages the sqlite instance that holds the settings and properties of the cache

import datetime as dt
import sqlite3
from pathlib import Path
from typing import Optional, Iterator
from overrides import overrides

from EntityHash import EntityHash
from .ifaces import (
    ModelCacheManagerConfig,
    I_SettingsManager,
    DC_CacheItem,
    I_AbstractItemID,
)
# from .cache_item import CacheItem


class SettingsManager[ItemID: (Path, I_AbstractItemID)](I_SettingsManager):
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

    def __init__(self, database_path: Path):
        self.database_path = database_path
        self._make_sure_db_exists()
        self._ensure_tables()

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
            hash TEXT PRIMARY KEY,
            filename TEXT,
            compute_time REAL,
            weight REAL,
            filesize REAL
        )
        """)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_filename ON Objects (filename)"
        )
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Accesses (
            hash TEXT,
            timestamp REAL,
            FOREIGN KEY (hash) REFERENCES Objects (hash)
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

        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        if populate_settings:
            self.store_config(ModelCacheManagerConfig())
        self.connection.commit()

    def _put_settings(self, settings: dict[str, str]):
        for key, value in settings.items():
            self.connection.execute(
                "UPDATE Settings SET value = ? WHERE key = ?", (value, key)
            )

    def _get_settings(self) -> dict[str, str]:
        cursor = self.connection.execute("SELECT key, value FROM Settings")
        return dict(cursor.fetchall())

    @overrides
    def add_object(self, object: DC_CacheItem):
        self.connection.execute(
            "INSERT INTO Objects (hash, filename, compute_time, weight, filesize) VALUES (?, ?, ?, ?, ?)",
            (
                object.hash.as_base64,
                str(object.serialized_filename),
                str(object.compute_time),
                str(object.weight),
                str(object.filesize),
            ),
        )

    @overrides
    def get_object_by_hash(self, hash: EntityHash) -> Optional[DC_CacheItem]:
        cursor = self.connection.execute(
            "SELECT filename, compute_time, weight, filesize FROM Objects WHERE hash=?",
            (hash.as_base64,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        filename, compute_time, weight, filesize = row

        if self.is_ItemID_Path():
            filename = Path(filename)
        else:
            filename = I_AbstractItemID.Unserialize(filename)

        return DC_CacheItem(
            hash=hash,
            filename=filename,
            compute_time=compute_time,
            filesize=filesize,
            weight=weight,
        )

    @overrides
    def get_object_by_filename(self, filename: ItemID) -> Optional[DC_CacheItem]:
        if isinstance(filename, Path):
            filename_str = str(filename)
        else:
            filename_str = filename.serialize()
        cursor = self.connection.execute(
            "SELECT hash, filename, compute_time, weight, filesize FROM Objects WHERE filename=?",
            (filename_str,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        hash, filename, compute_time, weight, filesize = row
        hash = EntityHash.FromBase64(hash)
        return DC_CacheItem(
            hash=hash,
            filename=Path(filename) / self.database_path,
            compute_time=compute_time,
            weight=weight,
            filesize=filesize,
        )

    @overrides
    def add_access_to_object(self, objectID: EntityHash, timestamp: dt.datetime):
        self.connection.execute(
            "INSERT INTO Accesses (hash, timestamp) VALUES (?, ?)",
            (objectID.as_base64, timestamp.timestamp()),
        )

    @overrides
    def get_accesses(self, hash: EntityHash) -> list[dt.datetime]:
        cursor = self.connection.execute(
            "SELECT timestamp FROM Accesses WHERE hash=?", (hash.as_base64,)
        )
        return [dt.datetime.fromtimestamp(row[0]) for row in cursor.fetchall()]

    @overrides
    def get_last_access(self, hash: EntityHash) -> Optional[dt.datetime]:
        cursor = self.connection.execute(
            "SELECT timestamp FROM Accesses WHERE hash=? ORDER BY timestamp DESC LIMIT 1",
            (hash.as_base64,),
        )
        return dt.datetime.fromtimestamp(cursor.fetchone())

    @overrides
    def remove_object(self, hash: EntityHash):
        self.connection.execute("DELETE FROM Objects WHERE hash=?", (hash.as_base64,))
        self.connection.execute("DELETE FROM Accesses WHERE hash=?", (hash.as_base64,))

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
    def store_config(self, options: ModelCacheManagerConfig):
        self._put_settings(
            {
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
        )

    @overrides
    def iterate_cacheitems(self) -> Iterator[DC_CacheItem]:
        cursor = self.connection.execute(
            "SELECT hash, filename, compute_time, weight, filesize FROM Objects"
        )
        for hash, filename, compute_time, weight, filesize in cursor:
            hash = EntityHash.FromBase64(hash)
            filename = self.database_path / Path(filename)
            ans = DC_CacheItem(
                hash=hash,
                filename=filename,
                compute_time=compute_time,
                weight=weight,
                filesize=filesize,
            )
            yield ans

    def clear_cacheitems(self):
        self.connection.execute("DELETE FROM Objects")
        self.connection.execute("DELETE FROM Accesses")
        self.connection.commit()
