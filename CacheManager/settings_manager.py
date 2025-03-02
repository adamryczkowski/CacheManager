# Manages the sqlite instance that holds the settings and properties of the cache

import datetime as dt
import sqlite3
from pathlib import Path
from typing import Optional, Iterator

from EntityHash import EntityHash

from .cache_config import ModelCacheManagerOptions
from .cache_item import CacheItem


class SettingsManager:
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

    managed_directory: Path
    connection: sqlite3.Connection

    def __init__(self, managed_directory: Path):
        self.managed_directory = managed_directory
        self._make_sure_db_exists()
        self._ensure_tables()

    def _make_sure_db_exists(self):
        if self.managed_directory.exists():
            self.connection = sqlite3.connect(
                self.managed_directory / ".metadata.sqlite"
            )
        self.managed_directory.mkdir(exist_ok=True)
        self.connection = sqlite3.connect(self.managed_directory / ".metadata.sqlite")

    def __del__(self):
        self.connection.close()

    def _ensure_tables(self):
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Objects (
            hash TEXT PRIMARY KEY,
            filename TEXT,
            compute_time REAL,
            weight REAL,
            file_size REAL
        )
        """)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_filename ON Objects (filename)"
        )
        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Accesses (
            hash TEXT,
            timestamp INTEGER,
            FOREIGN KEY (hash) REFERENCES Objects (hash)
        )
        """)
        # Add index to the timestamp column
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_Accesses_timestamp ON Accesses (timestamp)"
        )

        self.connection.execute("""
        CREATE TABLE IF NOT EXISTS Settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        self.connection.commit()

    #
    # def put_option(self, name: ModelCacheOptionName, value: str):
    #     self.connection.execute("INSERT INTO Settings (key, value) VALUES (?, ?)", (name.value, value))
    #     self.connection.commit()

    def _put_settings(self, settings: dict[str, str]):
        for key, value in settings.items():
            self.connection.execute(
                "INSERT INTO Settings (key, value) VALUES (?, ?)", (key, value)
            )
        self.connection.commit()

    def _get_settings(self) -> dict[str, str]:
        cursor = self.connection.execute("SELECT key, value FROM Settings")
        return dict(cursor.fetchall())

    def put_object(self, object: CacheItem, add_access: bool = True):
        self.connection.execute(
            "INSERT INTO Objects (hash, filename, compute_time, weight, file_size) VALUES (?, ?, ?, ?, ?)",
            (
                object.hash.as_base64,
                str(object.filename.name),
                str(object.compute_time),
                str(object.weight),
                str(object.size),
            ),
        )
        if add_access:
            self.store_access(
                object.hash, int(dt.datetime.now().timestamp()), commit=False
            )
        self.connection.commit()

    def get_object_by_hash(self, hash: EntityHash) -> Optional[CacheItem]:
        cursor = self.connection.execute(
            "SELECT filename, compute_time, weight FROM Objects WHERE hash=?",
            (hash.as_base64,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        filename, compute_time, weight = row
        return CacheItem(
            hash=hash,
            filename=Path(filename) / self.managed_directory,
            compute_time=compute_time,
            weight=weight,
        )

    def get_object_by_filename(self, filename: Path) -> Optional[CacheItem]:
        cursor = self.connection.execute(
            "SELECT hash, filename, compute_time, weight, file_size FROM Objects WHERE filename=?",
            (str(filename.name),),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        hash, filename, compute_time, weight, file_size = row
        hash = EntityHash.FromBase64(hash)
        return CacheItem(
            hash=hash,
            filename=Path(filename) / self.managed_directory,
            compute_time=compute_time,
            weight=weight,
            size=file_size,
        )

    def get_object_compute_time_and_weight(
        self, hash: EntityHash
    ) -> Optional[tuple[float, float]]:
        cursor = self.connection.execute(
            "SELECT compute_time, weight FROM Objects WHERE hash=?", (hash.as_base64,)
        )
        return cursor.fetchone()

    def store_access(self, hash: EntityHash, timestamp: int, commit: bool = True):
        self.connection.execute(
            "INSERT INTO Accesses (hash, timestamp) VALUES (?, ?)",
            (hash.as_base64, timestamp),
        )
        if commit:
            self.connection.commit()

    def get_accesses(self, hash: EntityHash) -> list[int]:
        cursor = self.connection.execute(
            "SELECT timestamp FROM Accesses WHERE hash=?", (hash.as_base64,)
        )
        return [row[0] for row in cursor.fetchall()]

    def get_last_access(self, hash: EntityHash) -> Optional[int]:
        cursor = self.connection.execute(
            "SELECT timestamp FROM Accesses WHERE hash=? ORDER BY timestamp DESC LIMIT 1",
            (hash.as_base64,),
        )
        return cursor.fetchone()

    def remove_object(self, hash: EntityHash):
        self.connection.execute("DELETE FROM Objects WHERE hash=?", (hash.as_base64,))
        self.connection.execute("DELETE FROM Accesses WHERE hash=?", (hash.as_base64,))
        self.connection.commit()

    def get_options(self) -> ModelCacheManagerOptions:
        settings = self._get_settings()
        return ModelCacheManagerOptions(
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
            cache_dir=self.managed_directory,
        )

    def store_options(self, options: ModelCacheManagerOptions):
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

    def iterate_objects(self) -> Iterator[CacheItem]:
        cursor = self.connection.execute(
            "SELECT hash, filename, compute_time, weight, file_size FROM Objects"
        )
        for hash, filename, compute_time, weight, file_size in cursor:
            hash = EntityHash.FromBase64(hash)
            filename = self.managed_directory / Path(filename)
            ans = CacheItem(
                hash=hash,
                filename=filename,
                compute_time=compute_time,
                weight=weight,
                size=file_size,
            )
            yield ans

    def clear_objects(self):
        self.connection.execute("DELETE FROM Objects")
        self.connection.execute("DELETE FROM Accesses")
        self.connection.commit()
