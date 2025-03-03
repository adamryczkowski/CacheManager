import datetime as dt
from pathlib import Path

from EntityHash import EntityHash
from humanize import naturalsize, naturaldelta
from pydantic import BaseModel, PositiveFloat

from .pretty_path import shorten_path


class CacheItem(BaseModel):
    hash: EntityHash
    filename: Path
    compute_time: PositiveFloat  # in minutes
    size: PositiveFloat  # in GB
    weight: PositiveFloat
    last_access_time: dt.datetime
    _utility: float

    @staticmethod
    def FilenameFromHash(
        hash: EntityHash,
        parent_dir: Path,
        file_extension: str,
        subfolder: Path = None,
        file_prefix: str = "",
    ):
        filename_length = 10
        if subfolder is None:
            subfolder = Path()
        basename = hash.as_base64[:filename_length].replace("/", "_").replace("+", "_")
        return parent_dir / subfolder / f"{file_prefix}{basename}.{file_extension}"

    def __init__(
        self,
        hash: EntityHash,
        filename: Path,
        compute_time: float,
        weight: float = 1.0,
        size: float = None,
        last_access_time: dt.datetime = None,
    ):
        if size is None:
            size = filename.stat().st_size / 1073741824.0
        if last_access_time is None:
            last_access_time = dt.datetime.now()
        super().__init__(
            hash=hash,
            filename=Path(filename.name),
            compute_time=compute_time,
            weight=weight,
            size=size,
            last_access_time=last_access_time,
        )
        self._utility = float("NaN")

    def __str__(self):
        ans = ""
        file_len = len(self.filename.name)
        ans += f"{shorten_path(self.filename.absolute(), 30 + file_len)}:\n"
        ans += f" object hash={self.hash}\n"
        ans += f" object size={self.pretty_size}\n"
        ans += f" compute time={self.pretty_compute_time}\n"
        ans += f" last accessed {naturaldelta(dt.datetime.now() - self.last_access_time, months=False, minimum_unit="seconds")} ago\n"
        if self.weight != 1.0:
            ans += f" weight={self.weight}\n"

        return ans

    def __repr__(self):
        return self.__str__()

    @property
    def age(self) -> float:
        """Age of the last access in hours"""
        return (dt.datetime.now() - self.last_access_time).total_seconds() / 60 / 60

    @property
    def utility(self) -> float:
        return self._utility

    @utility.setter
    def utility(self, value: float):
        self._utility = value

    def __lt__(self, other):
        return self.utility < other.utility

    def make_file_name(self, cache_dir: Path, extension: str = "bin") -> Path:
        return cache_dir / f"{self.hash.as_base64}.{extension}"

    @property
    def pretty_size(self) -> str:
        return naturalsize(self.size * 1024 * 1024 * 1024)

    @property
    def pretty_compute_time(self) -> str:
        return naturaldelta(
            self.compute_time * 60, months=False, minimum_unit="microseconds"
        )

    @property
    def exists_on_disk(self) -> bool:
        return self.filename.exists() and self.filename.stat().st_size > 0

    def verify_hash(self):
        if not self.exists_on_disk:
            return False
        if EntityHash.FromDiskFile(self.filename, "sha256") == self.hash:
            return True
        raise ResourceWarning("Hash mismatch")
