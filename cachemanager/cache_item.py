from pydantic import BaseModel, PositiveFloat
from pathlib import Path
from entityhash import EntityHash
from .pretty_path import shorten_path
from humanize import naturalsize, naturaldelta


class CacheItem(BaseModel):
    hash: EntityHash
    filename: Path
    compute_time: PositiveFloat  # in minutes
    size: PositiveFloat  # in GB
    weight: PositiveFloat

    def __init__(
        self,
        hash: str,
        filename: Path,
        compute_time: float,
        weight: float = 1.0,
        size: float = None,
    ):
        if size is None:
            size = filename.stat().st_size / 1073741824.0
        super().__init__(
            hash=hash,
            filename=filename,
            compute_time=compute_time,
            weight=weight,
            size=size,
        )

    def __str__(self):
        ans = ""
        file_len = len(self.filename.name)
        ans += f"{shorten_path(self.filename.absolute(), 30+file_len)}:\n"
        ans += f" object hash={self.hash}\n"
        ans += f" object size={naturalsize(self.size *1024*1024*1024) }\n"
        ans += f" compute time={naturaldelta(self.compute_time*60, months=False, minimum_unit="microsecond")}\n"
        if self.weight != 1.0:
            ans += f" weight={self.weight}\n"

        return ans
