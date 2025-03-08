from CacheManager import shorten_path
from pathlib import Path


def test1():
    p = Path("/home/user/.local/share/app/data/cache/file_0x1234567890abcdef")
    assert shorten_path(p, 500) == str(p)
    assert shorten_path(p, 20) == ".../file_0x1234567890abcdef"
    assert shorten_path(p, 30) == "/.../cache/file_0x1234567890abcdef"
    assert shorten_path(p, 40) == "/home/.../data/cache/file_0x1234567890abcdef"

    assert shorten_path(Path(), 100) == "."


if __name__ == "__main__":
    test1()
