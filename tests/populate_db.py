from CacheManager import SQLitePersistentDB
from pathlib import Path


def populate_db(db_path: Path):
    db = SQLitePersistentDB(db_path, itemID_type=Path)
    db.commit()


if __name__ == "__main__":
    populate_db(Path(__file__).parent / "metadata_test.db")
