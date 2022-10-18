import json
import os
import pathlib
import socket
import sqlite3
import threading
import time
from typing import List


def _get_db_filename() -> str:
    return f"{socket.gethostname()}_{int(time.time() * 1e6)}.db"


class Storage:
    _instance = None

    @classmethod
    def get(cls) -> "Storage":
        if not cls._instance:
            data_dir = pathlib.Path(os.getenv("DATA_DIR", "/tmp"))
            tables = os.getenv("TABLES", "searches,clicks").split(",")
            cls._instance = cls(data_dir, tables)
        return cls._instance

    def __init__(self, data_dir: pathlib.Path, tables: List[str]):
        self.path = data_dir / _get_db_filename()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.tables = tables
        for t in tables:
            self.db.execute(f"CREATE TABLE IF NOT EXISTS {t} (ts int, data text)")
        self.lock = threading.Lock()

    def close(self) -> str:
        with self.lock:
            self.db.close()
            self._instance = None
        return self.path

    def write(self, table: str, data: str):
        with self.lock:
            self.db.execute(
                f"INSERT INTO {table} (ts, data) VALUES (?, ?)",
                (int(time.time() * 1e6), data),
            )
            self.db.commit()

    def fetch(self, table: str, limit: int = 10):
        rows = []
        with self.lock:
            cursor = self.db.execute(f"SELECT ts, data FROM {table} LIMIT {limit}")
            rows = cursor.fetchall()
        ret = []
        for row in rows:
            data = json.loads(row[1])
            data["__ts"] = row[0]
            ret.append(data)
        return ret
