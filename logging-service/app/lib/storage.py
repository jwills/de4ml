import json
import sqlite3
import threading
import time
from typing import Optional


class Storage:
    def __init__(self, tables: Optional[list] = None):
        self.path = None
        self.db = None
        self.tables = tables or []
        self.lock = threading.Lock()

    def is_open(self) -> bool:
        return self.db is not None

    def open(self, new_file_path: str):
        last_path = self.path
        with self.lock:
            if self.db:
                self.db.close()
            self.db = sqlite3.connect(new_file_path, check_same_thread=False)
            for t in self.tables:
                self.db.execute(f"CREATE TABLE IF NOT EXISTS {t} (ts int, data text)")
            self.path = new_file_path
        return last_path

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
