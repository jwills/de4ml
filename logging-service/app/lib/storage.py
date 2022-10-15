import json
import sqlite3
import threading
import time
from typing import Optional


class Storage:
    def __init__(self, data_dir: Optional[str] = None):
        if data_dir is None:
            self.db = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            # TODO: make a file in data dir
            self.db = sqlite3.connect(data_dir, check_same_thread=False)
        self.lock = threading.Lock()

    def declare(self, table: str):
        cursor = self.db.cursor()
        with self.lock:
            cursor.execute(f"CREATE TABLE {table} (ts INT, data TEXT)")
            self.db.commit()

    def write(self, table: str, data: str):
        cursor = self.db.cursor()
        with self.lock:
            cursor.execute(
                f"INSERT INTO {table} (ts, data) VALUES (?, ?)",
                (int(time.time() * 1e6), data),
            )
            self.db.commit()

    def fetch(self, table: str, limit: int = 10):
        rows = []
        with self.lock:
            cursor = self.db.cursor().execute(
                f"SELECT ts, data FROM {table} LIMIT {limit}"
            )
            rows = cursor.fetchall()
        ret = []
        for row in rows:
            data = json.loads(row[1])
            data["__ts"] = row[0]
            ret.append(data)
        return ret
