from typing import Optional
import duckdb

def flatten(data: dict, prefix: str = "") -> dict:
    """Flatten a dict of dicts into a single dict"""
    flat = {}
    for key, value in data.items():
        if isinstance(value, dict):
            flat.update(flatten(value, prefix + key + "__"))
        elif isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], dict):
                nested = {}
                for item in value:
                    for k2, v2 in item.items():
                        if k2 not in nested:
                            nested[k2] = []
                        nested[k2].append(v2)
                for k2, v2 in nested.items():
                    flat[prefix + key + "__" + k2] = v2 
            else:
                flat[prefix + key] = value
        else:
            flat[prefix + key] = value
    return flat

class Storage:
    def __init__(self, data_dir: Optional[str] = None):
        if data_dir is None:
            self.db = duckdb.connect()
        else:
            # TODO: make a file in data dir
            self.db = duckdb.connect(data_dir)

    def intialize(self, init_sql_file: str):
        with open(init_sql_file, "r") as f:
            self.db.execute(f.read())

    def write(self, table: str, data: dict):
        fields, values = [], []
        for k, v in flatten(data).items():
            fields.append(k)
            values.append(v)
        sql = f"INSERT INTO {table} ({','.join(fields)}) VALUES ({','.join(['?'] * len(values))})"
        cursor = self.db.cursor()
        cursor.execute(sql, values)
        cursor.close()

    def query(self, q: str):
        cursor = self.db.cursor().execute(q)
        column_names = [d[0] for d in cursor.description]
        ret = []
        for row in cursor.fetchall():
            ret.append(dict(zip(column_names, row)))
        return ret
