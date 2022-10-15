import json
from typing import Dict, List

import duckdb

from lib import jsonschema


def etl(sqlite3_db: str, table: str, columns: List[str], json_schema: Dict):
    app_defs = jsonschema.get_app_defs(json_schema)
    table_schema = app_defs["tables"][table]
    structure = jsonschema.to_structure(table_schema, app_defs["schemas"])
    ddb = duckdb.connect(":memory:")  # TODO: fix me
    for ext in ("sqlite_scanner", "json"):
        ddb.install_extension(ext)
        ddb.load_extension(ext)
    ddb.execute(f"CALL sqlite_attach('{sqlite3_db}')")
    print(structure.to_json())
    ddb.execute(
        f"""
        CREATE VIEW {table}_parsed AS
        SELECT from_json(data, '{structure.to_json()}') AS d
        FROM {table}
    """
    )

    select = []
    for col in columns:
        if col in structure:
            select.append(f"d.{col} as {col}")
        else:
            pieces = col.split("__")
            expr, ddbt = f"d.{pieces[0]}", structure.fields[pieces[0]]
            for i in range(1, len(pieces) - 1):
                if isinstance(ddbt, jsonschema.StructType):
                    expr += f".{pieces[i]}"
                    ddbt = ddbt.fields[pieces[i]]
                elif isinstance(ddbt, jsonschema.ArrayType):
                    expr = f"list_transform({expr}, x -> x.{pieces[i]})"
                    ddbt = ddbt.element_type
                else:
                    raise Exception(f"Found unknown type {ddbt} for {col}")
            expr += f".{pieces[-1]}"
            select.append(f"{expr} as {col}")

    gen_parquet = (
        f"COPY (SELECT {', '.join(select)} FROM {table}_parsed) TO '{table}.parquet'"
    )
    print(gen_parquet)
    ddb.execute(gen_parquet)
    ddb.close()
