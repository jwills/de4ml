import os
from typing import Dict, List

import duckdb

from app.lib.jsonschema import AppDefs, StructType, ArrayType


def _columns_helper(table: str) -> List[str]:
    working_dir = os.path.dirname(os.path.realpath(__file__))
    with open(working_dir + f"/../config/{table}_columns.csv") as f:
        return f.read().splitlines()


def etl(sqlite3_db: str, table: str, output_dir: str):
    """ETLs the data from the SQLite3 database into DuckDB."""
    app_defs = AppDefs.current()
    structure = app_defs.to_structure(app_defs.get_schema_name(table))
    ddb = duckdb.connect(":memory:")  # TODO: fix me
    for ext in ("sqlite_scanner", "json", "parquet"):
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
    for col in _columns_helper(table):
        if col in structure.fields:
            select.append(f"d.{col} as {col}")
        else:
            # create an extractor macro
            pieces = col.split("__")
            expr, ddbt = f"d.{pieces[0]}", structure.fields[pieces[0]]
            for i in range(1, len(pieces)):
                if isinstance(ddbt, StructType):
                    expr += f".{pieces[i]}"
                    ddbt = ddbt.fields[pieces[i]]
                elif isinstance(ddbt, ArrayType):
                    # hack to get around https://github.com/duckdb/duckdb/issues/5005
                    macro_name = f"extract_{col}_{i}(x)"
                    macro = f"CREATE MACRO {macro_name} AS x.{pieces[i]}"
                    ddb.execute(macro)
                    expr = f"list_transform({expr}, x -> {macro_name})"
                    ddbt = ddbt.element_type
                else:
                    expr += f".{pieces[i]}"
                    ddbt = None
            select.append(f"{expr} as {col}")

    output_file = f"{output_dir}/{table}.parquet"
    ddb.execute(
        f"COPY (SELECT {', '.join(select)} FROM {table}_parsed) TO '{output_file}'"
    )
    ddb.close()
