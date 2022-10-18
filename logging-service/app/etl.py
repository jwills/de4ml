import os
import pathlib
import sys
from typing import List

import duckdb

from app.lib.jsonschema import AppDefs, StructType, ArrayType


def _columns_helper(table: str) -> List[str]:
    working_dir = os.path.dirname(os.path.realpath(__file__))
    with open(working_dir + f"/config/{table}_columns.csv") as f:
        return f.read().splitlines()


def etl(
    sqlite3_db: pathlib.Path, table: str, app_defs: AppDefs, output_dir: pathlib.Path
) -> pathlib.Path:
    """ETLs the data from the SQLite3 database table into a Parquet file using DuckDB."""

    # Open DuckDB, load the extensions we need and attach the SQLite3 database
    ddb = duckdb.connect(":memory:")  # TODO: fix me
    for ext in ("sqlite_scanner", "json", "parquet"):
        ddb.install_extension(ext)
        ddb.load_extension(ext)
    ddb.execute(f"CALL sqlite_attach('{sqlite3_db}')")

    # Get the DuckDB structure of the table from the app defs and define
    # a view for parsing the raw text into DuckDB JSON
    structure = app_defs.to_structure(app_defs.get_schema_name(table))
    ddb.execute(
        f"""
        CREATE VIEW {table}_parsed AS
        SELECT from_json(data, '{structure.to_json()}') AS d
        FROM {table}
    """
    )

    # Generates the SELECT statement we need to extract the flattened data
    # from the parsed JSON view
    select = []
    for col in _columns_helper(table):
        if col in structure.fields:
            # simple top-level field
            select.append(f"d.{col} as {col}")
        else:
            # complex nested field
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

    # Write the flattened JSON data out to a Parquet file
    output_file = output_dir / f"{table}.parquet"
    ddb.execute(
        f"COPY (SELECT {', '.join(select)} FROM {table}_parsed) TO '{output_file}'"
    )
    ddb.close()
    return output_file


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: python etl.py <sqlite3_db> <table> <output_dir>")
        sys.exit(1)

    sqldb, table, output_dir = (
        pathlib.Path(sys.argv[1]),
        sys.argv[2],
        pathlib.Path(sys.argv[3]),
    )
    app_defs = AppDefs.get_current()

    # ETL the data
    etl(sqlite3_db=sqldb, table=table, app_defs=app_defs, output_dir=output_dir)
