import json
from typing import Dict, List

import duckdb

from app.lib import jsonschema


def etl(sqlite3_db: str, table: str, columns: List[str], json_schema: Dict):
    app_defs = jsonschema.get_app_defs(json_schema)
    table_schema = app_defs["tables"][table]
    structure = jsonschema.to_structure(table_schema, app_defs["schemas"])
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
    for col in columns:
        if col in structure.fields:
            select.append(f"d.{col} as {col}")
        else:
            # create an extractor macro
            pieces = col.split("__")
            expr, ddbt = f"d.{pieces[0]}", structure.fields[pieces[0]]
            for i in range(1, len(pieces)):
                if isinstance(ddbt, jsonschema.StructType):
                    expr += f".{pieces[i]}"
                    ddbt = ddbt.fields[pieces[i]]
                elif isinstance(ddbt, jsonschema.ArrayType):
                    macro_name = f"extract_{col}_{i}(x)"
                    macro = f"CREATE MACRO {macro_name} AS x.{pieces[i]}"
                    ddb.execute(macro)
                    expr = f"list_transform({expr}, x -> {macro_name})"
                    ddbt = ddbt.element_type
                else:
                    expr += f".{pieces[i]}"
                    ddbt = None
            select.append(f"{expr} as {col}")

    gen_parquet = (
        f"COPY (SELECT {', '.join(select)} FROM {table}_parsed) TO '{table}.parquet'"
    )
    print(gen_parquet)
    ddb.execute(gen_parquet)
    ddb.close()
