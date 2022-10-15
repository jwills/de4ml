import graphlib
import json
import os
from typing import List, Dict

from fastapi.testclient import TestClient

from app.main import app
from app.lib.jsonschema import get_app_defs

working_dir = os.path.dirname(os.path.abspath(__file__))


def to_primitive_type(config_type: str, array_depth: int) -> str:
    base_type = None
    if config_type == "string":
        base_type = "STRING"
    elif config_type == "integer":
        base_type = "BIGINT"
    elif config_type == "number":
        base_type = "DOUBLE"
    elif config_type == "boolean":
        base_type = "BOOLEAN"
    elif config_type == "object":
        base_type = "JSON"
    else:
        raise Exception(f"Unknown type {config_type}")
    return base_type + "[]" * array_depth


def to_duckdb_fields(
    field: str, config: Dict, schemas: Dict, array_depth: int = 0
) -> List[str]:
    ret = []
    if "$ref" in config:
        schema_ref = config["$ref"].split("/")[-1]
        schema = schemas[schema_ref]
        for subfield, subconfig in schema["properties"].items():
            ret.extend(to_duckdb_fields(f"{field}__{subfield}", subconfig, schemas))
    elif config["type"] == "array":
        array_config = config["items"]
        if "$ref" in array_config:
            schema_ref = array_config["$ref"].split("/")[-1]
            schema = schemas[schema_ref]
            for subfield, subconfig in schema["properties"].items():
                ret.extend(
                    to_duckdb_fields(
                        f"{field}__{subfield}", subconfig, schemas, array_depth + 1
                    )
                )
        else:
            ret.extend(to_duckdb_fields(field, array_config, schemas, array_depth + 1))
    else:
        duckdb_type = to_primitive_type(config["type"], array_depth)
        field_value = f"{field} {duckdb_type}"
        ret.append(field_value)
    return ret


def gen_create_table(table_name: str, schema_name: str, schemas: Dict) -> str:
    schema = schemas[schema_name]
    assert schema["type"] == "object"
    init_fields = []
    for field, config in schema["properties"].items():
        init_fields.extend(to_duckdb_fields(field, config, schemas))
    field_defs = ",\n\t".join(init_fields)
    return f"CREATE TABLE {table_name} (\n\t{field_defs}\n);"


def gen_alter_table(
    table_name: str, table_diff_schema: Dict, schema_diffs: Dict
) -> List[str]:
    added_fields = []
    for field, config in table_diff_schema["properties"].items():
        added_fields.extend(to_duckdb_fields(field, config, schema_diffs))
    return [f"ALTER TABLE {table_name} ADD COLUMN {field};" for field in added_fields]


def main():
    client = TestClient(app)
    json_schema = client.get("/openapi.json").json()
    app_defs = get_app_defs(json_schema)
    openapi_file = os.path.join(working_dir, "openapi.json")
    init_sql_file = os.path.join(working_dir, "app/config/init.sql")

    write_mode = "w"
    existing_app_defs = {"tables": {}, "schemas": {}}
    if os.path.exists(openapi_file):
        existing_json_schema = json.load(open(openapi_file))
        existing_app_defs = get_app_defs(existing_json_schema)
        write_mode = "a"

    # Create/modify the init.sql config file
    with open(init_sql_file, write_mode) as f:
        schema_diffs = {}
        # We need to walk the schemas in topological sort order so that
        # we can be sure that the diffs account for dependencies between the
        # schemas
        for s in graphlib.TopologicalSorter(app_defs["schema_deps"]).static_order():
            if s in existing_app_defs["schemas"]:
                existing_schema = existing_app_defs["schemas"][s]
                new_schema = app_defs["schemas"][s]
                schema_diffs[s] = {"properties": {}}
                for field, config in new_schema["properties"].items():
                    if field not in existing_schema["properties"]:
                        schema_diffs[s]["properties"][field] = config
                    elif "$ref" in config:
                        refd_schema = config["$ref"].split("/")[-1]
                        if len(schema_diffs[refd_schema]["properties"]) > 0:
                            schema_diffs[s]["properties"][field] = config
                    elif config["type"] == "array" and "$ref" in config["items"]:
                        refd_schema = config["items"]["$ref"].split("/")[-1]
                        if len(schema_diffs[refd_schema]["properties"]) > 0:
                            schema_diffs[s]["properties"][field] = config
        for t in app_defs["tables"]:
            table_schema = app_defs["tables"][t]
            if t not in existing_app_defs["tables"]:
                f.write(gen_create_table(t, table_schema, app_defs["schemas"]))
                f.write("\n\n")
            elif len(schema_diffs[table_schema]["properties"]) > 0:
                for alter_tbl in gen_alter_table(
                    t, schema_diffs[table_schema], schema_diffs
                ):
                    f.write(alter_tbl)
                    f.write("\n")
                f.write("\n")

    # Finally, write the openapi.json file
    with open(openapi_file, "w") as f:
        json.dump(json_schema, f, indent=4)


if __name__ == "__main__":
    main()
