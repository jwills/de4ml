import graphlib
import os
from typing import List, Dict

from fastapi.testclient import TestClient

from app.api import app
from app.lib.jsonschema import AppDefs

working_dir = os.path.dirname(os.path.abspath(__file__))


def gen_columns(schema: dict, schemas: dict, prefix: str = "") -> List[str]:
    ret = []
    for field, config in schema["properties"].items():
        prefixed = prefix + field
        if "$ref" in config:
            refd_schema = config["$ref"].split("/")[-1]
            ret.extend(
                gen_columns(schemas[refd_schema], schemas, prefix=prefixed + "__")
            )
        elif config["type"] == "array" and "$ref" in config["items"]:
            refd_schema = config["items"]["$ref"].split("/")[-1]
            ret.extend(
                gen_columns(schemas[refd_schema], schemas, prefix=prefixed + "__")
            )
        else:
            ret.append(prefixed)
    return ret


def main():
    client = TestClient(app)
    json_schema = client.get("/openapi.json").json()
    app_defs = AppDefs.from_json_schema(json_schema)
    existing_app_defs = AppDefs.get_current()

    schema_diffs = {}
    # We need to walk the schemas in topological sort order so that
    # we can be sure that the diffs account for dependencies between the
    # schemas
    for s in graphlib.TopologicalSorter(app_defs.schema_deps).static_order():
        if s in existing_app_defs.schemas:
            existing_schema = existing_app_defs.schemas[s]
            new_schema = app_defs.schemas[s]
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

    for t in app_defs.tables:
        table_schema = app_defs.get_schema_name(t)
        columns, write_mode = [], "w"
        if t not in existing_app_defs.tables:
            columns = gen_columns(app_defs.schemas[table_schema], app_defs.schemas)
        elif len(schema_diffs[table_schema]["properties"]) > 0:
            columns = gen_columns(schema_diffs[table_schema], schema_diffs)
            write_mode = "a"
        if columns:
            with open(working_dir + f"/app/config/{t}_columns.csv", write_mode) as f:
                f.write("\n".join(columns))
                f.write("\n")

    # Finally, write the openapi.json file
    AppDefs.save_as_current(json_schema)


if __name__ == "__main__":
    main()
