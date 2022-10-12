import json
import os

from fastapi.testclient import TestClient

from app.main import app

working_dir = os.path.dirname(os.path.abspath(__file__))

def get_app_defs(json_schema):
    tabledefs = {}
    for path, path_item in json_schema["paths"].items():
        for method, method_item in path_item.items():
            if method.lower() == "post":
                table_name = path.split("/")[-1]
                schema_path = method_item["requestBody"]["content"]["application/json"]["schema"]["$ref"]
                tabledefs[table_name] = schema_path

    clean_schemas = json_schema["components"]["schemas"]
    if "HTTPValidationError" in clean_schemas:
        del clean_schemas["HTTPValidationError"]
    if "ValidationError" in clean_schemas:
        del clean_schemas["ValidationError"]
    return {"tables": tabledefs, "schemas": clean_schemas}


def to_primitive_type(config_type, array_depth: int):
    base_type = None
    if config_type == "string":
        base_type = "STRING"
    elif config_type == "integer":
        base_type = "BIGINT"
    elif config_type == "number":
        base_type = "DOUBLE"
    elif config_type == "boolean":
        base_type = "BOOLEAN"
    else:
        raise Exception(f"Unknown type {config_type}")
    return base_type + "[]" * array_depth

def to_duckdb_fields(field, config, schemas, array_depth: int = 0):
    ret = []
    if "$ref" in config:
        schema_ref = config["$ref"].split("/")[-1]
        schema = schemas[schema_ref]
        assert schema["type"] == "object"
        for subfield, subconfig in schema["properties"].items():
            ret.extend(to_duckdb_fields(f"{field}__{subfield}", subconfig, schemas))
    elif config["type"] == "array":
        array_config = config["items"]
        if "$ref" in array_config:
            schema_ref = array_config["$ref"].split("/")[-1]
            schema = schemas[schema_ref]
            assert schema["type"] == "object"
            for subfield, subconfig in schema["properties"].items():
                ret.extend(to_duckdb_fields(f"{field}__{subfield}", subconfig, schemas, array_depth + 1))
        else:
            ret.extend(to_duckdb_fields(field, array_config, schemas, array_depth + 1))
    else:
        duckdb_type = to_primitive_type(config["type"], array_depth)
        field_value = f"{field} {duckdb_type}"
        ret.append(field_value)
    return ret

def gen_create_table(table_name, schema_name, schemas):
    schema_name = schema_name.split("/")[-1]
    schema = schemas[schema_name]
    assert schema["type"] == "object"
    init_fields = []
    for field, config in schema["properties"].items():
        init_fields.extend(to_duckdb_fields(field, config, schemas))
    field_defs = ",\n\t".join(init_fields)
    return f"CREATE TABLE {table_name} (\n\t{field_defs}\n);"

def main():
    client = TestClient(app)
    json_schema = client.get("/openapi.json").json()
    app_defs = get_app_defs(json_schema)
    openapi_file = os.path.join(working_dir, "openapi.json")
    if not os.path.exists(openapi_file):
        # create a default init sql file as well with only create table definitions
        init_sql = os.path.join(working_dir, "app/config/init.sql")
        with open(init_sql, "w") as f:
            for t in app_defs["tables"]:
                f.write(gen_create_table(t, app_defs["tables"][t], app_defs["schemas"]))
                f.write("\n\n")
        with open(openapi_file, "w") as f:
            json.dump(json_schema, f, indent=4)


if __name__ == '__main__':
    main()