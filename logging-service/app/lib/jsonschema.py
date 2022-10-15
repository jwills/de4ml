import json
from typing import Dict


def get_app_defs(json_schema: Dict) -> Dict:
    tabledefs = {}
    for path, path_item in json_schema["paths"].items():
        for method, method_item in path_item.items():
            if method.lower() == "post":
                table_name = path.split("/")[-1]
                schema_path = method_item["requestBody"]["content"]["application/json"][
                    "schema"
                ]["$ref"]
                tabledefs[table_name] = schema_path.split("/")[-1]

    schemas = json_schema["components"]["schemas"]

    # Remove these as they are only for FastAPI's use
    if "HTTPValidationError" in schemas:
        del schemas["HTTPValidationError"]
    if "ValidationError" in schemas:
        del schemas["ValidationError"]

    schema_deps = {}
    for schema, schema_config in schemas.items():
        schema_deps[schema] = set()
        for field_config in schema_config["properties"].values():
            if "$ref" in field_config:
                ref = field_config["$ref"].split("/")[-1]
                schema_deps[schema].add(ref)
            elif field_config["type"] == "array":
                if "$ref" in field_config["items"]:
                    ref = field_config["items"]["$ref"].split("/")[-1]
                    schema_deps[schema].add(ref)
    return {"tables": tabledefs, "schemas": schemas, "schema_deps": schema_deps}


class DuckDBType:
    pass


class StructType(DuckDBType):
    def __init__(self, fields: Dict[str, DuckDBType]):
        self.fields = fields

    def to_json(self) -> str:
        as_str = {}
        for f in self.fields:
            as_str[f] = str(self.fields[f])
        return json.dumps(as_str)

    def __str__(self):
        return f"STRUCT({', '.join(f'{k} {v}' for k, v in self.fields.items())})"


class ArrayType(DuckDBType):
    def __init__(self, element_type: DuckDBType):
        self.element_type = element_type

    def __str__(self):
        return str(self.element_type) + "[]"


class PrimitiveType(DuckDBType):
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return self.name


def to_field_type(config: Dict, schemas: Dict) -> DuckDBType:
    if "$ref" in config:
        ref = config["$ref"].split("/")[-1]
        return to_structure(ref, schemas)
    elif config["type"] == "array":
        array_config = config["items"]
        return ArrayType(to_field_type(array_config, schemas))
    else:
        name = None
        if "duckdb_type" in config:
            name = config["duckdb_type"]
        elif config["type"] == "object":
            return "JSON"
        elif config["type"] == "string":
            name = "VARCHAR"
        elif config["type"] == "integer":
            name = "BIGINT"
        elif config["type"] == "number":
            name = "DOUBLE"
        elif config["type"] == "boolean":
            name = "BOOLEAN"
        else:
            raise Exception(f"Unknown type {config['type']}")
        return PrimitiveType(name)


def to_structure(schema_name: str, schemas: Dict) -> StructType:
    schema = schemas[schema_name]
    assert schema["type"] == "object"
    structure = {}
    for field, config in schema["properties"].items():
        structure[field] = to_field_type(config, schemas)
    return StructType(structure)
