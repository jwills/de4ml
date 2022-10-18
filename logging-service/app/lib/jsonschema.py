import json
import os
from typing import Dict, List, Optional


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


class AppDefs:
    @classmethod
    def get_current(cls) -> Optional["AppDefs"]:
        working_dir = os.path.dirname(os.path.realpath(__file__))
        openapi_path = f"{working_dir}/../config/openapi.json"
        if os.path.exists(openapi_path):
            with open(openapi_path) as f:
                json_schema = json.load(f)
                return cls.from_json_schema(json_schema)
        else:
            return AppDefs({}, {}, {})

    @classmethod
    def save_as_current(cls, json_schema: Dict):
        working_dir = os.path.dirname(os.path.realpath(__file__))
        with open(f"{working_dir}/../config/openapi.json", "w") as f:
            json.dump(json_schema, f, indent=4)

    @classmethod
    def from_json_schema(cls, json_schema: Dict) -> "AppDefs":
        tabledefs = {}
        for path, path_item in json_schema["paths"].items():
            for method, method_item in path_item.items():
                if method.lower() == "post":
                    table_name = path.split("/")[-1]
                    schema_path = method_item["requestBody"]["content"][
                        "application/json"
                    ]["schema"]["$ref"]
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
        return AppDefs(tabledefs, schemas, schema_deps)

    def __init__(
        self,
        tables: Dict[str, Dict],
        schemas: Dict[str, Dict],
        schema_deps: Dict[str, List[str]],
    ):
        self.tables = tables
        self.schemas = schemas
        self.schema_deps = schema_deps

    def get_schema_name(self, table_name: str) -> str:
        return self.tables[table_name]

    def to_structure(self, schema_name: str) -> StructType:
        schema = self.schemas[schema_name]
        assert schema["type"] == "object"
        structure = {}
        for field, config in schema["properties"].items():
            structure[field] = self.to_field_type(config)
        return StructType(structure)

    def to_field_type(self, config: Dict) -> DuckDBType:
        if "$ref" in config:
            ref = config["$ref"].split("/")[-1]
            return self.to_structure(ref)
        elif config["type"] == "array":
            array_config = config["items"]
            return ArrayType(self.to_field_type(array_config))
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
