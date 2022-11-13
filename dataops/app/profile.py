import json
import yaml
from pathlib import Path

from datamodel_code_generator import InputFileType, generate
import duckdb

from app import constants


def to_json_type(duckdb_type: str) -> str:
    """Convert a duckdb type to a json schema type."""
    if duckdb_type == "INTEGER":
        return "integer"
    elif duckdb_type == "DOUBLE":
        return "number"
    else:
        raise ValueError(f"Unknown type: {duckdb_type}")


def to_json_schema(title, summary_table) -> dict:
    """Convert a duckdb SUMMARIZE result to a JSON Schema."""
    res = {"type": "object", "title": title}
    props = {}
    required_fields = []
    for row in summary_table:
        col_type = row["column_type"]
        prop = {"type": to_json_type(col_type)}
        if row["null_percentage"] == "0.0%":
            required_fields.append(row["column_name"])
        if col_type in ("INTEGER", "DOUBLE"):
            prop["minimum"] = row["min"]
            prop["maximum"] = row["max"]
        props[row["column_name"]] = prop

    res["properties"] = props
    if required_fields:
        res["required"] = required_fields
    return res


if __name__ == "__main__":

    conn = duckdb.connect(constants.DUCKDB_FILE)
    curr = conn.cursor()
    curr.execute(f"SUMMARIZE {constants.DATA_TABLE}")
    cols = [x[0] for x in curr.description]
    summary = [dict(zip(cols, row)) for row in curr.fetchall()]
    conn.close()

    # Generate a JSON schema to use for type and domain validation
    json_schema = to_json_schema("AgrawalRequest", summary)
    generate(
        json.dumps(json_schema),
        input_file_type=InputFileType.JsonSchema,
        input_filename="agrawal.json",
        output=Path("app") / "contracts.py",
        field_constraints=True,
    )

    alert_rules = []
    for name, prop in json_schema["properties"].items():
        if "minimum" in prop or "maximum" in prop:
            alert_rules.append(
                {
                    "alert": f"AgrawalRequest_{name}",
                    "expr": f'increase({constants.VALIDATION_COUNTER}_total{{loc="{name}"}}[1m]) > 0',
                    "for": "0m",
                    "labels": {"severity": "warning"},
                    "annotations": {
                        "summary": f"DQ Violation {name}",
                        "description": f"Data quality rule violation for {name}",
                    },
                }
            )
    group = {"name": "agrawal", "rules": alert_rules}
    with open("promconfig/data_quality_rules.yml", "w") as f:
        yaml.dump({"groups": [group]}, f)
