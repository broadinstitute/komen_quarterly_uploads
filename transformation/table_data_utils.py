"""Utilities for building and normalizing Terra table data from CSV rows."""

import re
from pathlib import Path
from typing import Optional

from csv_schemas import CSV_SCHEMA_MAP
from csv_schemas.sub_models import ResearcherProjectMetadata

# Matches the dynamic metadata filename, e.g. researcher_id_62_project_id_115_metadata.csv
_METADATA_CSV_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata\.csv$")


def get_table_name(csv_path: str) -> str:
    """Return the Terra table name for a CSV path."""
    return f"{Path(csv_path).stem}_table"


def get_table_id_column(table_name: str) -> str:
    """Return the Terra row id column name for a Terra table."""
    return f"{table_name}_id"


def get_model_for_filename(filename: str):
    """Return the pydantic model used to validate and coerce a CSV row."""
    if _METADATA_CSV_PATTERN.match(filename):
        return ResearcherProjectMetadata
    return CSV_SCHEMA_MAP.get(filename)


def normalize_row_for_table_upload(row: dict, filename: str) -> dict:
    """Validate and coerce a CSV row using the schema mapped to the filename."""
    model = get_model_for_filename(filename)
    return model(**row).model_dump(mode="python")


def convert_csv_rows_to_table_data(
    csv_path: str,
    file_contents: list[dict],
) -> dict[str, dict[str, object]]:
    """
    Convert one CSV's row dictionaries into the batch-upsert payload Terra expects.

    Each row gets a synthetic row id column named ``{table_name}_id`` whose value
    is the 1-based row number from the source CSV.

    Returns an empty dict if file_contents has no rows so that callers never
    create or expect a table for a CSV that contains only a header.
    """
    if not file_contents:
        return {}

    filename = Path(csv_path).name
    table_name = get_table_name(csv_path)
    table_id_column = get_table_id_column(table_name)

    row_data = []
    for row_num, row in enumerate(file_contents, start=1):
        row_data.append(
            {
                table_id_column: str(row_num),
                **normalize_row_for_table_upload(row=row, filename=filename),
            }
        )
    return {
        table_name: {
            "table_id_column": table_id_column,
            "row_data": row_data,
        }
    }


def create_sequencing_files_table_data(
    participant_files: dict[str, dict[str, Optional[str]]],
) -> dict[str, dict[str, object]]:
    """Create the batch-upsert payload for the sequencing files table."""
    table_name = "sequencing_files_table"
    table_id_column = get_table_id_column(table_name)
    file_columns = [
        "cram",
        "crai",
        "cram_md5",
        "gvcf",
        "gvcf_tbi",
        "vcf",
        "vcf_md5",
        "vcf_tbi",
        "mapping_metrics",
        "coverage_metrics",
        "vc_metrics",
    ]

    row_data = []
    for row_num, (participant_id, files) in enumerate(sorted(participant_files.items()), start=1):
        row = {
            table_id_column: str(row_num),
            "participant_id": participant_id,
        }
        for file_column in file_columns:
            row[file_column] = files.get(file_column) or "NA"
        row_data.append(row)

    return {
        table_name: {
            "table_id_column": table_id_column,
            "row_data": row_data,
        }
    }
