"""Terra upload utilities."""

import logging
import re
from pathlib import Path
from typing import Optional

from ops_utils.terra_util import TerraWorkspace

from csv_schemas import CSV_SCHEMA_MAP
from csv_schemas.sub_models import ResearcherProjectMetadata
from transformation.column_order import TABLE_COLUMN_ORDER

# Matches the dynamic metadata filename and table name, e.g.
# researcher_id_62_project_id_115_metadata.csv / researcher_id_62_project_id_115_metadata_table
_METADATA_CSV_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata\.csv$")
_METADATA_TABLE_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata_table$")


class TerraUploader:
    """Builds Terra table payloads and uploads them with batch upsert."""

    def __init__(self, request_util):
        self.request_util = request_util

    @staticmethod
    def get_table_name(csv_path: str) -> str:
        """Return the Terra table name for a CSV path."""
        return f"{Path(csv_path).stem}_table"

    @staticmethod
    def get_table_id_column(table_name: str) -> str:
        """Return the Terra row id column name for a Terra table."""
        return f"{table_name}_id"

    @staticmethod
    def get_model_for_filename(filename: str):
        """Return the pydantic model used to validate and coerce a CSV row before upload."""
        if _METADATA_CSV_PATTERN.match(filename):
            return ResearcherProjectMetadata
        return CSV_SCHEMA_MAP.get(filename)

    @classmethod
    def convert_csv_rows_to_table_data(
        cls,
        csv_path: str,
        file_contents: list[dict],
    ) -> dict[str, dict[str, object]]:
        """
        Convert one CSV's row dictionaries into the batch-upsert payload Terra expects.

        Each row gets a synthetic row id column named ``{table_name}_id`` whose value
        is the 1-based row number from the source CSV.
        """
        filename = Path(csv_path).name
        table_name = cls.get_table_name(csv_path)
        table_id_column = cls.get_table_id_column(table_name)
        model = cls.get_model_for_filename(filename)

        row_data = []
        for row_num, row in enumerate(file_contents, start=1):
            # Convert data to match schema set in csv_schemas
            converted_row = model(**row).model_dump(mode="python")

            row_data.append(
                {
                    table_id_column: str(row_num),
                    **converted_row,
                }
            )

        return {
            table_name: {
                "table_id_column": table_id_column,
                "row_data": row_data,
            }
        }

    @classmethod
    def create_sequencing_files_table_data(
        cls,
        participant_files: dict[str, dict[str, Optional[str]]],
    ) -> dict[str, dict[str, object]]:
        """Create the batch-upsert payload for the sequencing files table."""
        table_name = "sequencing_files_table"
        table_id_column = cls.get_table_id_column(table_name)
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

    @staticmethod
    def set_column_order_for_uploaded_tables(workspace: TerraWorkspace, table_names: list[str]) -> None:
        """Set the Terra column order for all uploaded tables in a single call."""
        column_order_dict = {}
        for table_name in table_names:
            stem = table_name.removesuffix("_table")
            if stem in TABLE_COLUMN_ORDER:
                column_order_dict[table_name] = TABLE_COLUMN_ORDER[stem]
            elif _METADATA_TABLE_PATTERN.match(table_name):
                column_order_dict[table_name] = TABLE_COLUMN_ORDER["researcher_project_metadata"]
            else:
                logging.warning(
                    f"No column order defined for table '{table_name}' — skipping column order for this table"
                )

        if column_order_dict:
            logging.info(
                f"Setting column order for {len(column_order_dict)} table(s) in workspace '{workspace.workspace_name}'"
            )
            workspace.set_table_column_order(column_order=column_order_dict)

    def upload_table_data_to_workspace(
        self,
        workspace: TerraWorkspace,
        table_data: dict[str, dict[str, object]],
    ) -> bool:
        """Upload all table data to a workspace with a single batch upsert call."""
        logging.info(
            f"Uploading {len(table_data)} table(s) to workspace '{workspace.workspace_name}' with batch upsert"
        )
        workspace.upload_metadata_with_batch_upsert(table_data=table_data)
        self.set_column_order_for_uploaded_tables(workspace, list(table_data.keys()))
        return True
