"""Helpers for mapping source CSV files and Terra tables back to csv_schemas models."""

import re
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from csv_schemas import CSV_SCHEMA_MAP
from csv_schemas.sub_models import ResearcherProjectMetadata


_DYNAMIC_METADATA_CSV_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata\.csv$")
_DYNAMIC_METADATA_TABLE_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata_table$")


class CsvSchemaHelper:
    """Utility methods for schema-backed CSV files and their Terra table names."""

    @staticmethod
    def get_model_for_csv_filename(filename: str) -> Optional[type[BaseModel]]:
        """Return the pydantic model for a CSV filename, including dynamic metadata files."""
        if _DYNAMIC_METADATA_CSV_PATTERN.match(Path(filename).name):
            return ResearcherProjectMetadata
        return CSV_SCHEMA_MAP.get(Path(filename).name)

    @classmethod
    def get_model_for_table_name(cls, table_name: str) -> Optional[type[BaseModel]]:
        """Return the pydantic model for a Terra table name when one exists."""
        csv_filename = cls.table_name_to_csv_filename(table_name)
        if not csv_filename:
            return None
        return cls.get_model_for_csv_filename(csv_filename)

    @classmethod
    def get_expected_headers_for_csv_filename(cls, filename: str) -> list[str]:
        """Return model field names in schema order for a CSV filename."""
        model = cls.get_model_for_csv_filename(filename)
        return list(model.model_fields.keys()) if model else []

    @classmethod
    def get_expected_headers_for_table_name(cls, table_name: str) -> list[str]:
        """Return model field names in schema order for a Terra table name."""
        model = cls.get_model_for_table_name(table_name)
        return list(model.model_fields.keys()) if model else []

    @staticmethod
    def csv_filename_to_table_name(filename: str) -> str:
        """Convert a CSV filename like demographics.csv to demographics_table."""
        return f"{Path(filename).stem}_table"

    @staticmethod
    def table_name_to_csv_filename(table_name: str) -> Optional[str]:
        """Convert a Terra table name like demographics_table to demographics.csv."""
        if _DYNAMIC_METADATA_TABLE_PATTERN.match(table_name):
            return f"{table_name.removesuffix('_table')}.csv"
        if not table_name.endswith("_table"):
            return None
        return f"{table_name.removesuffix('_table')}.csv"

    @staticmethod
    def get_entity_column_name_for_table(table_name: str) -> str:
        """Return the Terra entity column name for a table."""
        return f"entity:{table_name}_id"

    @classmethod
    def build_expected_table_names(
        cls,
        csv_filenames: list[str],
        include_sequencing_table: bool = False,
        excluded_csv_filenames: Optional[set[str]] = None,
    ) -> list[str]:
        """Build expected Terra table names from CSV filenames."""
        excluded_csv_filenames = excluded_csv_filenames or set()
        table_names = [
            cls.csv_filename_to_table_name(Path(csv_filename).name)
            for csv_filename in csv_filenames
            if Path(csv_filename).name not in excluded_csv_filenames
        ]
        if include_sequencing_table:
            table_names.append("sequencing_files_table")
        return table_names

    @classmethod
    def get_main_expected_table_names(cls, main_dataset_files: list[str]) -> list[str]:
        """Return expected main-workspace table names."""
        return cls.build_expected_table_names(
            csv_filenames=main_dataset_files,
            include_sequencing_table=True,
        )

    @classmethod
    def get_sub_expected_table_names(
        cls,
        sub_dataset_files: list[str],
        include_sequencing_table: bool,
    ) -> list[str]:
        """Return expected sub-workspace table names."""
        return cls.build_expected_table_names(
            csv_filenames=sub_dataset_files,
            include_sequencing_table=include_sequencing_table,
            excluded_csv_filenames={"patient_enrollment_status.csv"},
        )

