"""Dataset validation logic."""
import logging
import re
from pathlib import Path
from typing import Any, Annotated, Union, get_args, get_origin

from pydantic import ValidationError

from csv_schemas import MAIN_CSVS, get_sub_list_with_research_metadata_file
from models.data_models import DatasetInfo
from schema_helpers import CsvSchemaHelper


class DatasetValidator:
    """Handles validation of CSV files in main and sub datasets."""

    _CASE_INSENSITIVE_STRING_VALUES = {"true", "false", "yes", "no"}

    @staticmethod
    def _normalize_string_value(value: str) -> str:
        """Normalize string values for comparison."""
        normalized_value = value.strip()
        if normalized_value.lower() in DatasetValidator._CASE_INSENSITIVE_STRING_VALUES:
            return normalized_value.lower()
        return normalized_value

    @staticmethod
    def _get_field_runtime_type(annotation: Any) -> Any:
        """Return the concrete runtime type hidden inside Optional[...] or Annotated[...]."""
        origin = get_origin(annotation)
        if origin is Annotated:
            return DatasetValidator._get_field_runtime_type(get_args(annotation)[0])
        if origin is Union:
            non_none_types = [arg for arg in get_args(annotation) if arg is not type(None)]
            if len(non_none_types) == 1:
                return DatasetValidator._get_field_runtime_type(non_none_types[0])
        return annotation

    @classmethod
    def _normalize_value_for_comparison(cls, value: Any, annotation: Any = None) -> Any:
        """Normalize one scalar value for comparison using the schema field type, defaulting to string."""
        if value is None:
            return None

        expected_type = cls._get_field_runtime_type(annotation) if annotation is not None else str
        coerced_value = cls._coerce_value_to_field_type(value=value, annotation=expected_type)

        if expected_type is str:
            return cls._normalize_string_value(str(coerced_value))
        if expected_type is int:
            return int(coerced_value) if isinstance(coerced_value, int) else cls._normalize_string_value(str(coerced_value))
        if expected_type is float:
            return float(coerced_value) if isinstance(coerced_value, (int, float)) else cls._normalize_string_value(str(coerced_value))
        if expected_type is bool:
            return bool(coerced_value) if isinstance(coerced_value, bool) else cls._normalize_string_value(str(coerced_value))

        return cls._normalize_string_value(str(coerced_value))

    @classmethod
    def _coerce_value_to_field_type(cls, value: Any, annotation: Any) -> Any:
        """Try to coerce a value into the model field's expected simple type."""
        if value is None:
            return None

        expected_type = cls._get_field_runtime_type(annotation)

        if expected_type is str:
            return value if isinstance(value, str) else str(value)

        if expected_type is int:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value) if value.is_integer() else value
            if isinstance(value, str):
                stripped_value = value.strip()
                if not stripped_value:
                    return value
                if re.fullmatch(r"[-+]?\d+", stripped_value):
                    return int(stripped_value)
            return value

        if expected_type is float:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                stripped_value = value.strip()
                if not stripped_value:
                    return value
                try:
                    return float(stripped_value)
                except ValueError:
                    return value
            return value

        if expected_type is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)) and value in (0, 1):
                return bool(value)
            if isinstance(value, str):
                stripped_value = value.strip().lower()
                if stripped_value in {"true", "1", "yes"}:
                    return True
                if stripped_value in {"false", "0", "no"}:
                    return False
            return value

        return value

    @classmethod
    def coerce_row_values_to_schema_types(cls, row: dict, filename: str) -> dict:
        """Convert row values toward the types expected by the schema for this CSV file."""
        model = CsvSchemaHelper.get_model_for_csv_filename(filename)
        if model is None:
            return row

        coerced_row = dict(row)
        for field_name, field_info in model.model_fields.items():
            if field_name in coerced_row:
                coerced_row[field_name] = cls._coerce_value_to_field_type(
                    value=coerced_row[field_name],
                    annotation=field_info.annotation,
                )
        return coerced_row

    @classmethod
    def _parse_row_with_schema(cls, row: dict, filename: str):
        """Run one row through the pydantic schema and return the parsed model instance."""
        model = CsvSchemaHelper.get_model_for_csv_filename(filename)
        if model is None:
            return None

        typed_row = cls.coerce_row_values_to_schema_types(row=row, filename=filename)
        return model(**typed_row)

    @classmethod
    def build_upload_row(cls, row: dict, filename: str) -> dict:
        """Validate one source row and return the normalized values that should be uploaded to Terra."""
        parsed_row = cls._parse_row_with_schema(row=row, filename=filename)
        if parsed_row is None:
            return dict(row)

        validated_row = parsed_row.model_dump()
        return {
            key: "" if value is None else value
            for key, value in validated_row.items()
        }

    @classmethod
    def build_comparison_row(cls, row: dict, filename: str) -> dict:
        """Validate one row and normalize it into the canonical form used for row comparisons."""
        parsed_row = cls._parse_row_with_schema(row=row, filename=filename)
        model = CsvSchemaHelper.get_model_for_csv_filename(filename)
        if parsed_row is None or model is None:
            return {key: cls._normalize_value_for_comparison(value=value) for key, value in row.items()}

        validated_row = parsed_row.model_dump()
        return {
            key: cls._normalize_value_for_comparison(
                value=value,
                annotation=model.model_fields[key].annotation if key in model.model_fields else None,
            )
            for key, value in validated_row.items()
        }

    @staticmethod
    def parse_sub_directory_name(dir_name: str) -> dict:
        """
        Parse researcher_id and project_id from subdirectory name.

        Args:
            dir_name: Directory name like 'researcher_id_62_project_id_115'

        Returns:
            Dict with 'researcher_id' and 'project_id' keys
        """
        match = re.match(r'researcher_id_(\d+)_project_id_(\d+)', dir_name)
        if not match:
            raise ValueError(f"Invalid sub directory name format: {dir_name}")

        return {
            "researcher_id": int(match.group(1)),
            "project_id": int(match.group(2))
        }

    @staticmethod
    def validate_csv_files(directory_name: str, expected_files: list[str], actual_files: list[str]) -> bool:
        """
        Validate that all expected CSV files are present in the directory.

        Args:
            directory_name: Name of directory being validated
            expected_files: List of expected CSV filenames
            actual_files: List of actual CSV filenames found

        Returns:
            True if validation passes, False otherwise
        """
        expected_set = set(expected_files)
        actual_set = set(actual_files)

        missing_files = expected_set - actual_set
        extra_files = actual_set - expected_set

        if missing_files:
            logging.error(f"Missing CSV files in {directory_name}: {sorted(missing_files)}")
            return False

        #TODO: Should it fail if there are extra files?
        if extra_files:
            logging.warning(f"Extra CSV files in {directory_name}: {sorted(extra_files)}")

        logging.info(f"{directory_name}: all {len(expected_files)} expected files found")
        return True

    @staticmethod
    def validate_rows_for_filename(rows: list[dict], filename: str, context: str) -> bool:
        """Validate rows for a single CSV filename against its matching schema model."""
        model = CsvSchemaHelper.get_model_for_csv_filename(filename)

        if model is None:
            return True

        errors_for_file = []
        for row_num, row in enumerate(rows, start=1):
            try:
                DatasetValidator._parse_row_with_schema(row=row, filename=filename)
            except ValidationError as e:
                for error in e.errors():
                    field = " -> ".join(str(loc) for loc in error["loc"])
                    errors_for_file.append(f"  Row {row_num}, field '{field}': {error['msg']}")

        if errors_for_file:
            logging.error(
                f"[{context}] Schema validation failed for {filename} "
                f"({len(errors_for_file)} error(s)):\n" + "\n".join(errors_for_file)
            )
            return False

        return True

    @classmethod
    def validate_csv_contents(cls, file_contents_map: dict[str, list[dict]], context: str) -> bool:
        """
        Validate the contents of each CSV against its pydantic model.

        Args:
            file_contents_map: Mapping of file_path -> list of row dicts (already loaded)
            context: Human-readable label for logging (e.g. "Main Dataset" or a sub dir name)

        Returns:
            True if all rows in all files pass validation, False if any row fails
        """
        all_valid = True

        for file_path, rows in file_contents_map.items():
            filename = Path(file_path).name
            if not cls.validate_rows_for_filename(rows=rows, filename=filename, context=context):
                all_valid = False
        return all_valid

    def validate_main_dataset(self, dataset_info: DatasetInfo) -> bool:
        """
        Validate that the main dataset has all expected CSV files.

        Args:
            dataset_info: Object containing SFTP dataset information

        Returns:
            True if validation passes, False otherwise
        """
        full_file_paths = dataset_info.main_dataset_files
        file_names = [Path(file_path).name for file_path in full_file_paths]

        logging.info(f"Validating main dataset files")
        return self.validate_csv_files(
            directory_name="Main Dataset",
            expected_files=MAIN_CSVS,
            actual_files=file_names
        )

    def validate_sub_datasets(self, dataset_info: DatasetInfo) -> dict:
        """
        Validate that all sub datasets have expected CSV files and read metadata.

        Args:
            dataset_info: Object containing SFTP dataset information

        Returns:
            Dict mapping subdirectory names to validation results
        """
        validation_results = {}

        for sub_dir_info in dataset_info.sub_datasets:
            full_file_paths = sub_dir_info.files
            file_names = [Path(file_path).name for file_path in full_file_paths]

            researcher_id = sub_dir_info.researcher_id
            project_id = sub_dir_info.project_id

            # Create a display name for logging
            display_name = f"researcher_id_{researcher_id}_project_id_{project_id}"
            logging.info(f"Validating sub dataset: {display_name}")

            if researcher_id is None or project_id is None:
                logging.error(f"Missing researcher_id or project_id in sub dataset")
                validation_results[display_name] = False
                continue

            # Get expected files including the metadata file
            expected_files = get_sub_list_with_research_metadata_file(researcher_id, project_id)

            # Validate
            is_valid = self.validate_csv_files(
                directory_name=display_name,
                # Will not use patient_enrollment_status.csv, but it is expected in sub dir
                expected_files=expected_files + ['patient_enrollment_status.csv'],
                actual_files=file_names
            )

            # Read metadata CSV if validation passed
            if is_valid:
                metadata_filename = f"researcher_id_{researcher_id}_project_id_{project_id}_metadata.csv"
                # Find the metadata file in the files list
                metadata_path = None
                for file_path in full_file_paths:
                    if file_path.endswith(metadata_filename):
                        metadata_path = file_path
                        break

                if not metadata_path:
                    logging.error(f"Metadata file not found: {metadata_filename}")
                    validation_results[display_name] = False
                    continue

                metadata_rows = sub_dir_info.file_contents_map.get(metadata_path, [])
                if not metadata_rows:
                    logging.error(f"Metadata file has no rows: {metadata_path}")
                    validation_results[display_name] = False
                    continue

                metadata = metadata_rows[0]

                # project_name is required
                project_name = metadata.get('project_name', '').strip()
                if not project_name:
                    logging.error(f"Required field 'project_name' is missing or empty in {metadata_path}")
                    validation_results[display_name] = False
                    continue
                # Set the project name and date created in the project-specific data model
                sub_dir_info.project_name = project_name
                sub_dir_info.date_created = metadata["date_created"].strip()
                logging.info(f"Read metadata: project_name={sub_dir_info.project_name}, date_created={sub_dir_info.date_created}")

            validation_results[display_name] = is_valid

        return validation_results

    def validate_all(self, dataset_info: DatasetInfo) -> bool:
        """
        Validate all datasets (main and sub).

        Args:
            dataset_info: Object containing SFTP dataset information

        Returns:
            True if all validations pass, False otherwise
        """
        valid = True
        # Validate main dataset
        if not self.validate_main_dataset(dataset_info):
            logging.error("Main dataset validation failed.")
            valid = False

        if not self.validate_csv_contents(dataset_info.main_file_contents_map, "Main Dataset"):
            logging.error("Main dataset content validation failed.")
            valid = False

        # Validate sub datasets
        sub_validation_results = self.validate_sub_datasets(dataset_info)

        # Check if all sub datasets passed validation
        failed_sub_datasets = [name for name, passed in sub_validation_results.items() if not passed]
        if failed_sub_datasets:
            logging.error(f"Sub dataset validation failed for: {failed_sub_datasets}")
            valid = False

        # Validate contents of each sub dataset
        for sub_dir_info in dataset_info.sub_datasets:
            display_name = f"researcher_id_{sub_dir_info.researcher_id}_project_id_{sub_dir_info.project_id}"
            if not self.validate_csv_contents(sub_dir_info.file_contents_map, display_name):
                logging.error(f"Content validation failed for sub dataset: {display_name}")
                valid = False
        if valid:
            logging.info("All dataset validations passed successfully")
        return valid

