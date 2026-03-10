"""Dataset validation logic."""
import logging
import re
from io import StringIO
from pathlib import Path
import csv

from pydantic import ValidationError
from ops_utils.gcp_utils import GCPCloudFunctions

from csv_schemas import MAIN_CSVS, get_sub_list_with_research_metadata_file, CSV_SCHEMA_MAP
from models.data_models import DatasetInfo


class DatasetValidator:
    """Handles validation of CSV files in main and sub datasets."""

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
    def read_metadata_csv(csv_path: str) -> dict:
        """
        Read metadata from a CSV file and return the first row as a dictionary.

        Args:
            csv_path: Path to the metadata CSV file

        Returns:
            Dictionary with column names as keys and values from first data row
        """
        file_contents = GCPCloudFunctions().read_file(cloud_path=csv_path)
        reader = csv.DictReader(StringIO(file_contents))

        return list(reader)[0]

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

        if extra_files:
            logging.warning(f"Extra CSV files in {directory_name}: {sorted(extra_files)}")

        logging.info(f"{directory_name}: all {len(expected_files)} expected files found")
        return True

    @staticmethod
    def validate_csv_contents(file_contents_map: dict[str, list[dict]], context: str) -> bool:
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
            model = CSV_SCHEMA_MAP.get(filename)

            if model is None:
                # Metadata file or unknown file — skip schema validation
                continue

            errors_for_file = []
            for row_num, row in enumerate(rows, start=1):
                try:
                    model(**row)
                except ValidationError as e:
                    for error in e.errors():
                        field = " -> ".join(str(loc) for loc in error["loc"])
                        errors_for_file.append(f"  Row {row_num}, field '{field}': {error['msg']}")

            if errors_for_file:
                logging.error(
                    f"[{context}] Schema validation failed for {filename} "
                    f"({len(errors_for_file)} error(s)):\n" + "\n".join(errors_for_file)
                )
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
                expected_files=expected_files,
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

                metadata = self.read_metadata_csv(metadata_path)

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

