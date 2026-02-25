"""Dataset validation logic."""

import logging
import re
from typing import List
from pathlib import Path

from ops_utils.csv_util import Csv
from csv_schemas import main_csvs, get_sub_list_with_research_metadata_file
from models import SFTPDatasetInfo


class DatasetValidator:
    """Handles validation of CSV files in main and sub datasets."""

    @staticmethod
    def parse_sub_directory_name(dir_name: str) -> dict:
        """
        Parse researcher_id and project_id from sub directory name.

        Args:
            dir_name: Directory name like 'researcher_id_62_project_id_115'

        Returns:
            Dict with 'researcher_id' and 'project_id' keys
        """
        match = re.match(r'researcher_id_(\d+)_project_id_(\d+)', dir_name)
        if not match:
            raise ValueError(f"Invalid sub directory name format: {dir_name}")

        return {
            'researcher_id': int(match.group(1)),
            'project_id': int(match.group(2))
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
        # Read CSV as list of dicts using Csv utility with comma delimiter
        metadata_list = Csv(file_path=csv_path, delimiter=',').create_list_of_dicts_from_tsv()

        # Return first row if exists, otherwise empty dict
        return metadata_list[0] if metadata_list else {}

    @staticmethod
    def validate_csv_files(directory_name: str, expected_files: List[str], actual_files: List[str]) -> bool:
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

        logging.info(f"Validation passed for {directory_name}: all {len(expected_files)} expected files found")
        return True

    def validate_main_dataset(self, sftp_info: SFTPDatasetInfo) -> bool:
        """
        Validate that main dataset has all expected CSV files.

        Args:
            sftp_info: Object containing SFTP dataset information

        Returns:
            True if validation passes, False otherwise
        """
        logging.info(f"Validating main dataset: {sftp_info.main_dataset_dir}")
        return self.validate_csv_files(
            directory_name=sftp_info.main_dataset_dir,
            expected_files=main_csvs,
            actual_files=sftp_info.main_dataset_files
        )

    def validate_sub_datasets(self, sftp_info: SFTPDatasetInfo) -> dict:
        """
        Validate that all sub datasets have expected CSV files and read metadata.

        Args:
            sftp_info: Object containing SFTP dataset information

        Returns:
            Dict mapping sub directory names to validation results
        """
        validation_results = {}

        for sub_dir_info in sftp_info.sub_dataset_dirs:
            dir_name = sub_dir_info.dir_name
            files = sub_dir_info.files

            logging.info(f"Validating sub dataset: {dir_name}")

            # Parse researcher_id and project_id from directory name
            try:
                ids = self.parse_sub_directory_name(dir_name)
                researcher_id = ids['researcher_id']
                project_id = ids['project_id']

                # Store parsed IDs in the object for later use
                sub_dir_info.researcher_id = researcher_id
                sub_dir_info.project_id = project_id
            except ValueError as e:
                logging.error(str(e))
                validation_results[dir_name] = False
                continue

            # Get expected files including the metadata file
            expected_files = get_sub_list_with_research_metadata_file(researcher_id, project_id)

            # Validate
            is_valid = self.validate_csv_files(
                directory_name=dir_name,
                expected_files=expected_files,
                actual_files=files
            )

            # Read metadata CSV if validation passed
            if is_valid and sub_dir_info.csv_directory_path:
                metadata_filename = f"researcher_id_{researcher_id}_project_id_{project_id}_metadata.csv"
                metadata_path = Path(sub_dir_info.csv_directory_path) / metadata_filename

                if not metadata_path.exists():
                    logging.error(f"Metadata file not found: {metadata_path}")
                    validation_results[dir_name] = False
                    continue

                metadata = self.read_metadata_csv(str(metadata_path))

                # project_name is required
                project_name = metadata.get('project_name', '').strip()
                if not project_name:
                    logging.error(f"Required field 'project_name' is missing or empty in {metadata_path}")
                    validation_results[dir_name] = False
                    continue

                sub_dir_info.project_name = project_name
                sub_dir_info.date_created = metadata.get('date_created', '').strip()
                logging.info(f"Read metadata: project_name={sub_dir_info.project_name}, date_created={sub_dir_info.date_created}")

            validation_results[dir_name] = is_valid

        return validation_results

    def validate_all(self, sftp_info: SFTPDatasetInfo) -> bool:
        """
        Validate all datasets (main and sub).

        Args:
            sftp_info: Object containing SFTP dataset information

        Returns:
            True if all validations pass, False otherwise
        """
        # Validate main dataset
        if not self.validate_main_dataset(sftp_info):
            logging.error("Main dataset validation failed.")
            return False

        # Validate sub datasets
        sub_validation_results = self.validate_sub_datasets(sftp_info)

        # Check if all sub datasets passed validation
        failed_sub_datasets = [name for name, passed in sub_validation_results.items() if not passed]
        if failed_sub_datasets:
            logging.error(f"Sub dataset validation failed for: {failed_sub_datasets}")
            return False

        logging.info("All dataset validations passed successfully")
        return True

