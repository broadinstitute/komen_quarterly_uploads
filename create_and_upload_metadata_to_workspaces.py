import logging
import re
from typing import List, Dict, Optional
from dataclasses import dataclass, field

from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.terra_util import TerraWorkspace
from argparse import ArgumentParser, Namespace
from datetime import datetime

from csv_schemas import main_csvs, get_sub_dict_with_research_metadata_file

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

MAIN_WORKSPACE_NAME = f"ShareForCures-Dataset-{datetime.now().strftime('%Y-%m')}"
# In the researcher_id_x_project_id_x_metadata.csv, there is a column named project_name and date_created.
# I was hoping we could use this to name the workspace a combination of the two, with project_name_year_month.
SUB_WORKSPACE_NAME = "{project_name}_{researcher_id}"
BILLING_PROJECT = "SFC-Research"


def get_args() -> Namespace:
    parser = ArgumentParser(description="Find new csvs in SFTP site and create new workspaces and upload metadata to them")
    parser.add_argument("--continue_if_workspace_exists", "-c", action="store_true",
                        help="Set this flag to true to continue uploading metadata if workspace already exists")
    return parser.parse_args()


@dataclass
class SubDatasetInfo:
    """Information about a sub dataset directory."""
    dir_name: str
    files: List[str]
    researcher_id: Optional[int] = None
    project_id: Optional[int] = None


@dataclass
class SFTPDatasetInfo:
    """
    Data class for SFTP dataset information.
    Contains information about main and sub datasets retrieved from SFTP.
    """
    main_dataset_dir: Optional[str] = None
    main_dataset_files: List[str] = field(default_factory=list)
    sub_dataset_dirs: List[SubDatasetInfo] = field(default_factory=list)


class DatasetValidator:
    """Handles validation of CSV files in main and sub datasets."""

    @staticmethod
    def parse_sub_directory_name(dir_name: str) -> Dict[str, int]:
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
    def validate_csv_files(directory_name: str, expected_files: Dict[str, str], actual_files: List[str]) -> bool:
        """
        Validate that all expected CSV files are present in the directory.

        Args:
            directory_name: Name of directory being validated
            expected_files: Dictionary of expected CSV filenames (keys) and their ID columns (values)
            actual_files: List of actual CSV filenames found

        Returns:
            True if validation passes, False otherwise
        """
        expected_set = set(expected_files.keys())
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

    def validate_sub_datasets(self, sftp_info: SFTPDatasetInfo) -> Dict[str, bool]:
        """
        Validate that all sub datasets have expected CSV files.

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
            expected_files = get_sub_dict_with_research_metadata_file(researcher_id, project_id)

            # Validate
            is_valid = self.validate_csv_files(
                directory_name=dir_name,
                expected_files=expected_files,
                actual_files=files
            )
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


class WorkspaceManager:
    """Manages Terra workspace creation and data upload operations."""

    def __init__(self, request_util: RunRequest, billing_project: str = BILLING_PROJECT):
        """
        Initialize WorkspaceManager.

        Args:
            request_util: Request utility for Terra API calls
            billing_project: Terra billing project name
        """
        self.request_util = request_util
        self.billing_project = billing_project

    def create_workspace(self, workspace_name: str, continue_if_exists: bool = False) -> TerraWorkspace:
        """
        Create a single Terra workspace.

        Args:
            workspace_name: Name of the workspace to create
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object
        """
        logging.info(f"Creating workspace: {workspace_name}")
        workspace = TerraWorkspace(
            billing_project=self.billing_project,
            workspace_name=workspace_name,
            request_util=self.request_util
        )
        workspace.create_workspace(continue_if_exists=continue_if_exists)
        return workspace

    def create_main_workspace(self, continue_if_exists: bool = False) -> TerraWorkspace:
        """
        Create the main dataset workspace.

        Args:
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object for main workspace
        """
        return self.create_workspace(MAIN_WORKSPACE_NAME, continue_if_exists)

    def create_sub_workspace(
        self,
        researcher_id: int,
        project_id: int,
        continue_if_exists: bool = False
    ) -> TerraWorkspace:
        """
        Create a sub dataset workspace.

        Args:
            researcher_id: Researcher ID
            project_id: Project ID
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object for sub workspace
        """
        workspace_name = SUB_WORKSPACE_NAME.format(
            project_id=project_id,
            researcher_id=researcher_id
        )
        return self.create_workspace(workspace_name, continue_if_exists)

    def create_all_workspaces(
        self,
        sftp_info: SFTPDatasetInfo,
        continue_if_exists: bool = False
    ) -> Dict[str, TerraWorkspace]:
        """
        Create Terra workspaces for main dataset and all sub datasets.

        Args:
            sftp_info: Object containing SFTP dataset information
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            Dict mapping workspace names to TerraWorkspace objects
        """
        workspaces = {}

        # Create main workspace
        main_workspace = self.create_main_workspace(continue_if_exists)
        workspaces[MAIN_WORKSPACE_NAME] = main_workspace

        # Create sub workspaces
        for sub_dir_info in sftp_info.sub_dataset_dirs:
            if sub_dir_info.researcher_id is None or sub_dir_info.project_id is None:
                logging.warning(f"Skipping workspace creation for {sub_dir_info.dir_name}: Missing IDs")
                continue

            try:
                sub_workspace = self.create_sub_workspace(
                    researcher_id=sub_dir_info.researcher_id,
                    project_id=sub_dir_info.project_id,
                    continue_if_exists=continue_if_exists
                )

                workspace_name = SUB_WORKSPACE_NAME.format(
                    project_id=sub_dir_info.project_id,
                    researcher_id=sub_dir_info.researcher_id
                )
                workspaces[workspace_name] = sub_workspace

            except Exception as e:
                logging.error(f"Failed to create workspace for {sub_dir_info.dir_name}: {e}")
                continue

        logging.info(f"Successfully created {len(workspaces)} workspace(s)")
        return workspaces


class CSVTransformer:
    """Handles CSV transformations and adjustments before upload."""

    def __init__(self):
        """Initialize CSVTransformer."""
        pass

    # TODO: Add methods for CSV transformation
    # def transform_csv_for_upload(self, csv_path: str, csv_name: str) -> str:
    #     """Transform a CSV file for upload to Terra."""
    #     pass

    # def extract_participant_ids(self, csv_data: List[Dict]) -> List[str]:
    #     """Extract participant IDs from CSV data."""
    #     pass


class ParticipantFileManager:
    """Manages finding and handling participant files from external bucket."""

    def __init__(self, bucket_path: Optional[str] = None):
        """
        Initialize ParticipantFileManager.

        Args:
            bucket_path: Path to the bucket containing participant files
        """
        self.bucket_path = bucket_path

    # TODO: Add methods for participant file management
    # def find_participant_files(self, participant_ids: List[str]) -> Dict[str, List[str]]:
    #     """Find files for given participant IDs."""
    #     pass

    # def validate_participant_files(self, files: List[str]) -> bool:
    #     """Validate that participant files exist and are accessible."""
    #     pass


class TerraUploader:
    """Handles uploading CSV and participant files to Terra workspaces."""

    def __init__(self, request_util: RunRequest):
        """
        Initialize TerraUploader.

        Args:
            request_util: Request utility for Terra API calls
        """
        self.request_util = request_util

    # TODO: Add methods for uploading to Terra
    # def upload_csvs_to_workspace(self, workspace: TerraWorkspace, csv_files: Dict[str, str]) -> bool:
    #     """Upload CSV files to a Terra workspace."""
    #     pass

    # def upload_participant_files_to_workspace(self, workspace: TerraWorkspace, files: List[str]) -> bool:
    #     """Upload participant files to a Terra workspace."""
    #     pass


if __name__ == '__main__':
    args = get_args()
    continue_if_workspace_exists = args.continue_if_workspace_exists

    # GET SFTP INFO
    # TODO: Replace with actual SFTP retrieval logic
    sftp_info = SFTPDatasetInfo()
    # sftp_info.main_dataset_dir = 'shareforcures_dataset_2026_02'
    # sftp_info.main_dataset_files = [list of files from SFTP]
    # sftp_info.sub_dataset_dirs = [SubDatasetInfo(dir_name='...', files=[...]), ...]

    # Initialize components
    validator = DatasetValidator()

    # Validate all datasets
    if not validator.validate_all(sftp_info):
        logging.error("Dataset validation failed. Exiting.")
        exit(1)

    # Create workspaces
    token = Token()
    request_util = RunRequest(token=token)
    workspace_manager = WorkspaceManager(request_util=request_util)

    workspaces = workspace_manager.create_all_workspaces(
        sftp_info=sftp_info,
        continue_if_exists=continue_if_workspace_exists
    )

    # TODO: Add CSV transformation and upload logic
    # csv_transformer = CSVTransformer()
    # participant_file_manager = ParticipantFileManager(bucket_path='...')
    # terra_uploader = TerraUploader(request_util=request_util)

    # for workspace_name, workspace in workspaces.items():
    #     # Transform and upload CSVs
    #     # Find and upload participant files
    #     pass

    logging.info(f"Successfully processed {len(workspaces)} workspace(s)")
