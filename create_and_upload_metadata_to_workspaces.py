"""
Main script for Komen Quarterly Uploads.

This script validates CSV datasets from SFTP, creates Terra workspaces,
transforms and uploads CSV data, and generates sequencing file manifests.
"""

import logging
import tempfile
import shutil
from pathlib import Path
from argparse import ArgumentParser, Namespace
from datetime import datetime

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.token_util import Token
from ops_utils.csv_util import Csv
from ops_utils.gcp_util import GCPCloudFunctions

from models.data_models import SFTPDatasetInfo
from validation.dataset_validator import DatasetValidator
from workspace.workspace_manager import WorkspaceManager
from transformation.csv_transformer import CSVTransformer
from transformation.terra_uploader import TerraUploader

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

# Constants
MAIN_WORKSPACE_NAME = f"ShareForCures-Dataset-{datetime.now().strftime('%Y-%m')}"
SUB_WORKSPACE_NAME_TEMPLATE = "{project_name}_{year}_{month}"
BILLING_PROJECT = "SFC-Research"

# Genomics Files Configuration
# Files are in workspace: SFC-Research/ShareForCures Genomics Files
GENOMICS_BUCKET = "gs://fc-secure-ba527f7b-105c-437e-84e3-fe7e944efdec/"

# Mapping File Configuration
# Mapping file is in workspace: SFC-Research/ShareForCures Operational Data Files
MAPPING_FILE_PATH = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/onyx_mapping/onyx_mapping.csv"


def get_args() -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description="Find new csvs in SFTP site and create new workspaces and upload metadata to them")
    parser.add_argument("--continue_if_workspace_exists", "-c", action="store_true",
                        help="Set this flag to true to continue uploading metadata if workspace already exists")
    return parser.parse_args()

def load_participant_to_sample_mapping() -> dict:
    """
    Load the mapping from participant IDs to sample IDs.

    Returns:
        Dictionary mapping participant_id -> sample_id (with K prefix)
    """
    mapping_file_contents = GCPCloudFunctions().read_file(cloud_path=MAPPING_FILE_PATH)
    # The mapping file is expected to have lines in the format: sample_id,participant_id
    mapping_dict = {
        # Add 'K' prefix to sample_id to match the format in the sequencing files naming
        line.split(',')[1]: f'K{line.split(",")[0]}'
        for line in mapping_file_contents.splitlines()
        if 'Participant ID' not in line and line.strip()  # Skip header and empty lines
    }
    logging.info(f"Loaded participant to sample mapping for {len(mapping_dict)} participants")
    return mapping_dict

def process_main_workspace(
    sftp_info: SFTPDatasetInfo,
    terra_workspace_obj: TerraWorkspace,
    csv_transformer: CSVTransformer,
    terra_uploader: TerraUploader,
    temp_dir: str,
    participant_to_sample: dict
) -> None:
    """
    Process and upload data for the main workspace.

    Args:
        sftp_info: SFTP dataset information
        workspace_info: Dictionary of workspace information
        csv_transformer: CSV transformer instance
        terra_uploader: Terra uploader instance
        temp_dir: Temporary directory for transformed files
        participant_to_sample: Mapping of participant IDs to sample IDs
    """
    main_csv_dir = Path(sftp_info.main_dataset_path)
    tsv_files = []

    # Transform and convert all CSVs to TSVs
    for csv_file in main_csv_dir.glob('*.csv'):
        logging.info(f"Transforming {csv_file.name}...")
        tsv_path = csv_transformer.transform_and_convert_csv(str(csv_file), temp_dir)
        if tsv_path:
            tsv_files.append(tsv_path)

    # Create master sequencing files TSV
    all_participants = set()

    # TODO what are the participants here??
    # Before it was initialized in the WorkspaceInfo schema as just set()
    for participant in ws_info.participants:
        all_participants.add(participant)

    if all_participants:
        sequencing_tsv_path = Path(temp_dir) / "sequencing_files.tsv"

        # Create master sequencing files TSV using genomics bucket
        csv_transformer.create_sequencing_files_tsv(
            participants=all_participants,
            genomics_bucket=GENOMICS_BUCKET,
            output_path=str(sequencing_tsv_path),
            participant_to_sample=participant_to_sample
        )

        tsv_files.append(str(sequencing_tsv_path))
        logging.info(f"Created master sequencing files TSV with {len(all_participants)} participants")

    # Upload all TSVs to main workspace
    terra_uploader.upload_all_tsvs_to_workspace(terra_workspace_obj, tsv_files)
    logging.info(f"Completed upload to main workspace: {len(tsv_files)} files")

def process_sub_workspaces(
    sftp_info: SFTPDatasetInfo,
    sub_workspace_metadata: list[dict],
    csv_transformer: CSVTransformer,
    terra_uploader: TerraUploader,
    temp_dir: str,
    participant_to_sample: dict
) -> None:
    """
    Process and upload data for all sub workspaces.

    Args:
        sftp_info: SFTP dataset information
        sub_workspace_metadata: List of dictionaries with sub workspace names and participant sets
        csv_transformer: CSV transformer instance
        terra_uploader: Terra uploader instance
        temp_dir: Temporary directory for transformed files
        participant_to_sample: Mapping of participant IDs to sample IDs
    """
    for sub_dir_info in sftp_info.sub_dataset_dirs:
        if not sub_dir_info.csv_directory_path:
            continue

        csv_dir = Path(sub_dir_info.csv_directory_path)
        tsv_files = []

        # Transform and convert all CSVs to TSVs
        for csv_file in csv_dir.glob("*.csv"):
            logging.info(f"Transforming {csv_file.name}...")
            tsv_path = csv_transformer.transform_and_convert_csv(str(csv_file), temp_dir)
            if tsv_path:
                tsv_files.append(tsv_path)

            # Determine workspace name
            if sub_dir_info.project_name and sub_dir_info.date_created:
                workspace_name = WorkspaceManager.format_workspace_name(
                    sub_dir_info.project_name, sub_dir_info.date_created
                )
            else:
                workspace_name = SUB_WORKSPACE_NAME_TEMPLATE.format(
                    project_id=sub_dir_info.project_id, researcher_id=sub_dir_info.researcher_id
                    )

            participants = [a for a in sub_workspace_metadata if a["workspace_name"] == workspace_name][0]["participants"]
            sub_workspace_terra_obj = [a for a in sub_workspace_metadata if a["workspace_name"] == workspace_name][0]["sub_workspace_terra_obj"]
            sequencing_tsv_path = Path(temp_dir) / f"sequencing_files_{workspace_name}.tsv"

            csv_transformer.create_sequencing_files_tsv(
                participants=participants,
                genomics_bucket=GENOMICS_BUCKET,
                output_path=str(sequencing_tsv_path),
                participant_to_sample=participant_to_sample
            )

            # Upload all TSVs to sub workspace
            terra_uploader.upload_all_tsvs_to_workspace(sub_workspace_terra_obj, tsv_files)
            logging.info(f"Completed upload to {workspace_name}: {len(tsv_files)} files")


def main():
    """Main execution function."""
    args = get_args()
    continue_if_workspace_exists = args.continue_if_workspace_exists

    # GET SFTP INFO
    # TODO: Replace with actual SFTP retrieval logic
    sftp_info = SFTPDatasetInfo()
    # sftp_info.main_dataset_dir = 'shareforcures_dataset_2026_02'
    # sftp_info.main_dataset_files = [list of files from SFTP]
    # sftp_info.main_dataset_path = '/path/to/main/dataset'
    # sftp_info.sub_dataset_dirs = [
    #     SubDatasetInfo(
    #         dir_name='researcher_id_62_project_id_115',
    #         files=[...],
    #         csv_directory_path='/path/to/sub/dataset'
    #     ),
    # ]

    # Initialize components
    validator = DatasetValidator()
    csv_transformer = CSVTransformer()
    token = Token()
    request_util = RunRequest(token=token)
    # Initialize uploader
    terra_uploader = TerraUploader(request_util=request_util)
    # Create temp directory for transformed files
    temp_dir = tempfile.mkdtemp(prefix="terra_upload_")
    logging.info(f"Using temp directory: {temp_dir}")

    # Validate all datasets
    if not validator.validate_all(sftp_info):
        logging.error("Dataset validation failed. Exiting.")
        exit(1)

    # Load participant to sample ID mapping
    participant_to_sample = load_participant_to_sample_mapping()
    if not participant_to_sample:
        logging.error("Failed to load participant to sample ID mapping. Exiting.")
        exit(1)

    # Initialize workspace manager object
    workspace_manager = WorkspaceManager(
        request_util=request_util,
        billing_project=BILLING_PROJECT,
        main_workspace_name=MAIN_WORKSPACE_NAME,
        sub_workspace_name_template=SUB_WORKSPACE_NAME_TEMPLATE
    )

    # Create the main workspace
    main_workspace_terra_obj = workspace_manager.create_main_workspace(continue_if_exists=continue_if_workspace_exists)
    # Process the main workspace
    process_main_workspace(
        sftp_info=sftp_info,
        terra_workspace_obj=main_workspace_terra_obj,
        csv_transformer=csv_transformer,
        terra_uploader=terra_uploader,
        temp_dir=temp_dir,
        participant_to_sample=participant_to_sample
    )

    # Create sub workspaces
    sub_workspaces: dict[str, TerraWorkspace] = workspace_manager.create_all_sub_workspaces(
        sftp_info=sftp_info, continue_if_exists=continue_if_workspace_exists
    )
    sub_workspace_metadata = []
    # Extract participants and get buckets for sub workspaces
    for sub_dir_info in sftp_info.sub_dataset_dirs:
        if not sub_dir_info.csv_directory_path:
            logging.warning(f"No CSV directory path for {sub_dir_info.dir_name}, skipping")
            continue

        # Determine workspace name (same logic as WorkspaceManager)
        if sub_dir_info.project_name and sub_dir_info.date_created:
            workspace_name = WorkspaceManager.format_workspace_name(
                sub_dir_info.project_name,
                sub_dir_info.date_created
            )
        else:
            workspace_name = SUB_WORKSPACE_NAME_TEMPLATE.format(
                project_id=sub_dir_info.project_id,
                researcher_id=sub_dir_info.researcher_id
            )

        sub_workspace_terra_obj = sub_workspaces.get(workspace_name)
        if not sub_workspace_terra_obj:
            logging.warning(f"Workspace not found for {workspace_name}")
            continue

        # Extract participant IDs
        logging.info(f"Extracting participant IDs from {sub_dir_info.dir_name}...")
        sub_participants = csv_transformer.extract_all_participant_ids_from_directory(
            sub_dir_info.csv_directory_path
        )

        sub_workspace_metadata.append(
            {
                "workspace_name": workspace_name,
                "participants": sub_participants,
                "sub_workspace_terra_obj": sub_workspace_terra_obj,
            }
        )

        logging.info(f"Workspace '{workspace_name}' has {len(sub_participants)} participants, bucket: {bucket_path}")

    # Process sub workspaces
    process_sub_workspaces(
        sftp_info=sftp_info,
        sub_workspace_metadata=sub_workspace_metadata,
        csv_transformer=csv_transformer,
        terra_uploader=terra_uploader,
        temp_dir=temp_dir,
        participant_to_sample=participant_to_sample
    )

    # Clean up temp directory
    shutil.rmtree(temp_dir)

    logging.info(f"Successfully processed 1 main workspaces and {len(sub_workspace_metadata)} sub-workspace(s)")


if __name__ == '__main__':
    main()

