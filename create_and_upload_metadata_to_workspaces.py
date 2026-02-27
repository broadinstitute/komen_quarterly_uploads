"""
Main script for Komen Quarterly Uploads.

This script validates CSV datasets from SFTP, creates Terra workspaces,
transforms and uploads CSV data, and generates sequencing file manifests.
"""

import logging
import tempfile
import shutil
import csv
import re
from io import StringIO
from pathlib import Path
from argparse import ArgumentParser, Namespace
from datetime import datetime

from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.terra_util import TerraGroups, MEMBER, TerraWorkspace

from models import SFTPDatasetInfo, WorkspaceInfo
from validation import DatasetValidator
from workspace import WorkspaceManager
from transformation import CSVTransformer, TerraUploader

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

# Constants
MAIN_WORKSPACE_NAME = f"ShareForCures-Dataset-{datetime.now().strftime('%Y-%m')}"
SUB_WORKSPACE_NAME = "{project_name}_{year}_{month}"
BILLING_PROJECT = "SFC-Research"

# Genomics Files Configuration
# Files are in workspace: SFC-Research/ShareForCures Genomics Files
GENOMICS_BUCKET = "gs://fc-secure-ba527f7b-105c-437e-84e3-fe7e944efdec/"

# Mapping File Configuration
# Mapping file is in workspace: SFC-Research/ShareForCures Operational Data Files
MAPPING_FILE_PATH = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/onyx_mapping/onyx_mapping.csv"

# CSV containing all users cleared for genomics file access
# Located in workspace: SFC-Research/ShareForCures Operational Data Files
GENOMICS_FILE_ACCESS_CSV = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/researcher_mapping/researchers_genomic_access.csv"

# Researcher email to ID mapping
RESEARCHER_ID_TO_EMAIL_MAPPING = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/researcher_mapping/all_researchers.csv"

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
    workspace_info: dict,
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
    if not sftp_info.main_dataset_path or MAIN_WORKSPACE_NAME not in workspace_info:
        return

    main_info = workspace_info[MAIN_WORKSPACE_NAME]
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

    for ws_name, ws_info in workspace_info.items():
        if ws_name == MAIN_WORKSPACE_NAME:
            continue
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
    terra_uploader.upload_all_tsvs_to_workspace(main_info.workspace, tsv_files)
    logging.info(f"Completed upload to main workspace: {len(tsv_files)} files")

def get_cloud_csv_contents_as_dict(cloud_path: str) -> list[dict]:
    file_contents = GCPCloudFunctions().read_file(cloud_path=cloud_path)
    csv_text = file_contents.lstrip("\ufeff")
    reader = csv.DictReader(StringIO(csv_text))
    return list(reader)

def process_sub_workspaces(
    sftp_info: SFTPDatasetInfo,
    workspace_info: dict,
    csv_transformer: CSVTransformer,
    terra_uploader: TerraUploader,
    temp_dir: str,
    participant_to_sample: dict,
    genomic_access_metadata: list[dict],
    researcher_id_mapping: list[dict],
) -> None:
    """
    Process and upload data for all sub workspaces.

    Args:
        sftp_info: SFTP dataset information
        workspace_info: Dictionary of workspace information
        csv_transformer: CSV transformer instance
        terra_uploader: Terra uploader instance
        temp_dir: Temporary directory for transformed files
        participant_to_sample: Mapping of participant IDs to sample IDs
    """
    for sub_dir_info in sftp_info.sub_dataset_dirs:
        if not sub_dir_info.csv_directory_path:
            continue

        # Determine workspace name
        if sub_dir_info.project_name and sub_dir_info.date_created:
            workspace_name = WorkspaceManager.format_workspace_name(
                sub_dir_info.project_name,
                sub_dir_info.date_created
            )
        else:
            workspace_name = SUB_WORKSPACE_NAME.format(
                project_id=sub_dir_info.project_id,
                researcher_id=sub_dir_info.researcher_id
            )

        if workspace_name not in workspace_info:
            logging.warning(f"Skipping {workspace_name} - not in workspace_info")
            continue

        ws_info = workspace_info[workspace_name]

        csv_dir = Path(sub_dir_info.csv_directory_path)
        tsv_files = []

        all_csv_files = csv_dir.glob('*.csv')

        researcher_id = None
        pattern = r"^researcher_id_\d+_project_id_\d+_metadata\.csv$"
        for csv_file in all_csv_files:
            if re.match(pattern, csv_file.name):
                project_metadata = get_cloud_csv_contents_as_dict(csv_file.name)
                researcher_id = [row["researcher_id"] for row in project_metadata][0]

        # Transform and convert all CSVs to TSVs
        for csv_file in csv_dir.glob('*.csv'):
            logging.info(f"Transforming {csv_file.name}...")
            tsv_path = csv_transformer.transform_and_convert_csv(str(csv_file), temp_dir)
            if tsv_path:
                tsv_files.append(tsv_path)

        # Create sequencing files TSV for this sub workspace
        if researcher_id in [user["Researcher ID"] for user in genomic_access_metadata]:
            if ws_info.participants:
                sequencing_tsv_path = Path(temp_dir) / f"sequencing_files_{workspace_name}.tsv"
                csv_transformer.create_sequencing_files_tsv(
                    participants=ws_info.participants,
                    genomics_bucket=GENOMICS_BUCKET,
                    output_path=str(sequencing_tsv_path),
                    participant_to_sample=participant_to_sample
                )
                tsv_files.append(str(sequencing_tsv_path))

        # Upload all TSVs to sub workspace
        terra_uploader.upload_all_tsvs_to_workspace(ws_info.workspace, tsv_files)
        logging.info(f"Completed upload to {workspace_name}: {len(tsv_files)} files")

        logging.info("Adding researcher to project-specific workspace as reader")
        terra_workspace_obj = TerraWorkspace(
            billing_project=BILLING_PROJECT,
            workspace_name=workspace_name,
            request_util=RunRequest(token=Token())
        )
        researcher_email = [u.get("Email") for u in researcher_id_mapping if u.get("Researcher ID") == researcher_id]
        if researcher_email:
            terra_workspace_obj.update_user_acl(
                email=researcher_email[0],
                access_level="READER",
                can_share=False,
                can_compute=False,
                invite_users_not_found=True
            )
        logging.info("Adding 'Research-Admin' group as owner to project-specific workspace")
        terra_workspace_obj.update_user_acl(
            email="Research-Admin@firecloud.org",
            access_level="OWNER",
        )


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

    # Validate all datasets
    if not validator.validate_all(sftp_info):
        logging.error("Dataset validation failed. Exiting.")
        exit(1)

    # Create workspaces
    token = Token()
    request_util = RunRequest(token=token)

    # Load participant to sample ID mapping
    participant_to_sample = load_participant_to_sample_mapping(request_util)
    if not participant_to_sample:
        logging.error("Failed to load participant to sample ID mapping. Exiting.")
        exit(1)

    workspace_manager = WorkspaceManager(
        request_util=request_util,
        billing_project=BILLING_PROJECT,
        main_workspace_name=MAIN_WORKSPACE_NAME,
        sub_workspace_name_template=SUB_WORKSPACE_NAME
    )

    workspaces = workspace_manager.create_all_workspaces(
        sftp_info=sftp_info,
        continue_if_exists=continue_if_workspace_exists
    )

    # Create workspace info dict with participants and buckets
    workspace_info: dict = {}

    # Get bucket for main workspace
    main_workspace = workspaces.get(MAIN_WORKSPACE_NAME)
    if main_workspace:
        main_bucket = main_workspace.get_workspace_bucket()
        main_bucket_path = f"gs://{main_bucket}/"
        workspace_info[MAIN_WORKSPACE_NAME] = WorkspaceInfo(
            workspace=main_workspace,
            workspace_name=MAIN_WORKSPACE_NAME,
            participants=set(),
            bucket=main_bucket_path
        )

    # Sub workspaces - extract participants and get buckets
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
            workspace_name = SUB_WORKSPACE_NAME.format(
                project_id=sub_dir_info.project_id,
                researcher_id=sub_dir_info.researcher_id
            )

        workspace = workspaces.get(workspace_name)
        if not workspace:
            logging.warning(f"Workspace not found for {workspace_name}")
            continue

        # Get workspace bucket
        bucket = workspace.get_workspace_bucket()
        bucket_path = f"gs://{bucket}/"

        # Extract participant IDs
        logging.info(f"Extracting participant IDs from {sub_dir_info.dir_name}...")
        sub_participants = csv_transformer.extract_all_participant_ids_from_directory(
            sub_dir_info.csv_directory_path
        )

        workspace_info[workspace_name] = WorkspaceInfo(
            workspace=workspace,
            workspace_name=workspace_name,
            participants=sub_participants,
            bucket=bucket_path
        )

        logging.info(f"Workspace '{workspace_name}' has {len(sub_participants)} participants, bucket: {bucket_path}")

    # Create temp directory for transformed files
    temp_dir = tempfile.mkdtemp(prefix="terra_upload_")
    logging.info(f"Using temp directory: {temp_dir}")

    logging.info("Adding researchers with genomics files access to the 'Genomics-Files-Access' group")
    file_access_contents = get_cloud_csv_contents_as_dict(cloud_path=GENOMICS_FILE_ACCESS_CSV)
    emails_with_genomic_file_access = [row["Email"] for row in file_access_contents]
    for email in emails_with_genomic_file_access:
        TerraGroups(request_util=request_util).add_user_to_group(
            group="Genomics-Files-Access", email=email, role=MEMBER, continue_if_exists=True
        )

    # Initialize uploader
    terra_uploader = TerraUploader(request_util=request_util)

    # Process main workspace
    process_main_workspace(
        sftp_info, workspace_info, csv_transformer, terra_uploader, temp_dir, participant_to_sample
    )

    researcher_id_mapping = get_cloud_csv_contents_as_dict(cloud_path=RESEARCHER_ID_TO_EMAIL_MAPPING)
    # Process sub workspaces
    process_sub_workspaces(
        sftp_info, workspace_info, csv_transformer, terra_uploader, temp_dir, participant_to_sample, file_access_contents, researcher_id_mapping
    )

    # Clean up temp directory
    shutil.rmtree(temp_dir)

    logging.info(f"Successfully processed {len(workspaces)} workspace(s)")



if __name__ == '__main__':
    main()

