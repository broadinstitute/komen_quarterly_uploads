"""
Main script for Komen Quarterly Uploads.

This script validates CSV datasets from a Google bucket, creates Terra workspaces,
transforms and uploads CSV data, and generates sequencing file manifests.
"""

import logging
import re
import tempfile
import shutil
import csv
from pathlib import Path
from argparse import ArgumentParser, Namespace
from datetime import datetime
from io import StringIO
from typing import Optional

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace, TerraGroups, MEMBER
from ops_utils.token_util import Token
from ops_utils.gcp_utils import GCPCloudFunctions

from models.data_models import DatasetInfo, SubDatasetInfo
from validation.dataset_validator import DatasetValidator
from workspace.workspace_manager import WorkspaceManager
from transformation.csv_transformer import CSVTransformer
from transformation.terra_uploader import TerraUploader
from transformation.genomics_file_checker import GenomicsFileChecker


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

# Constants
MAIN_WORKSPACE_NAME = f"ShareForCures-Dataset-{datetime.now().strftime('%Y-%m')}"
SUB_WORKSPACE_NAME_TEMPLATE = "{project_name}_{year}_{month}"
BILLING_PROJECT = "SFC-Research"
METADATA_CSVS_BUCKET = "fc-fa9fd891-996a-4624-864e-c4f81d165a90"

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
# Located in workspace: SFC-Research/ShareForCures Operational Data Files
RESEARCHER_ID_TO_EMAIL_MAPPING = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/researcher_mapping/all_researchers.csv"
GENOMICS_FILE_ACCESS_GROUP_NAME = "Genomics-Files-Access"
RESEARCH_ADMIN_GROUP_EMAIL = "Research-Admin@firecloud.org"

def get_args() -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description="Find new csvs in SFTP site and create new workspaces and upload metadata to them")
    parser.add_argument("--continue_if_workspace_exists", "-c", action="store_true",
                        help="Set this flag to true to continue uploading metadata if workspace already exists")
    parser.add_argument("--dry_run", "-d", action="store_true",
                        help="Log what would happen without creating workspaces, uploading TSVs, or modifying ACLs")
    return parser.parse_args()

def load_participant_to_sample_mapping(gcp: GCPCloudFunctions) -> dict:
    """
    Load the mapping from participant IDs to sample IDs.

    Returns:
        Dictionary mapping participant_id -> sample_id (with K prefix)
    """
    mapping_file_contents = gcp.read_file(cloud_path=MAPPING_FILE_PATH)
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
    dataset_info: DatasetInfo,
    terra_workspace_obj: TerraWorkspace,
    csv_transformer: CSVTransformer,
    terra_uploader: TerraUploader,
    temp_dir: str,
    participant_files: dict[str, dict[str, Optional[str]]],
    dry_run: bool = False,
) -> None:
    """
    Process and upload data for the main workspace.

    Args:
        dataset_info: Dataset information
        terra_workspace_obj: TerraWorkspace object for the main workspace
        csv_transformer: CSV transformer instance
        terra_uploader: Terra uploader instance
        temp_dir: Temporary directory for transformed files
        participant_files: Pre-checked dict of participant_id -> {file_type: path_or_None}
                           as returned by GenomicsFileChecker.check_all_participants().
        dry_run: If True, log what would be uploaded without actually uploading.
    """
    tsv_files = []

    for csv_file_path in dataset_info.main_dataset_files:
        logging.info(f"Transforming {Path(csv_file_path).name}...")
        file_contents = dataset_info.main_file_contents_map[csv_file_path]
        tsv_files.append(csv_transformer.transform_and_convert_csv(csv_path=csv_file_path, file_contents=file_contents, output_dir=temp_dir))

    sequencing_tsv_path = Path(temp_dir) / "sequencing_files.tsv"
    csv_transformer.create_sequencing_files_tsv(
        participant_files=participant_files,
        output_path=str(sequencing_tsv_path),
    )
    tsv_files.append(str(sequencing_tsv_path))
    logging.info(f"Created master sequencing files TSV with {len(participant_files)} participants")

    if dry_run:
        logging.info(f"DRY RUN: Would upload {len(tsv_files)} TSV(s) to main workspace '{terra_workspace_obj.workspace_name}'")
    else:
        terra_uploader.upload_all_tsvs_to_workspace(terra_workspace_obj, tsv_files)
        logging.info(f"Completed upload to main workspace: {len(tsv_files)} files")

def process_sub_workspaces(
    dataset_info: DatasetInfo,
    sub_workspace_metadata: list[dict],
    csv_transformer: CSVTransformer,
    terra_uploader: TerraUploader,
    temp_dir: str,
    all_participant_files: dict,
    workspace_manager_obj: WorkspaceManager,
    genomics_access_metadata: list[dict],
    researcher_id_mapping: list[dict],
    gcp: GCPCloudFunctions,
    dry_run: bool = False,
) -> None:
    """
    Process and upload data for all sub workspaces.

    Args:
        dataset_info: Dataset information
        sub_workspace_metadata: List of dictionaries with sub workspace name, participant set, and TerraWorkspace object
        csv_transformer: CSV transformer instance
        terra_uploader: Terra uploader instance
        temp_dir: Temporary directory for transformed files
        all_participant_files: Full dict of participant_id -> {file_type: path_or_None}
                               as returned by GenomicsFileChecker.check_all_participants().
                               Filtered per workspace inside this function.
        workspace_manager_obj: WorkspaceManager instance
        genomics_access_metadata: List of all researchers with clearance for genomics file access
        researcher_id_mapping: List of dictionaries mapping ALL researchers IDs to emails
        gcp: Shared GCPCloudFunctions instance
        dry_run: If True, log what would be uploaded/modified without actually doing it.
    """
    for sub_dataset in dataset_info.sub_datasets:
        tsv_files = []

        researcher_id = None
        for csv_file_path in sub_dataset.files:
            pattern = r"^researcher_id_\d+_project_id_\d+_metadata\.csv$"
            if re.match(pattern, Path(csv_file_path).name):
                project_metadata = get_cloud_csv_contents_as_dict(csv_file_path, gcp)
                researcher_id = int([row["researcher_id"] for row in project_metadata][0])
                if researcher_id != sub_dataset.researcher_id:
                    logging.warning(f"Researcher ID mismatch in {csv_file_path}: expected {sub_dataset.researcher_id}, found {researcher_id}")

            logging.info(f"Transforming {Path(csv_file_path).name}...")
            file_contents = sub_dataset.file_contents_map[csv_file_path]
            tsv_files.append(csv_transformer.transform_and_convert_csv(csv_path=csv_file_path, file_contents=file_contents, output_dir=temp_dir))

        workspace_name = determine_sub_workspace_name(
            workspace_manager_obj=workspace_manager_obj,
            project_id=sub_dataset.project_id,
            researcher_id=sub_dataset.researcher_id,
            project_name=sub_dataset.project_name,
            date_created=sub_dataset.date_created,
        )

        ws_meta = [a for a in sub_workspace_metadata if a["workspace_name"] == workspace_name][0]
        sub_workspace_terra_obj = ws_meta["sub_workspace_terra_obj"]

        if researcher_id in [user["Researcher ID"] for user in genomics_access_metadata]:
            logging.info("Researcher has genomics access - creating sequencing files TSV for sub workspace")
            participant_files = {p: all_participant_files[p] for p in ws_meta["participants"] if p in all_participant_files}
            sequencing_tsv_path = Path(temp_dir) / f"sequencing_files_{workspace_name}.tsv"
            csv_transformer.create_sequencing_files_tsv(
                participant_files=participant_files,
                output_path=str(sequencing_tsv_path),
            )
            tsv_files.append(str(sequencing_tsv_path))
        else:
            logging.info("Researcher does not have genomics access - skipping sequencing files TSV for sub workspace")

        if dry_run:
            logging.info(f"DRY RUN: Would upload {len(tsv_files)} TSV(s) to sub workspace '{workspace_name}'")
        else:
            terra_uploader.upload_all_tsvs_to_workspace(sub_workspace_terra_obj, tsv_files)
            logging.info(f"Completed upload to {workspace_name}: {len(tsv_files)} files")

        researcher_email = [u.get("Email") for u in researcher_id_mapping if u.get("Researcher ID") == researcher_id]
        if dry_run:
            if researcher_email:
                logging.info(f"DRY RUN: Would grant READER access to '{researcher_email[0]}' on workspace '{workspace_name}'")
            logging.info(f"DRY RUN: Would grant OWNER access to '{RESEARCH_ADMIN_GROUP_EMAIL}' on workspace '{workspace_name}'")
        else:
            if researcher_email:
                sub_workspace_terra_obj.update_user_acl(
                    email=researcher_email[0],
                    access_level="READER",
                    can_share=False,
                    can_compute=False,
                    invite_users_not_found=True
                )
            logging.info("Adding 'Research-Admin' group as owner to project-specific workspace")
            sub_workspace_terra_obj.update_user_acl(email=RESEARCH_ADMIN_GROUP_EMAIL, access_level="OWNER")

def parse_csv_paths_to_dataset_info(all_csv_paths: list[str], gcp: GCPCloudFunctions) -> DatasetInfo:
    """
    Parse a list of CSV file paths into a DatasetInfo structure.

    Separates files into the main dataset (under shareforcures_dataset_*/) and
    sub datasets (under researcher_id_*_project_id_*/).
    Reads all file contents in a single multithreaded call then organises them.

    Args:
        all_csv_paths: List of full GCS file paths to CSV files
        gcp: Shared GCPCloudFunctions instance

    Returns:
        DatasetInfo object with files organised by dataset type and their contents

    Example paths:
        - Main: "example_main_dir/shareforcures_dataset_2026_02/file.csv"
        - Sub: "example_main_dir/researcher_id_62_project_id_115/file.csv"
    """
    # TODO Update this directory pattern matching once we know how CSV files are saved in the bucket
    main_pattern = re.compile(r'/shareforcures_dataset_[^/]+/')
    sub_pattern = re.compile(r'/researcher_id_(\d+)_project_id_(\d+)/')

    # Read all files in one multithreaded call
    all_file_contents: dict[str, str] = gcp.read_files_multithreaded(full_paths=all_csv_paths)

    main_files = []
    main_file_contents = {}
    sub_datasets_dict = {}  # Key: (researcher_id, project_id), Value: dict with 'files' and 'contents'

    for file_path in all_csv_paths:
        raw_contents = all_file_contents[file_path]
        contents_as_list = list(csv.DictReader(StringIO(raw_contents)))

        if main_pattern.search(file_path):
            main_files.append(file_path)
            main_file_contents[file_path] = contents_as_list
        else:
            sub_match = sub_pattern.search(file_path)
            if sub_match:
                researcher_id = int(sub_match.group(1))
                project_id = int(sub_match.group(2))
                key = (researcher_id, project_id)

                if key not in sub_datasets_dict:
                    sub_datasets_dict[key] = {"files": [], "contents": {}}
                sub_datasets_dict[key]["files"].append(file_path)
                sub_datasets_dict[key]["contents"][file_path] = contents_as_list

    sub_dataset_dirs = [
        SubDatasetInfo(
            files=data["files"],
            file_contents_map=data["contents"],
            researcher_id=researcher_id,
            project_id=project_id
        )
        for (researcher_id, project_id), data in sub_datasets_dict.items()
    ]

    return DatasetInfo(
        main_dataset_files=main_files,
        main_file_contents_map=main_file_contents,
        sub_datasets=sub_dataset_dirs
    )

def get_cloud_csv_contents_as_dict(cloud_path: str, gcp: GCPCloudFunctions) -> list[dict]:
    file_contents = gcp.read_file(cloud_path=cloud_path)
    csv_text = file_contents.lstrip("\ufeff")
    reader = csv.DictReader(StringIO(csv_text))
    return list(reader)

# TODO is this necessary? Seems like we never have the project_name and date_created metadata for sub workspaces
def determine_sub_workspace_name(
        workspace_manager_obj: WorkspaceManager,
        project_id: int,
        researcher_id: int,
        project_name: Optional[str],
        date_created: Optional[str]
    ) -> str:

    if project_name and date_created:
        return workspace_manager_obj.format_workspace_name(project_name=project_name, date_created=date_created)
    else:
        return SUB_WORKSPACE_NAME_TEMPLATE.format(project_id=project_id, researcher_id=researcher_id)

def add_researchers_with_genomics_access_to_group(file_access_contents: list[dict], request_util_obj: RunRequest, dry_run: bool = False) -> None:
    emails_with_genomic_file_access = [row["Email"] for row in file_access_contents]
    if dry_run:
        logging.info(f"DRY RUN: Would add {len(emails_with_genomic_file_access)} researcher(s) to group '{GENOMICS_FILE_ACCESS_GROUP_NAME}'")
        return
    for email in emails_with_genomic_file_access:
        TerraGroups(request_util=request_util_obj).add_user_to_group(
            group=GENOMICS_FILE_ACCESS_GROUP_NAME,
            email=email,
            role=MEMBER,
            continue_if_exists=True
        )

def main():
    """Main execution function."""
    args = get_args()
    continue_if_workspace_exists = args.continue_if_workspace_exists
    dry_run = args.dry_run
    if dry_run:
        logging.info("DRY RUN mode enabled — no workspaces will be created, no data will be uploaded")

    # Single shared GCP client used throughout
    gcp = GCPCloudFunctions()

    blob_metadata = gcp.list_bucket_contents(bucket_name=METADATA_CSVS_BUCKET, file_extensions_to_include=[".csv"], file_name_only=True)
    all_csv_paths = [a["path"] for a in blob_metadata]
    dataset_info = parse_csv_paths_to_dataset_info(all_csv_paths, gcp)

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
    if not validator.validate_all(dataset_info):
        logging.error("Dataset validation failed. Exiting.")
        exit(1)

    # Load participant to sample ID mapping
    participant_to_sample = load_participant_to_sample_mapping(gcp)
    if not participant_to_sample:
        logging.error("Failed to load participant to sample ID mapping. Exiting.")
        exit(1)

    # Initialize the workspace manager object
    workspace_manager = WorkspaceManager(
        request_util=request_util,
        billing_project=BILLING_PROJECT,
        main_workspace_name=MAIN_WORKSPACE_NAME,
        sub_workspace_name_template=SUB_WORKSPACE_NAME_TEMPLATE,
        dry_run=dry_run,
    )

    # Create the main workspace
    main_workspace_terra_obj = workspace_manager.create_main_workspace(continue_if_exists=continue_if_workspace_exists)

    # Create sub workspaces
    sub_workspaces: dict[str, TerraWorkspace] = workspace_manager.create_all_sub_workspaces(
        dataset_info=dataset_info, continue_if_exists=continue_if_workspace_exists
    )

    sub_workspace_metadata = []
    for sub_dataset in dataset_info.sub_datasets:
        workspace_name = determine_sub_workspace_name(
            workspace_manager_obj=workspace_manager,
            project_id=sub_dataset.project_id,
            researcher_id=sub_dataset.researcher_id,
            project_name=sub_dataset.project_name,
            date_created=sub_dataset.date_created
        )

        sub_workspace_terra_obj = sub_workspaces[workspace_name]

        logging.info(f"Extracting participant IDs from researcher_id_{sub_dataset.researcher_id}_project_id_{sub_dataset.project_id}")
        sub_participants = csv_transformer.extract_all_participant_ids_from_files(
            file_contents_map=sub_dataset.file_contents_map
        )

        sub_workspace_metadata.append(
            {
                "workspace_name": workspace_name,
                "participants": sub_participants,
                "sub_workspace_terra_obj": sub_workspace_terra_obj,
            }
        )
        logging.info(f"Workspace '{workspace_name}' has {len(sub_participants)} participants")

    # Check genomics file existence once using all participants from the main dataset
    all_main_participants = csv_transformer.extract_all_participant_ids_from_files(
        file_contents_map=dataset_info.main_file_contents_map
    )
    logging.info(f"Checking genomics file existence for all {len(all_main_participants)} participants...")

    genomics_checker = GenomicsFileChecker(
        gcp=gcp,
        participant_to_sample=participant_to_sample,
        genomics_bucket=GENOMICS_BUCKET,
    )
    all_participant_files = genomics_checker.check_all_participants(all_main_participants)


    # Add researchers with clearance for genomics file access to the genomics access group
    logging.info(f"Adding researchers with genomics access to {GENOMICS_FILE_ACCESS_GROUP_NAME} group...")
    genomics_access_contents = get_cloud_csv_contents_as_dict(GENOMICS_FILE_ACCESS_CSV, gcp)
    add_researchers_with_genomics_access_to_group(file_access_contents=genomics_access_contents, request_util_obj=request_util, dry_run=dry_run)
    logging.info("Completed adding researchers to genomics access group")

    researcher_id_mapping = get_cloud_csv_contents_as_dict(cloud_path=RESEARCHER_ID_TO_EMAIL_MAPPING, gcp=gcp)

    # Process main workspace — all checked participants go into the master sequencing TSV
    process_main_workspace(
        dataset_info=dataset_info,
        terra_workspace_obj=main_workspace_terra_obj,
        csv_transformer=csv_transformer,
        terra_uploader=terra_uploader,
        temp_dir=temp_dir,
        participant_files=all_participant_files,
        dry_run=dry_run,
    )

    # Process sub workspaces
    process_sub_workspaces(
        dataset_info=dataset_info,
        sub_workspace_metadata=sub_workspace_metadata,
        csv_transformer=csv_transformer,
        terra_uploader=terra_uploader,
        temp_dir=temp_dir,
        all_participant_files=all_participant_files,
        workspace_manager_obj=workspace_manager,
        genomics_access_metadata=genomics_access_contents,
        researcher_id_mapping=researcher_id_mapping,
        gcp=gcp,
        dry_run=dry_run,
    )

    # Clean up temp directory
    shutil.rmtree(temp_dir)

    logging.info(f"Successfully processed 1 main workspaces and {len(sub_workspace_metadata)} sub-workspace(s)")

if __name__ == '__main__':
    main()

