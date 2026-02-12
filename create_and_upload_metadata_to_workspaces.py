"""
Main script for Komen Quarterly Uploads.

This script validates CSV datasets from SFTP, creates Terra workspaces,
transforms and uploads CSV data, and generates sequencing file manifests.
"""

import logging
import csv
import tempfile
import shutil
from typing import Dict, Set, List
from pathlib import Path
from argparse import ArgumentParser, Namespace
from datetime import datetime

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

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


def get_args() -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description="Find new csvs in SFTP site and create new workspaces and upload metadata to them")
    parser.add_argument("--continue_if_workspace_exists", "-c", action="store_true",
                        help="Set this flag to true to continue uploading metadata if workspace already exists")
    return parser.parse_args()


def process_main_workspace(
    sftp_info: SFTPDatasetInfo,
    workspace_info: Dict[str, WorkspaceInfo],
    csv_transformer: CSVTransformer,
    terra_uploader: TerraUploader,
    temp_dir: str
) -> None:
    """
    Process and upload data for the main workspace.

    Args:
        sftp_info: SFTP dataset information
        workspace_info: Dictionary of workspace information
        csv_transformer: CSV transformer instance
        terra_uploader: Terra uploader instance
        temp_dir: Temporary directory for transformed files
    """
    if not sftp_info.main_dataset_path or MAIN_WORKSPACE_NAME not in workspace_info:
        return

    logging.info("=" * 60)
    logging.info(f"Processing main workspace: {MAIN_WORKSPACE_NAME}")
    logging.info("=" * 60)

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
    participant_to_workspace: Dict[str, str] = {}
    participant_to_bucket: Dict[str, str] = {}

    for ws_name, ws_info in workspace_info.items():
        if ws_name == MAIN_WORKSPACE_NAME:
            continue
        for participant in ws_info.participants:
            all_participants.add(participant)
            participant_to_workspace[participant] = ws_name
            participant_to_bucket[participant] = ws_info.bucket

    if all_participants:
        sequencing_tsv_path = Path(temp_dir) / "sequencing_files.tsv"

        # Create master list with proper bucket paths
        with open(sequencing_tsv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(['entity:sequencing_files_id', 'participant_id', 'workspace_name', 'cram', 'crai', 'gvcf'])

            for idx, participant_id in enumerate(sorted(all_participants), start=1):
                workspace_name = participant_to_workspace.get(participant_id, '')
                sub_bucket = participant_to_bucket.get(participant_id, '')
                cram_path = f"{sub_bucket}cram/{participant_id}.cram"
                crai_path = f"{sub_bucket}cram/{participant_id}.cram.crai"
                gvcf_path = f"{sub_bucket}gvcf/{participant_id}.g.vcf.gz"
                writer.writerow([str(idx), participant_id, workspace_name, cram_path, crai_path, gvcf_path])

        tsv_files.append(str(sequencing_tsv_path))
        logging.info(f"Created master sequencing files TSV with {len(all_participants)} participants")

    # Upload all TSVs to main workspace
    terra_uploader.upload_all_tsvs_to_workspace(main_info.workspace, tsv_files)
    logging.info(f"Completed upload to main workspace: {len(tsv_files)} files")


def process_sub_workspaces(
    sftp_info: SFTPDatasetInfo,
    workspace_info: Dict[str, WorkspaceInfo],
    csv_transformer: CSVTransformer,
    terra_uploader: TerraUploader,
    temp_dir: str
) -> List[Dict[str, str]]:
    """
    Process and upload data for all sub workspaces.

    Args:
        sftp_info: SFTP dataset information
        workspace_info: Dictionary of workspace information
        csv_transformer: CSV transformer instance
        terra_uploader: Terra uploader instance
        temp_dir: Temporary directory for transformed files

    Returns:
        List of file manifests with source and destination paths
    """
    file_manifest = []

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

        logging.info("=" * 60)
        logging.info(f"Processing sub workspace: {workspace_name}")
        logging.info("=" * 60)

        csv_dir = Path(sub_dir_info.csv_directory_path)
        tsv_files = []

        # Transform and convert all CSVs to TSVs
        for csv_file in csv_dir.glob('*.csv'):
            logging.info(f"Transforming {csv_file.name}...")
            tsv_path = csv_transformer.transform_and_convert_csv(str(csv_file), temp_dir)
            if tsv_path:
                tsv_files.append(tsv_path)

        # Create sequencing files TSV for this sub workspace
        if ws_info.participants:
            sequencing_tsv_path = Path(temp_dir) / f"sequencing_files_{workspace_name}.tsv"
            csv_transformer.create_sequencing_files_tsv(
                participants=ws_info.participants,
                workspace_bucket=ws_info.bucket,
                output_path=str(sequencing_tsv_path),
                is_main=False
            )
            tsv_files.append(str(sequencing_tsv_path))

            # Add sequencing files to manifest
            for participant_id in ws_info.participants:
                # Assume source files exist in a standard location
                # In real implementation, this would come from actual file discovery
                for file_type, extension in [('cram', '.cram'), ('crai', '.cram.crai'), ('gvcf', '.g.vcf.gz')]:
                    file_manifest.append({
                        'source_file': f'/path/to/source/{file_type}/{participant_id}{extension}',
                        'full_destination_path': f"{ws_info.bucket}{file_type}/{participant_id}{extension}"
                    })

        # Upload all TSVs to sub workspace
        terra_uploader.upload_all_tsvs_to_workspace(ws_info.workspace, tsv_files)
        logging.info(f"Completed upload to {workspace_name}: {len(tsv_files)} files")

    return file_manifest


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
    workspace_info: Dict[str, WorkspaceInfo] = {}

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

    # Initialize uploader
    terra_uploader = TerraUploader(request_util=request_util)

    # Process main workspace
    process_main_workspace(sftp_info, workspace_info, csv_transformer, terra_uploader, temp_dir)

    # Process sub workspaces and get file manifest
    file_manifest = process_sub_workspaces(sftp_info, workspace_info, csv_transformer, terra_uploader, temp_dir)
    # Copy sequencing files to their destination buckets
    GCPCloudFunctions().multithread_copy_of_files_with_validation(files_to_copy=file_manifest)

    # Clean up temp directory
    shutil.rmtree(temp_dir)
    logging.info(f"Cleaned up temp directory: {temp_dir}")

    # Log summary
    logging.info("=" * 60)
    logging.info("PROCESSING COMPLETE")
    logging.info("=" * 60)
    logging.info(f"Successfully processed {len(workspaces)} workspace(s)")
    for ws_name, ws_info in workspace_info.items():
        if ws_name == MAIN_WORKSPACE_NAME:
            total_participants = sum(len(w.participants) for w in workspace_info.values() if w.workspace_name != MAIN_WORKSPACE_NAME)
            logging.info(f"  {ws_name}: {total_participants} total participants (master list)")
        else:
            logging.info(f"  {ws_name}: {len(ws_info.participants)} participants")

    # Log file manifest
    logging.info("=" * 60)
    logging.info("FILE MANIFEST")
    logging.info("=" * 60)
    logging.info(f"Total files to transfer: {len(file_manifest)}")
    for i, entry in enumerate(file_manifest[:10], 1):  # Show first 10 as example
        logging.info(f"  {i}. {entry['source_file']} -> {entry['full_destination_path']}")
    if len(file_manifest) > 10:
        logging.info(f"  ... and {len(file_manifest) - 10} more files")

    return file_manifest


if __name__ == '__main__':
    file_manifest = main()

