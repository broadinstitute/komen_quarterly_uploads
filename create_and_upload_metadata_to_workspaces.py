"""
Main script for Komen Quarterly Uploads.

This script validates CSV datasets from a Google bucket, creates Terra workspaces,
converts CSV rows into Terra table payloads, and uploads all metadata with batch upsert.
"""

import logging
import re
from pathlib import Path
from argparse import ArgumentParser, Namespace
from typing import Optional

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace, TerraGroups, MEMBER, ADMIN
from ops_utils.token_util import Token
from ops_utils.gcp_utils import GCPCloudFunctions

from constants import (
    MAIN_WORKSPACE_NAME,
    BILLING_PROJECT,
    METADATA_CSVS_BUCKET,
    GENOMICS_BUCKET,
    PARTICIPANT_TO_SAMPLE_MAPPING_FILE_PATH,
    GENOMICS_FILE_ACCESS_CSV,
    RESEARCHER_ID_TO_EMAIL_MAPPING,
    GENOMICS_FILE_ACCESS_GROUP_NAME,
    RESEARCH_ADMIN_GROUP_EMAIL,
)
from csv_schemas import MAIN_ONLY_CSVS
from models.data_models import DatasetInfo
from utilities import (
    list_bucket_path_and_parse_dataset_info,
    extract_all_participant_ids_from_files,
    get_cloud_csv_contents_as_dict,
    get_expected_main_table_names,
    get_expected_sub_table_names,
    load_participant_to_sample_mapping,
)
from validation.dataset_validator import DatasetValidator
from workspace.workspace_manager import WorkspaceManager
from transformation.table_data_utils import convert_csv_rows_to_table_data, create_sequencing_files_table_data
from transformation.genomics_file_checker import GenomicsFileChecker


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

def get_args() -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description="Find new csvs in SFTP site and create new workspaces and upload metadata to them")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Skip table existence checks and upload all data regardless of current workspace state")
    parser.add_argument("--dry_run", "-d", action="store_true",
                        help="Log what would happen without creating workspaces, uploading metadata, or modifying ACLs")
    parser.add_argument("--workspace_scope", "-w", choices=["all", "main", "sub"], default="all",
                        help="Which workspaces to create and upload: 'all' (default), 'main' only, or 'sub' only")
    parser.add_argument("--dataset_notes", "-n", default=None,
                        help="Optional path to a file whose contents will be set as the description on every workspace created")
    return parser.parse_args()


def process_main_workspace(
    dataset_info: DatasetInfo,
    terra_workspace_obj: TerraWorkspace,
    workspace_manager: WorkspaceManager,
    participant_files: dict[str, dict[str, Optional[str]]],
    dataset_notes: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Process and upload data for the main workspace.

    The caller is responsible for ensuring this is only called when the workspace
    actually needs uploading — no table existence check is performed here.

    Args:
        dataset_info: Dataset information
        terra_workspace_obj: TerraWorkspace object for the main workspace
        workspace_manager: WorkspaceManager instance used for setting description and column order
        participant_files: Pre-checked dict of participant_id -> {file_type: path_or_None}
                           as returned by GenomicsFileChecker.check_all_participants().
        dataset_notes: Optional workspace description string to set.
        dry_run: If True, log what would be uploaded without actually uploading.
    """
    if dataset_notes:
        workspace_manager.set_workspace_description(terra_workspace_obj, dataset_notes)

    table_data = {}
    for csv_file_path in dataset_info.main_dataset_files:
        file_contents = dataset_info.main_file_contents_map[csv_file_path]
        table_data.update(
            convert_csv_rows_to_table_data(
                csv_path=csv_file_path,
                file_contents=file_contents,
            )
        )

    table_data.update(
        create_sequencing_files_table_data(
            participant_files=participant_files,
        )
    )
    logging.info(f"Built master sequencing files table data with {len(participant_files)} participants")
    if dry_run:
        logging.info(
            f"DRY RUN: Would upload {len(table_data)} table(s) to main workspace '{terra_workspace_obj.workspace_name}'"
        )
    else:
        workspace_manager.upload_table_data_to_workspace(terra_workspace_obj, table_data)
        logging.info(f"Completed upload to main workspace: {len(table_data)} tables")

def process_sub_workspaces(
    dataset_info: DatasetInfo,
    sub_workspace_metadata: list[dict],
    workspace_manager_obj: WorkspaceManager,
    all_participant_files: dict,
    genomics_access_metadata: list[dict],
    researcher_id_mapping: list[dict],
    gcp: GCPCloudFunctions,
    workspaces_needing_upload: set[str],
    dataset_notes: Optional[str] = None,
    dry_run: bool = False,
) -> list[str]:
    """
    Process and upload data for all sub workspaces.

    The caller is responsible for pre-filtering via workspaces_needing_upload so that
    no table existence checks are repeated here.

    Args:
        dataset_info: Dataset information
        sub_workspace_metadata: List of dictionaries with sub workspace name, participant set, and TerraWorkspace object
        workspace_manager_obj: WorkspaceManager instance
        all_participant_files: Full dict of participant_id -> {file_type: path_or_None}
                               as returned by GenomicsFileChecker.check_all_participants().
                               Filtered per workspace inside this function.
        genomics_access_metadata: List of all researchers with clearance for genomics file access
        researcher_id_mapping: List of dictionaries mapping ALL researchers IDs to emails
        gcp: Shared GCPCloudFunctions instance
        workspaces_needing_upload: Set of workspace names that need uploading (pre-determined by caller)
        dataset_notes: Optional workspace description string to set.
        dry_run: If True, log what would be uploaded/modified without actually doing it.

    Returns:
        List of mapping failure strings for any researcher IDs not found in the researcher mapping.
    """
    mapping_failures = []

    for sub_dataset in dataset_info.sub_datasets:
        if sub_dataset.workspace_name not in workspaces_needing_upload:
            logging.info(f"Skipping sub workspace '{sub_dataset.workspace_name}' — all tables already present")
            continue

        ws_meta = [a for a in sub_workspace_metadata if a["workspace_name"] == sub_dataset.workspace_name][0]
        sub_workspace_terra_obj = ws_meta["sub_workspace_terra_obj"]

        researcher_id = None
        for csv_file_path in sub_dataset.files:
            if re.match(r"^researcher_id_\d+_project_id_\d+_metadata\.csv$", Path(csv_file_path).name):
                project_metadata = get_cloud_csv_contents_as_dict(csv_file_path, gcp)
                researcher_id = int([row["researcher_id"] for row in project_metadata][0])
                if researcher_id != sub_dataset.researcher_id:
                    logging.warning(f"Researcher ID mismatch in {csv_file_path}: expected {sub_dataset.researcher_id}, found {researcher_id}")

        has_genomics_access = researcher_id in {int(user["Researcher ID"]) for user in genomics_access_metadata}

        if dataset_notes:
            workspace_manager_obj.set_workspace_description(sub_workspace_terra_obj, dataset_notes)

        table_data = {}
        for csv_file_path in sub_dataset.files:
            # patient_enrollment_status.csv exists in sub directories but only uploads to the main workspace.
            if Path(csv_file_path).name not in MAIN_ONLY_CSVS:
                file_contents = sub_dataset.file_contents_map[csv_file_path]
                table_data.update(
                    convert_csv_rows_to_table_data(
                        csv_path=csv_file_path,
                        file_contents=file_contents,
                    )
                )

        if has_genomics_access:
            logging.info("Researcher has genomics access - building sequencing files table data for sub workspace")
            participant_files = {p: all_participant_files[p] for p in ws_meta["participants"] if p in all_participant_files}
            table_data.update(
                create_sequencing_files_table_data(
                    participant_files=participant_files,
                )
            )
        else:
            logging.info("Researcher does not have genomics access - skipping sequencing files table for sub workspace")
        if dry_run:
            logging.info(
                f"DRY RUN: Would upload {len(table_data)} table(s) to sub workspace '{sub_dataset.workspace_name}'"
            )
        else:
            workspace_manager_obj.upload_table_data_to_workspace(sub_workspace_terra_obj, table_data)
            logging.info(f"Completed upload to {sub_dataset.workspace_name}: {len(table_data)} tables")

        researcher_email = [u.get("Email") for u in researcher_id_mapping if u.get("Researcher ID") == researcher_id]
        if not researcher_email:
            failure = (
                f"Researcher ID '{researcher_id}' (workspace '{sub_dataset.workspace_name}') "
                f"not found in researcher mapping ({RESEARCHER_ID_TO_EMAIL_MAPPING})"
            )
            mapping_failures.append(failure)
            logging.warning(failure)
        if dry_run:
            if researcher_email:
                logging.info(f"DRY RUN: Would grant READER access to '{researcher_email[0]}' on workspace '{sub_dataset.workspace_name}'")
            logging.info(f"DRY RUN: Would grant OWNER access to '{RESEARCH_ADMIN_GROUP_EMAIL}' on workspace '{sub_dataset.workspace_name}'")
        else:
            if researcher_email:
                sub_workspace_terra_obj.update_user_acl(
                    email=researcher_email[0],
                    access_level="READER",
                    can_share=False,
                    can_compute=False,
                    invite_users_not_found=True
                )
            sub_workspace_terra_obj.update_user_acl(email=RESEARCH_ADMIN_GROUP_EMAIL, access_level="OWNER")

    return mapping_failures



def add_researchers_with_genomics_access_to_group(file_access_contents: list[dict], request_util_obj: RunRequest, dry_run: bool = False) -> None:
    emails_with_genomic_file_access = [row["Email"] for row in file_access_contents]
    if dry_run:
        logging.info(f"DRY RUN: Would add {len(emails_with_genomic_file_access)} researcher(s) to group '{GENOMICS_FILE_ACCESS_GROUP_NAME}'")
        return
    terra_group = TerraGroups(request_util=request_util_obj)
    group_members = terra_group.check_group_members(group=GENOMICS_FILE_ACCESS_GROUP_NAME, role=MEMBER).json()
    admin_members = terra_group.check_group_members(group=GENOMICS_FILE_ACCESS_GROUP_NAME, role=ADMIN).json()
    for email in emails_with_genomic_file_access:
        # Do not add users who are already members or admins of the group
        if email not in admin_members + group_members:
            terra_group.add_user_to_group(
                group=GENOMICS_FILE_ACCESS_GROUP_NAME,
                email=email,
                role=MEMBER,
                continue_if_exists=True
            )
        else:
            logging.info(f"User '{email}' already has access to genomics files group '{GENOMICS_FILE_ACCESS_GROUP_NAME}' — skipping")




def main():
    """Main execution function."""
    args = get_args()
    force = args.force
    dry_run = args.dry_run
    workspace_scope = args.workspace_scope
    if dry_run:
        logging.info("DRY RUN mode enabled — no workspaces will be created, no data will be uploaded")
    logging.info(f"Workspace scope: '{workspace_scope}'")

    dataset_notes = args.dataset_notes
    if dataset_notes:
        with open(dataset_notes, "r", encoding="utf-8") as f:
            dataset_notes = f.read()
        logging.info(f"Loaded dataset notes from {args.dataset_notes}")

    # Single shared GCP client used throughout
    gcp = GCPCloudFunctions()
    
    dataset_info = list_bucket_path_and_parse_dataset_info(bucket=METADATA_CSVS_BUCKET, gcp=gcp)
    
    # Initialize components
    validator = DatasetValidator()
    token = Token()
    request_util = RunRequest(token=token)

    # Validate all datasets
    if not validator.validate_all(dataset_info):
        logging.error("Dataset validation failed. Exiting.")
        exit(1)

    # Initialize the workspace manager object
    workspace_manager = WorkspaceManager(
        request_util=request_util,
        billing_project=BILLING_PROJECT,
        main_workspace_name=MAIN_WORKSPACE_NAME,
        dry_run=dry_run,
    )

    # Create the main workspace
    main_workspace_terra_obj = None
    if workspace_scope in ("all", "main"):
        main_workspace_terra_obj = workspace_manager.create_main_workspace()

    # Create sub workspaces
    sub_workspaces: dict[str, TerraWorkspace] = {}
    if workspace_scope in ("all", "sub"):
        sub_workspaces = workspace_manager.create_all_sub_workspaces(dataset_info=dataset_info)

    # Load genomics access list early so we can correctly determine expected tables per sub workspace
    # before deciding whether uploads are needed (avoids falsely flagging missing sequencing_files_table
    # for researchers who don't have genomics access).
    genomics_access_contents = get_cloud_csv_contents_as_dict(GENOMICS_FILE_ACCESS_CSV, gcp)
    researchers_with_genomics_access = {int(row["Researcher ID"]) for row in genomics_access_contents}

    # Determine which workspaces actually need uploads before doing any heavy processing.
    # A workspace is skipped only if all its expected tables already exist (and --force is not set).
    if workspace_scope in ("all", "main") and not dry_run:
        # Derive expected table names from the main dataset files using the shared utility
        main_expected_tables = get_expected_main_table_names(dataset_info.main_dataset_files)
        main_needs_upload = not workspace_manager.should_skip_uploads(main_workspace_terra_obj, main_expected_tables, force)
    elif workspace_scope in ("all", "main"):
        main_needs_upload = True  # dry_run always proceeds
    else:
        main_needs_upload = False

    sub_workspaces_needing_upload: set[str] = set()
    if workspace_scope in ("all", "sub") and not dry_run:
        for sub_dataset in dataset_info.sub_datasets:
            sub_workspace_terra_obj = sub_workspaces[sub_dataset.workspace_name]
            # Derive expected table names using the shared utility; MAIN_ONLY_CSVS are excluded
            sub_expected_tables = get_expected_sub_table_names(
                sub_files=sub_dataset.files,
                has_genomics_access=sub_dataset.researcher_id in researchers_with_genomics_access,
            )
            if not workspace_manager.should_skip_uploads(sub_workspace_terra_obj, sub_expected_tables, force):
                sub_workspaces_needing_upload.add(sub_dataset.workspace_name)
    elif workspace_scope in ("all", "sub"):
        sub_workspaces_needing_upload = {sd.workspace_name for sd in dataset_info.sub_datasets}  # dry_run always proceeds

    any_workspace_needs_upload = main_needs_upload or bool(sub_workspaces_needing_upload)

    if not any_workspace_needs_upload:
        logging.info("All workspaces already have all expected tables — nothing to upload.")
        return

    # From here on, only runs if at least one workspace needs uploading.

    # Always extract all main participants regardless of scope — sub workspace participants
    # must be a subset of main, so we need the full main set to validate against.
    all_main_participants = extract_all_participant_ids_from_files(
        file_contents_map=dataset_info.main_file_contents_map
    )

    sub_workspace_metadata = []
    unknown_participant_failures = 0
    if workspace_scope in ("all", "sub"):
        for sub_dataset in dataset_info.sub_datasets:
            if sub_dataset.workspace_name not in sub_workspaces_needing_upload:
                continue
            sub_workspace_terra_obj = sub_workspaces[sub_dataset.workspace_name]
            logging.info(f"Extracting participant IDs from researcher_id_{sub_dataset.researcher_id}_project_id_{sub_dataset.project_id}")
            sub_participants = extract_all_participant_ids_from_files(
                file_contents_map=sub_dataset.file_contents_map
            )

            unknown_participants = sub_participants - all_main_participants
            if unknown_participants:
                for participant_id in sorted(unknown_participants):
                    logging.error(f"Participant '{participant_id}' in sub workspace '{sub_dataset.workspace_name}' not found in main workspace")
                    unknown_participant_failures += 1
            else:
                sub_workspace_metadata.append(
                    {
                        "workspace_name": sub_dataset.workspace_name,
                        "participants": sub_participants,
                        "sub_workspace_terra_obj": sub_workspace_terra_obj,
                    }
                )
                logging.info(f"Workspace '{sub_dataset.workspace_name}' has {len(sub_participants)} participants — all present in main")

    if unknown_participant_failures:
        raise ValueError(
            f"{unknown_participant_failures} participant(s) across sub workspaces are not present in the main workspace. "
            f"See error log entries above for details."
        )

    # Determine which participants to check genomics files for.
    # If main is being uploaded we need all main participants because they all appear in the
    # main sequencing_files_table. If only sub workspaces are being uploaded we only need the
    # union of participants across those workspaces.
    if main_needs_upload:
        participants_to_check = all_main_participants
        logging.info(f"Checking genomics files for all {len(participants_to_check)} main participants")
    else:
        participants_to_check = set()
        for ws_meta in sub_workspace_metadata:
            participants_to_check.update(ws_meta["participants"])
        logging.info(f"Checking genomics files for {len(participants_to_check)} participant(s) across {len(sub_workspace_metadata)} sub workspace(s) needing upload")

    participant_to_sample = load_participant_to_sample_mapping(gcp)
    if not participant_to_sample:
        logging.error("Failed to load participant to sample ID mapping. Exiting.")
        exit(1)

    genomics_checker = GenomicsFileChecker(
        gcp=gcp,
        participant_to_sample=participant_to_sample,
        genomics_bucket=GENOMICS_BUCKET,
    )
    all_participant_files = genomics_checker.check_all_participants(participants_to_check)

    # Record any participants that had no entry in the onyx mapping CSV
    mapping_failures = []
    for participant_id in participants_to_check:
        if participant_id not in all_participant_files:
            failure = (
                f"Participant '{participant_id}' not found in onyx mapping "
                f"({PARTICIPANT_TO_SAMPLE_MAPPING_FILE_PATH})"
            )
            mapping_failures.append(failure)
            logging.warning(failure)

    # Add researchers with clearance for genomics file access to the genomics access group
    logging.info(f"Adding researchers with genomics access to {GENOMICS_FILE_ACCESS_GROUP_NAME} group...")
    add_researchers_with_genomics_access_to_group(file_access_contents=genomics_access_contents, request_util_obj=request_util, dry_run=dry_run)
    logging.info("Completed adding researchers to genomics access group")

    researcher_id_mapping = get_cloud_csv_contents_as_dict(cloud_path=RESEARCHER_ID_TO_EMAIL_MAPPING, gcp=gcp)

    # Process main workspace
    if workspace_scope in ("all", "main") and main_needs_upload:
        process_main_workspace(
            dataset_info=dataset_info,
            terra_workspace_obj=main_workspace_terra_obj,
            workspace_manager=workspace_manager,
            participant_files=all_participant_files,
            dataset_notes=dataset_notes,
            dry_run=dry_run,
        )

    # Process sub workspaces
    if workspace_scope in ("all", "sub") and sub_workspaces_needing_upload:
        sub_mapping_failures = process_sub_workspaces(
            dataset_info=dataset_info,
            sub_workspace_metadata=sub_workspace_metadata,
            workspace_manager_obj=workspace_manager,
            all_participant_files=all_participant_files,
            genomics_access_metadata=genomics_access_contents,
            researcher_id_mapping=researcher_id_mapping,
            gcp=gcp,
            workspaces_needing_upload=sub_workspaces_needing_upload,
            dataset_notes=dataset_notes,
            dry_run=dry_run,
        )
        mapping_failures.extend(sub_mapping_failures)


    logging.info(
        f"Successfully processed "
        f"{'1 main workspace' if workspace_scope in ('all', 'main') else '0 main workspaces'} and "
        f"{len(sub_workspace_metadata) if workspace_scope in ('all', 'sub') else 0} sub-workspace(s)"
    )

    if mapping_failures:
        for failure in mapping_failures:
            logging.error(f"MAPPING FAILURE: {failure}")
        raise RuntimeError(
            f"All workspace creation and metadata uploads completed successfully, but "
            f"{len(mapping_failures)} mapping failure(s) were encountered. "
            f"See MAPPING FAILURE log entries above for details."
        )

if __name__ == '__main__':
    main()
