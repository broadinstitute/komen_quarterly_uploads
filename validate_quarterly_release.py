"""
Post-validation script for Komen Quarterly Uploads.

After a quarterly release has been uploaded, this script confirms that:
  1. All participants in each sub workspace are present in the main workspace.
  2. Every participant (scoped by --workspace_scope) is "active" and "enrolled"
     according to patient_enrollment_status.csv.
  3. Every Terra workspace contains exactly the expected set of tables — no missing,
     no unexpected extras.

Validation stops on the first failure so that root-cause issues can be fixed
before deeper checks are attempted.
"""

import logging
from pathlib import Path
from typing import Any, Optional
from argparse import Namespace, ArgumentParser

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.token_util import Token

from constants import (
    METADATA_CSVS_BUCKET,
    BILLING_PROJECT,
    MAIN_WORKSPACE_NAME,
    GENOMICS_FILE_ACCESS_CSV,
    GENOMICS_BUCKET,
    MAIN,
    SUB,
    ALL
)
from csv_schemas import MAIN_ONLY_CSVS
from models.data_models import DatasetInfo, SubDatasetInfo
from transformation.genomics_file_checker import GenomicsFileChecker
from transformation.table_data_utils import convert_csv_rows_to_table_data, create_sequencing_files_table_data
from utilities import (
    list_bucket_path_and_parse_dataset_info,
    extract_all_participant_ids_from_files,
    get_cloud_csv_contents_as_dict,
    load_participant_to_sample_mapping,
    create_calculated_age_diagnosis_table_data,
)
from validation.participant_validator import ParticipantValidation
from workspace.workspace_manager import WorkspaceManager

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


class TerraTablePostValidation:
    """
    Validates that each Terra workspace contains exactly the expected set of tables
    and that the contents of all tables what was uploaded.
    """

    def __init__(
            self,
            dataset_info: DatasetInfo,
            workspace_scope: str,
            main_workspace: Optional[TerraWorkspace],
            sub_workspaces: dict[str, TerraWorkspace],
            researchers_with_genomics_access: set[int],
            gcp: GCPCloudFunctions,
            participant_to_sample: dict[str, str],
    ) -> None:

        self.dataset_info = dataset_info
        self.workspace_scope = workspace_scope
        self.main_workspace = main_workspace
        self.sub_workspaces = sub_workspaces
        self.researchers_with_genomics_access = researchers_with_genomics_access
        self.gcp_obj = gcp
        # Single GenomicsFileChecker instance reused across all workspaces to avoid
        # redundant construction (each construction reads the duplicate mapping CSV).
        self._genomics_checker = GenomicsFileChecker(
            gcp=self.gcp_obj,
            participant_to_sample=participant_to_sample,
            genomics_bucket=GENOMICS_BUCKET,
        )

    def _build_expected_table_data_for_main(
            self, participant_files: dict[str, dict], unique_participants: set[str]
    ) -> dict[str, Any]:
        """
        Build the expected table data for the main workspace.

        Calls convert_csv_rows_to_table_data for every main CSV file, then builds
        the expected sequencing_files_table rows from the pre-checked participant_files
        dict using create_sequencing_files_table_data — the same function used during upload.

        """
        expected_table_data: dict[str, Any] = {}

        for csv_path in self.dataset_info.main_dataset_files:
            file_contents = self.dataset_info.main_file_contents_map[csv_path]
            table_data = convert_csv_rows_to_table_data(csv_path=csv_path, file_contents=file_contents)
            for table_name, data in table_data.items():
                # Store only the row_data; table_id_column is not needed for validation
                expected_table_data[table_name] = data["row_data"]

        # Add the "calculated_age_diagnosis_table" to the list of expected tables to validate
        calculated_age_diagnosis_data = create_calculated_age_diagnosis_table_data(
                file_contents_map=self.dataset_info.main_file_contents_map,
                unique_patient_ids=unique_participants
            )
        for table_name, data in calculated_age_diagnosis_data.items():
            expected_table_data[table_name] = data["row_data"]

        # Build real expected sequencing rows using the live genomics file check results,
        # matching the exact output that create_and_upload_metadata_to_workspaces produces.
        seq_table_data = create_sequencing_files_table_data(participant_files=participant_files)
        expected_table_data["sequencing_files_table"] = seq_table_data["sequencing_files_table"]["row_data"]
        return expected_table_data

    def _build_expected_table_data_for_sub(
            self, sub_dataset: SubDatasetInfo, participant_files: dict[str, dict], unique_participants: set[str]
    ) -> dict[str, Any]:
        """
        Build the expected table data for a single sub workspace.

        Files in MAIN_ONLY_CSVS are skipped because they are only uploaded to the main
        workspace. The sequencing_files_table is included with real rows when the researcher
        has genomics access.

        """
        expected_table_data: dict[str, Any] = {}

        for csv_path in sub_dataset.files:
            # Skip files that are uploaded to the main workspace only
            if Path(csv_path).name in MAIN_ONLY_CSVS:
                continue
            file_contents = sub_dataset.file_contents_map[csv_path]
            table_data = convert_csv_rows_to_table_data(csv_path=csv_path, file_contents=file_contents)
            for table_name, data in table_data.items():
                expected_table_data[table_name] = data["row_data"]

        # Add the "calculated_age_diagnosis_table" to the list of expected tables to validate
        calculated_age_diagnosis_data = create_calculated_age_diagnosis_table_data(
            file_contents_map=sub_dataset.file_contents_map,
            unique_patient_ids=unique_participants
        )
        for table_name, data in calculated_age_diagnosis_data.items():
            expected_table_data[table_name] = data["row_data"]

        # Build real sequencing rows only for researchers with genomics file access
        if sub_dataset.researcher_id in self.researchers_with_genomics_access:
            seq_table_data = create_sequencing_files_table_data(
                participant_files=participant_files
            )
            expected_table_data["sequencing_files_table"] = seq_table_data["sequencing_files_table"]["row_data"]

        return expected_table_data

    @staticmethod
    def _validate_workspace_tables(
        workspace: TerraWorkspace,
        expected_tables: list[str],
        context: str,
    ) -> bool:
        """
        Validate that a workspace contains exactly the expected tables.

        Uses WorkspaceManager.workspace_has_all_tables with check_no_extra=True so
        both missing and unexpected extra tables are caught and logged.
        """
        logging.info(f"[{context}] Validating {len(expected_tables)} expected table(s): {sorted(expected_tables)}")
        return WorkspaceManager.workspace_has_all_tables(
            workspace=workspace,
            expected_tables=expected_tables,
            check_no_extra=True,
        )

    @staticmethod
    def _validate_table_contents(
        workspace: TerraWorkspace,
        table_name: str,
        expected_rows: list[dict],
        context: str,
    ) -> bool:
        """
        Compare expected rows for any Terra table against what is actually stored in Terra.

        Works for both CSV-backed tables (e.g. demographics_table) and
        sequencing_files_table by deriving the synthetic row-ID column name from
        the table name (``{table_name}_id``).

        Rows are compared as sets of (key, value) frozensets so that row order and
        the synthetic ID column do not affect the result.  All values are stringified
        for a consistent comparison regardless of Python type returned by model_dump.
        """
        # Fetch current rows from the Terra workspace
        actual_rows = WorkspaceManager.get_table_rows(workspace, table_name)

        # Each table has a synthetic ID column named {table_name}_id that is assigned
        # sequentially at upload time — exclude it so IDs don't cause false mismatches.
        id_col = f"{table_name}_id"

        def row_key(row: dict) -> frozenset:
            # frozenset is used because:
            #   - it is hashable, so it can be placed in a set for O(1) lookup
            #   - it is order-independent, so column order differences don't matter
            #   - each (key, value) pair becomes one element, making set difference
            #     straightforward for identifying missing/extra rows
            return frozenset(
                (k, str(v) if v is not None else "")
                for k, v in row.items()
                if k != id_col
            )

        # Build lookup dicts so we can retrieve the full original row (including the
        # ID column) when logging mismatches, rather than the stripped frozenset version.
        expected_key_to_row = {row_key(r): r for r in expected_rows}
        actual_key_to_row = {row_key(r): r for r in actual_rows}

        expected_keys = set(expected_key_to_row.keys())
        actual_keys = set(actual_key_to_row.keys())

        missing = expected_keys - actual_keys
        extra = actual_keys - expected_keys

        if missing:
            logging.error(
                f"[{context}] {table_name}: "
                f"{len(missing)} expected row(s) not found in Terra workspace"
            )
            for row_fs in sorted(missing, key=lambda fs: dict(fs).get("participant_id", "")):
                logging.error(
                    f"[{context}] {table_name} missing row: {expected_key_to_row[row_fs]}"
                )
        if extra:
            logging.error(
                f"[{context}] {table_name}: "
                f"{len(extra)} unexpected extra row(s) found in Terra workspace"
            )
            for row_fs in sorted(extra, key=lambda fs: dict(fs).get("participant_id", "")):
                logging.error(
                    f"[{context}] {table_name} extra row: {actual_key_to_row[row_fs]}"
                )

        if missing or extra:
            return False

        logging.info(
            f"[{context}] {table_name}: {len(expected_rows)} row(s) validated"
        )
        return True

    def run(self) -> bool:
        """
        Execute all Terra table existence and content checks in sequence.

        Runs the genomics file check once for all main participants upfront, then
        filters the result per sub workspace rather than re-running the check for each.
        Stops on the first failure to avoid misleading downstream results.
        """
        logging.info(f"Starting Terra table post-validation (workspace_scope='{self.workspace_scope}')")

        # Run the genomics file check once for all main participants.
        # Sub workspace results are sliced from this dict rather than re-checked,
        # avoiding redundant multithreaded GCP calls.
        all_main_participants = extract_all_participant_ids_from_files(self.dataset_info.main_file_contents_map)
        logging.info(f"Checking genomics files for {len(all_main_participants)} main participant(s)")
        all_participant_files = self._genomics_checker.check_all_participants(all_main_participants)

        # Validate the main workspace
        if self.workspace_scope in (ALL, MAIN):
            if self.main_workspace is None:
                logging.error("Main workspace object is None — cannot validate main workspace tables")
                return False

            logging.info("Building expected table data for main workspace...")
            expected_main_data = self._build_expected_table_data_for_main(
                participant_files=all_participant_files, unique_participants=all_main_participants
            )
            expected_main_tables = list(expected_main_data.keys())

            # Step 1: verify all expected tables exist with no extras
            if not self._validate_workspace_tables(
                workspace=self.main_workspace,
                expected_tables=expected_main_tables,
                context="Main",
            ):
                logging.error("Main workspace table validation failed — stopping post-validation")
                return False

            # Step 2: verify the row contents of every expected table against Terra.
            # This covers all CSV-backed tables (demographics, biomarker, etc.) as well
            # as sequencing_files_table, using the same comparison logic for each.
            for table_name, expected_rows in expected_main_data.items():
                if not self._validate_table_contents(
                    workspace=self.main_workspace,
                    table_name=table_name,
                    expected_rows=expected_rows,
                    context="Main",
                ):
                    logging.error(f"Main workspace '{table_name}' content validation failed — stopping post-validation")
                    return False

            logging.info(f"Main workspace validated successfully ({len(expected_main_tables)} table(s))")

        # Validate each sub workspace
        if self.workspace_scope in (ALL, SUB):
            for sub_dataset in self.dataset_info.sub_datasets:
                workspace_name = sub_dataset.workspace_name

                if workspace_name not in self.sub_workspaces:
                    logging.error(f"No TerraWorkspace object found for sub workspace '{workspace_name}' — stopping post-validation")
                    return False

                sub_workspace_obj = self.sub_workspaces[workspace_name]

                # Filter the already-checked participant file map to this sub workspace's
                # participants so we don't re-run the expensive GCP existence check.
                sub_participants = extract_all_participant_ids_from_files(sub_dataset.file_contents_map)
                sub_participant_files = {
                    p: all_participant_files[p]
                    for p in sub_participants
                    if p in all_participant_files
                }

                logging.info(f"Building expected table data for sub workspace '{workspace_name}'")
                expected_sub_data = self._build_expected_table_data_for_sub(
                    sub_dataset=sub_dataset, participant_files=sub_participant_files, unique_participants=sub_participants
                )
                expected_sub_tables = list(expected_sub_data.keys())

                # Step 1: verify all expected tables exist with no extras
                if not self._validate_workspace_tables(
                    workspace=sub_workspace_obj,
                    expected_tables=expected_sub_tables,
                    context=workspace_name,
                ):
                    logging.error(f"Sub workspace '{workspace_name}' table validation failed — stopping post-validation")
                    return False

                # Step 2: verify the row contents of every expected table against Terra.
                # Covers all CSV-backed tables and sequencing_files_table (where applicable).
                for table_name, expected_rows in expected_sub_data.items():
                    if not self._validate_table_contents(
                        workspace=sub_workspace_obj,
                        table_name=table_name,
                        expected_rows=expected_rows,
                        context=workspace_name,
                    ):
                        logging.error(f"Sub workspace '{workspace_name}' '{table_name}' content validation failed — stopping post-validation")
                        return False

                logging.info("Sub workspace '{workspace_name}' validated successfully ({len(expected_sub_tables)} table(s))")

        logging.info("All Terra table post-validation checks passed successfully")
        return True


def get_args() -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description="Validate quarterly release workspaces")
    parser.add_argument(
        "--workspace_scope", "-w",
        choices=[ALL, MAIN, SUB],
        default=ALL,
        help=f"Which workspaces to validate: '{ALL}' (default), '{MAIN}' only, or '{SUB}' only",
    )
    parser.add_argument(
        "--include_workspaces", "-i",
        nargs="+",
        help=(
            "Optional space-separated list of exact sub workspace names to validate. "
            "All other sub workspaces are skipped. "
            "If any name is not found in the dataset an error is raised."
        ),
    )
    parser.add_argument(
        "--exclude_workspaces", "-e",
        nargs="+",
        help=(
            "Optional space-separated list of exact sub workspace names to skip validation entirely. "
            "A warning is logged for any name not found in the dataset."
        ),
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    gcp_client = GCPCloudFunctions()
    token = Token()
    request_util = RunRequest(token=token)

    include_workspaces = args.include_workspaces
    if include_workspaces:
        logging.info(f"Include filter active — will only validate: {include_workspaces}")

    exclude_workspaces = args.exclude_workspaces
    if exclude_workspaces:
        logging.info(f"Exclude filter active — will skip: {exclude_workspaces}")

    # Step 1: Download all CSV file paths and their contents
    dataset_info = list_bucket_path_and_parse_dataset_info(
        bucket=METADATA_CSVS_BUCKET,
        gcp=gcp_client,
        include_workspaces=include_workspaces,
        exclude_workspaces=exclude_workspaces,
    )

    # Step 2: Initialise Terra API clients and build TerraWorkspace objects for the already-existing workspaces.
    main_workspace: TerraWorkspace = None
    sub_workspaces: dict[str, TerraWorkspace] = {}

    if args.workspace_scope in (ALL, MAIN):
        main_workspace = TerraWorkspace(
            billing_project=BILLING_PROJECT,
            workspace_name=MAIN_WORKSPACE_NAME,
            request_util=request_util,
        )

    if args.workspace_scope in (ALL, SUB):
        for sub_dataset in dataset_info.sub_datasets:
            sub_workspaces[sub_dataset.workspace_name] = TerraWorkspace(
                billing_project=BILLING_PROJECT,
                workspace_name=sub_dataset.workspace_name,
                request_util=request_util,
            )

    # Step 3: Load the genomics access list.
    # This determines whether sequencing_files_table is expected in each sub workspace when running TerraTablePostValidation
    genomics_access_contents = get_cloud_csv_contents_as_dict(GENOMICS_FILE_ACCESS_CSV, gcp_client)
    researchers_with_genomics_access = {int(row["Researcher ID"]) for row in genomics_access_contents}
    logging.info(f"Loaded genomics access list: {len(researchers_with_genomics_access)} researcher(s) have access")


    # Step 4: Run participant post-validation.
    # Checks that all participants are active/enrolled and that sub workspace participants are a subset of the main dataset.
    participant_validator = ParticipantValidation(
        dataset_info=dataset_info,
        workspace_scope=args.workspace_scope,
    )
    if not participant_validator.run():
        logging.error("Participant post-validation failed.")
        exit(1)

    # Step 5: Load the participant-to-sample mapping and run Terra
    # table post-validation.
    participant_to_sample = load_participant_to_sample_mapping(gcp=gcp_client)
    if not participant_to_sample:
        logging.error("Failed to load participant to sample ID mapping. Exiting.")
        exit(1)

    table_validator = TerraTablePostValidation(
        dataset_info=dataset_info,
        workspace_scope=args.workspace_scope,
        main_workspace=main_workspace,
        sub_workspaces=sub_workspaces,
        researchers_with_genomics_access=researchers_with_genomics_access,
        gcp=gcp_client,
        participant_to_sample=participant_to_sample,
    )
    if not table_validator.run():
        logging.error("Terra table post-validation failed.")
        exit(1)

    logging.info("Post-validation completed successfully.")
