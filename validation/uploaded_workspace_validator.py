"""Validation of uploaded Terra workspace tables against source datasets and schema expectations."""

import json
import logging
from collections import Counter, defaultdict
from pathlib import Path
from itertools import islice
from typing import Optional

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.terra_util import TerraWorkspace

from models.data_models import DatasetInfo, SubDatasetInfo
from schema_helpers import CsvSchemaHelper
from transformation.genomics_file_checker import GenomicsFileChecker
from validation.dataset_validator import DatasetValidator
from workspace.workspace_manager import WorkspaceManager


# Edit this list when the project-specific tables used for enrollment validation change.
SUB_WORKSPACE_ENROLLMENT_REQUIRED_CSVS = [
    "demographics.csv",
    "patient_profile_cancer_info.csv",
    "patient_profile_eligibility.csv",
]


class UploadedWorkspaceValidator:
    """Validates uploaded Terra workspaces against source CSV content and schema models."""

    def __init__(
        self,
        dataset_validator: DatasetValidator,
        workspace_manager: WorkspaceManager,
        gcp: GCPCloudFunctions,
    ):
        self.dataset_validator = dataset_validator
        self.workspace_manager = workspace_manager
        self.gcp = gcp

    @staticmethod
    def _normalize_text(value: Optional[object]) -> str:
        """Return a lowercase string for case-insensitive comparisons."""
        if value is None:
            return ""
        return str(value).strip().lower()

    @staticmethod
    def _collect_patient_ids(rows: list[dict], patient_id_column: str = "patient_id") -> set[str]:
        """Collect non-empty patient IDs from rows."""
        return {
            str(row[patient_id_column]).strip()
            for row in rows
            if str(row.get(patient_id_column, "")).strip()
        }

    def _get_main_enrollment_rows(self, dataset_info: DatasetInfo) -> list[dict]:
        """Return rows from the main patient_enrollment_status.csv file."""
        for file_path in dataset_info.main_dataset_files:
            if Path(file_path).name == "patient_enrollment_status.csv":
                return dataset_info.main_file_contents_map[file_path]
        raise ValueError("Main dataset is missing patient_enrollment_status.csv")

    @staticmethod
    def _get_sub_enrollment_rows(sub_dataset: SubDatasetInfo) -> list[dict]:
        """Return rows from a sub dataset patient_enrollment_status.csv file."""
        for file_path in sub_dataset.files:
            if Path(file_path).name == "patient_enrollment_status.csv":
                return sub_dataset.file_contents_map[file_path]
        raise ValueError(
            f"Sub dataset researcher_id_{sub_dataset.researcher_id}_project_id_{sub_dataset.project_id} "
            f"is missing patient_enrollment_status.csv"
        )

    def _validate_sub_workspace_enrollment_rules(
        self,
        sub_dataset: SubDatasetInfo,
        main_enrollment_patient_ids: set[str],
    ) -> list[str]:
        """Validate sub-workspace enrollment requirements using the project-specific enrollment CSV."""
        failures = []
        sub_enrollment_rows = self._get_sub_enrollment_rows(sub_dataset)
        sub_active_enrolled_patient_ids = {
            str(row["patient_id"]).strip()
            for row in sub_enrollment_rows
            if str(row.get("patient_id", "")).strip()
            and self._normalize_text(row.get("role_user_status")) == "active"
            and self._normalize_text(row.get("step")) == "enrolled"
        }
        all_sub_enrollment_patient_ids = self._collect_patient_ids(sub_enrollment_rows)

        required_patient_ids = set()
        for file_path, file_rows in sub_dataset.file_contents_map.items():
            if Path(file_path).name in SUB_WORKSPACE_ENROLLMENT_REQUIRED_CSVS:
                required_patient_ids.update(self._collect_patient_ids(file_rows))

        missing_active_enrolled = sorted(required_patient_ids - sub_active_enrolled_patient_ids)
        if missing_active_enrolled:
            failures.append(
                f"Workspace '{sub_dataset.workspace_name}': {len(missing_active_enrolled)} patient_id(s) present in "
                f"{SUB_WORKSPACE_ENROLLMENT_REQUIRED_CSVS} are not active+enrolled in project-specific "
                f"patient_enrollment_status.csv: {missing_active_enrolled}"
            )

        missing_from_main_enrollment = sorted(all_sub_enrollment_patient_ids - main_enrollment_patient_ids)
        if missing_from_main_enrollment:
            failures.append(
                f"Workspace '{sub_dataset.workspace_name}': {len(missing_from_main_enrollment)} patient_id(s) from "
                f"project-specific patient_enrollment_status.csv are missing from main patient_enrollment_status.csv: "
                f"{missing_from_main_enrollment}"
            )

        return failures

    @staticmethod
    def _prepare_uploaded_rows(
        uploaded_rows: list[dict],
        expected_headers: list[str],
        entity_column_name: str,
    ) -> tuple[list[dict], list[str]]:
        """Keep expected headers, fill missing values with empty strings, and report extra columns."""
        ignored_columns = {entity_column_name, entity_column_name.removeprefix("entity:")}
        unexpected_columns = sorted({
            column_name
            for row in uploaded_rows
            for column_name in row
            if column_name not in expected_headers and column_name not in ignored_columns
        })
        prepared_rows = [
            {header_name: row.get(header_name, "") for header_name in expected_headers}
            for row in uploaded_rows
        ]
        return prepared_rows, unexpected_columns

    @staticmethod
    def _row_to_counter_key(row: dict, ordered_headers: list[str]) -> str:
        """Serialize a row deterministically for multiset comparison."""
        ordered_row = {header_name: row.get(header_name) for header_name in ordered_headers}
        return json.dumps(ordered_row, sort_keys=True, default=str)

    def _normalize_rows_for_comparison(
        self,
        rows: list[dict],
        csv_filename: str,
    ) -> tuple[Counter, dict[str, list[int]]]:
        """Validate and normalize rows into comparison keys plus 1-based row numbers."""
        headers = CsvSchemaHelper.get_expected_headers_for_csv_filename(csv_filename)
        normalized_counter = Counter()
        row_numbers_by_key: dict[str, list[int]] = defaultdict(list)

        for row_num, row in enumerate(rows, start=1):
            normalized_key = self._row_to_counter_key(
                self.dataset_validator.build_comparison_row(row=row, filename=csv_filename),
                headers,
            )
            normalized_counter[normalized_key] += 1
            row_numbers_by_key[normalized_key].append(row_num)

        return normalized_counter, dict(row_numbers_by_key)

    @staticmethod
    def _format_row_diff_examples(
        diff_counter: Counter,
        row_numbers_by_key: dict[str, list[int]],
    ) -> list[str]:
        """Format up to three diff examples with row numbers when available."""
        examples = []
        for row_key, count in diff_counter.items():
            for row_num in islice(row_numbers_by_key.get(row_key, []), count):
                examples.append(f"row {row_num}: {row_key}")
                if len(examples) == 3:
                    return examples
        return examples

    @staticmethod
    def _collect_gcs_file_paths(rows: list[dict]) -> set[str]:
        """Collect all full GCS file paths present in uploaded table rows."""
        return {
            value.strip()
            for row in rows
            for value in row.values()
            if isinstance(value, str) and value.strip().startswith("gs://")
        }

    def _collect_workspace_file_paths(
        self,
        workspace: TerraWorkspace,
        file_paths_to_workspaces: defaultdict[str, set[str]],
    ) -> None:
        """Collect any listed file paths from the workspace sequencing files table."""
        actual_table_names = self.workspace_manager.get_workspace_table_names(workspace)
        if "sequencing_files_table" not in actual_table_names:
            return

        # A genomics file can be listed in multiple workspaces, so we collect every path first
        # and only hit GCS once later when _validate_uploaded_file_paths runs.
        uploaded_rows = self.workspace_manager.get_workspace_table_rows(
            workspace=workspace,
            table_name="sequencing_files_table",
        )
        for file_path in self._collect_gcs_file_paths(uploaded_rows):
            file_paths_to_workspaces[file_path].add(workspace.workspace_name)

    def _validate_uploaded_file_paths(
        self,
        file_paths_to_workspaces: defaultdict[str, set[str]],
    ) -> list[str]:
        """Validate that every listed genomics file path exists in GCS, checking each unique path once."""
        existence_map = GenomicsFileChecker.check_file_paths(
            gcp=self.gcp,
            file_paths=list(file_paths_to_workspaces.keys()),
        )
        return [
            f"Genomics file path '{file_path}' listed in workspace(s) {sorted(file_paths_to_workspaces[file_path])} does not exist"
            for file_path, exists in existence_map.items()
            if not exists
        ]

    def _validate_uploaded_table_matches_source(
        self,
        workspace: TerraWorkspace,
        csv_filename: str,
        source_rows: list[dict],
        context: str,
    ) -> list[str]:
        """Validate one uploaded Terra table against its source CSV rows and schema model."""
        failures = []
        table_name = CsvSchemaHelper.csv_filename_to_table_name(csv_filename)
        expected_headers = CsvSchemaHelper.get_expected_headers_for_csv_filename(csv_filename)
        entity_column_name = CsvSchemaHelper.get_entity_column_name_for_table(table_name)
        uploaded_rows = self.workspace_manager.get_workspace_table_rows(workspace=workspace, table_name=table_name)

        # Terra adds the entity id column that never exists in the source CSV. Drop that from
        # comparison so we only compare schema columns that came from the source file.
        normalized_uploaded_rows, unexpected_columns = self._prepare_uploaded_rows(
            uploaded_rows=uploaded_rows,
            expected_headers=expected_headers,
            entity_column_name=entity_column_name,
        )
        normalized_uploaded_rows = [
            self.dataset_validator.coerce_row_values_to_schema_types(row=row, filename=csv_filename)
            for row in normalized_uploaded_rows
        ]

        if unexpected_columns:
            failures.append(
                f"{context}: table '{table_name}' has unexpected uploaded column(s): {unexpected_columns}"
            )

        if not self.dataset_validator.validate_rows_for_filename(
            rows=normalized_uploaded_rows,
            filename=csv_filename,
            context=f"{context} / {table_name}",
        ):
            failures.append(f"{context}: table '{table_name}' failed schema validation")
            return failures

        if len(normalized_uploaded_rows) != len(source_rows):
            failures.append(
                f"{context}: table '{table_name}' row count mismatch. Expected {len(source_rows)} row(s), "
                f"found {len(normalized_uploaded_rows)}"
            )

        # Compare the normalized rows as multisets instead of line-by-line. Terra does not promise
        # to return rows in the same order they were uploaded, so order-only differences are ignored.
        expected_counter, expected_row_numbers = self._normalize_rows_for_comparison(
            rows=source_rows,
            csv_filename=csv_filename,
        )
        uploaded_counter, uploaded_row_numbers = self._normalize_rows_for_comparison(
            rows=normalized_uploaded_rows,
            csv_filename=csv_filename,
        )
        if expected_counter != uploaded_counter:
            missing_rows = self._format_row_diff_examples(
                diff_counter=expected_counter - uploaded_counter,
                row_numbers_by_key=expected_row_numbers,
            )
            unexpected_rows = self._format_row_diff_examples(
                diff_counter=uploaded_counter - expected_counter,
                row_numbers_by_key=uploaded_row_numbers,
            )
            failures.append(
                f"{context}: table '{table_name}' contents do not match source CSV '{csv_filename}'. \n"
                f"Example missing row(s): {missing_rows};\nexample unexpected row(s): {unexpected_rows}"
            )

        return failures

    def _validate_workspace_tables(
        self,
        workspace: TerraWorkspace,
        expected_table_names: list[str],
        source_file_contents_map: dict[str, list[dict]],
        source_csv_names_to_skip: Optional[set[str]] = None,
    ) -> list[str]:
        """Validate expected workspace tables exist and schema-backed uploaded tables match source rows."""
        source_csv_names_to_skip = source_csv_names_to_skip or set()
        failures = []
        actual_table_names = self.workspace_manager.get_workspace_table_names(workspace)
        actual_table_name_set = set(actual_table_names)

        missing_tables = sorted(set(expected_table_names) - actual_table_name_set)
        if missing_tables:
            failures.append(
                f"Workspace '{workspace.workspace_name}' is missing expected table(s): {missing_tables}"
            )

        unexpected_tables = sorted(actual_table_name_set - set(expected_table_names))
        if unexpected_tables:
            logging.warning(
                f"Workspace '{workspace.workspace_name}' has unexpected extra table(s): {unexpected_tables}"
            )

        for file_path, source_rows in source_file_contents_map.items():
            csv_filename = Path(file_path).name
            if csv_filename in source_csv_names_to_skip:
                continue

            table_name = CsvSchemaHelper.csv_filename_to_table_name(csv_filename)
            if table_name not in actual_table_names:
                continue

            if CsvSchemaHelper.get_model_for_csv_filename(csv_filename) is None:
                continue

            failures.extend(
                self._validate_uploaded_table_matches_source(
                    workspace=workspace,
                    csv_filename=csv_filename,
                    source_rows=source_rows,
                    context=f"Workspace '{workspace.workspace_name}'",
                )
            )

        return failures

    def validate_uploaded_workspaces(
        self,
        dataset_info: DatasetInfo,
        main_workspace: Optional[TerraWorkspace],
        sub_workspaces: dict[str, TerraWorkspace],
        researchers_with_genomics_access: set[int],
        workspace_scope: str,
    ) -> list[str]:
        """Validate all requested workspaces and return every failure encountered."""
        failures = []
        file_paths_to_workspaces: defaultdict[str, set[str]] = defaultdict(set)
        main_enrollment_rows = self._get_main_enrollment_rows(dataset_info)
        main_enrollment_patient_ids = self._collect_patient_ids(main_enrollment_rows)

        if workspace_scope in ("all", "main") and main_workspace is not None:
            self._collect_workspace_file_paths(main_workspace, file_paths_to_workspaces)
            failures.extend(
                self._validate_workspace_tables(
                    workspace=main_workspace,
                    expected_table_names=CsvSchemaHelper.get_main_expected_table_names(dataset_info.main_dataset_files),
                    source_file_contents_map=dataset_info.main_file_contents_map,
                )
            )

        if workspace_scope in ("all", "sub"):
            for sub_dataset in dataset_info.sub_datasets:
                sub_workspace = sub_workspaces[sub_dataset.workspace_name]
                self._collect_workspace_file_paths(sub_workspace, file_paths_to_workspaces)
                failures.extend(
                    self._validate_sub_workspace_enrollment_rules(
                        sub_dataset=sub_dataset,
                        main_enrollment_patient_ids=main_enrollment_patient_ids,
                    )
                )
                failures.extend(
                    self._validate_workspace_tables(
                        workspace=sub_workspace,
                        expected_table_names=CsvSchemaHelper.get_sub_expected_table_names(
                            sub_dataset_files=sub_dataset.files,
                            include_sequencing_table=sub_dataset.researcher_id in researchers_with_genomics_access,
                        ),
                        source_file_contents_map=sub_dataset.file_contents_map,
                        source_csv_names_to_skip={"patient_enrollment_status.csv"},
                    )
                )

        failures.extend(self._validate_uploaded_file_paths(file_paths_to_workspaces))
        return failures





