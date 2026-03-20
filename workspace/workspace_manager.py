"""Terra workspace management."""

import logging
import re

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace

from models.data_models import DatasetInfo, SubDatasetInfo
from transformation.column_order import TABLE_COLUMN_ORDER


_METADATA_TABLE_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata_table$")


class WorkspaceManager:
    """Manages Terra workspace creation and data upload operations."""

    def __init__(
            self, request_util: RunRequest, billing_project: str, main_workspace_name: str, dry_run: bool = False
    ):
        """
        Initialize WorkspaceManager.

        Args:
            request_util: Request utility for Terra API calls
            billing_project: Terra billing project name
            main_workspace_name: Main workspace name
            dry_run: If True, log workspace creation instead of actually creating workspaces
        """
        self.request_util = request_util
        self.billing_project = billing_project
        self.main_workspace_name = main_workspace_name
        self.dry_run = dry_run

    def set_workspace_description(self, workspace: TerraWorkspace, description: str) -> None:
        """
        Set the description attribute on a Terra workspace.

        Args:
            workspace: TerraWorkspace object to update
            description: String to set as the workspace description
        """
        if self.dry_run:
            logging.info(f"DRY RUN: Would set description on workspace '{workspace.workspace_name}'")
            return
        workspace.update_workspace_attributes([
            {
                "op": "AddUpdateAttribute",
                "attributeName": "description",
                "addUpdateAttribute": description,
            }
        ])
        logging.info(f"Set description on workspace '{workspace.workspace_name}'")

    @staticmethod
    def _set_column_order_for_uploaded_tables(workspace: TerraWorkspace, table_names: list[str]) -> None:
        """Set the Terra column order for all uploaded tables in a single call."""
        column_order_dict = {}
        for table_name in table_names:
            stem = table_name.removesuffix("_table")
            if stem in TABLE_COLUMN_ORDER:
                column_order_dict[table_name] = TABLE_COLUMN_ORDER[stem]
            elif _METADATA_TABLE_PATTERN.match(table_name):
                column_order_dict[table_name] = TABLE_COLUMN_ORDER["researcher_project_metadata"]
            else:
                logging.warning(
                    f"No column order defined for table '{table_name}' — skipping column order for this table"
                )

        if column_order_dict:
            logging.info(
                f"Setting column order for {len(column_order_dict)} table(s) in workspace '{workspace.workspace_name}'"
            )
            workspace.set_table_column_order(column_order=column_order_dict)

    def upload_table_data_to_workspace(
        self,
        workspace: TerraWorkspace,
        table_data: dict[str, dict[str, object]],
    ) -> bool:
        """Upload all table data to a workspace with a single batch upsert call."""
        if self.dry_run:
            logging.info(
                f"DRY RUN: Would upload {len(table_data)} table(s) to workspace '{workspace.workspace_name}'"
            )
            return True

        logging.info(
            f"Uploading {len(table_data)} table(s) to workspace '{workspace.workspace_name}' with batch upsert"
        )
        workspace.upload_metadata_with_batch_upsert(table_data=table_data)
        self._set_column_order_for_uploaded_tables(workspace, list(table_data.keys()))
        return True

    @staticmethod
    def workspace_has_all_tables(
        workspace: TerraWorkspace,
        expected_tables: list[str],
        check_no_extra: bool = False,
    ) -> bool:
        """
        Check whether all expected tables exist in the workspace, and optionally
        that no unexpected extra tables are present.

        Args:
            workspace:       TerraWorkspace object to inspect.
            expected_tables: List of table names that must be present
                             (e.g. ['demographics_table', 'biomarker_table']).
            check_no_extra:  When True, also fail if the workspace contains tables
                             that are not in expected_tables. Defaults to False so
                             existing callers (upload skip-check) are unaffected.

        Returns:
            True if validation passes, False otherwise.
        """
        # Fetch the current set of tables from the Terra workspace
        workspace_info = workspace.get_workspace_entity_info().json()
        workspace_tables = list(workspace_info.keys())

        # Check for expected tables that are missing from the workspace
        missing = [t for t in expected_tables if t not in workspace_tables]
        if missing:
            for t in sorted(missing):
                logging.error(
                    f"Workspace '{workspace.workspace_name}': expected table '{t}' is missing"
                )
            return False

        # Optionally check for tables in the workspace that were not expected
        if check_no_extra:
            extra = [t for t in workspace_tables if t not in expected_tables]
            if extra:
                for t in sorted(extra):
                    logging.error(
                        f"Workspace '{workspace.workspace_name}': unexpected extra table '{t}' found"
                    )
                return False

        logging.info(f"Workspace '{workspace.workspace_name}' has all expected tables")
        return True

    @staticmethod
    def get_table_rows(workspace: TerraWorkspace, table_name: str) -> list[dict]:
        """
        Fetch all rows for a given table from a Terra workspace.

        Wraps the TerraWorkspace entity query so callers don't need to know the
        underlying API method.  The returned dicts contain the same columns that
        were uploaded, including the synthetic row-ID column
        (e.g. ``sequencing_files_table_id``).

        Args:
            workspace:  TerraWorkspace object to query.
            table_name: Name of the Terra entity type / table to fetch rows from.

        Returns:
            List of row dicts, one per entity in the table.
        """
        return workspace.get_gcp_workspace_metrics(entity_type=table_name)

    def should_skip_uploads(self, workspace: TerraWorkspace, expected_tables: list[str], force: bool) -> bool:
        """
        Decide whether uploads for this workspace can be skipped.

        A workspace is skipped only when --force is NOT set AND every expected
        table already exists.  The workspace is always created with
        ``continue_if_exists=True`` before the check so the object is valid
        regardless.

        Args:
            workspace: TerraWorkspace object (already created/fetched)
            expected_tables: Full list of table names that should be uploaded
            force: If True, always upload regardless of existing tables

        Returns:
            True if uploads should be skipped, False if they should proceed
        """
        if force:
            logging.info(f"--force set: skipping table check for '{workspace.workspace_name}', will upload all tables")
            return False
        return self.workspace_has_all_tables(workspace, expected_tables)

    def create_workspace(self, workspace_name: str) -> TerraWorkspace:
        """
        Create a single Terra workspace, continuing silently if it already exists.

        Args:
            workspace_name: Name of the workspace to create

        Returns:
            TerraWorkspace object
        """
        workspace = TerraWorkspace(
            billing_project="ops-integration-billing",  # TODO CHANGE BACK
            workspace_name=workspace_name,
            request_util=self.request_util
        )
        if self.dry_run:
            logging.info(f"DRY RUN: Would create workspace '{workspace_name}'")
        else:
            workspace.create_workspace(continue_if_exists=True)
        return workspace

    def create_sub_workspace(self, sub_dataset_info: SubDatasetInfo) -> TerraWorkspace:
        """
        Create a sub dataset workspace.

        workspace_name is pre-populated on SubDatasetInfo by parse_csv_paths_to_dataset_info
        at load time, so it is used directly here without recomputation.

        Args:
            sub_dataset_info: SubDatasetInfo object with project metadata

        Returns:
            TerraWorkspace object for sub workspace
        """
        return self.create_workspace(sub_dataset_info.workspace_name)

    def create_main_workspace(self) -> TerraWorkspace:
        """
        Create the main Terra workspace, continuing silently if it already exists.

        Returns:
            TerraWorkspace object for the main workspace
        """
        return self.create_workspace(self.main_workspace_name)

    def create_all_sub_workspaces(self, dataset_info: DatasetInfo) -> dict[str, TerraWorkspace]:
        """
        Create Terra workspaces for all sub datasets.

        Args:
            dataset_info: Object containing SFTP dataset information

        Returns:
            Dict mapping workspace names to sub TerraWorkspace objects
        """
        workspaces = {}
        for sub_dir_info in dataset_info.sub_datasets:
            sub_workspace = self.create_sub_workspace(sub_dir_info)
            workspaces[sub_workspace.workspace_name] = sub_workspace
        logging.info(f"Successfully created {len(workspaces)} sub-workspace(s)")
        return workspaces
