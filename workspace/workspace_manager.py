"""Terra workspace management."""

import logging
import re
from datetime import datetime

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace

from models.data_models import DatasetInfo, SubDatasetInfo


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

    def get_workspace(self, workspace_name: str) -> TerraWorkspace:
        """Return a TerraWorkspace object without creating the workspace."""
        return TerraWorkspace(
            billing_project=self.billing_project,
            workspace_name=workspace_name,
            request_util=self.request_util,
        )

    def _get_sub_workspace_name(self, sub_dataset_info: SubDatasetInfo) -> str:
        """Build and store the Terra workspace name for one sub dataset."""
        workspace_name = self.format_workspace_name(
            project_name=sub_dataset_info.project_name,
            date_created=sub_dataset_info.date_created,
            researcher_id=sub_dataset_info.researcher_id,
        )
        sub_dataset_info.workspace_name = workspace_name
        return workspace_name

    @staticmethod
    def format_workspace_name(project_name: str, date_created: str, researcher_id: int) -> str:
        """
        Format workspace name as {project_name}_{year}_{month}.

        Args:
            project_name: Project name from metadata CSV
            date_created: Date created from metadata CSV (expected format: YYYY-MM-DD or similar)

        Returns:
            Formatted workspace name
        """
        if '-' in date_created:
            parts = date_created.split('-')
            year = parts[0]
            month = parts[1] if len(parts) > 1 else '01'
        else:
            # If we can't parse, use current date
            now = datetime.now()
            year = now.strftime('%Y')
            month = now.strftime('%m')

        # Clean up project name (remove spaces, special chars)
        clean_project_name = re.sub(r'[^\w-]', '_', project_name)

        return f"{clean_project_name}_researcher_id_{researcher_id}_{year}_{month}"

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
    def get_workspace_table_names(workspace: TerraWorkspace) -> list[str]:
        """Return the set of table names currently present in a workspace."""
        workspace_info = workspace.get_workspace_entity_info().json()
        return [table for table, attributes in workspace_info.items()]

    @staticmethod
    def get_workspace_table_rows(workspace: TerraWorkspace, table_name: str) -> list[dict]:
        """Fetch and normalize all rows for a workspace table."""
        table_contents = workspace.get_gcp_workspace_metrics(entity_type=table_name, remove_dicts=True)
        # Terra returns one dict per entity row with the entity id split from the attributes.
        # Fold that id back into the row so the validator can treat the data like normal CSV rows.
        return [
            {
                f"{row_dict['entityType']}_id": row_dict['name'],
                **row_dict['attributes']
            }
            for row_dict in table_contents
        ]

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

        existing_tables = self.get_workspace_table_names(workspace)
        missing_tables = [table_name for table_name in expected_tables if table_name not in existing_tables]
        if missing_tables:
            logging.info(f"Workspace '{workspace.workspace_name}' is missing {len(missing_tables)} table(s): {missing_tables}")
            return False

        logging.info(f"Workspace '{workspace.workspace_name}' already has all expected tables")
        return True

    def create_workspace(self, workspace_name: str) -> TerraWorkspace:
        """
        Create a single Terra workspace, continuing silently if it already exists.

        Args:
            workspace_name: Name of the workspace to create

        Returns:
            TerraWorkspace object
        """
        workspace = self.get_workspace(workspace_name)
        if self.dry_run:
            logging.info(f"DRY RUN: Would create workspace '{workspace_name}'")
        else:
            workspace.create_workspace(continue_if_exists=True)
        return workspace

    def build_sub_workspaces(self, dataset_info: DatasetInfo, create: bool = False) -> dict[str, TerraWorkspace]:
        """Build Terra workspace objects for all sub datasets and optionally create them."""
        workspaces = {}
        for sub_dir_info in dataset_info.sub_datasets:
            sub_workspace = self.get_workspace(self._get_sub_workspace_name(sub_dir_info))
            if create and not self.dry_run:
                sub_workspace.create_workspace(continue_if_exists=True)
            elif create:
                logging.info(f"DRY RUN: Would create workspace '{sub_workspace.workspace_name}'")
            workspaces[sub_workspace.workspace_name] = sub_workspace
        if create:
            logging.info(f"Successfully created {len(workspaces)} sub-workspace(s)")
        return workspaces
