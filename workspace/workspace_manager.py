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

    @staticmethod
    def format_workspace_name(project_name: str, date_created: str) -> str:
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

        return f"{clean_project_name}_{year}_{month}"

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

    def create_workspace(self, workspace_name: str, continue_if_exists: bool = False) -> TerraWorkspace:
        """
        Create a single Terra workspace.

        Args:
            workspace_name: Name of the workspace to create
            continue_if_exists: Whether to continue if workspace already exists

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
            workspace.create_workspace(continue_if_exists=continue_if_exists)
        return workspace

    def create_sub_workspace(
        self,
        sub_dataset_info: SubDatasetInfo,
        continue_if_exists: bool = False
    ) -> TerraWorkspace:
        """
        Create a sub dataset workspace.

        Args:
            sub_dataset_info: SubDatasetInfo object with project metadata
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object for sub workspace
        """
        workspace_name = self.format_workspace_name(
            sub_dataset_info.project_name,
            sub_dataset_info.date_created
        )
        # Set the workspace name in the data model for later use
        sub_dataset_info.workspace_name = workspace_name
        return self.create_workspace(workspace_name, continue_if_exists)

    def create_main_workspace(self, continue_if_exists: bool = False) -> TerraWorkspace:
        """
        Create the main Terra workspace.

        Args:
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object for the main workspace
        """
        return self.create_workspace(self.main_workspace_name, continue_if_exists)

    def create_all_sub_workspaces(
        self,
        dataset_info: DatasetInfo,
        continue_if_exists: bool = False
    ) -> dict[str, TerraWorkspace]:
        """
        Create Terra workspaces for the main dataset and all sub datasets.

        Args:
            dataset_info: Object containing SFTP dataset information
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            Dict mapping workspace names to sub TerraWorkspace objects
        """
        workspaces = {}

        # Create sub workspaces
        for sub_dir_info in dataset_info.sub_datasets:
            sub_workspace = self.create_sub_workspace(sub_dir_info, continue_if_exists)
            workspaces[sub_workspace.workspace_name] = sub_workspace
        logging.info(f"Successfully created {len(workspaces)} sub-workspace(s)")
        return workspaces
