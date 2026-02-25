"""Terra workspace management."""

import logging
import re
from datetime import datetime

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace

from models import SFTPDatasetInfo, SubDatasetInfo


class WorkspaceManager:
    """Manages Terra workspace creation and data upload operations."""

    def __init__(self, request_util: RunRequest, billing_project: str,
                 main_workspace_name: str, sub_workspace_name_template: str):
        """
        Initialize WorkspaceManager.

        Args:
            request_util: Request utility for Terra API calls
            billing_project: Terra billing project name
            main_workspace_name: Main workspace name
            sub_workspace_name_template: Template for sub workspace names
        """
        self.request_util = request_util
        self.billing_project = billing_project
        self.main_workspace_name = main_workspace_name
        self.sub_workspace_name_template = sub_workspace_name_template

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
        # Try to parse the date - handle various formats
        if '-' in date_created:
            parts = date_created.split('-')
            year = parts[0]
            month = parts[1] if len(parts) > 1 else '01'
        elif '/' in date_created:
            parts = date_created.split('/')
            # Assume MM/DD/YYYY format
            if len(parts[2]) == 4:
                year = parts[2]
                month = parts[0]
            # Or YYYY/MM/DD format
            else:
                year = parts[0]
                month = parts[1]
        else:
            # If we can't parse, use current date
            now = datetime.now()
            year = now.strftime('%Y')
            month = now.strftime('%m')

        # Clean up project name (remove spaces, special chars)
        clean_project_name = re.sub(r'[^\w-]', '_', project_name)

        return f"{clean_project_name}_{year}_{month}"

    def create_workspace(self, workspace_name: str, continue_if_exists: bool = False) -> TerraWorkspace:
        """
        Create a single Terra workspace.

        Args:
            workspace_name: Name of the workspace to create
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object
        """
        logging.info(f"Creating workspace: {workspace_name}")
        workspace = TerraWorkspace(
            billing_project=self.billing_project,
            workspace_name=workspace_name,
            request_util=self.request_util
        )
        workspace.create_workspace(continue_if_exists=continue_if_exists)
        return workspace

    def create_main_workspace(self, continue_if_exists: bool = False) -> TerraWorkspace:
        """
        Create the main dataset workspace.

        Args:
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object for main workspace
        """
        return self.create_workspace(self.main_workspace_name, continue_if_exists)

    def create_sub_workspace(
        self,
        sub_dir_info: SubDatasetInfo,
        continue_if_exists: bool = False
    ) -> TerraWorkspace:
        """
        Create a sub dataset workspace.

        Args:
            sub_dir_info: SubDatasetInfo object with project metadata
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            TerraWorkspace object for sub workspace
        """
        # Use project_name and date_created if available, otherwise fallback
        if sub_dir_info.project_name and sub_dir_info.date_created:
            workspace_name = self.format_workspace_name(
                sub_dir_info.project_name,
                sub_dir_info.date_created
            )
        else:
            # Fallback to old naming scheme
            workspace_name = self.sub_workspace_name_template.format(
                project_id=sub_dir_info.project_id,
                researcher_id=sub_dir_info.researcher_id
            )
            logging.warning(f"Using fallback workspace name: {workspace_name}")

        return self.create_workspace(workspace_name, continue_if_exists)

    def create_all_workspaces(
        self,
        sftp_info: SFTPDatasetInfo,
        continue_if_exists: bool = False
    ) -> dict:
        """
        Create Terra workspaces for main dataset and all sub datasets.

        Args:
            sftp_info: Object containing SFTP dataset information
            continue_if_exists: Whether to continue if workspace already exists

        Returns:
            Dict mapping workspace names to TerraWorkspace objects
        """
        workspaces = {}

        # Create main workspace
        main_workspace = self.create_main_workspace(continue_if_exists)
        workspaces[self.main_workspace_name] = main_workspace

        # Create sub workspaces
        for sub_dir_info in sftp_info.sub_dataset_dirs:
            if sub_dir_info.researcher_id is None or sub_dir_info.project_id is None:
                logging.warning(f"Skipping workspace creation for {sub_dir_info.dir_name}: Missing IDs")
                continue

            try:
                sub_workspace = self.create_sub_workspace(sub_dir_info, continue_if_exists)
                workspaces[sub_workspace.workspace_name] = sub_workspace

            except Exception as e:
                logging.error(f"Failed to create workspace for {sub_dir_info.dir_name}: {e}")
                continue

        logging.info(f"Successfully created {len(workspaces)} workspace(s)")
        return workspaces
