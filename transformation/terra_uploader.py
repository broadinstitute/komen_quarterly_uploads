"""Terra upload utilities."""

import logging
from pathlib import Path

from ops_utils.terra_util import TerraWorkspace


class TerraUploader:
    """Handles uploading CSV and participant files to Terra workspaces."""

    def __init__(self, request_util):
        """
        Initialize TerraUploader.

        Args:
            request_util: Request utility for Terra API calls
        """
        self.request_util = request_util

    @staticmethod
    def upload_tsv_to_workspace(workspace: TerraWorkspace, tsv_path: str) -> bool:
        """
        Upload a TSV file to a Terra workspace.

        Args:
            workspace: TerraWorkspace object
            tsv_path: Path to the TSV file

        Returns:
            True if successful
        """
        logging.info(f"Uploading {Path(tsv_path).name} to workspace {workspace.workspace_name}")
        workspace.upload_metadata_to_workspace_table(entities_tsv=tsv_path)
        return True

    def upload_all_tsvs_to_workspace(
        self,
        workspace: TerraWorkspace,
        tsv_files: list[str]
    ) -> bool:
        """
        Upload all TSV files to a workspace.

        Args:
            workspace: TerraWorkspace object
            tsv_files: List of paths to TSV files

        Returns:
            True if all successful
        """
        for tsv_path in tsv_files:
            self.upload_tsv_to_workspace(workspace, tsv_path)

        logging.info(f"Uploaded {len(tsv_files)} TSV files to {workspace.workspace_name}")
        return True
