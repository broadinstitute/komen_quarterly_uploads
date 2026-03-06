"""Terra upload utilities."""

import logging
import re
from pathlib import Path

from ops_utils.terra_util import TerraWorkspace

from transformation.column_order import TABLE_COLUMN_ORDER

# Matches the dynamic metadata table name, e.g. "researcher_id_62_project_id_115_metadata"
_METADATA_TABLE_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata$")


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
        workspace.upload_metadata_to_workspace_table(entities_tsv=tsv_path)
        return True

    @staticmethod
    def set_column_order_for_uploaded_tables(workspace: TerraWorkspace, tsv_files: list[str]) -> None:
        """
        Set the column display order in Terra for every table that was just uploaded.

        Builds a column-order dict from the TSV filenames and the TABLE_COLUMN_ORDER
        constant, then sends a single call to the workspace.

        Args:
            workspace: TerraWorkspace object
            tsv_files: List of TSV file paths that were uploaded (filenames are used
                       to look up the table name, e.g. ``biomarker.tsv`` -> ``biomarker``).
        """
        column_order_dict = {}
        for tsv_path in tsv_files:
            table_name = Path(tsv_path).stem  # e.g. "biomarker" from "biomarker.tsv"
            if table_name in TABLE_COLUMN_ORDER:
                # table name will be same as file + "_table" suffix, e.g. "biomarker_table"
                column_order_dict[f'{table_name}_table'] = TABLE_COLUMN_ORDER[table_name]
            elif _METADATA_TABLE_PATTERN.match(table_name):
                # Dynamic name like researcher_id_62_project_id_115_metadata — reuse the shared definition
                column_order_dict[f'{table_name}_table'] = TABLE_COLUMN_ORDER["researcher_project_metadata"]
            else:
                logging.warning(
                    f"No column order defined for table '{table_name}' — skipping column order for this table"
                )

        if column_order_dict:
            logging.info(
                f"Setting column order for {len(column_order_dict)} table(s) in workspace '{workspace.workspace_name}'"
            )
            workspace.set_table_column_order(column_order=column_order_dict)

    def upload_all_tsvs_to_workspace(
        self,
        workspace: TerraWorkspace,
        tsv_files: list[str]
    ) -> bool:
        """
        Upload all TSV files to a workspace and set column display order.

        Args:
            workspace: TerraWorkspace object
            tsv_files: List of paths to TSV files

        Returns:
            True if all successful
        """
        logging.info(f"Uploading all TSV files to workspace {workspace.workspace_name}")
        for tsv_path in tsv_files:
            self.upload_tsv_to_workspace(workspace, tsv_path)

        logging.info(f"Uploaded {len(tsv_files)} TSV files to {workspace.workspace_name}")
        self.set_column_order_for_uploaded_tables(workspace, tsv_files)
        return True
