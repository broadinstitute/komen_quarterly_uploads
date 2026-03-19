"""Terra upload utilities."""

import logging
import re
from pathlib import Path

from ops_utils.terra_util import TerraWorkspace

from transformation.column_order import TABLE_COLUMN_ORDER

# Matches the dynamic metadata table name, e.g. "researcher_id_62_project_id_115_metadata_table"
_METADATA_TABLE_PATTERN = re.compile(r"^researcher_id_\d+_project_id_\d+_metadata_table$")


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
    def _build_column_order_dict(tsv_files: list[str]) -> dict[str, dict[str, list[str]]]:
        """Build one Terra column-order payload for every uploaded TSV that has a known layout."""
        column_order_dict = {}
        for tsv_path in tsv_files:
            stem = Path(tsv_path).stem  # e.g. "biomarker" from "biomarker.tsv"
            table_name = f"{stem}_table"  # actual Terra table name, e.g. "biomarker_table"
            if stem in TABLE_COLUMN_ORDER:
                column_order_dict[table_name] = TABLE_COLUMN_ORDER[stem]
            elif _METADATA_TABLE_PATTERN.match(table_name):
                # Dynamic name like researcher_id_62_project_id_115_metadata_table — reuse the shared definition
                column_order_dict[table_name] = TABLE_COLUMN_ORDER["researcher_project_metadata"]
            else:
                logging.warning(
                    f"No column order defined for table '{table_name}' — skipping column order for this table"
                )
        return column_order_dict

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
            workspace.upload_metadata_to_workspace_table(entities_tsv=tsv_path)

        logging.info(f"Uploaded {len(tsv_files)} TSV files to {workspace.workspace_name}")
        column_order_dict = self._build_column_order_dict(tsv_files)
        if column_order_dict:
            logging.info(
                f"Setting column order for {len(column_order_dict)} table(s) in workspace '{workspace.workspace_name}'"
            )
            workspace.set_table_column_order(column_order=column_order_dict)
        return True
