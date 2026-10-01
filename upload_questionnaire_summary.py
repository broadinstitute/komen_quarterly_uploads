"""
Uploads a questionnaire_summary.csv (produced by sfc_questionnaire_analysis_pipeline.R) to the
main workspace's questionnaire_summary_table via a Terra batch upsert.
"""

import csv
import logging
from argparse import ArgumentParser, Namespace

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.token_util import Token

from constants import BILLING_PROJECT
from transformation.table_data_utils import convert_csv_rows_to_table_data
from utilities import get_main_workspace_name
from workspace.workspace_manager import WorkspaceManager

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Upload a questionnaire_summary.csv to the main workspace's questionnaire_summary_table"
    )
    parser.add_argument(
        "--release_directory", "-r", required=True,
        help="Quarterly release directory, used to derive the main workspace name"
    )
    parser.add_argument(
        "--summary_csv", required=True,
        help="Path to the questionnaire_summary.csv produced by sfc_questionnaire_analysis_pipeline.R"
    )
    parser.add_argument(
        "--dry_run", action="store_true",
        help="Log what would be uploaded without actually uploading"
    )
    parser.add_argument(
        "--billing_project",
        help=f"Terra billing project that owns the main workspace. Defaults to BILLING_PROJECT ('{BILLING_PROJECT}') from constants.py"
    )
    return parser.parse_args()


def main():
    args = get_args()
    main_workspace_name = get_main_workspace_name(args.release_directory)
    logging.info(
        f"Processing release directory '{args.release_directory}' -> main workspace '{main_workspace_name}'"
    )

    with open(args.summary_csv, newline="") as f:
        summary_rows = list(csv.DictReader(f))

    if not summary_rows:
        logging.info("questionnaire_summary.csv has no rows — nothing to upload")
        return

    table_data = convert_csv_rows_to_table_data(
        csv_path="questionnaire_summary.csv",
        file_contents=summary_rows,
    )

    billing_project = args.billing_project or BILLING_PROJECT

    token = Token()
    request_util = RunRequest(token=token)
    gcp = GCPCloudFunctions()
    workspace_manager = WorkspaceManager(
        request_util=request_util, billing_project=billing_project, gcp_util=gcp, dry_run=args.dry_run
    )
    terra_workspace = TerraWorkspace(
        billing_project=billing_project, workspace_name=main_workspace_name, request_util=request_util
    )

    workspace_manager.upload_table_data_to_workspace(terra_workspace, table_data)
    logging.info(
        f"Completed upload of questionnaire_summary_table ({len(summary_rows)} rows) to '{main_workspace_name}'"
    )


if __name__ == "__main__":
    main()
