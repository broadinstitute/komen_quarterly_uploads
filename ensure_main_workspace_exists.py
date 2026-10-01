"""
Ensures the main Terra workspace for a release exists (continues silently if it already does).

Run as its own fast, narrow step so UploadQuestionnaireSummary can depend on just this
precondition instead of all of create_and_upload_metadata_to_workspaces.py, which can fail for
reasons unrelated to the main workspace's existence (e.g. it deliberately raises an error at the
end if participant/researcher ID mapping failures were encountered, even after successfully
creating the workspace and uploading every table).
"""

import logging
from argparse import ArgumentParser, Namespace

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

from constants import BILLING_PROJECT
from utilities import get_main_workspace_name
from workspace.workspace_manager import WorkspaceManager

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Ensure the main Terra workspace for a release exists")
    parser.add_argument(
        "--release_directory", "-r", required=True,
        help="Quarterly release directory, used to derive the main workspace name"
    )
    parser.add_argument(
        "--dry_run", action="store_true",
        help="Log what would happen without actually creating the workspace"
    )
    parser.add_argument(
        "--billing_project",
        help=f"Terra billing project to create the workspace under. Defaults to BILLING_PROJECT ('{BILLING_PROJECT}') from constants.py"
    )
    return parser.parse_args()


def main():
    args = get_args()
    main_workspace_name = get_main_workspace_name(args.release_directory)
    billing_project = args.billing_project or BILLING_PROJECT
    logging.info(f"Ensuring main workspace '{main_workspace_name}' exists (billing project '{billing_project}')")

    token = Token()
    request_util = RunRequest(token=token)
    gcp = GCPCloudFunctions()
    workspace_manager = WorkspaceManager(
        request_util=request_util, billing_project=billing_project, gcp_util=gcp, dry_run=args.dry_run
    )

    workspace_manager.create_workspace(workspace_name=main_workspace_name)
    logging.info(f"Main workspace '{main_workspace_name}' exists")


if __name__ == "__main__":
    main()
