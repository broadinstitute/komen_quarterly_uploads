"""Validate uploaded Terra workspaces against expected tables, schemas, and enrollment rules."""

import logging
from argparse import ArgumentParser, Namespace

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

from dataset_loader import DatasetLoader
from validation.dataset_validator import DatasetValidator
from validation.uploaded_workspace_validator import UploadedWorkspaceValidator
from workspace.workspace_manager import WorkspaceManager
from workspace_config import (
    BILLING_PROJECT,
    GENOMICS_FILE_ACCESS_CSV,
    METADATA_CSVS_BUCKET,
    get_main_workspace_name,
)


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

def get_args() -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(
        description="Validate uploaded Terra workspaces against source CSVs and schema expectations"
    )
    parser.add_argument(
        "--workspace_scope",
        "-w",
        choices=["all", "main", "sub"],
        default="all",
        help="Which workspaces to validate: 'all' (default), 'main' only, or 'sub' only",
    )
    return parser.parse_args()


def main() -> None:
    """Main execution function."""
    args = get_args()
    logging.info(f"Workspace scope: '{args.workspace_scope}'")

    gcp = GCPCloudFunctions()
    dataset_loader = DatasetLoader(gcp)
    dataset_info = dataset_loader.parse_csv_paths_to_dataset_info(
        dataset_loader.list_bucket_csv_paths(METADATA_CSVS_BUCKET)
    )

    dataset_validator = DatasetValidator()
    if not dataset_validator.validate_all(dataset_info):
        raise ValueError("Source dataset validation failed before uploaded workspace validation could run")

    token = Token()
    request_util = RunRequest(token=token)
    workspace_manager = WorkspaceManager(
        request_util=request_util,
        billing_project=BILLING_PROJECT,
        main_workspace_name=get_main_workspace_name(),
        dry_run=False,
    )

    main_workspace = workspace_manager.get_workspace(workspace_manager.main_workspace_name) if args.workspace_scope in ("all", "main") else None
    sub_workspaces = workspace_manager.build_sub_workspaces(dataset_info, create=False) if args.workspace_scope in ("all", "sub") else {}

    genomics_access_rows = dataset_loader.read_cloud_csv_as_dicts(GENOMICS_FILE_ACCESS_CSV)
    researchers_with_genomics_access = {
        int(row["Researcher ID"])
        for row in genomics_access_rows
        if str(row.get("Researcher ID", "")).strip()
    }

    uploaded_workspace_validator = UploadedWorkspaceValidator(
        dataset_validator=dataset_validator,
        workspace_manager=workspace_manager,
        gcp=gcp,
    )
    validation_failures = uploaded_workspace_validator.validate_uploaded_workspaces(
        dataset_info=dataset_info,
        main_workspace=main_workspace,
        sub_workspaces=sub_workspaces,
        researchers_with_genomics_access=researchers_with_genomics_access,
        workspace_scope=args.workspace_scope,
    )

    if validation_failures:
        for failure in validation_failures:
            logging.error(failure)
        raise RuntimeError(
            f"Uploaded workspace validation failed with {len(validation_failures)} issue(s). "
            f"See log output above for full details."
        )

    logging.info("Uploaded workspace validation completed successfully")


if __name__ == "__main__":
    main()




