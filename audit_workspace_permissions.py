"""
Audit Terra workspace permissions for one or more workspaces in a billing project.

Usage examples
--------------
# Audit every workspace visible in a billing project
python audit_workspace_permissions.py --billing_project SFC-Research

# Audit specific workspaces only
python audit_workspace_permissions.py --billing_project SFC-Research \\
    --workspaces my_workspace_1 my_workspace_2

# Filter output to specific email addresses
python audit_workspace_permissions.py --billing_project SFC-Research \\
    --emails user@example.com admin@example.com

# Combine workspace and email filters
python audit_workspace_permissions.py --billing_project SFC-Research \\
    --workspaces my_workspace_1 \\
    --emails user@example.com
"""

import logging
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import Terra, TerraWorkspace
from ops_utils.token_util import Token
from ops_utils.csv_util import Csv

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

OUTPUT_TSV = "permission_audit.tsv"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def get_args() -> Namespace:
    """Parse command-line arguments."""
    parser = ArgumentParser(
        description="Audit Terra workspace permissions for a billing project."
    )
    parser.add_argument(
        "--billing_project", "-b",
        required=True,
        help="Terra billing project (namespace) to scope the audit.",
    )
    parser.add_argument(
        "--workspaces", "-w",
        nargs="+",
        default=None,
        help=(
            "Optional list of exact workspace names to audit. "
            "When omitted, every workspace in the billing project is audited."
        ),
    )
    parser.add_argument(
        "--emails", "-e",
        nargs="+",
        default=None,
        help=(
            "Optional list of email addresses to filter results to. "
            "When omitted, all principals are shown."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class PermissionEntry:
    """Represents a single principal's permissions on a Terra workspace."""
    email: str
    access_level: str
    can_share: bool
    can_compute: bool
    pending: bool


@dataclass
class WorkspaceInfo:
    """Lightweight workspace identity and caller access metadata from fetch_accessible_workspaces."""
    name: str
    access_level: str   # e.g. "OWNER", "WRITER", "READER", "PROJECT_OWNER"
    can_share: bool

    @property
    def can_audit(self) -> bool:
        """True when the caller has enough privilege to read the workspace ACL."""
        return self.access_level in ["OWNER", "PROJECT_OWNER"] or self.can_share


# ---------------------------------------------------------------------------
# WorkspaceFetcher — resolves the list of workspace names to audit
# ---------------------------------------------------------------------------

class WorkspaceFetcher:
    """
    Resolves which Terra workspaces should be audited.

    ``Terra.fetch_accessible_workspaces`` is used to retrieve accessible
    workspaces, and the response is filtered down to workspaces that belong to
    ``billing_project``. When specific workspace names are provided by the
    caller, they are validated and filtered against those accessible
    workspaces rather than returned directly.
    """

    # Only fetch the minimal fields needed to identify workspace identity.
    _FIELDS = ["workspace.namespace", "workspace.name", "accessLevel", "canShare"]

    def __init__(self, billing_project: str, terra: Terra, workspaces: list[str] | None = None) -> None:
        """
        Args:
            billing_project: Terra billing project used to filter workspace results.
            terra: Authenticated ``Terra`` client instance.
            workspaces: List of workspaces to audit.
        """
        self.billing_project = billing_project
        self.terra = terra
        self.workspaces_to_audit = workspaces

    def get_workspaces(self) -> list[WorkspaceInfo]:
        """
        Return ``WorkspaceInfo`` objects for every workspace to audit.

        Always fetches all accessible workspaces in the billing project, capturing
        the caller's ``accessLevel`` and ``canShare`` flag for each one.
        If ``self.workspaces`` is provided, only those names are returned (with a
        warning for any not found). Otherwise, all workspaces in the billing project
        are returned.

        Returns:
            List of ``WorkspaceInfo`` sorted by workspace name.
        """
        logging.info(
            f"Fetching all accessible workspaces and filtering to billing project '{self.billing_project}'..."
        )

        response = self.terra.fetch_accessible_workspaces(fields=self._FIELDS)
        all_workspaces: list[dict] = response.json()

        # Build WorkspaceInfo objects for every workspace in this billing project
        accessible: list[WorkspaceInfo] = sorted(
            [
                WorkspaceInfo(
                    name=ws["workspace"]["name"],
                    access_level=ws.get("accessLevel", "UNKNOWN"),
                    can_share=bool(ws.get("canShare", False)),
                )
                for ws in all_workspaces
                if ws.get("workspace", {}).get("namespace") == self.billing_project
            ],
            key=lambda w: w.name,
        )

        logging.info(
            f"Found {len(accessible)} workspace(s) in billing project '{self.billing_project}'"
        )

        # Return all workspaces if no specific workspaces requested
        if not self.workspaces_to_audit:
            return accessible

        # Filter to explicitly requested workspaces, warn about any not found
        accessible_names = {w.name for w in accessible}
        not_found = set(self.workspaces_to_audit) - accessible_names
        if not_found:
            for name in sorted(not_found):
                logging.warning(
                    f"Requested workspace '{name}' not found in accessible workspaces for "
                    f"billing project '{self.billing_project}'"
                )
                raise Exception(f"Requested workspace '{name}' not found in accessible workspaces for billing project '{self.billing_project}'")

        result = [w for w in accessible if w.name in self.workspaces_to_audit]
        logging.info(f"Using {len(result)} of {len(self.workspaces_to_audit)} requested workspace(s)")
        return result


# ---------------------------------------------------------------------------
# PermissionAuditor — fetches and filters ACL entries per workspace
# ---------------------------------------------------------------------------

class PermissionAuditor:
    """
    Retrieves and optionally filters the ACL for a Terra workspace.

    Uses ``TerraWorkspace.get_workspace_acl`` for all Terra API communication.
    """

    def __init__(self, billing_project: str, request_util: RunRequest) -> None:
        """
        Args:
            billing_project: Billing project that owns the workspaces being audited.
            request_util: Authenticated request utility passed to each TerraWorkspace.
        """
        self.billing_project = billing_project
        self.request_util = request_util

    def _get_acl(self, workspace_name: str) -> list[PermissionEntry]:
        """
        Fetch the full ACL for a workspace and return it as structured entries.

        The ``TerraWorkspace.get_workspace_acl`` response JSON has the shape:
            {"acl": {"<email>": {"accessLevel": str, "canShare": bool, ...}, ...}}

        Args:
            workspace_name: Name of the workspace to query.

        Returns:
            List of ``PermissionEntry`` — one per principal in the ACL.
        """
        workspace = TerraWorkspace(
            billing_project=self.billing_project,
            workspace_name=workspace_name,
            request_util=self.request_util,
        )
        acl_map: dict[str, dict] = workspace.get_workspace_acl().json().get("acl", {})

        return [
            PermissionEntry(
                email=email,
                access_level=str(entry.get("accessLevel", "UNKNOWN")),
                can_share=bool(entry.get("canShare", False)),
                can_compute=bool(entry.get("canCompute", False)),
                pending=bool(entry.get("pending", False)),
            )
            for email, entry in acl_map.items()
        ]

    def _filter_by_emails(
        self,
        entries: list[PermissionEntry],
        emails: list[str] | None,
    ) -> list[PermissionEntry]:
        """
        Filter ACL entries to a specific set of email addresses.

        Args:
            entries: Full ACL entry list for a workspace.
            emails: Emails to keep; ``None`` means keep everything.
                    Comparison is case-insensitive.

        Returns:
            Filtered (or unchanged) list of entries.
        """
        if not emails:
            return entries
        targets = {e.lower() for e in emails}
        return [e for e in entries if e.email.lower() in targets]

    def audit_workspace(
        self,
        workspace_name: str,
        emails: list[str] | None = None,
    ) -> list[PermissionEntry]:
        """
        Audit a single workspace, optionally filtering to specific emails.

        Args:
            workspace_name: Workspace to audit.
            emails: Optional email filter.

        Returns:
            List of matching ``PermissionEntry`` objects.
        """
        entries = self._get_acl(workspace_name)
        return self._filter_by_emails(entries, emails)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _format_permissions(entry: PermissionEntry) -> str:
    """
    Format a PermissionEntry's access level and pending status as a string.

    Args:
        entry: Permission entry to format.

    Returns:
        String like "OWNER" or "READER (pending)".
    """
    if entry.pending:
        return f"{entry.access_level} (pending)"
    return entry.access_level


def _collect_acl_rows(
    workspace_name: str,
    entries: list[PermissionEntry],
    billing_project: str,
) -> list[dict[str, str]]:
    """
    Convert ACL entries for a workspace into TSV-ready row dicts.

    Args:
        workspace_name: Name of the workspace.
        entries: ACL permission entries.
        billing_project: Billing project owning the workspace.

    Returns:
        List of dicts with keys: billing_project, workspace_name, email,
        permissions, can_share, can_compute.
    """
    rows = []
    for entry in sorted(entries, key=lambda e: (e.email.lower(), e.access_level)):
        rows.append({
            "billing_project": billing_project,
            "workspace_name": workspace_name,
            "email": entry.email,
            "permissions": _format_permissions(entry),
            "can_share": "true" if entry.can_share else "false",
            "can_compute": "true" if entry.can_compute else "false",
        })
    return rows


def main() -> None:
    """Parse arguments, resolve workspaces, audit ACLs, and output results as TSV."""
    args = get_args()
    billing_project = args.billing_project
    workspaces = args.workspaces
    emails = args.emails

    # Single token + request_util shared across all API calls
    token = Token()
    request_util = RunRequest(token=token)

    # --- Step 1: resolve workspace names ---
    terra = Terra(request_util=request_util)
    workspaces_info = WorkspaceFetcher(
        billing_project=billing_project,
        terra=terra,
        workspaces=workspaces
    ).get_workspaces()

    if not workspaces_info:
        logging.warning(
            f"No workspaces found for billing project '{billing_project}'. Nothing to audit."
        )
        return

    # --- Step 2: audit permissions for each workspace ---
    auditor = PermissionAuditor(
        billing_project=billing_project,
        request_util=request_util,
    )

    email_filter: list[str] | None = emails
    if email_filter:
        logging.info(f"Filtering results to {len(email_filter)} email(s): {email_filter}")

    logging.info(
        f"Auditing {len(workspaces_info)} workspace(s) in billing project '{billing_project}'"
    )

    # Collect all ACL rows from all workspaces
    all_rows: list[dict[str, str]] = []
    for ws in workspaces_info:
        if not ws.can_audit:
            # Caller is not OWNER and canShare is False — skip the ACL call and
            # record an informative sentinel row instead.
            logging.warning(
                f"Skipping ACL lookup for workspace '{ws.name}': caller access level is "
                f"'{ws.access_level}' and canShare is False — insufficient permissions to read ACL."
            )
            all_rows.append({
                "billing_project": billing_project,
                "workspace_name": ws.name,
                "email": "PERMISSION DENIED",
                "permissions": (
                    f"Could not retrieve ACL — caller access level is '{ws.access_level}' "
                    f"and canShare is False."
                ),
                "can_share": "false",
                "can_compute": "N/A",
            })
            continue

        try:
            entries = auditor.audit_workspace(workspace_name=ws.name, emails=email_filter)
        except Exception as exc:
            # Log, add an informative sentinel row, and continue — a single
            # inaccessible workspace should not abort the audit.
            logging.error(
                f"No permission / failed to retrieve ACL for workspace '{ws.name}': {exc}"
            )
            all_rows.append({
                "billing_project": billing_project,
                "workspace_name": ws.name,
                "email": "PERMISSION DENIED",
                "permissions": "Could not retrieve ACL — insufficient permissions or workspace is inaccessible.",
                "can_share": "N/A",
                "can_compute": "N/A",
            })
            continue

        rows = _collect_acl_rows(
            workspace_name=ws.name,
            entries=entries,
            billing_project=billing_project,
        )
        all_rows.extend(rows)

    logging.info(f"Collected {len(all_rows)} ACL entry/entries across {len(workspaces_info)} workspace(s)")

    # --- Step 3: output as TSV ---
    Csv(file_path=OUTPUT_TSV, delimiter="\t").create_tsv_from_list_of_dicts(
        list_of_dicts=all_rows,
        header_list=["workspace_name", "email", "permissions", "can_share", "can_compute"],
    )
    logging.info(f"Output written to '{OUTPUT_TSV}'")


if __name__ == "__main__":
    main()
