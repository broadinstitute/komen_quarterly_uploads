import logging
from argparse import ArgumentParser, Namespace

from ops_utils.terra_util import TerraGroups
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.csv_util import Csv

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

MEMBER = "member"
ADMIN = "admin"

OUTPUT_TSV = "group_membership_audit.tsv"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def get_args() -> Namespace:
    """Parse command-line arguments."""
    parser = ArgumentParser(
        description="Audit Terra group membership. Outputs a TSV report of admins and members for each group."
    )
    parser.add_argument(
        "--groups", "-g",
        nargs="+",
        default=None,
        help=(
            "One or more Terra group names to audit (without the '@firecloud.org' suffix). "
            "When omitted, all groups the user is an admin of are audited automatically."
        ),
    )
    return parser.parse_args()

# ---------------------------------------------------------------------------
# GroupAuditor
# ---------------------------------------------------------------------------

class GroupAuditor:
    """Retrieves member and admin lists for Terra groups."""

    def __init__(self, request_util: RunRequest) -> None:
        """
        Args:
            request_util: Authenticated request utility passed to TerraGroups.
        """
        self.terra_groups = TerraGroups(request_util=request_util)

    def resolve_groups(self, groups: list[str] | None) -> tuple[list[str], list[str]]:
        """
        Determine which groups to audit and which the user can only see as a member.

        When ``groups`` is provided those names are returned as the admin list with
        an empty member-only list (the caller supplied them explicitly).

        When ``groups`` is ``None`` the method calls ``get_all_groups()`` to
        discover every group the user belongs to and splits them by role:

        - Groups where the user is an **admin** are returned in ``admin_groups``
          and will be fully audited.
        - Groups where the user is only a **member** are returned in
          ``member_only_groups``; the caller should warn and ultimately fail.

        Args:
            groups: Explicit list of group names, or ``None`` to auto-discover.

        Returns:
            Tuple of ``(admin_groups, member_only_groups)``.
        """
        if groups:
            return groups, []

        logging.info("No groups specified — discovering all groups the user belongs to...")
        all_groups: list[dict] = self.terra_groups.get_all_groups().json()

        admin_groups: list[str] = []
        member_only_groups: list[str] = []

        for entry in all_groups:
            name = entry.get("groupName", "")
            role = entry.get("role", "").lower()
            if role == ADMIN:
                admin_groups.append(name)
            else:
                member_only_groups.append(name)

        logging.info(
            f"Found {len(admin_groups)} group(s) where user is admin "
            f"and {len(member_only_groups)} group(s) where user is only a member."
        )
        return sorted(admin_groups), sorted(member_only_groups)

    def get_group_membership(self, group: str) -> dict[str, list[str]]:
        """
        Return the members and admins of a Terra group.

        Args:
            group: Terra group name (without the ``@firecloud.org`` suffix).

        Returns:
            A dict with two keys:
                - ``"members"``: list of member email addresses.
                - ``"admins"``:  list of admin email addresses.
        """
        members: list[str] = self.terra_groups.check_group_members(group=group, role=MEMBER).json()
        admins: list[str] = self.terra_groups.check_group_members(group=group, role=ADMIN).json()
        return {"members": members, "admins": admins}


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _collect_group_rows(group: str, membership: dict[str, list[str]]) -> list[dict[str, str]]:
    """
    Convert membership dicts for a group into TSV-ready row dicts.

    Admins are listed before members within each group, then sorted
    alphabetically by email within each role.

    Args:
        group: Terra group name.
        membership: Dict with ``"admins"`` and ``"members"`` lists of emails.

    Returns:
        List of dicts with keys: ``group``, ``role``, ``email``.
    """
    rows = []
    # Admins first so they stand out at the top of each group block
    for role in (ADMIN, MEMBER):
        for email in sorted(membership[f"{role}s"], key=str.lower):
            rows.append({"group": group, "role": role, "email": email})
    return rows


def main():
    """Parse arguments, audit groups, and output results as TSV."""
    args = get_args()

    # Single token + request_util shared across all API calls
    token = Token()
    request_util = RunRequest(token=token)

    auditor = GroupAuditor(request_util=request_util)

    # Resolve which groups to audit
    admin_groups, member_only_groups = auditor.resolve_groups(args.groups)

    # Warn immediately about groups the user cannot fully audit
    for group in member_only_groups:
        logging.warning(
            f"Cannot retrieve access information for group '{group}': user is a member, not an admin."
        )

    if not admin_groups:
        logging.warning("No groups available to audit. Exiting.")
        return

    logging.info(f"Auditing {len(admin_groups)} group(s): {admin_groups}")

    all_rows: list[dict[str, str]] = []
    for group in admin_groups:
        logging.info(f"Fetching membership for group '{group}'...")
        membership = auditor.get_group_membership(group)
        admin_count = len(membership["admins"])
        member_count = len(membership["members"])
        logging.info(f"'{group}': {admin_count} admin(s), {member_count} member(s)")
        all_rows.extend(_collect_group_rows(group, membership))

    logging.info(f"Writing {len(all_rows)} row(s) to '{OUTPUT_TSV}'")
    Csv(file_path=OUTPUT_TSV, delimiter="\t").create_tsv_from_list_of_dicts(
        list_of_dicts=all_rows,
        header_list=["group", "role", "email"],
    )
    logging.info("Done.")

    # Fail after writing output if any groups were inaccessible
    if member_only_groups:
        failed_list = ", ".join(member_only_groups)
        raise PermissionError(
            f"Audit completed with errors. User is only a member (not an admin) of "
            f"{len(member_only_groups)} group(s) and their full membership could not be retrieved: "
            f"{failed_list}"
        )


if __name__ == '__main__':
    main()