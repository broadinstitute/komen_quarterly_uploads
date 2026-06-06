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
        description="Audit Terra workspace permissions for a billing project."
    )
    parser.add_argument(
        "--groups", "-g",
        required=True,
        nargs="+",
        help="Terra groups to audit. Do NOT include the '@firecloud' suffix. Separate multiple groups with spaces",
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
    groups = args.groups

    # Single token + request_util shared across all API calls
    token = Token()
    request_util = RunRequest(token=token)

    auditor = GroupAuditor(request_util=request_util)

    logging.info(f"Auditing {len(groups)} group(s): {groups}")

    all_rows: list[dict[str, str]] = []
    for group in groups:
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


if __name__ == '__main__':
    main()