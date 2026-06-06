# AuditTerraGroups

## Overview

`AuditTerraGroups` is a WDL workflow that audits the membership of one or more Terra groups and outputs a tab-separated report.

It calls a single task (`AuditTerraGroupsTask`) which executes `audit_terra_groups.py` to:

1. Accept one or more Terra group names
2. Retrieve the admins and members of each group from Terra
3. Output a TSV report with one row per group-role-email entry

This workflow is read-only. It does not create, update, or delete groups or their memberships.

---

## Inputs

| Input Name | Description                                                                                                                                                          | Type      | Required | Default                                                                                    |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------|--------------------------------------------------------------------------------------------|
| `groups`   | Space-separated list of Terra group names to audit (e.g. `"group-a group-b"`). Do **not** include the `@firecloud.org` suffix.                                      | `String`  | Yes      | _(none)_                                                                                   |
| `docker`   | Docker image to use for the task. If not provided, the latest production image is used.                                                                              | `String?` | No       | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest` |

---

## Outputs

| Output Name    | Description                                                                              | Type   |
|----------------|------------------------------------------------------------------------------------------|--------|
| `audit_report` | TSV file containing membership entries with columns: `group`, `role`, `email`.           | `File` |

---

## What `audit_terra_groups.py` does

### 1. Accept group names
The script takes one or more group names via `--groups`. Names should be provided without the `@firecloud.org` suffix.

### 2. Retrieve group membership
For each group the script calls `TerraGroups.check_group_members` twice — once for the `admin` role and once for the `member` role — and collects the returned email lists.

### 3. Write TSV rows
Each email is emitted as one TSV row with the following columns:

- `group` — the Terra group name
- `role` — `admin` or `member`
- `email` — the principal's email address

Within each group, **admins are listed before members**, and emails within each role are sorted alphabetically. Groups appear in the order they were supplied on the command line.

---

## Finding the Output

After the workflow finishes, retrieve the audit report through the Terra UI:

1. Open the workspace where the workflow was submitted and go to the **Submission History** tab.
2. Click on the submission where **AuditTerraGroups** was run.
3. Under the **Links** column, click the **Job Manager** icon (it looks like a bulleted checklist).
4. In Job Manager, click the **Outputs** tab.
5. The report is listed as **`audit_report`** — click the link to download the TSV file.

### If the workflow fails

The script writes the output file before any error is raised, so the TSV may still contain partial or complete results even when the job is marked as failed. In that case:

- Navigate to the **execution directory** for the failed task. The `audit_report` TSV (`group_membership_audit.tsv`) will be in the same directory as the `stderr` log file.

---

## Notes

- The workflow only reports group membership; it does not modify groups or their members.
- The output is sorted to make side-by-side comparison of multiple groups as easy as possible: groups are kept in input order, admins appear before members within each group, and emails are sorted alphabetically within each role block.

