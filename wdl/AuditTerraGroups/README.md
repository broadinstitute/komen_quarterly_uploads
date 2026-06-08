# AuditTerraGroups

## Overview

`AuditTerraGroups` is a WDL workflow that audits the membership of one or more Terra groups and outputs a tab-separated report.

It calls a single task (`AuditTerraGroupsTask`) which executes `audit_terra_groups.py` to:

1. Resolve which groups to audit — either from explicit input or by auto-discovering all groups the user belongs to
2. Retrieve the admins and members of each group the user is an admin of
3. Warn about any groups the user is only a member of (full membership cannot be retrieved)
4. Output a TSV report with one row per group-role-email entry
5. Fail if any groups could not be fully audited due to insufficient permissions

This workflow is read-only. It does not create, update, or delete groups or their memberships.

---

## Inputs

| Input Name | Description                                                                                                                                                                                                                                            | Type      | Required | Default                                                                                    |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------|--------------------------------------------------------------------------------------------|
| `groups`   | Space-separated list of Terra group names to audit (e.g. `"group-a group-b"`). Do **not** include the `@firecloud.org` suffix. When omitted, the script auto-discovers all groups the user belongs to and audits the ones where the user is an admin.  | `String?` | No       | _(none — all groups the user is an admin of are audited automatically)_                    |
| `docker`   | Docker image to use for the task. If not provided, the latest production image is used.                                                                                                                                                                | `String?` | No       | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest` |

---

## Outputs

| Output Name    | Description                                                                    | Type   |
|----------------|--------------------------------------------------------------------------------|--------|
| `audit_report` | TSV file containing membership entries with columns: `group`, `role`, `email`. | `File` |

---

## What `audit_terra_groups.py` does

### 1. Resolve group list

- If `--groups` **is** provided: those group names are used directly.
- If `--groups` is **not** provided: the script calls `TerraGroups.get_all_groups()` to discover every group the user belongs to and splits them by role:
  - Groups where the user is an **admin** → will be fully audited.
  - Groups where the user is only a **member** → a warning is logged for each and they are collected for the final failure step.

### 2. Retrieve group membership
For each group the user is an admin of, the script calls `TerraGroups.check_group_members` twice — once for the `admin` role and once for the `member` role — and collects the returned email lists.

### 3. Write TSV rows
Each email is emitted as one TSV row with the following columns:

- `group` — the Terra group name
- `role` — `admin` or `member`
- `email` — the principal's email address

Within each group, **admins are listed before members**, and emails within each role are sorted alphabetically. Groups are sorted alphabetically when auto-discovered, or appear in the order they were supplied when provided explicitly.

### 4. Fail on permission errors
After the output file is written, if there were any groups the user could not fully audit (member-only), the script raises a `PermissionError` listing all affected group names. This ensures the output is always preserved even when the job is marked as failed.

---

## Finding the Output

After the workflow finishes, retrieve the audit report through the Terra UI:

1. Open the workspace where the workflow was submitted and go to the **Submission History** tab.
2. Click on the submission where **AuditTerraGroups** was run.
3. Under the **Links** column, click the **Job Manager** icon (it looks like a bulleted checklist).
4. In Job Manager, click the **Outputs** tab.
5. The report is listed as **`audit_report`** — click the link to download the TSV file.

### If the workflow fails due to permission errors

The script writes the output file **before** raising a permission error, so the TSV will contain all data that was successfully collected even when the job is marked as failed. In that case:

- The `audit_report` output listed in Job Manager may still be populated with the data that was successfully collected.
- If it is not available there, navigate to the **execution directory** for the failed task. The `audit_report` TSV (`group_membership_audit.tsv`) will be in the same directory as the `stderr` log file.

---

## Notes

- The workflow only reports group membership; it does not modify groups or their members.
- The output is sorted to make scanning multiple groups easy: admins appear before members within each group, and emails are sorted alphabetically within each role block.
- A group is skipped (with a warning) if the user is only a member of it, because Terra does not allow members to view the full membership list. The job will fail at the end to surface these skipped groups.
