# AuditTerraGroups

## Overview

`AuditTerraGroups` is a WDL workflow that audits the membership of one or more Terra groups and outputs a tab-separated report.

It calls a single task (`AuditTerraGroupsTask`) which executes `audit_terra_groups.py` to:

1. Resolve which groups to audit — either from explicit input or by auto-discovering all groups the user belongs to
2. Retrieve the admins and members of each group the user is an admin of
3. For any group the user is only a member of, write an informative row in the output indicating that membership could not be retrieved
4. Output a TSV report with one row per group-role-email entry

This workflow is read-only. It does not create, update, or delete groups or their memberships. The workflow **always succeeds** — permission issues are recorded as informative rows in the output TSV rather than causing the job to fail.

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
  - Groups where the user is only a **member** → a `PERMISSION DENIED` sentinel row is written to the output for each and no API call is made.

### 2. Retrieve group membership
For each group the user is an admin of, the script calls `TerraGroups.check_group_members` twice — once for the `admin` role and once for the `member` role — and collects the returned email lists.

### 3. Write TSV rows
Each email is emitted as one TSV row. Groups where the user lacked admin access get a single sentinel row instead:

| `group`           | `role`            | `email`                                                                       |
|-------------------|-------------------|-------------------------------------------------------------------------------|
| my-group          | admin             | alice@example.com                                                             |
| my-group          | member            | bob@example.com                                                               |
| restricted-group  | PERMISSION DENIED | Could not retrieve membership — user is a member of this group, not an admin. |

Within auditable groups, **admins are listed before members**, and emails within each role are sorted alphabetically. Groups are sorted alphabetically when auto-discovered, or appear in the order they were supplied when provided explicitly.

---

## Finding the Output

After the workflow finishes, retrieve the audit report through the Terra UI:

1. Open the workspace where the workflow was submitted and go to the **Submission History** tab.
2. Click on the submission where **AuditTerraGroups** was run.
3. Under the **Links** column, click the **Job Manager** icon (it looks like a bulleted checklist).
4. In Job Manager, click the **Outputs** tab.
5. The report is listed as **`audit_report`** — click the link to download the TSV file.

---

## Notes

- The workflow only reports group membership; it does not modify groups or their members.
- The workflow will **not** fail due to permission errors. Groups the user cannot fully audit appear as `PERMISSION DENIED` rows in the output TSV, making them easy to identify without breaking the run.
- The output is sorted to make scanning multiple groups easy: admins appear before members within each group, and emails are sorted alphabetically within each role block.
