# AuditWorkspacePermissions

## Overview

`AuditWorkspacePermissions` is a WDL workflow that audits Terra workspace ACLs and outputs a tab-separated permissions report.

It calls a single task (`AuditWorkspacesTask`) which executes `audit_workspace_permissions.py` to:

1. Resolve workspaces to audit for a billing project, capturing each workspace's access level and `canShare` flag
2. For workspaces where the caller has sufficient access (OWNER or canShare), retrieve the full ACL from Terra
3. For workspaces where the caller lacks sufficient access, write an informative row in the output instead of making an API call
4. Optionally filter ACL rows to specific email addresses
5. Output a TSV report with one row per workspace-principal permission entry

This workflow is read-only with respect to workspace metadata and table data. It does not create, update, or delete workspaces. The workflow **always succeeds** — permission issues are recorded as informative rows in the output TSV rather than causing the job to fail.

---

## Inputs

| Input Name        | Description                                                                                                                                                                                                                                  | Type      | Required | Default                                                                                    |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------|--------------------------------------------------------------------------------------------|
| `billing_project` | Terra billing project (namespace) to audit.                                                                                                                                                                                                  | `String`  | Yes      | _(none)_                                                                                   |
| `workspaces`      | Optional space-separated list of exact workspace names to audit (e.g. `"WorkspaceA WorkspaceB"`). Even when provided, the script still fetches all accessible workspaces for the billing project and warns for any requested name not found. | `String?` | No       | _(none — all accessible workspaces in the billing project are audited)_                    |
| `emails`          | Optional space-separated list of email addresses to filter output rows (e.g. `"user1@example.org user2@example.org"`).                                                                                                                       | `String?` | No       | _(none — all ACL principals are included)_                                                 |
| `docker`          | Docker image to use for the task. If not provided, the latest production image is used.                                                                                                                                                      | `String?` | No       | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest` |

---

## Outputs

| Output Name    | Description                                                                                                                                    | Type   |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| `audit_report` | TSV file containing permission entries with columns: `billing_project`, `workspace_name`, `email`, `permissions`, `can_share`, `can_compute`.  | `File` |

---

## What `audit_workspace_permissions.py` does

### 1. Resolve workspace list
The script calls `Terra.fetch_accessible_workspaces(...)` and filters results to the requested `billing_project`. For each workspace it captures:
- `accessLevel` — the caller's role (e.g. `OWNER`, `WRITER`, `READER`)
- `canShare` — whether the caller can share the workspace

A workspace is considered **auditable** if the caller is an `OWNER` **or** `canShare` is `true`.

- If `workspaces` is **not** provided: all workspaces in the billing project are included.
- If `workspaces` **is** provided: only those names are kept if found in the accessible set; any not found are logged as a warning.

### 2. Check caller access before retrieving ACLs
Before making an ACL API call, the script checks whether the workspace is auditable:

- **Auditable** (OWNER or canShare) → calls `TerraWorkspace.get_workspace_acl()` to retrieve the full ACL.
- **Not auditable** → skips the API call entirely and writes a `PERMISSION DENIED` sentinel row to the output.

Any unexpected API error also results in a sentinel row rather than aborting the run.

### 3. Apply optional email filter
If `emails` is provided, ACL rows are filtered to those principals only (case-insensitive match).

### 4. Write TSV rows
Each ACL entry is emitted as one TSV row. Workspaces where the caller lacked sufficient access get a single sentinel row instead:

| `workspace_name`    | `email`           | `permissions`                                                                   | `can_share` | `can_compute` |
|---------------------|-------------------|---------------------------------------------------------------------------------|-------------|---------------|
| MyWorkspace         | alice@example.com | OWNER                                                                           | true        | true          |
| RestrictedWorkspace | PERMISSION DENIED | Could not retrieve ACL — caller access level is 'READER' and canShare is False. | false       | N/A           |

`permissions` reflects the Terra access level (for example `OWNER`, `WRITER`, `READER`) and includes a pending marker when applicable.

---

## Finding the Output

After the workflow finishes, retrieve the audit report through the Terra UI:

1. Open the workspace where the workflow was submitted and go to the **Submission History** tab.
2. Click on the submission where **AuditWorkspacePermissions** was run.
3. Under the **Links** column, click the **Job Manager** icon (it looks like a bulleted checklist).
4. In Job Manager, click the **Outputs** tab.
5. The report is listed as **`audit_report`** — click the link to download the TSV file.

---

## Notes

- The workflow only reports permissions; it does not modify ACLs.
- The workflow will **not** fail due to permission errors. Workspaces the caller cannot fully audit appear as `PERMISSION DENIED` rows in the output TSV, making them easy to identify without breaking the run.
- Workspaces are considered auditable when the caller's access level is `OWNER` or `canShare` is `true`. All other access levels (`WRITER`, `READER`, etc. without canShare) result in a sentinel row.
