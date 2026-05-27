# AuditWorkspacePermissions

## Overview

`AuditWorkspacePermissions` is a WDL workflow that audits Terra workspace ACLs and outputs a tab-separated permissions report.

It calls a single task (`AuditWorkspacesTask`) which executes `audit_workspace_permissions.py` to:

1. Resolve workspaces to audit for a billing project
2. Retrieve workspace ACLs from Terra
3. Optionally filter ACL rows to specific email addresses
4. Output a TSV report with one row per workspace-principal permission entry

This workflow is read-only with respect to workspace metadata and table data. It does not create, update, or delete workspaces.

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
The script always calls `Terra.fetch_accessible_workspaces(...)` and filters results to the requested `billing_project`.

- If `workspaces` is **not** provided: all accessible workspaces in that billing project are audited.
- If `workspaces` **is** provided: only those names are kept **if** found in the accessible set.
- Any explicitly requested workspace not found is logged as a warning.

### 2. Retrieve ACLs
For each workspace selected in step 1, the script constructs a `TerraWorkspace` object and calls `TerraWorkspace.get_workspace_acl()`.

### 3. Apply optional email filter
If `emails` is provided, ACL rows are filtered to those principals only (case-insensitive match).

### 4. Write TSV rows
Each matching ACL entry is emitted as one TSV row with the following columns:

- `billing_project`
- `workspace_name`
- `email`
- `permissions`
- `can_share`
- `can_compute`

`permissions` reflects the Terra access level (for example `OWNER`, `WRITER`, `READER`) and includes a pending marker when applicable.

---

## Notes

- The workflow only reports permissions; it does not modify ACLs.
- Workspaces that fail ACL retrieval are logged and skipped so the workflow can continue processing other workspaces.

