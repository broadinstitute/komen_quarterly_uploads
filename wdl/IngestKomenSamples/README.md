# IngestKomenSamples

## Overview

`IngestKomenSamples` is a WDL workflow that runs the quarterly ShareForCures data ingest pipeline inside a Docker container on Terra.

It calls a single task (`CreateWorkspacesAndUploadMetadata`) which executes `create_and_upload_metadata_to_workspaces.py` to:

1. Read all CSV files from the metadata GCS bucket
2. Validate every CSV against its expected Pydantic schema (column presence, types, no extra columns)
3. Validate that all sub workspace participants are a subset of the main workspace participants
4. Create Terra workspaces (main and/or sub, depending on `workspace_scope`)
5. Skip any workspace where all expected tables already exist (unless `--force` is set)
6. Convert each CSV's rows through its schema model (coercing types, normalising booleans, etc.) and upload all tables to the appropriate workspace in a single batch upsert call
7. Build a `sequencing_files_table` from GCS genomics file paths (CRAM, CRAI, GVCF, VCF, QC metrics) for workspaces whose researcher has genomics file access
8. Grant each researcher READER access to their sub workspace and add them to the genomics access group where applicable
9. Raise a clear error at the end if any participant or researcher ID mapping failures were encountered

---

## Inputs

| Input Name            | Description                                                                                                                                                                                                                                                                                        | Type       | Required | Default                                                                                     |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|----------|---------------------------------------------------------------------------------------------|
| `workspace_scope`     | Which workspaces to create and upload to. `all` creates the main workspace and all sub workspaces. `main` creates only the main workspace. `sub` creates only sub workspaces (still reads main participants to validate sub participants are a subset).                                            | `String`   | No       | `"all"`                                                                                     |
| `include_workspaces`  | Space-separated string of exact sub workspace names to create and upload (e.g. `"WorkspaceA WorkspaceB"`). When provided, only those sub workspaces are processed and all others are skipped. Any name not found in the dataset raises an error. Has no effect when `workspace_scope` is `main`.   | `String?`  | No       | _(none — all sub workspaces are processed)_                                                 |
| `exclude_workspaces`  | Space-separated string of exact sub workspace names to skip entirely (e.g. `"WorkspaceA WorkspaceB"`). Has no effect when `workspace_scope` is `main`. A warning is logged for any name not found in the dataset.                                                                                  | `String?`  | No       | _(none — no sub workspaces are skipped)_                                                    |
| `force`               | Skip the table existence check and upload all data regardless of what is already in each workspace.                                                                                                                                                                                                | `Boolean`  | No       | `true`                                                                                      |
| `dry_run`             | Log everything that would happen without actually creating workspaces, uploading metadata, or modifying ACLs.                                                                                                                                                                                      | `Boolean`  | No       | `false`                                                                                     |
| `dataset_notes`       | Path to a plain-text file whose contents will be set as the description attribute on every workspace created or updated.                                                                                                                                                                           | `File?`    | No       | _(none)_                                                                                    |
| `docker`              | Docker image to use for the task. If not provided, the latest production image is used.                                                                                                                                                                                                            | `String?`  | No       | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest`  |

---

## What `create_and_upload_metadata_to_workspaces.py` does

### 1. Load and parse CSV files
All CSV files are listed from the metadata GCS bucket and read in parallel with multithreading. Files are separated into:
- **Main dataset** — files under a `shareforcures_dataset_*/` directory
- **Sub datasets** — files under `researcher_id_<id>_project_id_<id>/` directories

If `include_workspaces` is provided, only sub datasets whose derived workspace name appears in that space-separated list are kept. All other sub datasets are skipped before any validation or upload work begins. If any name in the list does not match a sub dataset found in the bucket, the script raises an error immediately.

If `exclude_workspaces` is provided, any sub dataset whose derived workspace name appears in that space-separated list is skipped. A warning is logged for any name that did not match a sub dataset.

### 2. Validate datasets
Every CSV is validated against its Pydantic model from `csv_schemas`. Validation checks:
- All expected columns exist (even optional ones must be present as a column)
- No extra columns beyond what the model defines
- Values can be coerced to their expected types (int, float, bool, year, etc.)
- `project_name` in each sub dataset's metadata CSV is present and non-empty
- All sub workspace participants exist in the main dataset

If any validation fails the script exits before creating or modifying any workspace.

### 3. Create Terra workspaces
- The main workspace is named `ShareForCures-Dataset-YYYY-MM`
- Sub workspaces are named `{project_name}_researcher_id_{researcher_id}_{YYYY}_{MM}` derived from the metadata CSV
- All workspaces are created with `continue_if_exists=True` so re-runs are safe

### 4. Check whether uploads are needed
Before any heavy processing, each workspace is checked for whether all its expected tables already exist. If they do (and `--force` is not set) that workspace is skipped entirely. This avoids re-processing when the script is re-run on an already-complete workspace.

### 5. Build and upload table data
For each CSV file:
- Rows are run through their Pydantic model which coerces values to the correct Python types (e.g. `"yes"` → `True`, `"1.0"` → `1.0`)
- A synthetic row-ID column (`{table_name}_id`) is added counting from 1
- All tables for a workspace are uploaded in a single batch upsert call via `upload_metadata_with_batch_upsert`
- Column display order is set in Terra after upload

### 6. Build the sequencing files table
For each workspace whose researcher is listed in the genomics access CSV:
- Participant IDs are mapped to sample IDs via `onyx_mapping.csv` (adding a `K` prefix, e.g. sample `100` → `K100`)
- Duplicate participant entries are resolved via the duplicate participant mapping CSV
- GCS file existence is checked in parallel for all participants (CRAM, CRAI, GVCF, VCF, and QC metric files)
- A `sequencing_files_table` row is created per participant with paths to all files that exist, and `NA` for any that do not

The main workspace receives a master sequencing files table covering all main participants. Each sub workspace receives a sequencing files table filtered to its own participants.

### 7. Permissions
- Each researcher is granted `READER` access to their sub workspace
- The Research Admins group is granted `OWNER` access to every sub workspace
- Researchers with genomics file access are added to the `Genomics-Files-Access` Terra group

### 8. Mapping failure reporting
If any participant ID is not found in `onyx_mapping.csv`, or any researcher ID is not found in `all_researchers.csv`, these are collected and reported together at the very end. The script raises a `RuntimeError` clearly stating that all uploads completed but mapping failures exist, with each failure logged individually.

