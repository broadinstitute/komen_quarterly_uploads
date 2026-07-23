# IngestKomenSamples

## Overview

`IngestKomenSamples` is a WDL workflow that runs the quarterly ShareForCures data ingest pipeline inside a Docker container on Terra.

It calls a single task (`CreateWorkspacesAndUploadMetadata`) which executes `create_and_upload_metadata_to_workspaces.py` to:

1. Read all CSV files for the given `release_directory` from the metadata GCS bucket
2. Validate every CSV against its expected Pydantic schema (column presence, types, no extra columns)
3. Validate that all sub workspace participants are a subset of the main workspace participants
4. Create Terra workspaces (main and/or sub, depending on `workspace_scope`), creating a dedicated auth-domain group for each sub workspace
5. Set the main workspace description from `release_notes.md` (if present) and each sub workspace's description from the general sub-workspace notes template plus that same `release_notes.md`
6. Skip any workspace where all expected tables already exist (unless `--force` is set)
7. Convert each CSV's rows through its schema model (coercing types, normalising booleans, etc.) and upload all tables to the appropriate workspace in a single batch upsert call
8. Build a `sequencing_files_table` from GCS genomics file paths (CRAM, CRAI, GVCF, VCF, QC metrics) for workspaces whose researcher has genomics file access
9. Grant each researcher READER access to their sub workspace and add them to the genomics access group where applicable
10. Raise a clear error at the end if any participant or researcher ID mapping failures were encountered

---

## Inputs

| Input Name            | Description                                                                                                                                                                                                                                                                                      | Type       | Required | Default                                                                                     |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|----------|---------------------------------------------------------------------------------------------|
| `release_directory`   | Quarterly release directory to process, e.g. `"shareforcures_dataset_2026_07"`. CSVs are read from `gs://{METADATA_BUCKET}/shareforcures_quarterly_releases/{release_directory}/`, and the main workspace name is derived from the `YYYY_MM` suffix of this directory name.                      | `String`   | Yes      | _(none)_                                                                                    |
| `workspace_scope`     | Which workspaces to create and upload to. `all` creates the main workspace and all sub workspaces. `main` creates only the main workspace. `sub` creates only sub workspaces (still reads main participants to validate sub participants are a subset).                                          | `String`   | No       | `"all"`                                                                                     |
| `include_workspaces`  | Space-separated string of exact sub workspace names to create and upload (e.g. `"WorkspaceA WorkspaceB"`). When provided, only those sub workspaces are processed and all others are skipped. Any name not found in the dataset raises an error. Has no effect when `workspace_scope` is `main`. | `String?`  | No       | _(none — all sub workspaces are processed)_                                                 |
| `exclude_workspaces`  | Space-separated string of exact sub workspace names to skip entirely (e.g. `"WorkspaceA WorkspaceB"`). Has no effect when `workspace_scope` is `main`. A warning is logged for any name not found in the dataset.                                                                                | `String?`  | No       | _(none — no sub workspaces are skipped)_                                                    |
| `force`               | Skip the table existence check and upload all data regardless of what is already in each workspace.                                                                                                                                                                                              | `Boolean`  | No       | `true`                                                                                      |
| `dry_run`             | Log everything that would happen without actually creating workspaces, uploading metadata, or modifying ACLs.                                                                                                                                                                                    | `Boolean`  | No       | `false`                                                                                     |
| `docker`              | Docker image to use for the task. If not provided, the latest production image is used.                                                                                                                                                                                                          | `String?`  | No       | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest`  |

---

## GCS Bucket Layout

All CSV files for a quarterly release must be placed in the metadata bucket before running this workflow.

**Bucket:** `gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f`

### Top-level release prefix

All releases live under:
```
shareforcures_quarterly_releases/
```

### Release directory

Each quarterly release has its own subdirectory named with the pattern `shareforcures_dataset_YYYY_MM`, e.g.:
```
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/
```

This directory name is what you supply as the `release_directory` workflow input. The `YYYY_MM` suffix is used to derive the main workspace name (`ShareForCures-Dataset-YYYY-MM`).

### Main dataset CSVs

The main dataset CSVs are placed **directly** inside the release directory (no subdirectory):
```
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/demographics.csv
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/biomarker.csv
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/patient_enrollment_status.csv
... (one file per expected table)
```

An optional `release_notes.md` may also be placed here; its contents become the main workspace description and are appended to every sub workspace's description:
```
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/release_notes.md
```

### Sub dataset subdirectories

Each researcher's data lives in a subdirectory **inside** the release directory, named exactly:
```
researcher_id_<researcher_id>_project_id_<project_id>/
```
For example:
```
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/researcher_id_62_project_id_115/
```

Inside each subdirectory, the files are the same set of common CSVs **plus** a required metadata CSV named:
```
researcher_id_<researcher_id>_project_id_<project_id>_metadata.csv
```
For example:
```
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/researcher_id_62_project_id_115/demographics.csv
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/researcher_id_62_project_id_115/biomarker.csv
shareforcures_quarterly_releases/shareforcures_dataset_2026_07/researcher_id_62_project_id_115/researcher_id_62_project_id_115_metadata.csv
... (one file per expected table, plus the metadata CSV)
```

The metadata CSV must contain at minimum a `project_name` column and a `date_created` column (`YYYY-MM` format). These values are used to derive the sub workspace name (`{project_name}_researcher_id_{researcher_id}_{YYYY}_{MM}`).

> **Note:** `patient_enrollment_status.csv` is expected to be present in each sub directory for completeness, but its contents are only uploaded to the **main** workspace — it is ignored when uploading sub workspace tables.

### Shared release notes template

A general sub-workspace description template shared across all releases lives at:
```
shareforcures_quarterly_releases/subworkspace_general_release_notes.md
```
This file must contain `{researcher_id}` and `{research_project_id}` placeholders which are filled in per sub workspace at upload time.

---

## What `create_and_upload_metadata_to_workspaces.py` does

### 1. Load and parse CSV files
All CSV files are listed from `gs://{METADATA_BUCKET}/shareforcures_quarterly_releases/{release_directory}/` and read in parallel with multithreading. Files directly under that path are the main dataset; files nested under a `researcher_id_<id>_project_id_<id>/` subdirectory are a sub dataset.

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

### 3. Create Terra workspaces and auth-domain groups
- The main workspace is named `ShareForCures-Dataset-YYYY-MM`, with `YYYY-MM` derived from the `release_directory` input
- Sub workspaces are named `{project_name}_researcher_id_{researcher_id}_{YYYY}_{MM}` derived from the metadata CSV
- Each sub workspace gets a dedicated auth-domain group named `researcher_id_{researcher_id}_project_id_{project_id}_{hash}`. The requesting researcher is added as a `MEMBER`, and both `Research-Admins@firecloud.org` and `Komen-Super-Admins@firecloud.org` are added as `ADMIN`
- All workspaces are created with `continue_if_exists=True` so re-runs are safe

### 4. Set workspace descriptions
- The release-specific `gs://{METADATA_BUCKET}/shareforcures_quarterly_releases/{release_directory}/release_notes.md` file, if present, is set as the main workspace description
- Each sub workspace's description is built from the general `gs://{METADATA_BUCKET}/shareforcures_quarterly_releases/subworkspace_general_release_notes.md` template (with its `{researcher_id}` and `{research_project_id}` placeholders filled in), followed by that same release-specific `release_notes.md` appended on a new line if it exists

### 5. Check whether uploads are needed
Before any heavy processing, each workspace is checked for whether all its expected tables already exist. If they do (and `--force` is not set) that workspace is skipped entirely. This avoids re-processing when the script is re-run on an already-complete workspace.

### 6. Build and upload table data
For each CSV file:
- Rows are run through their Pydantic model which coerces values to the correct Python types (e.g. `"yes"` → `True`, `"1.0"` → `1.0`)
- A synthetic row-ID column (`{table_name}_id`) is added counting from 1
- All tables for a workspace are uploaded in a single batch upsert call via `upload_metadata_with_batch_upsert`
- Column display order is set in Terra after upload

### 7. Build the sequencing files table
For each workspace whose researcher is listed in the genomics access CSV:
- Participant IDs are mapped to sample IDs via `onyx_mapping.csv` (adding a `K` prefix, e.g. sample `100` → `K100`)
- Duplicate participant entries are resolved via the duplicate participant mapping CSV
- GCS file existence is checked in parallel for all participants (CRAM, CRAI, GVCF, VCF, and QC metric files)
- A `sequencing_files_table` row is created per participant with paths to all files that exist, and `NA` for any that do not

The main workspace receives a master sequencing files table covering all main participants. Each sub workspace receives a sequencing files table filtered to its own participants.

### 8. Permissions
- Each researcher is granted `READER` access to their sub workspace
- The Research Admins group is granted `OWNER` access to every sub workspace
- Researchers with genomics file access are added to the `Genomics-Files-Access` Terra group

### 9. Mapping failure reporting
If any participant ID is not found in `onyx_mapping.csv`, or any researcher ID is not found in `all_researchers.csv`, these are collected and reported together at the very end. The script raises a `RuntimeError` clearly stating that all uploads completed but mapping failures exist, with each failure logged individually.

