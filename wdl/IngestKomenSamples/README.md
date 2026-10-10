# IngestKomenSamples

## Overview

`IngestKomenSamples` is a WDL workflow that runs the quarterly ShareForCures data ingest pipeline inside a Docker container on Terra.

It calls a task (`CreateWorkspacesAndUploadMetadata`) which executes `create_and_upload_metadata_to_workspaces.py` to:

1. Read all CSV files for the given `release_directory` from the metadata GCS bucket
2. Validate every CSV against its expected Pydantic schema (column presence, types, no extra columns)
3. Validate that all sub workspace participants are a subset of the main workspace participants
4. Create Terra workspaces (main and/or sub, depending on `workspace_scope`), creating a dedicated auth-domain group for each sub workspace
5. Set the main workspace description from `release_notes.md` (if present) and each sub workspace's description from the general sub-workspace notes template plus that same `release_notes.md`
6. Skip any workspace where all expected tables already exist (unless `--force` is set)
7. Convert each CSV's rows through its schema model (coercing types, normalising booleans, etc.) and upload all tables to the appropriate workspace in a single batch upsert call
8. Build a `sequencing_files_table` from GCS genomics file paths (CRAM, CRAI, GVCF, VCF, QC metrics) for workspaces whose researcher has genomics file access
9. Grant each researcher READER access to their sub workspace and add them to the genomics access group where applicable
10. Record any participant or researcher ID mapping failures to `mapping_failures.txt` (and log each one) — this no longer fails the task itself; see below for why

---

## Inputs

| Input Name            | Description                                                                                                                                                                                                                                                                                      | Type       | Required | Default                                                                                     |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|----------|---------------------------------------------------------------------------------------------|
| `release_directory`   | Quarterly release directory to process, e.g. `"shareforcures_dataset_2026_07"`. CSVs are read from `gs://{METADATA_BUCKET}/shareforcures_quarterly_releases/{release_directory}/`, and the main workspace name is derived from the `YYYY_MM` suffix of this directory name.                      | `String`   | Yes      | _(none)_                                                                                    |
| `workspace_scope`     | Which workspaces to create and upload to. `all` creates the main workspace and all sub workspaces. `main` creates only the main workspace. `sub` creates only sub workspaces (still reads main participants to validate sub participants are a subset).                                          | `String`   | No       | `"all"`                                                                                     |
| `include_workspaces`  | Space-separated string of exact sub workspace names to create and upload (e.g. `"WorkspaceA WorkspaceB"`). When provided, only those sub workspaces are processed and all others are skipped. Any name not found in the dataset raises an error. Has no effect when `workspace_scope` is `main`. | `String?`  | No       | _(none — all sub workspaces are processed)_                                                 |
| `exclude_workspaces`  | Space-separated string of exact sub workspace names to skip entirely (e.g. `"WorkspaceA WorkspaceB"`). Has no effect when `workspace_scope` is `main`. A warning is logged for any name not found in the dataset.                                                                                | `String?`  | No       | _(none — no sub workspaces are skipped)_                                                    |
| `force`               | Skip the table existence check and upload all data regardless of what is already in each workspace.                                                                                                                                                                                              | `Boolean`  | No       | `false`                                                                                     |
| `dry_run`             | Log everything that would happen without actually creating workspaces, uploading metadata, or modifying ACLs.                                                                                                                                                                                    | `Boolean`  | No       | `false`                                                                                     |
| `docker`              | Docker image to use for the task. If not provided, the latest production image is used.                                                                                                                                                                                                          | `String?`  | No       | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest`  |
| `billing_project`     | Terra billing project to create/use workspaces under. If not provided, each python script falls back to `BILLING_PROJECT` from `constants.py`.                                                                                                                                                   | `String?`  | No       | `BILLING_PROJECT` from `constants.py`                                                        |
| `five_year_diagnosis` | Passed through to `sfc_questionnaire_analysis_pipeline.R` as `--five_year_diagnosis`, restricting the analysis to participants diagnosed since 2020. `RunQuestionnaireAnalysis` always runs regardless of `workspace_scope` (see below). | `Boolean` | No | `false` |
| `r_docker`            | Docker image used for the `RunQuestionnaireAnalysis` task. If not provided, the latest production R image is used. | `String?` | No | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_questionnaire_r:latest` |
| `metadata_bucket`     | GCS bucket `RunQuestionnaireAnalysis` pulls the release's main dataset CSVs from. Must match `METADATA_BUCKET` in `constants.py`. | `String` | No | `fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f` |
| `quarterly_releases_prefix` | GCS prefix under `metadata_bucket` that releases live under. Must match `QUARTERLY_RELEASES_PREFIX` in `constants.py`. | `String` | No | `shareforcures_quarterly_releases` |

---

## Optional: questionnaire analysis and summary upload

`RunQuestionnaireAnalysis` and `UploadQuestionnaireSummary` always run, regardless of `workspace_scope`:

1. **`RunQuestionnaireAnalysis`** downloads the release's main dataset CSVs from GCS into `Data/`, copies the `questionnaire_manifest.csv` baked into the image (see `Dockerfile.r`) into the working directory, runs `sfc_questionnaire_analysis_pipeline.R` (in a dedicated R Docker image built from `Dockerfile.r`), and produces:
   - `questionnaire_summary.csv` — one row per `question` / `response` pair (e.g. `gender` / `male`, `country` / `usa`) with `total`, `percentage`, and `survey_title` (the source survey table name) columns, computed from the same one-row-per-participant survey tables used for the docx summaries, over the full participant count for that survey (not just non-missing values)
   - a `_summary.docx` per survey (unchanged from the original script)
   - `data_collection_counts.txt` (unchanged)
2. **`UploadQuestionnaireSummary`** uploads `questionnaire_summary.csv` to the main workspace's `questionnaire_summary_table` via a single batch upsert, using the same CSV-schema/Terra-upload path as every other table in this pipeline (`csv_schemas.QuestionnaireSummaryRow` → `convert_csv_rows_to_table_data`). It waits for `CreateWorkspacesAndUploadMetadata` to finish so the main workspace is guaranteed to exist first, but is a no-op (no upload attempted) when `workspace_scope` is `sub` — this table only ever belongs to the main workspace.

Both run unconditionally (not gated by `workspace_scope` via an `if` block in the WDL) so their outputs are always real values, never an optional/possibly-absent one — see below for why that matters.

**Known limitations — column name collisions in `questionnaire_manifest.csv`:** `question` is derived from the column name only (no survey prefix), which creates two distinct risks depending on where the collision occurs:
- **Within a survey (silent data loss):** if two CSVs mapped to the *same* survey define the same column — e.g. `patient_profile_more_about_you.csv` and `patient_profile_supplemental_about_you.csv` (both `about_you`) both define `gender`, `sex_assigned_at_birth`, and `sexual_orientation` — `sfc_questionnaire_analysis_pipeline.R`'s `bind_rows()` merges them into one column, and the one-row-per-participant collapse (`first(na.omit(.x))`) silently picks whichever source has a non-NA value first if a participant has differing values in both. There's no way to tell which source won.
- **Across surveys (ambiguous key, not an overwrite):** if two CSVs in *different* surveys define the same column — e.g. `has_genetic_test` appears in both `patient_profile_provider_info.csv` (`about_you`) and `family_history_you.csv` (`family_health_history`) — both surveys produce their own row with the identical `question` and `response` (e.g. `has_genetic_test` / `yes`). These do **not** overwrite each other in Terra (each gets its own sequential row id), but querying by `question` and `response` alone can't distinguish which survey's cohort a row represents (use `survey_title` for that).

Check `questionnaire_manifest.csv` and the underlying CSV schemas for column name collisions — both within and across survey groups — before relying on this in production.

---

## Why a `CreateWorkspacesAndUploadMetadata` mapping failure doesn't block anything else

`create_and_upload_metadata_to_workspaces.py` can encounter participant/researcher ID mapping failures even after successfully creating workspaces and uploading all metadata. It used to raise a `RuntimeError` for this, purely to flag the issue to a human — but in WDL/Cromwell, a task failure stops the scheduler from starting **any** new task, not just ones that depend on the failed one (Cromwell's default `workflowFailureMode` is `NoNewCalls`). That meant a mapping failure could prevent `RunQuestionnaireAnalysis`/`UploadQuestionnaireSummary` from ever starting, even though they have no real dependency on it.

Instead, the script now records mapping failures to `mapping_failures.txt` (always written, even when empty) and logs each one, but no longer fails the task for this reason — all other failure modes (schema validation, participant validation, etc., which happen earlier and before any workspace exists) still fail the task normally. A final `CheckMappingFailures` task — which depends on `CreateWorkspacesAndUploadMetadata.mapping_failures_file` and (to force it to run last) `UploadQuestionnaireSummary`'s completion — reads that file and is what actually fails the workflow run if it's non-empty, so the original issue still surfaces to you without having blocked anything.

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
- A synthetic row-ID column (`{table_name}_id`) is added counting from 1, zero-padded to 6 digits (e.g. `000001`, `000825`) so that sorting the column as text still matches numeric order. The width is fixed rather than sized to each table's row count — a `--force` re-run with a different row count would otherwise shift every id's padding and leave stale, differently-padded duplicate rows behind, since Terra's upsert matches on the id column's exact string value (see `format_row_id` in `transformation/table_data_utils.py`)
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
If any participant ID is not found in `onyx_mapping.csv`, or any researcher ID is not found in `all_researchers.csv`, these are collected and written to `mapping_failures.txt` (one per line, and always written even if empty) with each failure also logged individually. This does not fail the script — see "Why a `CreateWorkspacesAndUploadMetadata` mapping failure doesn't block anything else" above for why, and how the failure still surfaces to you via `CheckMappingFailures`.

