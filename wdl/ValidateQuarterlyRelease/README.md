# ValidateQuarterlyRelease

## Overview

`ValidateQuarterlyRelease` is a WDL workflow that runs the quarterly ShareForCures post-upload validation workflow.

It calls a single task (`ValidateRelease`) which executes `validate_quarterly_release.py` to:

1. Download all CSV files from the metadata GCS bucket and parse them into main and sub dataset structures
2. Connect to existing Terra workspaces (no workspaces are created or modified)
3. Verify that every participant in each sub workspace is present in the main dataset
4. Verify that every participant is `active` and `enrolled` according to `patient_enrollment_status.csv`
5. Verify that every Terra workspace contains exactly the expected set of tables — no missing tables, no unexpected extras
6. Verify that the row contents of every table in Terra match what is expected from the source CSVs, with rows compared after passing through the same Pydantic schema transformation used during upload
7. Verify that `sequencing_files_table` contents match a live check of GCS genomics file paths for each participant

Validation stops on the first failure so that root-cause issues can be addressed before deeper checks are attempted. The script exits with a non-zero code if any check fails.

---

## Inputs

| Input Name         | Description                                                                                                                                                             | Type     | Required | Default                                                                                    |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|--------------------------------------------------------------------------------------------|
| `workspace_scope`  | Which workspaces to validate. `all` validates the main workspace and all sub workspaces. `main` validates only the main workspace. `sub` validates only sub workspaces. | `String` | No       | `"main"`                                                                                   |
| `docker`           | Docker image to use for the task. If not provided, the latest production image is used.                                                                                 | `String` | No       | `us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest` |

---

## What `validate_quarterly_release.py` does

### 1. Load and parse CSV files
All CSV files are listed from the metadata GCS bucket and read in parallel with multithreading. Files are separated 
into main nad subdatasets.
Each sub dataset's metadata CSV is read at parse time to populate `project_name`, `date_created`, and the derived 
`workspace_name` on the data model. 

### 2. Connect to existing Terra workspaces
`TerraWorkspace` objects are constructed directly from known workspace names without calling any create or modify 
APIs. The main workspace is identified by its constant name `ShareForCures-Dataset-YYYY-MM`. Sub workspace names are 
derived from each sub dataset's metadata CSV using the same naming convention as the upload script (`{project_name}_researcher_id_{researcher_id}_{YYYY}_{MM}`).

### 3. Load the genomics access list
The genomics access CSV is read from GCS to determine which researchers have clearance for genomics file access. This controls whether `sequencing_files_table` is expected to be present in each sub workspace during table validation.

### 4. Participant post-validation
Two checks are run in order, and validation stops on the first failure:

**Sub-participant membership check** _(scope: `all`, `sub`)_
Every participant ID found in any sub workspace CSV must also be present in the main dataset. Any participant not found in main is logged individually.

**Enrollment status check** _(scope: `all`, `main`/`sub` respectively)_
Every participant must have `role_user_status = "active"` and `step = "enrolled"` in `patient_enrollment_status.csv`. The enrollment status file is scanned across both main and sub dataset directories; the first occurrence of each participant ID is authoritative. Participants absent from the file entirely, or with unexpected status values, are each logged individually.

### 5. Terra table post-validation
Two checks are run per workspace, and validation stops on the first failure across all workspaces:

**Table existence check**
Each workspace is queried for its current set of tables. The check fails if any expected table is missing or if any unexpected extra table is present. Expected tables are derived from the same source CSV file list used by the upload script, with `sequencing_files_table` included for any sub workspace whose researcher has genomics access.

**Table content check**
For each expected table, its rows in Terra are fetched and compared against the rows built from the source CSVs. Expected rows are produced by passing each CSV through its Pydantic schema model, ensuring type coercion is applied consistently on both sides of the comparison. Rows are compared as order-independent sets — the synthetic row-ID column (`{table_name}_id`) is excluded from the comparison. Any missing or extra rows are logged in full.

### 6. Sequencing files table validation
The `sequencing_files_table` content check is handled by the same general table content check described above, but its expected rows are built differently:

- The participant-to-sample mapping is loaded from `onyx_mapping.csv` (adding a `K` prefix to each sample ID)
- A live GCS existence check is run once for all main participants using `GenomicsFileChecker`, which checks for CRAM, CRAI, GVCF, VCF, and QC metric files in parallel
- Sub workspace expected rows are produced by filtering the main participant results to only that workspace's participants — the expensive GCS check is not repeated per sub workspace
- Any file that does not exist in GCS is represented as `NA` in the expected rows, matching the upload script's convention

The resulting expected rows are then compared against what is currently stored in Terra using the same row-level comparison logic as all other tables.

