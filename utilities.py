import csv
import logging
import re
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Optional

from ops_utils.gcp_utils import GCPCloudFunctions

from constants import PARTICIPANT_TO_SAMPLE_MAPPING_FILE_PATH
from csv_schemas import MAIN_ONLY_CSVS
from models.data_models import DatasetInfo, SubDatasetInfo
from transformation.table_data_utils import get_table_id_column

# Matches the dynamic metadata filename, e.g. researcher_id_62_project_id_115_metadata.csv
_METADATA_FILE_PATTERN = re.compile(r"researcher_id_\d+_project_id_\d+_metadata\.csv$")


def format_workspace_name(project_name: str, date_created: str, researcher_id: int) -> str:
    """
    Derive the Terra workspace name for a sub dataset.
    """
    if '-' in date_created:
        parts = date_created.split('-')
        year = parts[0]
        month = parts[1] if len(parts) > 1 else '01'
    else:
        # Fall back to the current month if the date cannot be parsed
        now = datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')

    # Replace spaces and special characters so the name is a valid Terra workspace identifier
    clean_project_name = re.sub(r'[^\w-]', '_', project_name)
    return f"{clean_project_name}_researcher_id_{researcher_id}_{year}_{month}"


def parse_csv_paths_to_dataset_info(
    all_csv_paths: list[str],
    gcp: GCPCloudFunctions,
    include_workspaces: Optional[list[str]] = None,
    exclude_workspaces: Optional[list[str]] = None,
) -> DatasetInfo:
    """
    Parse a list of CSV file paths into a DatasetInfo structure.

    Separates files into the main dataset (under shareforcures_dataset_*/) and
    sub datasets (under researcher_id_*_project_id_*/).
    Read all file contents in a single multithreaded call then organize them.

    Args:
        all_csv_paths: Full list of GCS paths to process.
        gcp: Shared GCPCloudFunctions instance.
        include_workspaces: Optional list of sub workspace names to include.
            When provided, only sub datasets whose derived workspace_name appears in
            this list are kept. Any name not matched raises a ValueError.
        exclude_workspaces: Optional list of sub workspace names to skip entirely.
            Any workspace in this list is excluded before any validation or upload.
            A warning is logged for any name in the list that was not found.
    """
    # TODO Update this directory pattern matching once we know how CSV files are saved in the bucket
    main_pattern = re.compile(r'/shareforcures_dataset_[^/]+/')
    sub_pattern = re.compile(r'/researcher_id_(\d+)_project_id_(\d+)/')

    # Read all files in one multithreaded call
    all_file_contents: dict[str, str] = gcp.read_files_multithreaded(full_paths=all_csv_paths)

    main_files = []
    main_file_contents = {}
    sub_datasets_dict = {}  # Key: (researcher_id, project_id), Value: dict with 'files' and 'contents'

    for file_path in all_csv_paths:
        raw_contents = all_file_contents[file_path]
        # Some source CSVs may include a UTF-8 BOM on the first header; strip it once here
        # so the parsed headers match the pydantic schema field names exactly.
        contents_as_list = list(csv.DictReader(StringIO(raw_contents.lstrip("\ufeff"))))

        if main_pattern.search(file_path):
            main_files.append(file_path)
            main_file_contents[file_path] = contents_as_list
        else:
            sub_match = sub_pattern.search(file_path)
            if sub_match:
                researcher_id = int(sub_match.group(1))
                project_id = int(sub_match.group(2))
                key = (researcher_id, project_id)

                if key not in sub_datasets_dict:
                    sub_datasets_dict[key] = {"files": [], "contents": {}}
                sub_datasets_dict[key]["files"].append(file_path)
                sub_datasets_dict[key]["contents"][file_path] = contents_as_list

    sub_datasets = []
    found_include_names: list[str] = []   # tracks which include_workspaces names were matched
    found_exclude_names: set[str] = set() # tracks which exclude_workspaces names were matched

    for (researcher_id, project_id), data in sub_datasets_dict.items():
        project_name = None
        date_created = None
        workspace_name = None

        # The metadata CSV (researcher_id_X_project_id_Y_metadata.csv) is already loaded
        # in the contents map — extract project_name and date_created from its first row
        # so that SubDatasetInfo is fully populated without any additional GCP calls.
        for file_path, rows in data["contents"].items():
            if _METADATA_FILE_PATTERN.search(file_path) and rows:
                project_name = rows[0].get("project_name", "").strip() or None
                date_created = rows[0].get("date_created", "").strip() or None
                break

        # Derive the workspace name immediately once both fields are available
        if project_name and date_created:
            workspace_name = format_workspace_name(
                project_name=project_name,
                date_created=date_created,
                researcher_id=researcher_id,
            )
        else:
            logging.warning(
                f"researcher_id_{researcher_id}_project_id_{project_id}: "
                f"could not derive workspace_name — "
                f"project_name={project_name!r}, date_created={date_created!r}"
            )

        # Skip workspaces explicitly excluded by the caller
        if exclude_workspaces and workspace_name in exclude_workspaces:
            found_exclude_names.add(workspace_name)
            logging.info(f"Excluding sub workspace '{workspace_name}' as requested")
            continue

        # If an include list is active, skip sub datasets not in it
        if include_workspaces is not None:
            if workspace_name not in include_workspaces:
                continue
            found_include_names.append(workspace_name)

        sub_datasets.append(SubDatasetInfo(
            files=data["files"],
            file_contents_map=data["contents"],
            researcher_id=researcher_id,
            project_id=project_id,
            project_name=project_name,
            date_created=date_created,
            workspace_name=workspace_name,
        ))

    # Warn about any excluded workspace names that were not actually found in the dataset
    if exclude_workspaces:
        for name in exclude_workspaces:
            if name not in found_exclude_names:
                logging.warning(
                    f"Excluded workspace '{name}' was not found in the dataset — check the workspace name"
                )

    # If an include list was used, error if any requested name was not found
    if include_workspaces is not None:
        not_found = [name for name in include_workspaces if name not in found_include_names]
        if not_found:
            for name in not_found:
                logging.error(f"Requested sub workspace '{name}' was not found in the dataset")
            raise ValueError(
                f"{len(not_found)} requested sub workspace(s) were not found in the dataset: "
                f"{not_found}. Check the workspace names and try again."
            )

    return DatasetInfo(
        main_dataset_files=main_files,
        main_file_contents_map=main_file_contents,
        sub_datasets=sub_datasets
    )

def list_bucket_path_and_parse_dataset_info(
    bucket: str,
    gcp: GCPCloudFunctions,
    include_workspaces: Optional[list[str]] = None,
    exclude_workspaces: Optional[list[str]] = None,
) -> DatasetInfo:
    blob_metadata = gcp.list_bucket_contents(bucket_name=bucket, file_extensions_to_include=[".csv"], file_name_only=True)
    all_csv_paths = [a["path"] for a in blob_metadata]
    dataset_info = parse_csv_paths_to_dataset_info(
        all_csv_paths,
        gcp,
        include_workspaces=include_workspaces,
        exclude_workspaces=exclude_workspaces,
    )
    return dataset_info


def extract_all_participant_ids_from_files(
    file_contents_map: dict[str, list[dict[str, Any]]],
    patient_id_column: str = "patient_id",
) -> set[str]:
    """
    Extract all unique participant IDs from a mapping of file paths to their parsed rows.

    Iterates over every row in every file and collects non-empty values from
    patient_id_column. Files that do not contain the column are silently skipped.
    """
    all_participant_ids: set[str] = set()

    for csv_file_path, file_contents in file_contents_map.items():
        for row in file_contents:
            # strip() guards against leading/trailing whitespace in source CSVs
            if patient_id := row.get(patient_id_column, "").strip():
                all_participant_ids.add(patient_id)

    logging.info(
        f"Extracted {len(all_participant_ids)} unique participant ID(s) "
        f"from {len(file_contents_map)} file(s)"
    )
    return all_participant_ids

def create_calculated_age_diagnosis_table_data(file_contents_map: dict[str, list], unique_patient_ids: set[str]) -> dict:
    patient_profile_eligibility_csv = "patient_profile_eligibility.csv"
    patient_profile_eligibility = next((v for k, v in file_contents_map.items() if k.endswith(patient_profile_eligibility_csv)), [])
    calculated_age_diagnosis_table_name = "calculated_age_diagnosis_table"

    calculated_age_of_diagnosis_table_data = []
    current_year = datetime.now().year
    table_id = get_table_id_column(table_name=calculated_age_diagnosis_table_name)
    row_counter = 1

    for patient_id in unique_patient_ids:
        if patient_record := [row for row in patient_profile_eligibility if row.get("patient_id") == patient_id]:
            year_of_diagnosis = int(patient_record[0]["year_of_first_breast_cancer_diagnosis"])
            year_of_birth = int(patient_record[0]["date_of_birth"])

            calculated_age_of_diagnosis_table_data.append(
                {
                    table_id: str(row_counter),
                    "patient_id": patient_id,
                    "time_since_diagnosis": str(current_year - year_of_diagnosis),
                    "age_at_diagnosis": str(year_of_diagnosis - year_of_birth),
                    "current_age": str(current_year - year_of_birth),
                }
            )
            row_counter += 1
    return {
        calculated_age_diagnosis_table_name: {
            "table_id_column": table_id,
            "row_data": calculated_age_of_diagnosis_table_data,
        }
    }

def get_cloud_csv_contents_as_dict(cloud_path: str, gcp: GCPCloudFunctions) -> list[dict]:
    """
    Read a single CSV file from a GCS path and return its rows as a list of dicts.

    Strips a UTF-8 BOM if present so that header names match schema field names exactly.
    """
    file_contents = gcp.read_file(cloud_path=cloud_path)
    # Strip UTF-8 BOM and parse into row dicts
    csv_text = file_contents.lstrip("\ufeff")
    reader = csv.DictReader(StringIO(csv_text))
    return list(reader)


def get_expected_main_table_names(
    main_dataset_files: list[str],
    file_contents_map: dict[str, list],
) -> list[str]:
    """
    Compute the expected Terra table names for the main workspace.

    Every main CSV file with at least one data row produces one table named ``{stem}_table``.
    CSVs that contain only a header (no data rows) are excluded — no table is created for them.
    The sequencing_files_table and calculated_age_diagnosis_table are always appended.
    """
    tables = [
        f"{Path(f).stem}_table"
        for f in main_dataset_files
        if file_contents_map.get(f)  # exclude CSVs with no data rows
    ]
    tables.append("sequencing_files_table")
    # The "calculated_age_diagnosis_table" should always be present, but is a constructed table and doesn't come from a CSV
    tables.append("calculated_age_diagnosis_table")
    return tables


def get_expected_sub_table_names(
    sub_files: list[str],
    file_contents_map: dict[str, list],
    has_genomics_access: bool,
) -> list[str]:
    """
    Compute the expected Terra table names for a sub workspace.

    CSVs listed in MAIN_ONLY_CSVS and CSVs with no data rows are excluded.
    The sequencing_files_table is included only when the researcher has genomics access.
    """
    tables = [
        f"{Path(f).stem}_table"
        for f in sub_files
        if Path(f).name not in MAIN_ONLY_CSVS
        and file_contents_map.get(f)  # exclude CSVs with no data rows
    ]
    # The "calculated_age_diagnosis_table" should always be present, but is a constructed table and doesn't come from a CSV
    tables.append("calculated_age_diagnosis_table")
    if has_genomics_access:
        # Sequencing files table is only present in workspaces where the
        # researcher has been granted genomics data access.
        tables.append("sequencing_files_table")
    return tables


def load_participant_to_sample_mapping(gcp: GCPCloudFunctions) -> dict[str, str]:
    """
    Load the mapping from participant IDs to sample IDs from the onyx mapping CSV.

    The mapping file contains columns 'Participant ID' and 'Sample ID'.
    Each sample ID is prefixed with 'K' to match the format used in the genomics bucket.
    """
    mapping_file_contents = gcp.read_file(cloud_path=PARTICIPANT_TO_SAMPLE_MAPPING_FILE_PATH)
    reader = csv.DictReader(StringIO(mapping_file_contents.lstrip("\ufeff")))
    mapping_dict = {
        row["Participant ID"].strip(): f"K{row['Sample ID'].strip()}"
        for row in reader
        if row.get("Participant ID") and row.get("Sample ID")
    }
    logging.info(f"Loaded participant to sample mapping for {len(mapping_dict)} participants")
    return mapping_dict
