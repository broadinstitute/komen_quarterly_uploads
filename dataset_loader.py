"""Shared dataset loading helpers for source CSV files stored in GCS."""

import csv
import re
from io import StringIO
from typing import TypedDict

from ops_utils.gcp_utils import GCPCloudFunctions

from models.data_models import DatasetInfo, SubDatasetInfo


class _SubDatasetAccumulator(TypedDict):
    """Temporary grouping structure used while building sub-dataset models.

    `files` preserves the original list of matched CSV paths for a single
    `(researcher_id, project_id)` pair, while `file_contents_map` stores the
    parsed row data for each of those files.
    """

    # Full GCS paths for all CSV files that belong to one sub-dataset.
    files: list[str]
    # Parsed CSV rows keyed by the corresponding file path in `files`.
    file_contents_map: dict[str, list[dict[str, str]]]


class DatasetLoader:
    """Loads source CSV files from GCS and organizes them into DatasetInfo objects."""

    MAIN_DATASET_PATTERN = re.compile(r"/shareforcures_dataset_[^/]+/")
    SUB_DATASET_PATTERN = re.compile(r"/researcher_id_(\d+)_project_id_(\d+)/")

    def __init__(self, gcp: GCPCloudFunctions):
        self.gcp = gcp

    @staticmethod
    def parse_csv_text_to_list_of_dicts(file_contents: str) -> list[dict[str, str]]:
        """Parse CSV text into a list of row dictionaries."""
        csv_text = file_contents.lstrip("\ufeff")
        return list(csv.DictReader(StringIO(csv_text)))

    def read_cloud_csv_as_dicts(self, cloud_path: str) -> list[dict[str, str]]:
        """Read a CSV from GCS and return its rows as dictionaries."""
        return self.parse_csv_text_to_list_of_dicts(self.gcp.read_file(cloud_path=cloud_path))

    def list_bucket_csv_paths(self, bucket_name: str) -> list[str]:
        """List full GCS paths to CSV files in a bucket."""
        blob_metadata = self.gcp.list_bucket_contents(
            bucket_name=bucket_name,
            file_extensions_to_include=[".csv"],
            file_name_only=True,
        )
        return [blob_info["path"] for blob_info in blob_metadata]

    def parse_csv_paths_to_dataset_info(self, all_csv_paths: list[str]) -> DatasetInfo:
        """Load all CSVs in one batch and organize them into main and sub datasets."""
        # Dict where key is file name and value is file contents
        all_file_contents = self.gcp.read_files_multithreaded(full_paths=all_csv_paths)

        main_files: list[str] = []
        main_file_contents_map: dict[str, list[dict[str, str]]] = {}
        sub_dataset_map: dict[tuple[int, int], _SubDatasetAccumulator] = {}

        for file_path in all_csv_paths:
            file_rows = self.parse_csv_text_to_list_of_dicts(all_file_contents[file_path])

            # Check if it's a main dataset file, which is determined by the presence of MAIN_DATASET_PATTERN in the path.
            if self.MAIN_DATASET_PATTERN.search(file_path):
                main_files.append(file_path)
                main_file_contents_map[file_path] = file_rows
                continue

            # Check if it's a sub-dataset file, which is determined by the presence of SUB_DATASET_PATTERN in path
            sub_match = self.SUB_DATASET_PATTERN.search(file_path)
            if not sub_match:
                continue

            researcher_id = int(sub_match.group(1))
            project_id = int(sub_match.group(2))
            dataset_key = (researcher_id, project_id)
            # Dataset not already seen — initialize the dict for this dataset
            if dataset_key not in sub_dataset_map:
                sub_dataset_map[dataset_key] = {"files": [], "file_contents_map": {}}

            sub_dataset_map[dataset_key]["files"].append(file_path)
            sub_dataset_map[dataset_key]["file_contents_map"][file_path] = file_rows

        sub_datasets = [
            SubDatasetInfo(
                files=sub_dataset_data["files"],
                file_contents_map=sub_dataset_data["file_contents_map"],
                researcher_id=researcher_id,
                project_id=project_id,
            )
            for (researcher_id, project_id), sub_dataset_data in sub_dataset_map.items()
        ]

        return DatasetInfo(
            main_dataset_files=main_files,
            main_file_contents_map=main_file_contents_map,
            sub_datasets=sub_datasets,
        )


