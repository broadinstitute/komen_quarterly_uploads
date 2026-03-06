"""Data models for Komen Quarterly Uploads."""

from typing import Optional, Any
from dataclasses import dataclass, field


@dataclass
class SubDatasetInfo:
    """Information about a sub dataset directory."""
    # Full file paths to CSV files in this sub dataset
    files: list[str]
    # Mapping of file_path -> file contents as a list of dictionaries
    file_contents_map: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    researcher_id: int = None
    project_id: int = None
    project_name: str = None
    date_created: str = None
    workspace_name: str = None


@dataclass
class DatasetInfo:
    """
    Data class for dataset information.
    Contains information about main and sub datasets retrieved from a Google bucket.
    """
    # Full file paths to main dataset CSV files
    main_dataset_files: list[str] = field(default_factory=list)
    # Mapping of file_path -> file contents as a list of dictionaries
    main_file_contents_map: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    sub_datasets: list[SubDatasetInfo] = field(default_factory=list)
