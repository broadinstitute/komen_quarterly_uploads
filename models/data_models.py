"""Data models for Komen Quarterly Uploads."""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class SubDatasetInfo:
    """Information about a sub dataset directory."""
    files: List[str]  # Full file paths to CSV files in this sub dataset
    file_contents_map: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)  # Map of file_path -> list of row dicts
    researcher_id: int = None
    project_id: int = None
    project_name: Optional[str] = None
    date_created: Optional[str] = None


@dataclass
class DatasetInfo:
    """
    Data class for SFTP dataset information.
    Contains information about main and sub datasets retrieved from SFTP.
    """
    main_dataset_files: List[str] = field(default_factory=list)  # Full file paths to main dataset CSV files
    main_file_contents_map: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)  # Map of file_path -> list of row dicts
    sub_datasets: List[SubDatasetInfo] = field(default_factory=list)
