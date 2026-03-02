"""Data models for Komen Quarterly Uploads."""

from typing import List, Optional
from dataclasses import dataclass, field

# TODO add file_contents to data model to run future content validation
@dataclass
class SubDatasetInfo:
    """Information about a sub dataset directory."""
    files: List[str]  # Full file paths to CSV files in this sub dataset
    researcher_id: Optional[int] = None
    project_id: int = None
    project_name: str = None
    date_created: Optional[str] = None


@dataclass
class DatasetInfo:
    """
    Data class for SFTP dataset information.
    Contains information about main and sub datasets retrieved from SFTP.
    """
    main_dataset_files: List[str] = field(default_factory=list)  # Full file paths to main dataset CSV files
    sub_dataset_dirs: List[SubDatasetInfo] = field(default_factory=list)
