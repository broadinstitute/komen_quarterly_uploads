"""Data models for Komen Quarterly Uploads."""

from typing import List, Optional, Set
from dataclasses import dataclass, field

try:
    from ops_utils.terra_util import TerraWorkspace
except ImportError:
    TerraWorkspace = None  # For type hints when ops_utils not available


@dataclass
class SubDatasetInfo:
    """Information about a sub dataset directory."""
    dir_name: str
    files: List[str]
    researcher_id: Optional[int] = None
    project_id: Optional[int] = None
    project_name: Optional[str] = None
    date_created: Optional[str] = None
    csv_directory_path: Optional[str] = None  # Path to the directory containing CSVs


@dataclass
class WorkspaceInfo:
    """Information about a created workspace."""
    workspace: 'TerraWorkspace'
    workspace_name: str
    participants: Set[str] = field(default_factory=set)
    bucket: Optional[str] = None  # Full bucket path including gs://


@dataclass
class SFTPDatasetInfo:
    """
    Data class for SFTP dataset information.
    Contains information about main and sub datasets retrieved from SFTP.
    """
    main_dataset_dir: Optional[str] = None
    main_dataset_files: List[str] = field(default_factory=list)
    main_dataset_path: Optional[str] = None  # Full path to main dataset directory
    sub_dataset_dirs: List[SubDatasetInfo] = field(default_factory=list)

