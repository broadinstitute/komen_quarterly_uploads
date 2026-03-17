"""Shared runtime configuration for workspace creation and validation scripts."""

from datetime import datetime
from typing import Optional

MAIN_WORKSPACE_PREFIX = "ShareForCures-Dataset"
BILLING_PROJECT = "SFC-Research"
METADATA_CSVS_BUCKET = "fc-fa9fd891-996a-4624-864e-c4f81d165a90"

# Files are in workspace: SFC-Research/ShareForCures Genomics Files
GENOMICS_BUCKET = "gs://fc-secure-ba527f7b-105c-437e-84e3-fe7e944efdec/"

# Mapping files are in workspace: SFC-Research/ShareForCures Operational Data Files
PARTICIPANT_TO_SAMPLE_MAPPING_FILE_PATH = (
    "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/onyx_mapping/onyx_mapping.csv"
)
GENOMICS_FILE_ACCESS_CSV = (
    "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/researcher_mapping/researchers_genomic_access.csv"
)
RESEARCHER_ID_TO_EMAIL_MAPPING = (
    "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/researcher_mapping/all_researchers.csv"
)
GENOMICS_FILE_ACCESS_GROUP_NAME = "Genomics-Files-Access"
RESEARCH_ADMIN_GROUP_EMAIL = "Research-Admins@firecloud.org"


def get_main_workspace_name(reference_datetime: Optional[datetime] = None) -> str:
    """Return the main workspace name for the given month, or the current month by default."""
    reference_datetime = reference_datetime or datetime.now()
    return f"{MAIN_WORKSPACE_PREFIX}-{reference_datetime.strftime('%Y-%m')}"

