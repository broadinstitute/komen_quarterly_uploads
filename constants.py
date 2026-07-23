MAIN_WORKSPACE_NAME_TEMPLATE = "ShareForCures-Dataset-{year}-{month}"
SUB_WORKSPACE_NAME = "{clean_project_name}_researcher_id_{researcher_id}_{year}_{month}"

# TODO CHANGE BACK - FOR TESTING ONLY
#BILLING_PROJECT = "ops-integration-billing"
BILLING_PROJECT = "SFC-Research"

# Bucket and top-level folder where all quarterly release CSVs live.
# A given release's CSVs are found under gs://{METADATA_BUCKET}/{QUARTERLY_RELEASES_PREFIX}/{release_directory}/
METADATA_BUCKET = "fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f"
QUARTERLY_RELEASES_PREFIX = "shareforcures_quarterly_releases"

# General sub-workspace description template, shared by every sub workspace in every release.
# Contains {researcher_id} and {research_project_id} placeholders to be filled in per sub dataset.
SUBWORKSPACE_GENERAL_RELEASE_NOTES_FILE = f"gs://{METADATA_BUCKET}/{QUARTERLY_RELEASES_PREFIX}/subworkspace_general_release_notes.md"
# Optional per-release notes file. If present under a release directory, its contents become the
# main workspace description and are appended to every sub workspace's description.
RELEASE_NOTES_FILENAME = "release_notes.md"

# Genomics Files Configuration
# Files are in workspace: SFC-Research/ShareForCures Genomics Files
GENOMICS_BUCKET = "gs://fc-secure-ba527f7b-105c-437e-84e3-fe7e944efdec/"

# Mapping File Configuration
# Mapping file is in workspace: SFC-Research/ShareForCures Operational Data Files
PARTICIPANT_TO_SAMPLE_MAPPING_FILE_PATH = f"gs://{METADATA_BUCKET}/onyx_mapping/onyx_mapping.csv"

# CSV containing all users cleared for genomics file access
# Located in workspace: SFC-Research/ShareForCures Operational Data Files
GENOMICS_FILE_ACCESS_CSV = f"gs://{METADATA_BUCKET}/researcher_mapping/researchers_genomic_access.csv"

# Researcher email to ID mapping
# Located in workspace: SFC-Research/ShareForCures Operational Data Files
RESEARCHER_ID_TO_EMAIL_MAPPING = f"gs://{METADATA_BUCKET}/researcher_mapping/all_researchers.csv"
VIEW_DATA_NOTEBOOK_FILE = f"gs://{METADATA_BUCKET}/notebooks/View_data.ipynb"
GENOMICS_FILE_ACCESS_GROUP_NAME = "Genomics-Files-Access"
RESEARCH_ADMIN_GROUP_EMAIL = "Research-Admins@firecloud.org"
KOMEN_SUPER_ADMINS_GROUP_EMAIL = "Komen-Super-Admins@firecloud.org"

# SCOPES FOR SCRIPTS
MAIN = "main"
SUB = "sub"
ALL = "all"