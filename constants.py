from datetime import datetime

MAIN_WORKSPACE_NAME = f"ShareForCures-Dataset-{datetime.now().strftime('%Y-%m')}"

SUB_WORKSPACE_NAME_TEMPLATE = "{project_name}_{year}_{month}"

# TODO CHANGE BACK - FOR TESTING ONLY
BILLING_PROJECT = "ops-integration-billing"
#BILLING_PROJECT = "SFC-Research"

# TODO: This is our test bucket, will need to be changed when Komen gives us their bucket where files are saved
METADATA_CSVS_BUCKET = "fc-fa9fd891-996a-4624-864e-c4f81d165a90"

# Genomics Files Configuration
# Files are in workspace: SFC-Research/ShareForCures Genomics Files
GENOMICS_BUCKET = "gs://fc-secure-ba527f7b-105c-437e-84e3-fe7e944efdec/"

# Mapping File Configuration
# Mapping file is in workspace: SFC-Research/ShareForCures Operational Data Files
PARTICIPANT_TO_SAMPLE_MAPPING_FILE_PATH = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/onyx_mapping/onyx_mapping.csv"

# CSV containing all users cleared for genomics file access
# Located in workspace: SFC-Research/ShareForCures Operational Data Files
GENOMICS_FILE_ACCESS_CSV = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/researcher_mapping/researchers_genomic_access.csv"

# Researcher email to ID mapping
# Located in workspace: SFC-Research/ShareForCures Operational Data Files
RESEARCHER_ID_TO_EMAIL_MAPPING = "gs://fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f/researcher_mapping/all_researchers.csv"
GENOMICS_FILE_ACCESS_GROUP_NAME = "Genomics-Files-Access"
RESEARCH_ADMIN_GROUP_EMAIL = "Research-Admins@firecloud.org"

# SCOPES FOR SCRIPTS
MAIN = "main"
SUB = "sub"
ALL = "all"