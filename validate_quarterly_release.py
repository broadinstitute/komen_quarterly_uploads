# Get all csvs (and download contents)
# Get all expected workspaces + main
# Get all active participants - new
# Go through and collect all participants from csvs
# Make sure all participants in sub are in main and that all are active and enrolled - new
    # Stop if fail (in logging write did not check contents match yet)

# Convert csvs content to list of dicts with table_id added - use existing one but just use table_data in table_data_utils
# Validate all expected tables exist (and not extra)
    # stop if fail

# Download each table info from terra and convert to list of dicts - new
    #  Add to pyops? (will have name as table id and atttributes and will need to flatten)
# Compare converted csv contents and terra downloaded data to see if match - new
# Compare terra contents to csv_schema validation pydantics - maybe we do?
# Check all files exist from file tables
