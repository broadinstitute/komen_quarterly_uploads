version 1.0

workflow IngestKomenSamples {
	input {
		String workspace_scope = "main"
		Boolean force = false
		Boolean dry_run = false
		File? dataset_notes
		String? docker
	}

	# TODO Update this once final storage location for Docker is determined
	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call CreateWorkspacesAndUploadMetadata {
		input:
			workspace_scope = workspace_scope,
			force = force,
			dry_run = dry_run,
			dataset_notes = dataset_notes,
			docker_name = docker_name
	}
}

task CreateWorkspacesAndUploadMetadata {
	input {
		String workspace_scope
		Boolean force
		Boolean dry_run
		File? dataset_notes
		String docker_name
	}

	command <<<
		python /app/create_and_upload_metadata_to_workspaces.py \
			--workspace_scope ~{workspace_scope} \
			~{"--dataset_notes " + dataset_notes} \
			~{if force then "--force" else ""} \
			~{if dry_run then "--dry_run" else ""}

	>>>

	runtime {
		docker: docker_name
	}
}
