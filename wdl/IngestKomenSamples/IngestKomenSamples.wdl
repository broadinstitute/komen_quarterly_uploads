version 1.0

workflow IngestKomenSamples {
	input {
		String release_directory
		String workspace_scope = "all"
		Boolean force = false
		Boolean dry_run = false
		String? include_workspaces
		String? exclude_workspaces
		String? docker
	}

	# TODO Update this once final storage location for Docker is determined
	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call CreateWorkspacesAndUploadMetadata {
		input:
			release_directory = release_directory,
			workspace_scope = workspace_scope,
			force = force,
			dry_run = dry_run,
			include_workspaces = include_workspaces,
			exclude_workspaces = exclude_workspaces,
			docker_name = docker_name
	}
}

task CreateWorkspacesAndUploadMetadata {
	input {
		String release_directory
		String workspace_scope
		Boolean force
		Boolean dry_run
		String? include_workspaces
		String? exclude_workspaces
		String docker_name
	}

	command <<<
		python /app/create_and_upload_metadata_to_workspaces.py \
			--release_directory ~{release_directory} \
			--workspace_scope ~{workspace_scope} \
			~{"--include_workspaces " + include_workspaces} \
			~{"--exclude_workspaces " + exclude_workspaces} \
			~{if force then "--force" else ""} \
			~{if dry_run then "--dry_run" else ""}

	>>>

	runtime {
		docker: docker_name
	}
}
