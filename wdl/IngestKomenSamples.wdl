version 1.0

workflow IngestKomenSamples {
	input {
		Boolean continue_if_workspace_exists = true
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call CreateWorkspacesAndUploadMetadata {
		input:
			continue_if_workspace_exists = continue_if_workspace_exists,
			docker_name = docker_name
	}
}

task CreateWorkspacesAndUploadMetadata {
	input {
		Boolean continue_if_workspace_exists
		String docker_name
	}

	command <<<
		python /app/create_and_upload_metadata_to_workspaces.py \
			~{if continue_if_workspace_exists then "--continue_if_workspace_exists" else ""}
	>>>

	runtime {
		docker: docker_name
	}
}
