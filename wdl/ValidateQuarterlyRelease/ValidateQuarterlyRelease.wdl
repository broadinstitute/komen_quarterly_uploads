version 1.0

workflow ValidateQuarterlyRelease {
	input {
		String workspace_scope = "main"
		String? sub_workspaces_to_check
		String? docker
	}

	# TODO Update this once final storage location for Docker is determined
	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call ValidateRelease {
		input:
			workspace_scope = workspace_scope,
			docker_name = docker_name,
			sub_workspaces_to_check = sub_workspaces_to_check
	}
}

task ValidateRelease {
	input {
		String workspace_scope
		String docker_name
		String? sub_workspaces_to_check
	}

	command <<<
		python /app/validate_quarterly_release.py \
			--workspace_scope ~{workspace_scope} \
			~{"--sub_workspaces " + sub_workspaces_to_check}
	>>>

	runtime {
		docker: docker_name
	}
}
