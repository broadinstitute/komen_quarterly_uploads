version 1.0

workflow ValidateQuarterlyRelease {
	input {
		String workspace_scope = "main"
		String? include_workspaces
		String? exclude_workspaces
		String? docker
	}

	# TODO Update this once final storage location for Docker is determined
	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call ValidateRelease {
		input:
			workspace_scope = workspace_scope,
			include_workspaces = include_workspaces,
			exclude_workspaces = exclude_workspaces,
			docker_name = docker_name
	}
}

task ValidateRelease {
	input {
		String workspace_scope
		String? include_workspaces
		String? exclude_workspaces
		String docker_name
	}

	command <<<
		python /app/validate_quarterly_release.py \
			--workspace_scope ~{workspace_scope} \
			~{"--include_workspaces " + include_workspaces} \
			~{"--exclude_workspaces " + exclude_workspaces}
	>>>

	runtime {
		docker: docker_name
	}
}
