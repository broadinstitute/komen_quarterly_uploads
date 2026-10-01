version 1.0

workflow ValidateQuarterlyRelease {
	input {
		String release_directory
		String workspace_scope = "all"
		String? include_workspaces
		String? exclude_workspaces
		String? docker
		String? billing_project
	}

	# TODO Update this once final storage location for Docker is determined
	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call ValidateRelease {
		input:
			release_directory = release_directory,
			workspace_scope = workspace_scope,
			include_workspaces = include_workspaces,
			exclude_workspaces = exclude_workspaces,
			docker_name = docker_name,
			billing_project = billing_project
	}
}

task ValidateRelease {
	input {
		String release_directory
		String workspace_scope
		String? include_workspaces
		String? exclude_workspaces
		String docker_name
		String? billing_project
	}

	command <<<
		python /app/validate_quarterly_release.py \
			--release_directory ~{release_directory} \
			--workspace_scope ~{workspace_scope} \
			~{"--include_workspaces " + include_workspaces} \
			~{"--exclude_workspaces " + exclude_workspaces} \
			~{"--billing_project " + billing_project}
	>>>

	runtime {
		docker: docker_name
	}
}
