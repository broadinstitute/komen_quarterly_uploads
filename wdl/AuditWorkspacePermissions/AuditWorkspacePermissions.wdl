version 1.0

workflow AuditWorkspacePermissions {
	input {
		String billing_project
		String? workspaces
		String? emails
		String? docker
	}

	# TODO Update this once final storage location for Docker is determined
	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call AuditWorkspacesTask {
		input:
            billing_project = billing_project,
            workspaces = workspaces,
            emails = emails,
			docker_name = docker_name
	}

    output {
        File audit_report = AuditWorkspacesTask.audit_report
    }
}

task AuditWorkspacesTask {
	input {
		String billing_project
        String? workspaces
        String? emails
        String docker_name
	}

	command <<<
		python /app/audit_workspace_permissions.py \
			--billing_project ~{billing_project} \
			~{"--workspaces " + workspaces} \
			~{"--emails " + emails}
	>>>

	runtime {
		docker: docker_name
	}

    output {
        File audit_report = "permission_audit.tsv"
    }
}