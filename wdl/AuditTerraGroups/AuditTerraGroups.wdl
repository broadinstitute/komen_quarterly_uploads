version 1.0

workflow AuditTerraGroups {
	input {
		String? groups
		String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])

	call AuditTerraGroupsTask {
		input:
			groups = groups,
			docker_name = docker_name
	}

	output {
		File audit_report = AuditTerraGroupsTask.audit_report
	}
}

task AuditTerraGroupsTask {
	input {
		String? groups
		String docker_name
	}

	command <<<
		python /app/audit_terra_groups.py \
			~{"--groups " + groups}
	>>>

	runtime {
		docker: docker_name
	}

	output {
		File audit_report = "group_membership_audit.tsv"
	}
}

