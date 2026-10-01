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
		String? billing_project

		# questionnaire_manifest.csv is baked into r_docker (it only changes when the
		# underlying CSV schemas do, which already requires a Docker rebuild).
		Boolean five_year_diagnosis = false
		String? r_docker
		String metadata_bucket = "fc-secure-4a43e11f-e9ae-40b4-a449-cdd8ec55b17f"
		String quarterly_releases_prefix = "shareforcures_quarterly_releases"
	}

	# TODO Update this once final storage location for Docker is determined
	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest"])
	String r_docker_name = select_first([r_docker, "us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_questionnaire_r:latest"])

	call CreateWorkspacesAndUploadMetadata {
		input:
			release_directory = release_directory,
			workspace_scope = workspace_scope,
			force = force,
			dry_run = dry_run,
			include_workspaces = include_workspaces,
			exclude_workspaces = exclude_workspaces,
			docker_name = docker_name,
			billing_project = billing_project
	}

	# Always runs, regardless of workspace_scope or whether CreateWorkspacesAndUploadMetadata
	# encountered mapping failures — see that task's docstring for why it no longer fails the
	# task for that case. UploadQuestionnaireSummary itself no-ops when workspace_scope is "sub"
	# (this table only ever belongs to the main workspace). Keeping both calls unconditional
	# (rather than gated by workspace_scope via an `if` block) means their outputs are always
	# produced — never an optional/possibly-absent value — which keeps CheckMappingFailures'
	# dependency on UploadQuestionnaireSummary below simple.
	call RunQuestionnaireAnalysis {
		input:
			release_directory = release_directory,
			five_year_diagnosis = five_year_diagnosis,
			metadata_bucket = metadata_bucket,
			quarterly_releases_prefix = quarterly_releases_prefix,
			docker_name = r_docker_name
	}

	call UploadQuestionnaireSummary {
		input:
			release_directory = release_directory,
			workspace_scope = workspace_scope,
			questionnaire_summary_csv = RunQuestionnaireAnalysis.questionnaire_summary_csv,
			dry_run = dry_run,
			docker_name = docker_name,
			billing_project = billing_project,
			wait_for = CreateWorkspacesAndUploadMetadata.done
	}

	# Placed last and fed UploadQuestionnaireSummary's result (unused in the command, just to
	# create a real dependency) so this always runs after everything else has had a chance to.
	# This is what actually fails the workflow run if CreateWorkspacesAndUploadMetadata recorded
	# any mapping failures — surfacing them to a human without having blocked anything above.
	call CheckMappingFailures {
		input:
			mapping_failures_file = CreateWorkspacesAndUploadMetadata.mapping_failures_file,
			upload_questionnaire_summary_done = UploadQuestionnaireSummary.done,
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
		String? billing_project
	}

	# create_and_upload_metadata_to_workspaces.py no longer fails the task when
	# participant/researcher mapping failures are found — it records them to
	# mapping_failures.txt (declared below) and logs each one, but by that point all workspace
	# creation and metadata uploads have already completed successfully, and a failure here
	# would otherwise block UploadQuestionnaireSummary (an unrelated, independent step) from
	# ever running. CheckMappingFailures reads mapping_failures_file afterward and is what
	# actually fails the workflow run so the failure isn't silently lost.
	command <<<
		python /app/create_and_upload_metadata_to_workspaces.py \
			--release_directory ~{release_directory} \
			--workspace_scope ~{workspace_scope} \
			~{"--include_workspaces " + include_workspaces} \
			~{"--exclude_workspaces " + exclude_workspaces} \
			~{if force then "--force" else ""} \
			~{if dry_run then "--dry_run" else ""} \
			~{"--billing_project " + billing_project}

	>>>

	output {
		Boolean done = true
		File mapping_failures_file = "mapping_failures.txt"
	}

	runtime {
		docker: docker_name
	}
}

task RunQuestionnaireAnalysis {
	input {
		String release_directory
		Boolean five_year_diagnosis
		String metadata_bucket
		String quarterly_releases_prefix
		String docker_name
	}

	command <<<
		set -euo pipefail

		mkdir -p Data
		gsutil -m cp "gs://~{metadata_bucket}/~{quarterly_releases_prefix}/~{release_directory}/*.csv" Data/
		cp /app/questionnaire_manifest.csv questionnaire_manifest.csv

		Rscript /app/sfc_questionnaire_analysis_pipeline.R ~{if five_year_diagnosis then "--five_year_diagnosis" else ""}
	>>>

	output {
		File questionnaire_summary_csv = "questionnaire_summary.csv"
		File data_collection_counts = "data_collection_counts.txt"
		Array[File] survey_summary_docs = glob("*_summary.docx")
	}

	runtime {
		docker: docker_name
	}
}

task UploadQuestionnaireSummary {
	input {
		String release_directory
		String workspace_scope
		File questionnaire_summary_csv
		Boolean dry_run
		String docker_name
		String? billing_project
		Boolean wait_for
	}

	command <<<
		python /app/upload_questionnaire_summary.py \
			--release_directory ~{release_directory} \
			--workspace_scope ~{workspace_scope} \
			--summary_csv ~{questionnaire_summary_csv} \
			~{if dry_run then "--dry_run" else ""} \
			~{"--billing_project " + billing_project}
	>>>

	output {
		Boolean done = true
	}

	runtime {
		docker: docker_name
	}
}

task CheckMappingFailures {
	input {
		File mapping_failures_file
		# Unused in the command below — only accepted so the workflow-level call site can wire
		# it in to force this task to run after UploadQuestionnaireSummary (see call site comment).
		Boolean upload_questionnaire_summary_done
		String docker_name
	}

	command <<<
		if [ -s "~{mapping_failures_file}" ]; then
			echo "CreateWorkspacesAndUploadMetadata recorded participant/researcher mapping failures:" >&2
			cat "~{mapping_failures_file}" >&2
			exit 1
		fi
		echo "No mapping failures recorded."
	>>>

	runtime {
		docker: docker_name
	}
}
