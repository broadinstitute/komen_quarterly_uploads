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

		# Optional questionnaire analysis + Terra upload, main workspace only.
		# Omit questionnaire_manifest to skip this part of the workflow entirely.
		File? questionnaire_manifest
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
			docker_name = docker_name
	}

	if (defined(questionnaire_manifest)) {
		call RunQuestionnaireAnalysis {
			input:
				release_directory = release_directory,
				questionnaire_manifest = select_first([questionnaire_manifest]),
				five_year_diagnosis = five_year_diagnosis,
				metadata_bucket = metadata_bucket,
				quarterly_releases_prefix = quarterly_releases_prefix,
				docker_name = r_docker_name
		}

		call UploadQuestionnaireSummary {
			input:
				release_directory = release_directory,
				questionnaire_summary_csv = RunQuestionnaireAnalysis.questionnaire_summary_csv,
				dry_run = dry_run,
				docker_name = docker_name,
				wait_for = CreateWorkspacesAndUploadMetadata.done
		}
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

	output {
		Boolean done = true
	}

	runtime {
		docker: docker_name
	}
}

task RunQuestionnaireAnalysis {
	input {
		String release_directory
		File questionnaire_manifest
		Boolean five_year_diagnosis
		String metadata_bucket
		String quarterly_releases_prefix
		String docker_name
	}

	command <<<
		set -euo pipefail

		mkdir -p Data
		gsutil -m cp "gs://~{metadata_bucket}/~{quarterly_releases_prefix}/~{release_directory}/*.csv" Data/
		cp ~{questionnaire_manifest} questionnaire_manifest.csv

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
		File questionnaire_summary_csv
		Boolean dry_run
		String docker_name
		Boolean wait_for
	}

	command <<<
		python /app/upload_questionnaire_summary.py \
			--release_directory ~{release_directory} \
			--summary_csv ~{questionnaire_summary_csv} \
			~{if dry_run then "--dry_run" else ""}
	>>>

	runtime {
		docker: docker_name
	}
}
