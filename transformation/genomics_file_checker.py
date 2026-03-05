"""Genomics file existence checking utilities."""

import logging
from collections import defaultdict
from typing import Optional
from ops_utils.gcp_utils import GCPCloudFunctions


# All file types and their path templates relative to the genomics bucket.
# {} is replaced with the sample ID (e.g. K100).
GENOMICS_FILE_TEMPLATES: dict[str, str] = {
    "cram":             "CRAM/{sample_id}.cram",
    "crai":             "CRAM/{sample_id}.cram.crai",
    "cram_md5":         "CRAM/{sample_id}.cram.md5sum",
    "gvcf":             "GVCF/{sample_id}.hard-filtered.gvcf.gz",
    "gvcf_tbi":         "GVCF/{sample_id}.hard-filtered.gvcf.gz.tbi",
    "vcf":              "VCF/{sample_id}.hard-filtered.vcf.gz",
    "vcf_md5":          "VCF/{sample_id}.hard-filtered.vcf.gz.md5sum",
    "vcf_tbi":          "VCF/{sample_id}.hard-filtered.vcf.gz.tbi",
    "mapping_metrics":  "QC_Metrics/{sample_id}.mapping_metrics.csv",
    "coverage_metrics": "QC_Metrics/{sample_id}.qc-coverage-region-1_coverage_metrics.csv",
    "vc_metrics":       "QC_Metrics/{sample_id}.vc_metrics.csv",
}


class GenomicsFileChecker:
    """
    Checks GCP for the existence of every expected genomics file for a set of participants.

    Returns a dict keyed by participant_id where each value is a dict of
    file_type -> full GCS path if the file exists, or None if it does not.
    """

    def __init__(self, gcp: GCPCloudFunctions, participant_to_sample: dict[str, str], genomics_bucket: str):
        """
        Args:
            gcp: Shared GCPCloudFunctions instance.
            participant_to_sample: Mapping of participant_id -> sample_id (with K prefix).
            genomics_bucket: Base GCS bucket path (e.g. 'gs://fc-secure-xxx/').
        """
        self.gcp = gcp
        self.participant_to_sample = participant_to_sample
        self.genomics_bucket = genomics_bucket

    def check_all_participants(
        self, participants: set[str]
    ) -> dict[str, dict[str, Optional[str]]]:
        """
        Check every expected genomics file for every participant using a single
        multithreaded call rather than one request per file.

        Args:
            participants: Set of participant IDs to check.

        Returns:
            Dict of participant_id -> {file_type: full_path_or_None}.
            Participants with no sample ID mapping are omitted and logged as warnings.
        """
        # For each file path, track which participant it belongs to and what type of file it is.
        # Used to reassemble results after the multithreaded existence check.
        file_path_ownership: dict[str, tuple[str, str]] = {}

        for participant_id in sorted(participants):
            sample_id = self.participant_to_sample.get(participant_id)
            if not sample_id:
                logging.warning(
                    f"No sample ID found for participant {participant_id} — skipping genomics file check"
                )
                continue

            for file_type, template in GENOMICS_FILE_TEMPLATES.items():
                full_path = f"{self.genomics_bucket}{template.format(sample_id=sample_id)}"
                file_path_ownership[full_path] = (participant_id, file_type)

        # If no participants had sample mappings, return an empty result rather than making an unnecessary GCP call.
        if not file_path_ownership:
            return {}

        # Single multithreaded call for all paths at once
        existence_map: dict[str, bool] = self.gcp.check_files_exist_multithreaded(
            full_paths=list(file_path_ownership.keys())
        )

        # Reassemble into participant_id -> {file_type: path_or_None}
        participant_file_map: defaultdict[str, dict[str, Optional[str]]] = defaultdict(dict)

        for full_path, (participant_id, file_type) in file_path_ownership.items():
            exists = existence_map.get(full_path, False)
            participant_file_map[participant_id][file_type] = full_path if exists else None

            if not exists:
                logging.warning(
                    f"Genomics file not found for participant {participant_id}: {full_path}"
                )

        logging.info(
            f"Genomics file check complete: {len(participant_file_map)} participant(s) checked, "
            f"{len(participants) - len(participant_file_map)} skipped (no sample mapping)"
        )
        return dict(participant_file_map)



