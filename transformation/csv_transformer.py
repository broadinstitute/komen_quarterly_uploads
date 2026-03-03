"""CSV transformation and conversion utilities."""

import logging
import csv
from io import StringIO
from typing import Optional, Set, List, Dict, Any
from pathlib import Path

from ops_utils.csv_util import Csv
from ops_utils.gcp_utils import GCPCloudFunctions


class CSVTransformer:
    """Handles CSV transformations and adjustments before upload."""

    @staticmethod
    def get_entity_column_name(csv_filename: str) -> str:
        """
        Generate entity column name from CSV filename.

        Args:
            csv_filename: CSV filename (e.g., 'demographics.csv')

        Returns:
            Entity column name (e.g., 'entity:demographics_id')
        """
        # Remove .csv extension
        table_name = csv_filename.replace('.csv', '')
        return f"entity:{table_name}_table_id"

    def add_entity_id_column(self, csv_path: str, output_path: str) -> None:
        """
        Add entity ID column to CSV file.

        The entity column will be named 'entity:{table_name}_id' and will contain
        row numbers starting from 1.

        Args:
            csv_path: Path to input CSV file
            output_path: Path to output CSV file with entity ID column

        Returns:
            True if successful, False otherwise
        """
        csv_filename = Path(csv_path).name
        entity_col_name = self.get_entity_column_name(csv_filename)

        # TODO check if initializing this in main and passing into CSVTransformer would prevent multiple tokens from being generated
        file_contents = GCPCloudFunctions().read_file(cloud_path=csv_path)
        reader = csv.DictReader(StringIO(file_contents))
        fieldnames = [entity_col_name] + list(reader.fieldnames or [])

        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row_num, row in enumerate(reader, start=1):
                row[entity_col_name] = str(row_num)
                writer.writerow(row)

        logging.info(f"Added entity ID column '{entity_col_name}' to {csv_filename}")

    def transform_csv_for_upload(self, csv_path: str, output_dir: str) -> Optional[str]:
        """
        Transform a CSV file for upload to Terra by adding entity ID column.

        Args:
            csv_path: Path to input CSV file
            output_dir: Directory to write transformed CSV

        Returns:
            Path to the transformed CSV file, or None if failed
        """
        csv_filename = Path(csv_path).name
        output_path = Path(output_dir) / csv_filename

        if self.add_entity_id_column(csv_path, str(output_path)):
            return str(output_path)
        return None

    @staticmethod
    def extract_participant_ids(csv_path: str, patient_id_column: str = "patient_id") -> Set[str]:
        """
        Extract participant IDs from a CSV file.

        Args:
            csv_path: Path to CSV file
            patient_id_column: Name of the column containing patient IDs

        Returns:
            Set of participant IDs found in the CSV
        """
        participant_ids = set()

        # Read CSV as list of dicts using Csv utility with comma delimiter
        cloud_file_contents = GCPCloudFunctions().read_file(cloud_path=csv_path)
        reader = csv.DictReader(StringIO(cloud_file_contents))
        csv_data = list(reader)

        # Check if patient_id column exists
        if csv_data and patient_id_column not in csv_data[0]:
            logging.warning(f"Column '{patient_id_column}' not found in {csv_path}")
            return participant_ids

        # Extract participant IDs
        for row in csv_data:
            patient_id = row.get(patient_id_column, "").strip()
            if patient_id:
                participant_ids.add(patient_id)

        logging.info(f"Extracted {len(participant_ids)} participant IDs from {Path(csv_path).name}")
        return participant_ids

    def extract_all_participant_ids_from_files(
        self,
        csv_file_paths: List[str],
        file_contents_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        patient_id_column: str = "patient_id"
    ) -> Set[str]:
        """
        Extract all participant IDs from a list of CSV file paths.

        Args:
            csv_file_paths: List of full file paths to CSV files
            file_contents_map: Optional dict mapping file paths to their contents (list of row dictionaries).
                              If provided, this is used instead of reading files again.
            patient_id_column: Name of the column containing patient IDs (default: 'patient_id')

        Returns:
            Set of all unique participant IDs found
        """
        all_participant_ids = set()

        # Process all CSV files
        for csv_file_path in csv_file_paths:
            # If files contents mapping is provided, use it directly
            if file_contents_map and csv_file_path in file_contents_map:
                csv_data = file_contents_map[csv_file_path]

                # Check if patient_id column exists
                if csv_data and patient_id_column not in csv_data[0]:
                    logging.warning(f"Column '{patient_id_column}' not found in {csv_file_path}")
                    continue

                # Extract participant IDs
                for row in csv_data:
                    patient_id = row.get(patient_id_column, "").strip()
                    if patient_id:
                        all_participant_ids.add(patient_id)
            else:
                # Fall back to reading the file if the mapping isn't provided or doesn't contain this file
                participant_ids = self.extract_participant_ids(csv_file_path)
                all_participant_ids.update(participant_ids)

        logging.info(f"Extracted {len(all_participant_ids)} unique participant IDs from {len(csv_file_paths)} files")
        return all_participant_ids

    @staticmethod
    def convert_csv_to_tsv(csv_path: str, output_path: str) -> None:
        """
        Convert CSV file to TSV format.

        Args:
            csv_path: Path to input CSV file
            output_path: Path to output TSV file

        Returns:
            True if successful
        """
        # Read CSV as list of dicts with comma delimiter
        csv_data = Csv(file_path=csv_path, delimiter=',').create_list_of_dicts_from_tsv()

        # Get headers from first row or empty list
        header_list = []
        if csv_data:
            header_list = list(csv_data[0].keys())
        else:
            csv_data = []

        # Write as TSV with tab delimiter
        Csv(file_path=output_path, delimiter='\t').create_tsv_from_list_of_dicts(
            list_of_dicts=csv_data,
            header_list=header_list
        )

        logging.info(f"Converted {Path(csv_path).name} to TSV: {Path(output_path).name}")

    def transform_and_convert_csv(self, csv_path: str, output_dir: str) -> Optional[str]:
        """
        Transform CSV (add entity ID) and convert to TSV for Terra upload.

        Args:
            csv_path: Path to input CSV file
            output_dir: Directory to write output TSV

        Returns:
            Path to output TSV file, or None if failed
        """
        csv_filename = Path(csv_path).name
        temp_csv_path = Path(output_dir) / f"temp_{csv_filename}"
        tsv_filename = csv_filename.replace('.csv', '.tsv')
        output_tsv_path = Path(output_dir) / tsv_filename

        # Add entity ID column
        self.add_entity_id_column(csv_path, str(temp_csv_path))

        # Convert to TSV
        self.convert_csv_to_tsv(str(temp_csv_path), str(output_tsv_path))

        # Clean up temp file
        temp_csv_path.unlink()
        return str(output_tsv_path)

    @staticmethod
    def create_sequencing_files_tsv(
        participants: Set[str],
        genomics_bucket: str,
        output_path: str,
        participant_to_sample: dict
    ) -> str:
        """
        Create a TSV file listing sequencing files for participants.

        Args:
            participants: Set of participant IDs
            genomics_bucket: Genomics files bucket path (e.g., 'gs://fc-secure-xxx/')
            output_path: Path to write output TSV
            participant_to_sample: Dict mapping participant_id to sample_id (with K prefix)

        Returns:
            Path to created TSV file
        """
        sequencing_data = []

        # Create rows for each participant
        for idx, participant_id in enumerate(sorted(participants), start=1):
            # Get sample ID for this participant
            sample_id = participant_to_sample.get(participant_id)
            if not sample_id:
                logging.warning(f"No sample ID found for participant {participant_id}, skipping")
                continue

            # Build file paths
            cram_path = f"{genomics_bucket}CRAM/{sample_id}.cram"
            crai_path = f"{genomics_bucket}CRAM/{sample_id}.cram.crai"
            cram_md5_path = f"{genomics_bucket}CRAM/{sample_id}.cram.md5sum"

            gvcf_path = f"{genomics_bucket}GVCF/{sample_id}.hard-filtered.gvcf.gz"
            gvcf_tbi_path = f"{genomics_bucket}GVCF/{sample_id}.hard-filtered.gvcf.gz.tbi"

            vcf_path = f"{genomics_bucket}VCF/{sample_id}.hard-filtered.vcf.gz"
            vcf_md5_path = f"{genomics_bucket}VCF/{sample_id}.hard-filtered.vcf.gz.md5sum"
            vcf_tbi_path = f"{genomics_bucket}VCF/{sample_id}.hard-filtered.vcf.gz.tbi"

            mapping_metrics_path = f"{genomics_bucket}QC_Metrics/{sample_id}.mapping_metrics.csv"
            coverage_metrics_path = f"{genomics_bucket}QC_Metrics/{sample_id}.qc-coverage-region-1_coverage_metrics.csv"
            vc_metrics_path = f"{genomics_bucket}QC_Metrics/{sample_id}.vc_metrics.csv"

            row_data = {
                'entity:sequencing_files_id': str(idx),
                'participant_id': participant_id,
                'sample_id': sample_id,
                'cram': cram_path,
                'crai': crai_path,
                'cram_md5': cram_md5_path,
                'gvcf': gvcf_path,
                'gvcf_tbi': gvcf_tbi_path,
                'vcf': vcf_path,
                'vcf_md5': vcf_md5_path,
                'vcf_tbi': vcf_tbi_path,
                'mapping_metrics': mapping_metrics_path,
                'coverage_metrics': coverage_metrics_path,
                'vc_metrics': vc_metrics_path
            }

            sequencing_data.append(row_data)

        # Define headers
        header_list = [
            'entity:sequencing_files_id', 'participant_id', 'sample_id',
            'cram', 'crai', 'cram_md5',
            'gvcf', 'gvcf_tbi',
            'vcf', 'vcf_md5', 'vcf_tbi',
            'mapping_metrics', 'coverage_metrics', 'vc_metrics'
        ]

        # Create TSV using Csv utility
        Csv(file_path=output_path, delimiter='\t').create_tsv_from_list_of_dicts(
            list_of_dicts=sequencing_data,
            header_list=header_list
        )

        logging.info(f"Created sequencing files TSV with {len(sequencing_data)} participants: {output_path}")
        return output_path

