"""CSV transformation and conversion utilities."""

import logging
import csv
from typing import Optional, Set, Dict
from pathlib import Path

from ops_utils.csv_util import Csv


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
        return f"entity:{table_name}_id"

    def add_entity_id_column(self, csv_path: str, output_path: str) -> bool:
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

        with open(csv_path, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            fieldnames = [entity_col_name] + list(reader.fieldnames or [])

            with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

                for row_num, row in enumerate(reader, start=1):
                    row[entity_col_name] = str(row_num)
                    writer.writerow(row)

        logging.info(f"Added entity ID column '{entity_col_name}' to {csv_filename}")
        return True

    def transform_csv_for_upload(self, csv_path: str, output_dir: str) -> Optional[str]:
        """
        Transform a CSV file for upload to Terra by adding entity ID column.

        Args:
            csv_path: Path to input CSV file
            output_dir: Directory to write transformed CSV

        Returns:
            Path to transformed CSV file, or None if failed
        """
        csv_filename = Path(csv_path).name
        output_path = Path(output_dir) / csv_filename

        if self.add_entity_id_column(csv_path, str(output_path)):
            return str(output_path)
        return None

    @staticmethod
    def extract_participant_ids(csv_path: str, patient_id_column: str = 'patient_id') -> Set[str]:
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
        csv_data = Csv(file_path=csv_path, delimiter=',').create_list_of_dicts_from_tsv()

        # Check if patient_id column exists
        if csv_data and patient_id_column not in csv_data[0]:
            logging.warning(f"Column '{patient_id_column}' not found in {csv_path}")
            return participant_ids

        # Extract participant IDs
        for row in csv_data:
            patient_id = row.get(patient_id_column, '').strip()
            if patient_id:
                participant_ids.add(patient_id)

        logging.info(f"Extracted {len(participant_ids)} participant IDs from {Path(csv_path).name}")
        return participant_ids

    def extract_all_participant_ids_from_directory(self, csv_directory: str) -> Set[str]:
        """
        Extract all participant IDs from all CSVs in a directory.

        Args:
            csv_directory: Directory containing CSV files

        Returns:
            Set of all unique participant IDs found
        """
        all_participant_ids = set()
        csv_dir_path = Path(csv_directory)

        if not csv_dir_path.exists():
            logging.error(f"Directory does not exist: {csv_directory}")
            return all_participant_ids

        # Process all CSV files in directory
        for csv_file in csv_dir_path.glob('*.csv'):
            participant_ids = self.extract_participant_ids(str(csv_file))
            all_participant_ids.update(participant_ids)

        logging.info(f"Total unique participant IDs in {csv_directory}: {len(all_participant_ids)}")
        return all_participant_ids

    def convert_csv_to_tsv(self, csv_path: str, output_path: str) -> bool:
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
        if csv_data:
            header_list = list(csv_data[0].keys())
        else:
            # If no data, just read headers from file
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                header_list = list(reader.fieldnames or [])
            csv_data = []

        # Write as TSV with tab delimiter
        Csv(file_path=output_path, delimiter='\t').create_tsv_from_list_of_dicts(
            list_of_dicts=csv_data,
            header_list=header_list
        )

        logging.info(f"Converted {Path(csv_path).name} to TSV: {Path(output_path).name}")
        return True

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
        if not self.add_entity_id_column(csv_path, str(temp_csv_path)):
            return None

        # Convert to TSV
        if not self.convert_csv_to_tsv(str(temp_csv_path), str(output_tsv_path)):
            return None

        # Clean up temp file
        temp_csv_path.unlink()

        return str(output_tsv_path)

    def create_sequencing_files_tsv(
        self,
        participants: Set[str],
        workspace_bucket: str,
        output_path: str,
        is_main: bool = False,
        source_workspaces: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Create a TSV file listing sequencing files for participants.

        Args:
            participants: Set of participant IDs
            workspace_bucket: Workspace bucket path (e.g., 'gs://fc-bucket-path/')
            output_path: Path to write output TSV
            is_main: If True, create master list pointing to sub workspaces
            source_workspaces: Dict mapping participant_id to workspace_name (for main only)

        Returns:
            Path to created TSV file
        """
        sequencing_data = []

        # Create rows for each participant
        for idx, participant_id in enumerate(sorted(participants), start=1):
            if is_main:
                # Main workspace: point to files in sub workspaces
                workspace_name = source_workspaces.get(participant_id, '') if source_workspaces else ''
                # Files live in sub workspace buckets
                sub_workspace_bucket = workspace_bucket  # This will be set properly in caller
                cram_path = f"{sub_workspace_bucket}cram/{participant_id}.cram"
                crai_path = f"{sub_workspace_bucket}cram/{participant_id}.cram.crai"
                gvcf_path = f"{sub_workspace_bucket}gvcf/{participant_id}.g.vcf.gz"
                sequencing_data.append({
                    'entity:sequencing_files_id': str(idx),
                    'participant_id': participant_id,
                    'workspace_name': workspace_name,
                    'cram': cram_path,
                    'crai': crai_path,
                    'gvcf': gvcf_path
                })
            else:
                # Sub workspace: files in this workspace's bucket
                cram_path = f"{workspace_bucket}cram/{participant_id}.cram"
                crai_path = f"{workspace_bucket}cram/{participant_id}.cram.crai"
                gvcf_path = f"{workspace_bucket}gvcf/{participant_id}.g.vcf.gz"
                sequencing_data.append({
                    'entity:sequencing_files_id': str(idx),
                    'participant_id': participant_id,
                    'cram': cram_path,
                    'crai': crai_path,
                    'gvcf': gvcf_path
                })

        # Define headers based on is_main
        if is_main:
            header_list = ['entity:sequencing_files_id', 'participant_id', 'workspace_name', 'cram', 'crai', 'gvcf']
        else:
            header_list = ['entity:sequencing_files_id', 'participant_id', 'cram', 'crai', 'gvcf']

        # Create TSV using Csv utility
        Csv(file_path=output_path, delimiter='\t').create_tsv_from_list_of_dicts(
            list_of_dicts=sequencing_data,
            header_list=header_list
        )

        logging.info(f"Created sequencing files TSV with {len(participants)} participants: {output_path}")
        return output_path

