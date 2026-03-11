"""CSV transformation and conversion utilities."""

import logging
import csv
from typing import Any, Optional
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
        return f"entity:{table_name}_table_id"

    def add_entity_id_column(self, csv_path: str, output_path: str, file_contents: list[dict]) -> None:
        """
        Add entity ID column to CSV file.

        The entity column will be named 'entity:{table_name}_id' and will contain
        row numbers starting from 1.

        Args:
            csv_path: Path to input CSV file
            output_path: Path to output CSV file with entity ID column
            file_contents: List of dictionaries containing all file contents for the CSV (used to determine headers and row count)

        Returns:
            True if successful, False otherwise
        """
        csv_filename = Path(csv_path).name
        entity_col_name = self.get_entity_column_name(csv_filename)

        fieldnames = [entity_col_name] + [header for header in file_contents[0].keys()]
        with open(output_path, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row_num, row in enumerate(file_contents, start=1):
                row[entity_col_name] = str(row_num)
                writer.writerow(row)

    @staticmethod
    def extract_all_participant_ids_from_files(
        file_contents_map: dict[str, list[dict[str, Any]]],
        patient_id_column: str = "patient_id"
    ) -> set[str]:
        """
        Extract all participant IDs from a list of CSV file paths.

        Args:
            file_contents_map: Dictionary mapping file paths to their contents (list of row dictionaries).
            patient_id_column: Name of the column containing patient IDs (default: 'patient_id')

        Returns:
            Set of all unique participant IDs found
        """
        all_participant_ids = set()

        # Process all CSV files
        for csv_file_path, file_contents in file_contents_map.items():
            # Extract participant IDs
            for row in file_contents:
                if patient_id := row.get(patient_id_column, "").strip():
                    all_participant_ids.add(patient_id)

        logging.info(f"Extracted {len(all_participant_ids)} unique participant IDs from {len(file_contents_map)} files")
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

        # Get headers from the first row or empty list
        header_list = []
        if csv_data:
            header_list = list(csv_data[0].keys())
        else:
            csv_data = []

        # Write as TSV with tab delimiter
        Csv(file_path=output_path, delimiter='\t', verbose=False).create_tsv_from_list_of_dicts(
            list_of_dicts=csv_data,
            header_list=header_list
        )

    def transform_and_convert_csv(self, csv_path: str, file_contents: list[dict], output_dir: str) -> str:
        """
        Transform CSV (add entity ID) and convert to TSV for Terra upload.

        Args:
            csv_path: Path to input CSV file
            output_dir: Directory to write output TSV
            file_contents: List of dictionaries containing all file contents for the CSV

        Returns:
            Path to output TSV file, or None if failed
        """
        csv_filename = Path(csv_path).name
        temp_csv_path = Path(output_dir) / f"temp_{csv_filename}"
        tsv_filename = csv_filename.replace(".csv", ".tsv")
        output_tsv_path = Path(output_dir) / tsv_filename

        # Add entity ID column
        self.add_entity_id_column(csv_path=csv_path, output_path=str(temp_csv_path), file_contents=file_contents)

        # Convert to TSV
        self.convert_csv_to_tsv(csv_path=str(temp_csv_path), output_path=str(output_tsv_path))

        # Clean up temp file
        temp_csv_path.unlink()
        return str(output_tsv_path)

    @staticmethod
    def create_sequencing_files_tsv(
        participant_files: dict[str, dict[str, Optional[str]]],
        output_path: str,
    ) -> str:
        """
        Create a TSV file listing sequencing files for participants.

        Args:
            participant_files: Dict returned by GenomicsFileChecker.check_all_participants().
                               Keys are participant_ids; values are dicts of
                               file_type -> full GCS path if the file exists, or None.
            output_path: Path to write output TSV.

        Returns:
            Path to created TSV file.
        """
        header_list = [
            "entity:sequencing_files_table_id",
            "participant_id",
            "cram",
            "crai",
            "cram_md5",
            "gvcf",
            "gvcf_tbi",
            "vcf",
            "vcf_md5",
            "vcf_tbi",
            "mapping_metrics",
            "coverage_metrics",
            "vc_metrics",
        ]

        sequencing_data = []
        for idx, (participant_id, files) in enumerate(sorted(participant_files.items()), start=1):
            row_data = {"entity:sequencing_files_table_id": str(idx), "participant_id": participant_id}
            for file_type in header_list[2:]:  # skip the two id columns
                # Use the file path if it exists, otherwise "NA"
                row_data[file_type] = files.get(file_type) or "NA"
            sequencing_data.append(row_data)

        Csv(file_path=output_path, delimiter='\t', verbose=False).create_tsv_from_list_of_dicts(
            list_of_dicts=sequencing_data,
            header_list=header_list
        )

        logging.info(f"Created sequencing files TSV with {len(sequencing_data)} participants: {output_path}")
        return output_path
