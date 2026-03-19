"""CSV transformation and conversion utilities."""

import logging
import csv
from typing import Any, Optional
from pathlib import Path

from schema_helpers import CsvSchemaHelper
from validation.dataset_validator import DatasetValidator


class CSVTransformer:
    """Handles CSV transformations and adjustments before upload."""

    @staticmethod
    def _write_tsv_rows(output_path: str, fieldnames: list[str], rows: list[dict]) -> None:
        """Write TSV rows, quoting string values so Terra keeps text fields as text."""
        with open(output_path, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames=fieldnames,
                delimiter='\t',
                quoting=csv.QUOTE_NONNUMERIC,
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

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

    def write_tsv_with_entity_id_column(self, csv_path: str, output_path: str, file_contents: list[dict]) -> None:
        """Write a Terra-ready TSV from schema-normalized rows with the Terra entity id column."""
        csv_filename = Path(csv_path).name
        entity_col_name = self.get_entity_column_name(csv_filename)
        # Uploads should use the model-normalized values rather than the raw CSV text.
        # That keeps booleans / years / task_version aligned with the schema we validated against.
        normalized_rows = [
            DatasetValidator.build_upload_row(row=row, filename=csv_filename)
            for row in file_contents
        ]

        data_fieldnames = CsvSchemaHelper.get_expected_headers_for_csv_filename(csv_filename)
        if not data_fieldnames and normalized_rows:
            data_fieldnames = list(normalized_rows[0].keys())

        fieldnames = [entity_col_name] + data_fieldnames
        rows_to_write = [
            {entity_col_name: str(row_num), **row}
            for row_num, row in enumerate(normalized_rows, start=1)
        ]
        self._write_tsv_rows(output_path=output_path, fieldnames=fieldnames, rows=rows_to_write)

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

    def transform_and_convert_csv(self, csv_path: str, file_contents: list[dict], output_dir: str) -> str:
        """
        Transform CSV (add entity ID) and convert to TSV for Terra upload.

        Args:
            csv_path: Path to input CSV file
            output_dir: Directory to write output TSV
            file_contents: Already-loaded CSV rows. These stay in memory the whole time, so there is no
                           second parse step that could reinterpret values like "1.0" as a number.

        Returns:
            Path to output TSV file, or None if failed
        """
        csv_filename = Path(csv_path).name
        tsv_filename = csv_filename.replace(".csv", ".tsv")
        output_tsv_path = Path(output_dir) / tsv_filename

        self.write_tsv_with_entity_id_column(
            csv_path=csv_path,
            output_path=str(output_tsv_path),
            file_contents=file_contents,
        )
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

        CSVTransformer._write_tsv_rows(output_path=output_path, fieldnames=header_list, rows=sequencing_data)

        logging.info(f"Created sequencing files TSV with {len(sequencing_data)} participants: {output_path}")
        return output_path
