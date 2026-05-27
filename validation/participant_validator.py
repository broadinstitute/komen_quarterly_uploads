import logging
from pathlib import Path

from constants import SUB, MAIN, ALL
from models.data_models import DatasetInfo
from utilities import extract_all_participant_ids_from_files

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


class ParticipantValidation:
    """
    Validates participant status and consistency across main and sub datasets
    after a quarterly release upload.

    Checks performed (in order):
        1. All participants in each sub workspace are present in the main workspace.
        2. All participants (scoped by workspace_scope) are "active" and "enrolled"
           according to patient_enrollment_status.csv.

    If any check fails, later checks are skipped and run() returns False,
    preventing misleading results from downstream validations.
    """

    # Name of the CSV file that contains per-participant enrollment status.
    # This file lives in sub dataset directories and may also appear in the main dataset.
    ENROLLMENT_STATUS_FILE = "patient_enrollment_status.csv"

    # Expected field values that indicate a fully active, participating subject
    EXPECTED_STATUS = "active"
    EXPECTED_ENROLLMENT_STATUS = "enrolled"

    def __init__(self, dataset_info: DatasetInfo, workspace_scope: str) -> None:
        self.dataset_info = dataset_info
        self.workspace_scope = workspace_scope

    def _build_enrollment_status_map(
        self, file_contents_map: dict, workspace_name: str
    ) -> dict[str, dict[str, str]]:
        """
        Build a patient_id -> enrollment status mapping from a single workspace's
        file contents map (main or one sub dataset).  Only rows from that workspace's
        own patient_enrollment_status.csv are included, so each workspace is validated
        against its own copy of the file rather than a merged global map.
        """
        enrollment_map: dict[str, dict[str, str]] = {}

        for file_path, rows in file_contents_map.items():
            if Path(file_path).name == self.ENROLLMENT_STATUS_FILE:
                for row in rows:
                    patient_id = row.get("patient_id", "").strip()
                    if patient_id and patient_id not in enrollment_map:
                        enrollment_map[patient_id] = {
                            "role_user_status": row.get("role_user_status", "").strip().lower(),
                            "step": row.get("step", "").strip().lower(),
                        }
                break  # only one enrollment file per workspace

        logging.info(
            f"[{workspace_name}] Built enrollment status map for {len(enrollment_map)} participant(s) "
            f"from '{self.ENROLLMENT_STATUS_FILE}'"
        )
        return enrollment_map

    def _validate_participants_active_and_enrolled(
        self,
        participant_ids: set[str],
        enrollment_map: dict[str, dict[str, str]],
        workspace_name: str,
    ) -> bool:
        """
        Verify that every participant ID in participant_ids is active and enrolled.

        A participant fails if:
          - They are absent from enrollment_map entirely, OR
          - Their role_user_status != EXPECTED_STATUS, OR
          - Their step != EXPECTED_ENROLLMENT_STATUS

        """
        failed: list[str] = []

        for participant_id in sorted(participant_ids):
            if participant_id not in enrollment_map:
                # Participant present in a CSV but missing from the enrollment file entirely
                logging.error(
                    f"{workspace_name} participant '{participant_id}' not found in "
                    f"'{self.ENROLLMENT_STATUS_FILE}'"
                )
                failed.append(participant_id)
                continue

            status = enrollment_map[participant_id]
            # Headers in patient_enrollment_status.csv are "patient_id", "role_user_status" and "step"
            # "role_user_status" indicates if the participant's role is active, inactive, etc.
            # "step" indicates if the participant is enrolled or not
            role_status = status["role_user_status"]
            enrollment_status = status["step"]

            if role_status != self.EXPECTED_STATUS or enrollment_status != self.EXPECTED_ENROLLMENT_STATUS:
                logging.error(
                    f"{workspace_name} participant '{participant_id}' has unexpected enrollment status — "
                    f"role_user_status='{role_status}' (expected '{self.EXPECTED_STATUS}'), "
                    f"step='{enrollment_status}' (expected '{self.EXPECTED_ENROLLMENT_STATUS}')"
                )
                failed.append(participant_id)

        if failed:
            logging.error(f"{workspace_name}: {len(failed)} participant(s) failed enrollment validation")
            return False

        logging.info(
            f"{workspace_name}: All {len(participant_ids)} participant(s) are "
            f"'{self.EXPECTED_STATUS}' and '{self.EXPECTED_ENROLLMENT_STATUS}'"
        )
        return True

    def _validate_sub_participants_in_main(self, main_participants: set[str]) -> bool:
        """
        Confirm that every participant ID found in a sub dataset's files is also
        present in the main dataset.
        """
        all_valid = True

        for sub_dataset in self.dataset_info.sub_datasets:
            # Extract all participant IDs referenced in this sub dataset's CSV files
            sub_participants = extract_all_participant_ids_from_files(
                sub_dataset.file_contents_map
            )
            unknown = sub_participants - main_participants

            if unknown:
                for participant_id in sorted(unknown):
                    logging.error(
                        f"Participant '{participant_id}' found in sub workspace "
                        f"'{sub_dataset.workspace_name}' is not present in the main dataset"
                    )
                all_valid = False
            else:
                logging.info(
                    f"Sub workspace '{sub_dataset.workspace_name}': all "
                    f"{len(sub_participants)} participant(s) present in main dataset"
                )

        return all_valid

    def run(self) -> bool:
        """
        Execute all post-validation checks in sequence.

        Checks stop on the first failure to avoid surfacing misleading downstream results.
        Each workspace's participants are validated against that workspace's own copy of
        patient_enrollment_status.csv rather than a merged global map.
        """
        logging.info(f"Starting validation (workspace_scope='{self.workspace_scope}')")

        # Extract all unique participant IDs from the main dataset.
        main_participants = extract_all_participant_ids_from_files(
            self.dataset_info.main_file_contents_map
        )
        logging.info(f"Extracted {len(main_participants)} unique participant(s) from main dataset")

        # Verify that every sub workspace participant exists in the main dataset.
        if self.workspace_scope in (ALL, SUB):
            logging.info("Checking that all sub workspace participants are present in the main dataset")
            if not self._validate_sub_participants_in_main(main_participants):
                logging.error("Sub workspace participant check failed — skipping enrollment status check (contents match not performed)")
                return False

        # Validate main dataset participants against the main workspace's own enrollment file.
        if self.workspace_scope in (ALL, MAIN):
            enrollment_map = self._build_enrollment_status_map(
                file_contents_map=self.dataset_info.main_file_contents_map,
                workspace_name="Main",
            )
            if not enrollment_map:
                logging.error(f"No enrollment data found in main dataset '{self.ENROLLMENT_STATUS_FILE}' — cannot validate participant status")
                return False
            logging.info("Validating enrollment status for main dataset participants")
            if not self._validate_participants_active_and_enrolled(main_participants, enrollment_map, workspace_name="Main"):
                logging.error("Main dataset enrollment validation failed — contents match check not performed")
                return False

        # Validate each sub dataset's participants against that sub workspace's own enrollment file.
        if self.workspace_scope in (ALL, SUB):
            for sub_dataset in self.dataset_info.sub_datasets:
                sub_participants = extract_all_participant_ids_from_files(sub_dataset.file_contents_map)
                enrollment_map = self._build_enrollment_status_map(
                    file_contents_map=sub_dataset.file_contents_map,
                    workspace_name=sub_dataset.workspace_name,
                )
                if not enrollment_map:
                    logging.error(f"No enrollment data found in '{sub_dataset.workspace_name}' '{self.ENROLLMENT_STATUS_FILE}' — cannot validate participant status")
                    return False
                logging.info(f"Validating enrollment status for sub workspace '{sub_dataset.workspace_name}' ({len(sub_participants)} participant(s))")
                if not self._validate_participants_active_and_enrolled(sub_participants, enrollment_map, workspace_name=sub_dataset.workspace_name):
                    logging.error(f"Enrollment validation failed for sub workspace '{sub_dataset.workspace_name}' — contents match check not performed")
                    return False

        logging.info("All participant validation checks passed successfully")
        return True
