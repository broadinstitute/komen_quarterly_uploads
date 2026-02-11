"""
CSV Schema module for Komen Quarterly Uploads.

This module defines the expected CSV files for both main and sub datasets,
and provides utilities for generating dynamic filenames.
"""

# Common CSV files present in both main_dataset and sub_dataset
# Key: CSV filename, Value: ID column name
common_csvs = {
    "biomarker_test_detail.csv": "", # Doesn't exist
    "biomarker.csv": "biomarker_id",
    "biomarkers_genes.csv": "", # Doesn't exist
    "demographics.csv": "", # Doesn't exist
    "disease_characteristic.csv": "", # Doesn't exist
    "disease_characteristics_primary_site.csv": "", # Doesn't exist
    "disease_characteristics_progression_site.csv": "", # Doesn't exist
    "disease_characteristics_progression.csv": "", # Doesn't exist
    "family_history_biological_father.csv": "", # Doesn't exist
    "family_history_biological_mother.csv": "", # Doesn't exist
    "family_history_biological_siblings_intro.csv": "", # Doesn't exist
    "family_history_biological_siblings.csv": "", # Doesn't exist
    "family_history_cancer.csv": "", # Doesn't exist
    "family_history_other_family_members_relatives.csv": "", # Doesn't exist
    "family_history_other_family_members.csv": "", # Doesn't exist
    "family_history_you.csv": "", # Doesn't exist
    "imaging.csv": "imaging_id",
    "lab.csv": "lab_id",
    "med_list.csv": "", # Doesn't exist
    "patient_enrollment_status.csv": "", # Doesn't exist
    "patient_profile_cancer_info.csv": "", # Doesn't exist
    "patient_profile_contact_info.csv": "", # Doesn't exist
    "patient_profile_eligibility.csv": "", # Doesn't exist
    "patient_profile_more_about_you.csv": "", # Doesn't exist
    "patient_profile_provider_info.csv": "task_id",
    "patient_profile_supplemental_about_you.csv": "task_id",
    "payor.csv": "payor_id",
    "performance_score.csv": "ps_id",
    "pro.csv": "pro_id",
    "problem_list.csv": "problem_list_id",
    "procedures.csv": "procedure_id",
    "quality_of_life_general.csv": "task_id",
    "quality_of_life_mental_health.csv": "task_id",
    "quality_of_life_physical_ability.csv": "task_id",
    "quality_of_life_social_connectedness.csv": "task_id",
    "quality_of_life_spirituality.csv": "task_id",
    "radiation_therapy.csv": "radiation_therapy_id",
    "regimen.csv": "regimen_id",
    "social_determinant_social_and_community_context.csv": "task_id",
    "social_determinants_economic_stability.csv": "task_id",
    "social_determinants_education_access_and_quality.csv": "task_id",
    "social_determinants_health_care_access_and_quality.csv": "task_id",
    "social_determinants_neighborhood_and_built_environment.csv": "task_id",
    "symptom.csv": "visit_symptoms_id",
    "trial.csv": "trial_id",
    "tumor_response.csv": "tumor_response_id",
    "visit.csv": "visit_id",
}

# CSV files unique to main_dataset only
# Key: CSV filename, Value: ID column name
main_only_csvs = {
    "patient_id_map.csv": "patient_id",
}

# CSV files unique to sub_dataset only
# Key: CSV filename, Value: ID column name
sub_only_csvs = {}

# Expected CSV files in main_dataset
main_csvs = {**common_csvs, **main_only_csvs}

# Expected CSV files in sub_dataset
sub_csvs = {**common_csvs, **sub_only_csvs}


def get_sub_dict_with_research_metadata_file(researcher_id: int, project_id: int) -> dict[str, str]:
    """
    Generate the sub dict with the metadata filename for a given researcher and project ID included.

    Args:
        researcher_id: The researcher ID
        project_id: The project ID

    Returns:
        Dictionary with CSV filenames as keys and ID column names as values
    """
    metadata_filename = f"researcher_id_{researcher_id}_project_id_{project_id}_metadata.csv"
    return {**sub_csvs, metadata_filename: "researcher_id"}
