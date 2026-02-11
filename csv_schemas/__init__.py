"""
CSV Schema module for Komen Quarterly Uploads.

This module defines the expected CSV files for both main and sub datasets,
and provides utilities for generating dynamic filenames.
"""

# Common CSV files present in both main_dataset and sub_dataset
common_csvs = [
    "biomarker_test_detail.csv",
    "biomarker.csv",
    "biomarkers_genes.csv",
    "demographics.csv",
    "disease_characteristic.csv",
    "disease_characteristics_primary_site.csv",
    "disease_characteristics_progression_site.csv",
    "disease_characteristics_progression.csv",
    "family_history_biological_father.csv",
    "family_history_biological_mother.csv",
    "family_history_biological_siblings_intro.csv",
    "family_history_biological_siblings.csv",
    "family_history_cancer.csv",
    "family_history_other_family_members_relatives.csv",
    "family_history_other_family_members.csv",
    "family_history_you.csv",
    "imaging.csv",
    "lab.csv",
    "med_list.csv",
    "patient_enrollment_status.csv",
    "patient_profile_cancer_info.csv",
    "patient_profile_contact_info.csv",
    "patient_profile_eligibility.csv",
    "patient_profile_more_about_you.csv",
    "patient_profile_provider_info.csv",
    "patient_profile_supplemental_about_you.csv",
    "payor.csv",
    "performance_score.csv",
    "pro.csv",
    "problem_list.csv",
    "procedures.csv",
    "quality_of_life_general.csv",
    "quality_of_life_mental_health.csv",
    "quality_of_life_physical_ability.csv",
    "quality_of_life_social_connectedness.csv",
    "quality_of_life_spirituality.csv",
    "radiation_therapy.csv",
    "regimen.csv",
    "social_determinant_social_and_community_context.csv",
    "social_determinants_economic_stability.csv",
    "social_determinants_education_access_and_quality.csv",
    "social_determinants_health_care_access_and_quality.csv",
    "social_determinants_neighborhood_and_built_environment.csv",
    "symptom.csv",
    "trial.csv",
    "tumor_response.csv",
    "visit.csv",
]

# CSV files unique to main_dataset only
main_only_csvs = [
    "patient_id_map.csv",
]

# CSV files unique to sub_dataset only
sub_only_csvs = []

# Expected CSV files in main_dataset
main_csvs = common_csvs + main_only_csvs

# Expected CSV files in sub_dataset
sub_csvs = common_csvs + sub_only_csvs


def get_sub_list_with_research_metadata_file(researcher_id: int, project_id: int) -> list[str]:
    """
    Generate the sub list with the metadata filename for a given researcher and project ID included.

    Args:
        researcher_id: The researcher ID
        project_id: The project ID

    Returns:
        List of CSV filenames including the metadata file
    """
    metadata_filename = f"researcher_id_{researcher_id}_project_id_{project_id}_metadata.csv"
    return sub_csvs + [metadata_filename]
