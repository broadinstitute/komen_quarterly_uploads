"""
CSV Schema module for Komen Quarterly Uploads.

This module defines the expected CSV files for both main and sub datasets,
and provides utilities for generating dynamic filenames.
"""

from pydantic import BaseModel

from csv_schemas.sub_models import (
    ResearcherProjectMetadata, Demographics, Biomarker, BiomarkerTestDetail,
    BiomarkersGenes, DiseaseCharacteristic, DiseaseCharacteristicsPrimarySite,
    DiseaseCharacteristicsProgression, DiseaseCharacteristicsProgressionSite,
    FamilyHistoryBiologicalFather, FamilyHistoryBiologicalMother,
    FamilyHistoryBiologicalSiblings, FamilyHistoryBiologicalSiblingsIntro,
    FamilyHistoryCancer, FamilyHistoryOtherFamilyMembers,
    FamilyHistoryOtherFamilyMembersRelatives, FamilyHistoryYou,
    Imaging, Lab, MedList, PatientEnrollmentStatus,
    PatientProfileCancerInfo, PatientProfileContactInfo, PatientProfileEligibility,
    PatientProfileMoreAboutYou, PatientProfileProviderInfo,
    PatientProfileSupplementalAboutYou, Payor, PerformanceScore, Pro,
    ProblemList, Procedures, QualityOfLifeGeneral, QualityOfLifeMentalHealth,
    QualityOfLifePhysicalAbility, QualityOfLifeSocialConnectedness,
    QualityOfLifeSpirituality, RadiationTherapy, Regimen,
    SocialDeterminantSocialAndCommunityContext, SocialDeterminantsEconomicStability,
    SocialDeterminantsEducationAccessAndQuality, SocialDeterminantsHealthCareAccessAndQuality,
    SocialDeterminantsNeighborhoodAndBuiltEnvironment,
    Symptom, Trial, TumorResponse, Visit,
)
from csv_schemas.main_models import PatientIdMap

# Maps each CSV filename to its pydantic model class.
# Used to validate the contents of each CSV against the expected schema.
CSV_SCHEMA_MAP: dict[str, type[BaseModel]] = {
    "biomarker_test_detail.csv": BiomarkerTestDetail,
    "biomarker.csv": Biomarker,
    "biomarkers_genes.csv": BiomarkersGenes,
    "demographics.csv": Demographics,
    "disease_characteristic.csv": DiseaseCharacteristic,
    "disease_characteristics_primary_site.csv": DiseaseCharacteristicsPrimarySite,
    "disease_characteristics_progression_site.csv": DiseaseCharacteristicsProgressionSite,
    "disease_characteristics_progression.csv": DiseaseCharacteristicsProgression,
    "family_history_biological_father.csv": FamilyHistoryBiologicalFather,
    "family_history_biological_mother.csv": FamilyHistoryBiologicalMother,
    "family_history_biological_siblings_intro.csv": FamilyHistoryBiologicalSiblingsIntro,
    "family_history_biological_siblings.csv": FamilyHistoryBiologicalSiblings,
    "family_history_cancer.csv": FamilyHistoryCancer,
    "family_history_other_family_members_relatives.csv": FamilyHistoryOtherFamilyMembersRelatives,
    "family_history_other_family_members.csv": FamilyHistoryOtherFamilyMembers,
    "family_history_you.csv": FamilyHistoryYou,
    "imaging.csv": Imaging,
    "lab.csv": Lab,
    "med_list.csv": MedList,
    "patient_enrollment_status.csv": PatientEnrollmentStatus,
    "patient_profile_cancer_info.csv": PatientProfileCancerInfo,
    "patient_profile_contact_info.csv": PatientProfileContactInfo,
    "patient_profile_eligibility.csv": PatientProfileEligibility,
    "patient_profile_more_about_you.csv": PatientProfileMoreAboutYou,
    "patient_profile_provider_info.csv": PatientProfileProviderInfo,
    "patient_profile_supplemental_about_you.csv": PatientProfileSupplementalAboutYou,
    "payor.csv": Payor,
    "performance_score.csv": PerformanceScore,
    "pro.csv": Pro,
    "problem_list.csv": ProblemList,
    "procedures.csv": Procedures,
    "quality_of_life_general.csv": QualityOfLifeGeneral,
    "quality_of_life_mental_health.csv": QualityOfLifeMentalHealth,
    "quality_of_life_physical_ability.csv": QualityOfLifePhysicalAbility,
    "quality_of_life_social_connectedness.csv": QualityOfLifeSocialConnectedness,
    "quality_of_life_spirituality.csv": QualityOfLifeSpirituality,
    "radiation_therapy.csv": RadiationTherapy,
    "regimen.csv": Regimen,
    "social_determinant_social_and_community_context.csv": SocialDeterminantSocialAndCommunityContext,
    "social_determinants_economic_stability.csv": SocialDeterminantsEconomicStability,
    "social_determinants_education_access_and_quality.csv": SocialDeterminantsEducationAccessAndQuality,
    "social_determinants_health_care_access_and_quality.csv": SocialDeterminantsHealthCareAccessAndQuality,
    "social_determinants_neighborhood_and_built_environment.csv": SocialDeterminantsNeighborhoodAndBuiltEnvironment,
    "symptom.csv": Symptom,
    "trial.csv": Trial,
    "tumor_response.csv": TumorResponse,
    "visit.csv": Visit,
    # Main-only
    "patient_id_map.csv": PatientIdMap,
}

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
MAIN_CSVS = common_csvs + main_only_csvs

# Expected CSV files in sub_dataset
SUB_CSVS = common_csvs + sub_only_csvs


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
    return SUB_CSVS + [metadata_filename]
