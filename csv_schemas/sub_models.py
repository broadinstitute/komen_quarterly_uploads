"""
Pydantic models for sub_dataset CSV files.
These models are designed to validate data converted from CSV to list of dictionaries.
"""

from typing import Optional, Any
from pydantic import BaseModel, ConfigDict, model_validator, Field


class CsvModel(BaseModel):
    """
    Base model for all CSV row models.

    Validation rules:
    - Every field defined on the model must exist as a column in the CSV row,
      even if optional. A missing column fails validation; an empty value is fine.
    - Extra columns in the CSV (not defined in the model) also fail validation.
      Headers must match the model exactly.
    """
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def require_all_columns_present(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        missing = [field for field in cls.model_fields if field not in data]
        if missing:
            raise ValueError(f"Missing columns (present in model but not in CSV): {missing}")
        return data


class ResearcherProjectMetadata(CsvModel):
    """Model for researcher_id_62_project_id_115_metadata.csv"""
    researcher_id: Optional[str] = None
    project_id: Optional[str] = None
    project_name: Optional[str] = None
    date_created: Optional[str] = None
    dataset_version: Optional[str] = None
    project_size: Optional[str] = None
    project_filter: Optional[str] = None


class Demographics(CsvModel):
    """Model for demographics.csv"""
    patient_id: str
    birth_year: Optional[str] = None
    age_at_diagnosis: Optional[str] = None
    sex: Optional[str] = None
    race: Optional[str] = None
    ethnicity: Optional[str] = None
    state: Optional[str] = None
    employment_status: Optional[str] = None
    education_level: Optional[str] = None
    marital_status: Optional[str] = None
    pregnancy_cancer: Optional[str] = None
    menopause_yn: Optional[bool]
    menopause: Optional[int] = Field(None, ge=1000, le=9999)
    vital_status: Optional[str] = None
    death_date: Optional[int] = Field(None, ge=1000, le=9999)
    death_source: Optional[str] = None
    primary_residential_type: Optional[str] = None
    nic_status: Optional[str] = None
    drug_use: Optional[str] = None
    marijuana_use: Optional[str] = None
    alcohol_use: Optional[str] = None


class Biomarker(CsvModel):
    """Model for biomarker.csv"""
    patient_id: str
    biomarker_id: str
    biomarker_yn: Optional[bool]
    biomarker_test: Optional[str] = None
    biomarker_provenance: Optional[str] = None
    biomarker_lab: Optional[str] = None
    biomarker_tissue: Optional[str] = None
    biomarker_genomic: Optional[str] = None


class BiomarkerTestDetail(CsvModel):
    """Model for biomarker_test_detail.csv"""
    patient_id: str
    biomarker_id: str
    biomarker_detail_id: str
    biomarker_name: Optional[str] = None
    biomarker_result_date: Optional[int] = Field(None, ge=1000, le=9999)
    biomarker_result_status: Optional[str] = None
    biomarker_result_status_other: Optional[str] = None
    biomarker_result_units: Optional[str] = None
    biomarker_result_percentage: Optional[str] = None


class BiomarkersGenes(CsvModel):
    """Model for biomarkers_genes.csv"""
    patient_id: str
    biomarker_id: str
    biomarker_gene_id: str
    biomarker_gene: Optional[str] = None
    biomarker_gene_date: Optional[int] = Field(None, ge=1000, le=9999)
    biomarker_gene_finding: Optional[str] = None
    biomarker_gene_variant_type: Optional[str] = None


class DiseaseCharacteristic(CsvModel):
    """Model for disease_characteristic.csv"""
    patient_id: str
    disease_id: str
    histology_yn: Optional[bool]
    histology_initial_dx: Optional[str] = None
    stage_provenance_dx: Optional[str] = None
    t_stage_initial_dx: Optional[str] = None
    n_stage_initial_dx: Optional[str] = None
    m_stage_initial_dx: Optional[str] = None
    m_number_initial_dx: Optional[str] = None
    grade_initial_dx: Optional[str] = None
    group_stage_initial_dx: Optional[str] = None
    group_stage_dx_date: Optional[int] = Field(None, ge=1000, le=9999)
    progression_yn: Optional[bool]


class DiseaseCharacteristicsPrimarySite(CsvModel):
    """Model for disease_characteristics_primary_site.csv"""
    patient_id: str
    disease_id: str
    primary_site_id: str
    anatomic_site_loc: Optional[str] = None
    tumor_size_initial: Optional[str] = None
    tumor_size_initial_unit: Optional[str] = None
    multifocal_initial: Optional[str] = None


class DiseaseCharacteristicsProgression(CsvModel):
    """Model for disease_characteristics_progression.csv"""
    patient_id: str
    progression_id: str
    disease_id: str
    stage_provenance_progression_dx: Optional[str] = None
    t_stage_progression_dx: Optional[str] = None
    n_stage_progression_dx: Optional[str] = None
    m_stage_progression_dx: Optional[str] = None
    m_number_progression_dx: Optional[str] = None
    grade_progression_dx: Optional[str] = None
    group_stage_progression: Optional[str] = None
    group_stage_progression_date: Optional[str] = None = Field(None, ge=1000, le=9999)


class DiseaseCharacteristicsProgressionSite(CsvModel):
    """Model for disease_characteristics_progression_site.csv"""
    patient_id: str
    progression_id: str
    progression_site_id: str
    progression_site_yn: Optional[bool]
    anatomic_site_progression: Optional[str] = None
    tumor_size_progression: Optional[str] = None
    tumor_size_progression_unit: Optional[str] = None
    multifocal_progression: Optional[str] = None


class FamilyHistoryBiologicalFather(CsvModel):
    """Model for family_history_biological_father.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    was_father_adopted: Optional[bool]
    was_father_twin_triplet: Optional[bool]
    father_ethnicity: Optional[str] = None
    father_age: Optional[str] = None
    father_death_age: Optional[str] = None
    father_diagnosed_cancer: Optional[bool]
    father_first_cancer_type: Optional[str] = None
    father_age_first_cancer_diagnosis: Optional[str] = None
    father_diagnosed_second_cancer: Optional[bool]
    father_second_cancer_type: Optional[str] = None
    father_age_second_cancer_diagnosis: Optional[str] = None


class FamilyHistoryBiologicalMother(CsvModel):
    """Model for family_history_biological_mother.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    was_mother_adopted: Optional[bool]
    was_mother_twin_triplet: Optional[bool]
    mother_ethnicity: Optional[str] = None
    mother_age: Optional[str] = None
    mother_death_age: Optional[str] = None
    mother_diagnosed_cancer: Optional[bool]
    mother_first_cancer_type: Optional[str] = None
    mother_age_first_cancer_diagnosis: Optional[str] = None
    mother_diagnosed_second_cancer: Optional[bool]
    mother_second_cancer_type: Optional[str] = None
    mother_age_second_cancer_diagnosis: Optional[str] = None


class FamilyHistoryBiologicalSiblings(CsvModel):
    """Model for family_history_biological_siblings.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    sibling_birth_gender: Optional[str] = None
    was_sibling_twin_triplet: Optional[bool]
    sibling_age: Optional[str] = None
    sibling_death_age: Optional[str] = None
    sibling_first_cancer_type: Optional[str] = None
    sibling_age_first_cancer_diagnosis: Optional[str] = None
    sibling_diagnosed_second_cancer: Optional[str] = None
    sibling_second_cancer_type: Optional[str] = None
    sibling_age_second_cancer_diagnosis: Optional[str] = None
    sibling_diagnosed_cancer: Optional[bool]


class FamilyHistoryBiologicalSiblingsIntro(CsvModel):
    """Model for family_history_biological_siblings_intro.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    is_patient_twin_triplet: Optional[bool]
    number_of_siblings: Optional[str] = None
    number_of_siblings_with_cancer: Optional[str] = None


class FamilyHistoryCancer(CsvModel):
    """Model for family_history_cancer.csv"""
    patient_id: str
    family_history_id: str
    cancer_history_yn: Optional[bool]
    cancer_type: Optional[str] = None
    relative: Optional[str] = None
    relative_other: Optional[str] = None


class FamilyHistoryOtherFamilyMembers(CsvModel):
    """Model for family_history_other_family_members.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    maternal_grandmother_diagnosed_cancer: Optional[bool]
    maternal_grandmother_cancer_diagnosis: Optional[str] = None
    maternal_grandmother_age_first_diagnosis: Optional[str] = None
    paternal_grandmother_diagnosed_cancer: Optional[bool]
    paternal_grandmother_cancer_diagnosis: Optional[str] = None
    paternal_grandmother_age_first_diagnosis: Optional[str] = None
    mother_number_biological_sisters: Optional[str] = None
    mother_number_biological_sisters_cancer: Optional[str] = None
    father_number_biological_sisters: Optional[str] = None
    father_number_biological_sisters_cancer: Optional[str] = None
    number_half_siblings: Optional[str] = None
    number_half_siblings_cancer: Optional[str] = None
    number_of_children: Optional[str] = None
    number_of_children_cancer: Optional[str] = None
    number_grandfathers_uncles_had_cancer: Optional[str] = None


class FamilyHistoryOtherFamilyMembersRelatives(CsvModel):
    """Model for family_history_other_family_members_relatives.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    cancer_diagnosis: Optional[bool]
    age_first_diagnosis: Optional[str] = None
    relative: Optional[str] = None
    relative_type: Optional[str] = None
    birth_sex: Optional[str] = None


class FamilyHistoryYou(CsvModel):
    """Model for family_history_you.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    received_genetic_counseling: Optional[bool]
    has_genetic_test: Optional[bool]
    genetic_test_indicates_mutation: Optional[bool]
    family_has_ashkenazi_ancestry: Optional[bool]
    is_adopted: Optional[bool]
    family_members_with_history_answers: Optional[str] = None


class Imaging(CsvModel):
    """Model for imaging.csv"""
    patient_id: str
    imaging_id: str
    imaging_yn: Optional[bool]
    imaging_perform_date: Optional[int] = Field(None, ge=1000, le=9999)
    imaging_interpret_date: Optional[int] = Field(None, ge=1000, le=9999)
    imaging_finding: Optional[str] = None
    imaging_finding_detail: Optional[str] = None
    imaging_score: Optional[str] = None
    imaging_score_system: Optional[str] = None
    imaging_type: Optional[str] = None
    imaging_breast_density: Optional[str] = None
    imaging_body_part: Optional[str] = None
    imaging_body_part_side: Optional[str] = None
    imaging_photos_yn: Optional[str] = None


class Lab(CsvModel):
    """Model for lab.csv"""
    patient_id: str
    lab_id: str
    labs_yn: Optional[bool]
    lab_test_name: Optional[str] = None
    lab_result: Optional[str] = None
    lab_result_unit: Optional[str] = None
    lab_collect_date: Optional[int] = Field(None, ge=1000, le=9999)
    lab_result_date: Optional[int] = Field(None, ge=1000, le=9999)
    lab_specimen_type: Optional[str] = None


class MedList(CsvModel):
    """Model for med_list.csv"""
    patient_id: str
    med_id: str
    med_yn: Optional[bool]
    med_name: Optional[str] = None
    med_start_date: Optional[int] = Field(None, ge=1000, le=9999)
    med_end_yn: Optional[str] = None
    med_end_date: Optional[int] = Field(None, ge=1000, le=9999)


class PatientEnrollmentStatus(CsvModel):
    """Model for patient_enrollment_status.csv"""
    patient_id: str
    role_user_status: Optional[str] = None
    step: Optional[str] = None


class PatientProfileCancerInfo(CsvModel):
    """Model for patient_profile_cancer_info.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    cancer_type: Optional[str] = None
    cancer_subtype: Optional[str] = None
    cancer_stage: Optional[str] = None
    cancer_recurrence: Optional[str] = None
    cancer_recurrence_year: Optional[str] = None
    living_with_mbc: Optional[str] = None
    living_with_mbc_year: Optional[str] = None


class PatientProfileContactInfo(CsvModel):
    """Model for patient_profile_contact_info.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    mailing_country: Optional[str] = None
    mailing_state: Optional[str] = None


class PatientProfileEligibility(CsvModel):
    """Model for patient_profile_eligibility.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    date_of_birth: Optional[int] = Field(None, ge=1000, le=9999)
    year_of_first_breast_cancer_diagnosis: Optional[str] = None


class PatientProfileMoreAboutYou(CsvModel):
    """Model for patient_profile_more_about_you.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    gender: Optional[str] = None
    sex_assigned_at_birth: Optional[str] = None
    sexual_orientation: Optional[str] = None
    ethnicity: Optional[str] = None
    country_of_birth: Optional[str] = None
    state_of_birth: Optional[str] = None
    years_of_education_completed: Optional[str] = None


class PatientProfileProviderInfo(CsvModel):
    """Model for patient_profile_provider_info.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    breast_cancer_care_state: Optional[str] = None
    breast_cancer_care_currently_on_treatment: Optional[str] = None
    breast_cancer_care_start_date: Optional[int] = Field(None, ge=1000, le=9999)
    breast_cancer_care_end_date: Optional[int] = Field(None, ge=1000, le=9999)
    has_genetic_test: Optional[str] = None
    genetic_test: Optional[str] = None


class PatientProfileSupplementalAboutYou(CsvModel):
    """Model for patient_profile_supplemental_about_you.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    gender: Optional[str] = None
    sex_assigned_at_birth: Optional[str] = None
    sexual_orientation: Optional[str] = None


class Payor(CsvModel):
    """Model for payor.csv"""
    patient_id: str
    payor_id: str
    payor_yn: Optional[bool]
    payor: Optional[str] = None
    insurance_type: Optional[str] = None
    payor_effective_date: Optional[int] = Field(None, ge=1000, le=9999)
    payor_date: Optional[str] = None
    insurance_status: Optional[str] = None
    disenroll_date: Optional[int] = Field(None, ge=1000, le=9999)


class PerformanceScore(CsvModel):
    """Model for performance_score.csv"""
    patient_id: str
    ps_id: str
    ps_yn: Optional[bool]
    ps_date: Optional[int] = Field(None, ge=1000, le=9999)
    ps_type: Optional[str] = None
    ecog_score: Optional[str] = None
    karnofsky_score: Optional[str] = None


class Pro(CsvModel):
    """Model for pro.csv"""
    patient_id: str
    pro_id: str
    pro_yn: Optional[bool]
    pro_name: Optional[str] = None
    pro_topic: Optional[str] = None
    pro_result_quantitative: Optional[str] = None
    pro_result_qualitative: Optional[str] = None
    pro_date: Optional[int] = Field(None, ge=1000, le=9999)


class ProblemList(CsvModel):
    """Model for problem_list.csv"""
    patient_id: str
    problem_list_id: str
    problem_list_yn: Optional[bool]
    diagnosis: Optional[str] = None
    diagnosis_date: Optional[int] = Field(None, ge=1000, le=9999)
    diagnosis_documented_date: Optional[int] = Field(None, ge=1000, le=9999)


class Procedures(CsvModel):
    """Model for procedures.csv"""
    patient_id: str
    procedure_id: str
    procedure_yn: Optional[bool]
    procedure_provenance: Optional[str] = None
    procedure_name: Optional[str] = None
    procedure_location: Optional[str] = None
    tumor_dimension: Optional[str] = None
    tumor_dimension_unit: Optional[str] = None
    procedure_start_date: Optional[int] = Field(None, ge=1000, le=9999)
    procedure_stop_date: Optional[int] = Field(None, ge=1000, le=9999)


class QualityOfLifeGeneral(CsvModel):
    """Model for quality_of_life_general.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    currently_on_treatment: Optional[str] = None
    overall_quality_of_life_today: Optional[str] = None
    overall_quality_of_life_before: Optional[str] = None
    significant_events: Optional[str] = None


class QualityOfLifeMentalHealth(CsvModel):
    """Model for quality_of_life_mental_health.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    overall_mental_emotional_wellbeing_today: Optional[str] = None
    overall_mental_wellbeing_compared_to_pre_diagnosis: Optional[str] = None
    feeling_in_control: Optional[str] = None
    feeling_depressed: Optional[str] = None
    feeling_happy: Optional[str] = None
    feeling_hopeful: Optional[str] = None
    feeling_anxious: Optional[str] = None
    feeling_sad: Optional[str] = None
    stressed_or_overwhelmed: Optional[str] = None
    feeling_useful: Optional[str] = None
    trouble_communicating_feelings_to_others: Optional[str] = None
    trouble_with_mood_swings: Optional[str] = None
    trouble_coping_with_my_cancer: Optional[str] = None
    trouble_remembering_things: Optional[str] = None
    trouble_concentrating: Optional[str] = None
    future_holds: Optional[str] = None
    stress_on_breast_cancer: Optional[str] = None
    future_diagnostic_scans_tests: Optional[str] = None
    cancer_metastasis: Optional[str] = None
    family_members_getting_cancer: Optional[str] = None
    worries_death: Optional[str] = None
    self_conscious_appearance: Optional[str] = None
    unattractive: Optional[str] = None
    sexually_attractive: Optional[str] = None


class QualityOfLifePhysicalAbility(CsvModel):
    """Model for quality_of_life_physical_ability.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    everyday_routine_today: Optional[str] = None
    everyday_routine_compared_to_before: Optional[str] = None
    bothering_levels_pain: Optional[str] = None
    bothering_levels_fatigue: Optional[str] = None
    bothering_levels_lack_of_appetite: Optional[str] = None
    bothering_levels_joint_aches_or_stiffness: Optional[str] = None
    bothering_levels_breast_tenderness_or_pain: Optional[str] = None
    bothering_levels_swelling_or_pain_hands_arms: Optional[str] = None
    bothering_levels_shortness_of_breath: Optional[str] = None
    bothering_levels_hair_loss_or_thinning: Optional[str] = None
    bothering_levels_dizziness: Optional[str] = None
    bothering_levels_hot_flashes: Optional[str] = None
    bothering_levels_lack_of_quality_sleep: Optional[str] = None
    bothering_levels_hallucinations_or_nightmares: Optional[str] = None
    bothering_levels_pain_with_sexual_intercourse: Optional[str] = None
    bothering_levels_reduced_interest_in_sex: Optional[str] = None
    bothering_levels_vaginal_discharge: Optional[str] = None
    bothering_levels_nausea: Optional[str] = None
    bothering_levels_diarrhea: Optional[str] = None
    bothering_levels_frequent_urinary_tract_infections: Optional[str] = None
    bothering_levels_unintentional_weight_gain_or_loss: Optional[str] = None
    bothering_levels_headaches_or_migraines: Optional[str] = None
    bothering_levels_mood_swings: Optional[str] = None
    interference_daily_activity_pain: Optional[str] = None
    interference_daily_activity_fatigue: Optional[str] = None
    interference_daily_activity_lack_of_appetite: Optional[str] = None
    interference_daily_activity_joint_aches_or_stiffness: Optional[str] = None
    interference_daily_activity_breast_tenderness_or_pain: Optional[str] = None
    interference_daily_activity_swelling_or_pain_hands_arms: Optional[str] = None
    interference_daily_activity_shortness_of_breath: Optional[str] = None
    interference_daily_activity_hair_loss_or_thinning: Optional[str] = None
    interference_daily_activity_dizziness: Optional[str] = None
    interference_daily_activity_hot_flashes: Optional[str] = None
    interference_daily_activity_lack_of_quality_sleep: Optional[str] = None
    interference_daily_activity_hallucinations_or_nightmares: Optional[str] = None
    interference_daily_activity_pain_with_sexual_intercourse: Optional[str] = None
    interference_daily_activity_reduced_interest_in_sex: Optional[str] = None
    interference_daily_activity_vaginal_discharge: Optional[str] = None
    interference_daily_activity_nausea: Optional[str] = None
    interference_daily_activity_diarrhea: Optional[str] = None
    interference_daily_activity_urinary_tract_infections: Optional[str] = None
    interference_daily_activity_weight_gain_or_loss: Optional[str] = None
    interference_daily_activity_headaches_or_migraines: Optional[str] = None
    interference_daily_activity_mood_swings: Optional[str] = None


class QualityOfLifeSocialConnectedness(CsvModel):
    """Model for quality_of_life_social_connectedness.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    connected_to_others: Optional[str] = None
    connected_now_to_before_diagnosis: Optional[str] = None
    living_alone: Optional[str] = None
    number_people_living_in_household: Optional[str] = None
    caregivers: Optional[str] = None
    feeling_isolated: Optional[str] = None
    avoiding_social_gatherings: Optional[str] = None
    feeling_close_to_friends: Optional[str] = None
    feeling_close_to_primary_support_person: Optional[str] = None
    received_needed_support: Optional[str] = None
    trouble_meeting_family_needs: Optional[str] = None
    lacking_interest_in_sex: Optional[str] = None
    able_to_work: Optional[str] = None
    reluctant_to_start_relationships: Optional[str] = None


class QualityOfLifeSpirituality(CsvModel):
    """Model for quality_of_life_spirituality.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    religious_or_spiritual: Optional[str] = None
    importance_of_spiritual_activities: Optional[str] = None
    spirituality_changes_because_cancer_or_treatment: Optional[str] = None
    spirituality_helping_accept_cancer_or_treatment: Optional[str] = None
    last_month_sense_of_purpose: Optional[str] = None


class RadiationTherapy(CsvModel):
    """Model for radiation_therapy.csv"""
    patient_id: str
    radiation_therapy_id: str
    radiation_therapy_yn: Optional[str] = None
    radiation_clinical_trial_yn: Optional[str] = None
    radiation_energy: Optional[str] = None
    radiation_technique: Optional[str] = None
    radiation_anatomic_site: Optional[str] = None
    radiation_anatomic_site_side: Optional[str] = None
    radiation_total_dose_received_value: Optional[str] = None
    radiation_dose_received_unit: Optional[str] = None
    radiation_total_fractions_received: Optional[str] = None
    radiation_discontinuation: Optional[str] = None
    radiation_dc_reason: Optional[str] = None
    radiation_start_date: Optional[str] = None
    radiation_end_date: Optional[str] = None


class Regimen(CsvModel):
    """Model for regimen.csv"""
    patient_id: str
    regimen_id: str
    regimen_yn: Optional[str] = None
    regimen_clinical_trial_yn: Optional[str] = None
    regimen_name: Optional[str] = None
    regimen_drugs: Optional[str] = None
    regimen_route_of_administration: Optional[str] = None
    regimen_intent: Optional[str] = None
    regimen_start_date: Optional[str] = None
    regimen_end_date: Optional[str] = None
    regimen_discontinuation: Optional[str] = None
    regimen_dc_reason: Optional[str] = None


class SocialDeterminantSocialAndCommunityContext(CsvModel):
    """Model for social_determinant_social_and_community_context.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    impact_of_support_system_on_decisions: Optional[str] = None
    help_received_with_daily_activities_past_12_months: Optional[str] = None
    discussing_breast_cancer_with_others: Optional[str] = None
    participated_in_cancer_support_groups: Optional[str] = None
    felt_lonely_or_isolated_past_12_months: Optional[str] = None
    worried_about_self_or_family_past_12_months: Optional[str] = None
    felt_discriminated_against_in_daily_life: Optional[str] = None
    frequency_felt_discriminated_against_in_daily_life: Optional[str] = None
    area_of_experienced_discrimination_past_12_months: Optional[str] = None
    received_inferior_service: Optional[str] = None
    frequency_received_inferior_service: Optional[str] = None
    area_of_experienced_inferior_service: Optional[str] = None


class SocialDeterminantsEconomicStability(CsvModel):
    """Model for social_determinants_economic_stability.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    food_impact: Optional[str] = None
    clothing_impact: Optional[str] = None
    childcare_impact: Optional[str] = None
    housing_impact: Optional[str] = None
    internet_impact: Optional[str] = None
    phone_impact: Optional[str] = None
    transportation_impact: Optional[str] = None
    utilities_impact: Optional[str] = None
    medications_or_prescriptions_impact: Optional[str] = None
    health_care_impact: Optional[str] = None
    experienced_financial_situations: Optional[str] = None
    extent_of_impact_to_financial_stress: Optional[str] = None
    applied_for_financial_assistance: Optional[str] = None
    missed_scheduled_doctor_appointments: Optional[str] = None
    missed_medications_or_prescriptions: Optional[str] = None
    working_paid_position: Optional[str] = None
    type_of_employment: Optional[str] = None
    annual_household_income_before_taxes: Optional[str] = None
    lost_or_left_job: Optional[str] = None
    reason_for_leaving_or_losing_job: Optional[str] = None


class SocialDeterminantsEducationAccessAndQuality(CsvModel):
    """Model for social_determinants_education_access_and_quality.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    highest_level_of_school_completed: Optional[str] = None
    unable_to_finish_school_training: Optional[str] = None
    needed_help_understanding_diagnosis_or_treatment: Optional[str] = None
    received_help_understanding_diagnosis_or_treatment: Optional[str] = None
    freq_help_received_understanding_diagnosis_or_treatment: Optional[str] = None
    who_helped_understand_diagnosis_or_treatment: Optional[str] = None
    help_gained_regarding_diagnosis_or_treatment: Optional[str] = None


class SocialDeterminantsHealthCareAccessAndQuality(CsvModel):
    """Model for social_determinants_health_care_access_and_quality.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    past_12_months_access_to_medical_care: Optional[str] = None
    past_12_months_avoided_medical_care: Optional[str] = None
    past_12_months_experienced_delays_in_medical_care: Optional[str] = None
    past_12_months_discussed_with_health_care_team: Optional[str] = None
    help_from_counselor_nurse_patient_navigator: Optional[str] = None
    freq_of_help_from_counselor_nurse_patient_navigator: Optional[str] = None
    took_part_in_clinical_trials: Optional[str] = None
    did_not_participate_in_clinical_trials_reason: Optional[str] = None
    type_of_insurance_during_breast_cancer_diagnosis: Optional[str] = None
    time_with_no_health_insurance: Optional[str] = None
    total_amount_of_time_with_no_health_insurance: Optional[str] = None
    worked_to_keep_health_insurance: Optional[str] = None


class SocialDeterminantsNeighborhoodAndBuiltEnvironment(CsvModel):
    """Model for social_determinants_neighborhood_and_built_environment.csv"""
    patient_id: str
    task_id: Optional[str] = None
    task_version: Optional[str] = None
    patient_task_id: Optional[str] = None
    description_of_general_area_living_today: Optional[str] = None
    childcare_dependent_care: Optional[str] = None
    community_recreation_center: Optional[str] = None
    entertainment: Optional[str] = None
    hospital: Optional[str] = None
    primary_physicians_office: Optional[str] = None
    parks_or_green_spaces: Optional[str] = None
    pharmacy_or_drug_store: Optional[str] = None
    public_transportation: Optional[str] = None
    place_for_cancer_screenings: Optional[str] = None
    place_for_receiving_cancer_care: Optional[str] = None
    place_for_rehabilitation_or_physical_therapy: Optional[str] = None
    place_for_scans_or_other_testing_during_cancer_treatment: Optional[str] = None
    urgent_care: Optional[str] = None
    shops_markets_stores: Optional[str] = None
    living_situation: Optional[str] = None
    concerns_about_living_place: Optional[str] = None
    times_moved_from_one_home_to_another_past_12_months: Optional[str] = None
    transportation_used_for_medical_appointments: Optional[str] = None


class Symptom(CsvModel):
    """Model for symptom.csv"""
    patient_id: str
    visit_symptoms_id: str
    visit_id: str
    symptom_yn: Optional[str] = None
    symptom: Optional[str] = None
    symptom_start_date: Optional[str] = None
    symptom_end_date: Optional[str] = None


class Trial(CsvModel):
    """Model for trial.csv"""
    patient_id: str
    trial_id: str
    clinical_trial_yn: Optional[str] = None
    trial_code: Optional[str] = None
    trial_phase: Optional[str] = None
    trial_enroll_date: Optional[str] = None
    trial_complete_date: Optional[str] = None
    clinical_trial_outcome: Optional[str] = None


class TumorResponse(CsvModel):
    """Model for tumor_response.csv"""
    patient_id: str
    tumor_response_id: str
    tumor_response_yn: Optional[str] = None
    response_date: Optional[str] = None
    response_result: Optional[str] = None
    response_source_biopsy: Optional[str] = None
    response_source_imaging: Optional[str] = None
    response_source_provider: Optional[str] = None


class Visit(CsvModel):
    """Model for visit.csv"""
    patient_id: str
    visit_id: str
    visit_yn: Optional[str] = None
    visit_type: Optional[str] = None
    speciality_dept: Optional[str] = None
    presented_date: Optional[str] = None
    leave_date: Optional[str] = None
