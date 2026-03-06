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
    researcher_id: Optional[str] 
    project_id: Optional[str] 
    project_name: Optional[str] 
    date_created: Optional[str] 
    dataset_version: Optional[str] 
    project_size: Optional[str] 
    project_filter: Optional[str] 


class Demographics(CsvModel):
    """Model for demographics.csv"""
    patient_id: str
    birth_year: Optional[str]
    age_at_diagnosis: Optional[str]
    sex: Optional[str]
    race: Optional[str]
    ethnicity: Optional[str]
    state: Optional[str]
    employment_status: Optional[str]
    education_level: Optional[str]
    marital_status: Optional[str]
    pregnancy_cancer: Optional[str]
    menopause_yn: Optional[bool]
    menopause: Optional[int] = Field(None, ge=1000, le=9999)
    vital_status: Optional[str]
    death_date: Optional[int] = Field(None, ge=1000, le=9999)
    death_source: Optional[str]
    primary_residential_type: Optional[str]
    nic_status: Optional[str]
    drug_use: Optional[str]
    marijuana_use: Optional[str]
    alcohol_use: Optional[str]


class Biomarker(CsvModel):
    """Model for biomarker.csv"""
    patient_id: str
    biomarker_id: str
    biomarker_yn: Optional[bool]
    biomarker_test: Optional[str]
    biomarker_provenance: Optional[str]
    biomarker_lab: Optional[str]
    biomarker_tissue: Optional[str]
    biomarker_genomic: Optional[str]


class BiomarkerTestDetail(CsvModel):
    """Model for biomarker_test_detail.csv"""
    patient_id: str
    biomarker_id: str
    biomarker_detail_id: str
    biomarker_name: Optional[str]
    biomarker_result_date: Optional[int] = Field(None, ge=1000, le=9999)
    biomarker_result_status: Optional[str]
    biomarker_result_status_other: Optional[str]
    biomarker_result_units: Optional[str]
    biomarker_result_percentage: Optional[str]


class BiomarkersGenes(CsvModel):
    """Model for biomarkers_genes.csv"""
    patient_id: str
    biomarker_id: str
    biomarker_gene_id: str
    biomarker_gene: Optional[str]
    biomarker_gene_date: Optional[int] = Field(None, ge=1000, le=9999)
    biomarker_gene_finding: Optional[str]
    biomarker_gene_variant_type: Optional[str]


class DiseaseCharacteristic(CsvModel):
    """Model for disease_characteristic.csv"""
    patient_id: str
    disease_id: str
    histology_yn: Optional[bool]
    histology_initial_dx: Optional[str]
    stage_provenance_dx: Optional[str]
    t_stage_initial_dx: Optional[str]
    n_stage_initial_dx: Optional[str]
    m_stage_initial_dx: Optional[str]
    m_number_initial_dx: Optional[str]
    grade_initial_dx: Optional[str]
    group_stage_initial_dx: Optional[str]
    group_stage_dx_date: Optional[int] = Field(None, ge=1000, le=9999)
    progression_yn: Optional[bool]


class DiseaseCharacteristicsPrimarySite(CsvModel):
    """Model for disease_characteristics_primary_site.csv"""
    patient_id: str
    disease_id: str
    primary_site_id: str
    anatomic_site_loc: Optional[str]
    tumor_size_initial: Optional[str]
    tumor_size_initial_unit: Optional[str]
    multifocal_initial: Optional[str]


class DiseaseCharacteristicsProgression(CsvModel):
    """Model for disease_characteristics_progression.csv"""
    patient_id: str
    progression_id: str
    disease_id: str
    stage_provenance_progression_dx: Optional[str]
    t_stage_progression_dx: Optional[str] 
    n_stage_progression_dx: Optional[str] 
    m_stage_progression_dx: Optional[str] 
    m_number_progression_dx: Optional[str] 
    grade_progression_dx: Optional[str] 
    group_stage_progression: Optional[str] 
    group_stage_progression_date: Optional[str] = Field(None, ge=1000, le=9999)


class DiseaseCharacteristicsProgressionSite(CsvModel):
    """Model for disease_characteristics_progression_site.csv"""
    patient_id: str
    progression_id: str
    progression_site_id: str
    progression_site_yn: Optional[bool]
    anatomic_site_progression: Optional[str] 
    tumor_size_progression: Optional[str] 
    tumor_size_progression_unit: Optional[str] 
    multifocal_progression: Optional[str] 


class FamilyHistoryBiologicalFather(CsvModel):
    """Model for family_history_biological_father.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    was_father_adopted: Optional[bool]
    was_father_twin_triplet: Optional[bool]
    father_ethnicity: Optional[str] 
    father_age: Optional[str] 
    father_death_age: Optional[str] 
    father_diagnosed_cancer: Optional[bool]
    father_first_cancer_type: Optional[str] 
    father_age_first_cancer_diagnosis: Optional[str] 
    father_diagnosed_second_cancer: Optional[bool]
    father_second_cancer_type: Optional[str] 
    father_age_second_cancer_diagnosis: Optional[str] 


class FamilyHistoryBiologicalMother(CsvModel):
    """Model for family_history_biological_mother.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    was_mother_adopted: Optional[bool]
    was_mother_twin_triplet: Optional[bool]
    mother_ethnicity: Optional[str] 
    mother_age: Optional[str] 
    mother_death_age: Optional[str] 
    mother_diagnosed_cancer: Optional[bool]
    mother_first_cancer_type: Optional[str] 
    mother_age_first_cancer_diagnosis: Optional[str] 
    mother_diagnosed_second_cancer: Optional[bool]
    mother_second_cancer_type: Optional[str] 
    mother_age_second_cancer_diagnosis: Optional[str] 


class FamilyHistoryBiologicalSiblings(CsvModel):
    """Model for family_history_biological_siblings.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    sibling_birth_gender: Optional[str] 
    was_sibling_twin_triplet: Optional[str] 
    sibling_age: Optional[str] 
    sibling_death_age: Optional[str] 
    sibling_first_cancer_type: Optional[str] 
    sibling_age_first_cancer_diagnosis: Optional[str] 
    sibling_diagnosed_second_cancer: Optional[str] 
    sibling_second_cancer_type: Optional[str] 
    sibling_age_second_cancer_diagnosis: Optional[str] 


class FamilyHistoryBiologicalSiblingsIntro(CsvModel):
    """Model for family_history_biological_siblings_intro.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    is_patient_twin_triplet: Optional[str] 
    number_of_siblings: Optional[str] 
    number_of_siblings_with_cancer: Optional[str] 


class FamilyHistoryCancer(CsvModel):
    """Model for family_history_cancer.csv"""
    patient_id: str
    family_history_id: str
    cancer_history_yn: Optional[str] 
    cancer_type: Optional[str] 
    relative: Optional[str] 
    relative_other: Optional[str] 


class FamilyHistoryOtherFamilyMembers(CsvModel):
    """Model for family_history_other_family_members.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    maternal_grandmother_diagnosed_cancer: Optional[str] 
    maternal_grandmother_cancer_diagnosis: Optional[str] 
    maternal_grandmother_age_first_diagnosis: Optional[str] 
    paternal_grandmother_diagnosed_cancer: Optional[str] 
    paternal_grandmother_cancer_diagnosis: Optional[str] 
    paternal_grandmother_age_first_diagnosis: Optional[str] 
    mother_number_biological_sisters: Optional[str] 
    mother_number_biological_sisters_cancer: Optional[str] 
    father_number_biological_sisters: Optional[str] 
    father_number_biological_sisters_cancer: Optional[str] 
    number_half_siblings: Optional[str] 
    number_half_siblings_cancer: Optional[str] 
    number_of_children: Optional[str] 
    number_of_children_cancer: Optional[str] 
    number_grandfathers_uncles_had_cancer: Optional[str] 


class FamilyHistoryOtherFamilyMembersRelatives(CsvModel):
    """Model for family_history_other_family_members_relatives.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    cancer_diagnosis: Optional[str] 
    age_first_diagnosis: Optional[str] 
    relative: Optional[str] 
    relative_type: Optional[str] 
    birth_sex: Optional[str] 


class FamilyHistoryYou(CsvModel):
    """Model for family_history_you.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    received_genetic_counseling: Optional[str] 
    has_genetic_test: Optional[str] 
    genetic_test_indicates_mutation: Optional[str] 
    family_has_ashkenazi_ancestry: Optional[str] 
    is_adopted: Optional[str] 
    family_members_with_history_answers: Optional[str] 


class Imaging(CsvModel):
    """Model for imaging.csv"""
    patient_id: str
    imaging_id: str
    imaging_yn: Optional[str] 
    imaging_perform_date: Optional[str] 
    imaging_interpret_date: Optional[str] 
    imaging_finding: Optional[str] 
    imaging_finding_detail: Optional[str] 
    imaging_score: Optional[str] 
    imaging_score_system: Optional[str] 
    imaging_type: Optional[str] 
    imaging_breast_density: Optional[str] 
    imaging_body_part: Optional[str] 
    imaging_body_part_side: Optional[str] 
    imaging_photos_yn: Optional[str] 


class Lab(CsvModel):
    """Model for lab.csv"""
    patient_id: str
    lab_id: str
    labs_yn: Optional[str] 
    lab_test_name: Optional[str] 
    lab_result: Optional[str] 
    lab_result_unit: Optional[str] 
    lab_collect_date: Optional[str] 
    lab_result_date: Optional[str] 
    lab_specimen_type: Optional[str] 


class MedList(CsvModel):
    """Model for med_list.csv"""
    patient_id: str
    med_id: str
    med_yn: Optional[str] 
    med_name: Optional[str] 
    med_start_date: Optional[str] 
    med_end_yn: Optional[str] 
    med_end_date: Optional[str] 


class PatientEnrollmentStatus(CsvModel):
    """Model for patient_enrollment_status.csv"""
    patient_id: str
    role_user_status: Optional[str] 
    step: Optional[str] 


class PatientProfileCancerInfo(CsvModel):
    """Model for patient_profile_cancer_info.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    cancer_type: Optional[str] 
    cancer_subtype: Optional[str] 
    cancer_stage: Optional[str] 
    cancer_recurrence: Optional[str] 
    cancer_recurrence_year: Optional[str] 
    living_with_mbc: Optional[str] 
    living_with_mbc_year: Optional[str] 


class PatientProfileContactInfo(CsvModel):
    """Model for patient_profile_contact_info.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    mailing_country: Optional[str] 
    mailing_state: Optional[str] 


class PatientProfileEligibility(CsvModel):
    """Model for patient_profile_eligibility.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    date_of_birth: Optional[str] 
    year_of_first_breast_cancer_diagnosis: Optional[str] 


class PatientProfileMoreAboutYou(CsvModel):
    """Model for patient_profile_more_about_you.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    gender: Optional[str] 
    sex_assigned_at_birth: Optional[str] 
    sexual_orientation: Optional[str] 
    ethnicity: Optional[str] 
    country_of_birth: Optional[str] 
    state_of_birth: Optional[str] 
    years_of_education_completed: Optional[str] 


class PatientProfileProviderInfo(CsvModel):
    """Model for patient_profile_provider_info.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    breast_cancer_care_state: Optional[str] 
    breast_cancer_care_currently_on_treatment: Optional[str] 
    breast_cancer_care_start_date: Optional[str] 
    breast_cancer_care_end_date: Optional[str] 
    has_genetic_test: Optional[str] 
    genetic_test: Optional[str] 


class PatientProfileSupplementalAboutYou(CsvModel):
    """Model for patient_profile_supplemental_about_you.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    gender: Optional[str] 
    sex_assigned_at_birth: Optional[str] 
    sexual_orientation: Optional[str] 


class Payor(CsvModel):
    """Model for payor.csv"""
    patient_id: str
    payor_id: str
    payor_yn: Optional[str] 
    payor: Optional[str] 
    insurance_type: Optional[str] 
    payor_effective_date: Optional[str] 
    payor_date: Optional[str] 
    insurance_status: Optional[str] 
    disenroll_date: Optional[str] 


class PerformanceScore(CsvModel):
    """Model for performance_score.csv"""
    patient_id: str
    ps_id: str
    ps_yn: Optional[str] 
    ps_date: Optional[str] 
    ps_type: Optional[str] 
    ecog_score: Optional[str] 
    karnofsky_score: Optional[str] 


class Pro(CsvModel):
    """Model for pro.csv"""
    patient_id: str
    pro_id: str
    pro_yn: Optional[str] 
    pro_name: Optional[str] 
    pro_topic: Optional[str] 
    pro_result_quantitative: Optional[str] 
    pro_result_qualitative: Optional[str] 
    pro_date: Optional[str] 


class ProblemList(CsvModel):
    """Model for problem_list.csv"""
    patient_id: str
    problem_list_id: str
    problem_list_yn: Optional[str] 
    diagnosis: Optional[str] 
    diagnosis_date: Optional[str] 
    diagnosis_documented_date: Optional[str] 


class Procedures(CsvModel):
    """Model for procedures.csv"""
    patient_id: str
    procedure_id: str
    procedure_yn: Optional[str] 
    procedure_provenance: Optional[str] 
    procedure_name: Optional[str] 
    procedure_location: Optional[str] 
    tumor_dimension: Optional[str] 
    tumor_dimension_unit: Optional[str] 
    procedure_start_date: Optional[str] 
    procedure_stop_date: Optional[str] 


class QualityOfLifeGeneral(CsvModel):
    """Model for quality_of_life_general.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    currently_on_treatment: Optional[str] 
    overall_quality_of_life_today: Optional[str] 
    overall_quality_of_life_before: Optional[str] 
    significant_events: Optional[str] 


class QualityOfLifeMentalHealth(CsvModel):
    """Model for quality_of_life_mental_health.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    overall_mental_emotional_wellbeing_today: Optional[str] 
    overall_mental_wellbeing_compared_to_pre_diagnosis: Optional[str] 
    feeling_in_control: Optional[str] 
    feeling_depressed: Optional[str] 
    feeling_happy: Optional[str] 
    feeling_hopeful: Optional[str] 
    feeling_anxious: Optional[str] 
    feeling_sad: Optional[str] 
    stressed_or_overwhelmed: Optional[str] 
    feeling_useful: Optional[str] 
    trouble_communicating_feelings_to_others: Optional[str] 
    trouble_with_mood_swings: Optional[str] 
    trouble_coping_with_my_cancer: Optional[str] 
    trouble_remembering_things: Optional[str] 
    trouble_concentrating: Optional[str] 
    future_holds: Optional[str] 
    stress_on_breast_cancer: Optional[str] 
    future_diagnostic_scans_tests: Optional[str] 
    cancer_metastasis: Optional[str] 
    family_members_getting_cancer: Optional[str] 
    worries_death: Optional[str] 
    self_conscious_appearance: Optional[str] 
    unattractive: Optional[str] 
    sexually_attractive: Optional[str] 


class QualityOfLifePhysicalAbility(CsvModel):
    """Model for quality_of_life_physical_ability.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    everyday_routine_today: Optional[str] 
    everyday_routine_compared_to_before: Optional[str] 
    bothering_levels_pain: Optional[str] 
    bothering_levels_fatigue: Optional[str] 
    bothering_levels_lack_of_appetite: Optional[str] 
    bothering_levels_joint_aches_or_stiffness: Optional[str] 
    bothering_levels_breast_tenderness_or_pain: Optional[str] 
    bothering_levels_swelling_or_pain_hands_arms: Optional[str] 
    bothering_levels_shortness_of_breath: Optional[str] 
    bothering_levels_hair_loss_or_thinning: Optional[str] 
    bothering_levels_dizziness: Optional[str] 
    bothering_levels_hot_flashes: Optional[str] 
    bothering_levels_lack_of_quality_sleep: Optional[str] 
    bothering_levels_hallucinations_or_nightmares: Optional[str] 
    bothering_levels_pain_with_sexual_intercourse: Optional[str] 
    bothering_levels_reduced_interest_in_sex: Optional[str] 
    bothering_levels_vaginal_discharge: Optional[str] 
    bothering_levels_nausea: Optional[str] 
    bothering_levels_diarrhea: Optional[str] 
    bothering_levels_frequent_urinary_tract_infections: Optional[str] 
    bothering_levels_unintentional_weight_gain_or_loss: Optional[str] 
    bothering_levels_headaches_or_migraines: Optional[str] 
    bothering_levels_mood_swings: Optional[str] 
    interference_daily_activity_pain: Optional[str] 
    interference_daily_activity_fatigue: Optional[str] 
    interference_daily_activity_lack_of_appetite: Optional[str] 
    interference_daily_activity_joint_aches_or_stiffness: Optional[str] 
    interference_daily_activity_breast_tenderness_or_pain: Optional[str] 
    interference_daily_activity_swelling_or_pain_hands_arms: Optional[str] 
    interference_daily_activity_shortness_of_breath: Optional[str] 
    interference_daily_activity_hair_loss_or_thinning: Optional[str] 
    interference_daily_activity_dizziness: Optional[str] 
    interference_daily_activity_hot_flashes: Optional[str] 
    interference_daily_activity_lack_of_quality_sleep: Optional[str] 
    interference_daily_activity_hallucinations_or_nightmares: Optional[str] 
    interference_daily_activity_pain_with_sexual_intercourse: Optional[str] 
    interference_daily_activity_reduced_interest_in_sex: Optional[str] 
    interference_daily_activity_vaginal_discharge: Optional[str] 
    interference_daily_activity_nausea: Optional[str] 
    interference_daily_activity_diarrhea: Optional[str] 
    interference_daily_activity_urinary_tract_infections: Optional[str] 
    interference_daily_activity_weight_gain_or_loss: Optional[str] 
    interference_daily_activity_headaches_or_migraines: Optional[str] 
    interference_daily_activity_mood_swings: Optional[str] 


class QualityOfLifeSocialConnectedness(CsvModel):
    """Model for quality_of_life_social_connectedness.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    connected_to_others: Optional[str] 
    connected_now_to_before_diagnosis: Optional[str] 
    living_alone: Optional[str] 
    number_people_living_in_household: Optional[str] 
    caregivers: Optional[str] 
    feeling_isolated: Optional[str] 
    avoiding_social_gatherings: Optional[str] 
    feeling_close_to_friends: Optional[str] 
    feeling_close_to_primary_support_person: Optional[str] 
    received_needed_support: Optional[str] 
    trouble_meeting_family_needs: Optional[str] 
    lacking_interest_in_sex: Optional[str] 
    able_to_work: Optional[str] 
    reluctant_to_start_relationships: Optional[str] 


class QualityOfLifeSpirituality(CsvModel):
    """Model for quality_of_life_spirituality.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    religious_or_spiritual: Optional[str] 
    importance_of_spiritual_activities: Optional[str] 
    spirituality_changes_because_cancer_or_treatment: Optional[str] 
    spirituality_helping_accept_cancer_or_treatment: Optional[str] 
    last_month_sense_of_purpose: Optional[str] 


class RadiationTherapy(CsvModel):
    """Model for radiation_therapy.csv"""
    patient_id: str
    radiation_therapy_id: str
    radiation_therapy_yn: Optional[str] 
    radiation_clinical_trial_yn: Optional[str] 
    radiation_energy: Optional[str] 
    radiation_technique: Optional[str] 
    radiation_anatomic_site: Optional[str] 
    radiation_anatomic_site_side: Optional[str] 
    radiation_total_dose_received_value: Optional[str] 
    radiation_dose_received_unit: Optional[str] 
    radiation_total_fractions_received: Optional[str] 
    radiation_discontinuation: Optional[str] 
    radiation_dc_reason: Optional[str] 
    radiation_start_date: Optional[str] 
    radiation_end_date: Optional[str] 


class Regimen(CsvModel):
    """Model for regimen.csv"""
    patient_id: str
    regimen_id: str
    regimen_yn: Optional[str] 
    regimen_clinical_trial_yn: Optional[str] 
    regimen_name: Optional[str] 
    regimen_drugs: Optional[str] 
    regimen_route_of_administration: Optional[str] 
    regimen_intent: Optional[str] 
    regimen_start_date: Optional[str] 
    regimen_end_date: Optional[str] 
    regimen_discontinuation: Optional[str] 
    regimen_dc_reason: Optional[str] 


class SocialDeterminantSocialAndCommunityContext(CsvModel):
    """Model for social_determinant_social_and_community_context.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    impact_of_support_system_on_decisions: Optional[str] 
    help_received_with_daily_activities_past_12_months: Optional[str] 
    discussing_breast_cancer_with_others: Optional[str] 
    participated_in_cancer_support_groups: Optional[str] 
    felt_lonely_or_isolated_past_12_months: Optional[str] 
    worried_about_self_or_family_past_12_months: Optional[str] 
    felt_discriminated_against_in_daily_life: Optional[str] 
    frequency_felt_discriminated_against_in_daily_life: Optional[str] 
    area_of_experienced_discrimination_past_12_months: Optional[str] 
    received_inferior_service: Optional[str] 
    frequency_received_inferior_service: Optional[str] 
    area_of_experienced_inferior_service: Optional[str] 


class SocialDeterminantsEconomicStability(CsvModel):
    """Model for social_determinants_economic_stability.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    food_impact: Optional[str] 
    clothing_impact: Optional[str] 
    childcare_impact: Optional[str] 
    housing_impact: Optional[str] 
    internet_impact: Optional[str] 
    phone_impact: Optional[str] 
    transportation_impact: Optional[str] 
    utilities_impact: Optional[str] 
    medications_or_prescriptions_impact: Optional[str] 
    health_care_impact: Optional[str] 
    experienced_financial_situations: Optional[str] 
    extent_of_impact_to_financial_stress: Optional[str] 
    applied_for_financial_assistance: Optional[str] 
    missed_scheduled_doctor_appointments: Optional[str] 
    missed_medications_or_prescriptions: Optional[str] 
    working_paid_position: Optional[str] 
    type_of_employment: Optional[str] 
    annual_household_income_before_taxes: Optional[str] 
    lost_or_left_job: Optional[str] 
    reason_for_leaving_or_losing_job: Optional[str] 


class SocialDeterminantsEducationAccessAndQuality(CsvModel):
    """Model for social_determinants_education_access_and_quality.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    highest_level_of_school_completed: Optional[str] 
    unable_to_finish_school_training: Optional[str] 
    needed_help_understanding_diagnosis_or_treatment: Optional[str] 
    received_help_understanding_diagnosis_or_treatment: Optional[str] 
    freq_help_received_understanding_diagnosis_or_treatment: Optional[str] 
    who_helped_understand_diagnosis_or_treatment: Optional[str] 
    help_gained_regarding_diagnosis_or_treatment: Optional[str] 


class SocialDeterminantsHealthCareAccessAndQuality(CsvModel):
    """Model for social_determinants_health_care_access_and_quality.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    past_12_months_access_to_medical_care: Optional[str] 
    past_12_months_avoided_medical_care: Optional[str] 
    past_12_months_experienced_delays_in_medical_care: Optional[str] 
    past_12_months_discussed_with_health_care_team: Optional[str] 
    help_from_counselor_nurse_patient_navigator: Optional[str] 
    freq_of_help_from_counselor_nurse_patient_navigator: Optional[str] 
    took_part_in_clinical_trials: Optional[str] 
    did_not_participate_in_clinical_trials_reason: Optional[str] 
    type_of_insurance_during_breast_cancer_diagnosis: Optional[str] 
    time_with_no_health_insurance: Optional[str] 
    total_amount_of_time_with_no_health_insurance: Optional[str] 
    worked_to_keep_health_insurance: Optional[str] 


class SocialDeterminantsNeighborhoodAndBuiltEnvironment(CsvModel):
    """Model for social_determinants_neighborhood_and_built_environment.csv"""
    patient_id: str
    task_id: Optional[str] 
    task_version: Optional[str] 
    patient_task_id: Optional[str] 
    description_of_general_area_living_today: Optional[str] 
    childcare_dependent_care: Optional[str] 
    community_recreation_center: Optional[str] 
    entertainment: Optional[str] 
    hospital: Optional[str] 
    primary_physicians_office: Optional[str] 
    parks_or_green_spaces: Optional[str] 
    pharmacy_or_drug_store: Optional[str] 
    public_transportation: Optional[str] 
    place_for_cancer_screenings: Optional[str] 
    place_for_receiving_cancer_care: Optional[str] 
    place_for_rehabilitation_or_physical_therapy: Optional[str] 
    place_for_scans_or_other_testing_during_cancer_treatment: Optional[str] 
    urgent_care: Optional[str] 
    shops_markets_stores: Optional[str] 
    living_situation: Optional[str] 
    concerns_about_living_place: Optional[str] 
    times_moved_from_one_home_to_another_past_12_months: Optional[str] 
    transportation_used_for_medical_appointments: Optional[str] 


class Symptom(CsvModel):
    """Model for symptom.csv"""
    patient_id: str
    visit_symptoms_id: str
    visit_id: str
    symptom_yn: Optional[str] 
    symptom: Optional[str] 
    symptom_start_date: Optional[str] 
    symptom_end_date: Optional[str] 


class Trial(CsvModel):
    """Model for trial.csv"""
    patient_id: str
    trial_id: str
    clinical_trial_yn: Optional[str] 
    trial_code: Optional[str] 
    trial_phase: Optional[str] 
    trial_enroll_date: Optional[str] 
    trial_complete_date: Optional[str] 
    clinical_trial_outcome: Optional[str] 


class TumorResponse(CsvModel):
    """Model for tumor_response.csv"""
    patient_id: str
    tumor_response_id: str
    tumor_response_yn: Optional[str] 
    response_date: Optional[str] 
    response_result: Optional[str] 
    response_source_biopsy: Optional[str] 
    response_source_imaging: Optional[str] 
    response_source_provider: Optional[str] 


class Visit(CsvModel):
    """Model for visit.csv"""
    patient_id: str
    visit_id: str
    visit_yn: Optional[str] 
    visit_type: Optional[str] 
    speciality_dept: Optional[str] 
    presented_date: Optional[str] 
    leave_date: Optional[str] 
