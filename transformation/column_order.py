"""
Terra table column order configuration.

Defines the display order of columns for every table uploaded to Terra workspaces.
Column order matches the order found in the source CSVs.
"""

# Maps Terra table name -> {"shown": [...]}
# The "shown" list determines both which columns are visible and their left-to-right order.
TABLE_COLUMN_ORDER: dict[str, dict[str, list[str]]] = {
    "biomarker": {
        "shown": [
            "patient_id", "biomarker_id", "biomarker_yn", "biomarker_test",
            "biomarker_provenance", "biomarker_lab", "biomarker_tissue", "biomarker_genomic",
        ],
    },
    "biomarker_test_detail": {
        "shown": [
            "patient_id", "biomarker_id", "biomarker_detail_id", "biomarker_name",
            "biomarker_result_date", "biomarker_result_status", "biomarker_result_status_other",
            "biomarker_result_units", "biomarker_result_percentage",
        ],
    },
    "biomarkers_genes": {
        "shown": [
            "patient_id", "biomarker_gene_id", "biomarker_id", "biomarker_gene",
            "biomarker_gene_date", "biomarker_gene_finding", "biomarker_gene_variant_type",
        ],
    },
    "demographics": {
        "shown": [
            "patient_id", "birth_year", "age_at_diagnosis", "sex", "race", "ethnicity",
            "state", "employment_status", "education_level", "marital_status",
            "pregnancy_cancer", "menopause_yn", "menopause", "vital_status", "death_date",
            "death_source", "primary_residential_type", "nic_status", "drug_use",
            "marijuana_use", "alcohol_use",
        ],
    },
    "disease_characteristic": {
        "shown": [
            "patient_id", "disease_id", "histology_yn", "histology_initial_dx",
            "stage_provenance_dx", "t_stage_initial_dx", "n_stage_initial_dx",
            "m_stage_initial_dx", "m_number_initial_dx", "grade_initial_dx",
            "group_stage_initial_dx", "group_stage_dx_date", "progression_yn",
        ],
    },
    "disease_characteristics_primary_site": {
        "shown": [
            "patient_id", "disease_id", "primary_site_id", "anatomic_site_loc",
            "tumor_size_initial", "tumor_size_initial_unit", "multifocal_initial",
        ],
    },
    "disease_characteristics_progression": {
        "shown": [
            "patient_id", "progression_id", "disease_id", "stage_provenance_progression_dx",
            "t_stage_progression_dx", "n_stage_progression_dx", "m_stage_progression_dx",
            "m_number_progression_dx", "grade_progression_dx", "group_stage_progression",
            "group_stage_progression_date",
        ],
    },
    "disease_characteristics_progression_site": {
        "shown": [
            "patient_id", "progression_id", "progression_site_id", "progression_site_yn",
            "anatomic_site_progression", "tumor_size_progression", "tumor_size_progression_unit",
            "multifocal_progression",
        ],
    },
    "family_history_biological_father": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "was_father_adopted", "was_father_twin_triplet", "father_ethnicity",
            "father_age", "father_death_age", "father_diagnosed_cancer",
            "father_first_cancer_type", "father_age_first_cancer_diagnosis",
            "father_diagnosed_second_cancer", "father_second_cancer_type",
            "father_age_second_cancer_diagnosis",
        ],
    },
    "family_history_biological_mother": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "was_mother_adopted", "was_mother_twin_triplet", "mother_ethnicity",
            "mother_age", "mother_death_age", "mother_diagnosed_cancer",
            "mother_first_cancer_type", "mother_age_first_cancer_diagnosis",
            "mother_diagnosed_second_cancer", "mother_second_cancer_type",
            "mother_age_second_cancer_diagnosis",
        ],
    },
    "family_history_biological_siblings": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "sibling_birth_gender", "was_sibling_twin_triplet", "sibling_age",
            "sibling_death_age", "sibling_first_cancer_type",
            "sibling_age_first_cancer_diagnosis", "sibling_diagnosed_second_cancer",
            "sibling_second_cancer_type", "sibling_age_second_cancer_diagnosis",
        ],
    },
    "family_history_biological_siblings_intro": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "is_patient_twin_triplet", "number_of_siblings", "number_of_siblings_with_cancer",
        ],
    },
    "family_history_cancer": {
        "shown": [
            "patient_id", "family_history_id", "cancer_history_yn", "cancer_type",
            "relative", "relative_other",
        ],
    },
    "family_history_other_family_members": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "maternal_grandmother_diagnosed_cancer", "maternal_grandmother_cancer_diagnosis",
            "maternal_grandmother_age_first_diagnosis", "paternal_grandmother_diagnosed_cancer",
            "paternal_grandmother_cancer_diagnosis", "paternal_grandmother_age_first_diagnosis",
            "mother_number_biological_sisters", "mother_number_biological_sisters_cancer",
            "father_number_biological_sisters", "father_number_biological_sisters_cancer",
            "number_half_siblings", "number_half_siblings_cancer", "number_of_children",
            "number_of_children_cancer", "number_grandfathers_uncles_had_cancer",
        ],
    },
    "family_history_other_family_members_relatives": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "cancer_diagnosis", "age_first_diagnosis", "relative", "relative_type", "birth_sex",
        ],
    },
    "family_history_you": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "received_genetic_counseling", "has_genetic_test", "genetic_test_indicates_mutation",
            "family_has_ashkenazi_ancestry", "is_adopted", "family_members_with_history_answers",
        ],
    },
    "imaging": {
        "shown": [
            "patient_id", "imaging_id", "imaging_yn", "imaging_perform_date",
            "imaging_interpret_date", "imaging_finding", "imaging_finding_detail",
            "imaging_score", "imaging_score_system", "imaging_type", "imaging_breast_density",
            "imaging_body_part", "imaging_body_part_side", "imaging_photos_yn",
        ],
    },
    "lab": {
        "shown": [
            "patient_id", "lab_id", "labs_yn", "lab_test_name", "lab_result",
            "lab_result_unit", "lab_collect_date", "lab_result_date", "lab_specimen_type",
        ],
    },
    "med_list": {
        "shown": [
            "patient_id", "med_id", "med_yn", "med_name", "med_start_date",
            "med_end_yn", "med_end_date",
        ],
    },
    "patient_enrollment_status": {
        "shown": ["patient_id", "role_user_status", "step"],
    },
    "patient_profile_cancer_info": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "cancer_type", "cancer_subtype", "cancer_stage", "cancer_recurrence",
            "cancer_recurrence_year", "living_with_mbc", "living_with_mbc_year",
        ],
    },
    "patient_profile_contact_info": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "mailing_country", "mailing_state",
        ],
    },
    "patient_profile_eligibility": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "date_of_birth", "year_of_first_breast_cancer_diagnosis",
        ],
    },
    "patient_profile_more_about_you": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "gender", "sex_assigned_at_birth", "sexual_orientation", "ethnicity",
            "country_of_birth", "state_of_birth", "years_of_education_completed",
        ],
    },
    "patient_profile_provider_info": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "breast_cancer_care_state", "breast_cancer_care_currently_on_treatment",
            "breast_cancer_care_start_date", "breast_cancer_care_end_date",
            "has_genetic_test", "genetic_test",
        ],
    },
    "patient_profile_supplemental_about_you": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "gender", "sex_assigned_at_birth", "sexual_orientation",
        ],
    },
    "payor": {
        "shown": [
            "patient_id", "payor_id", "payor_yn", "payor", "insurance_type",
            "payor_effective_date", "payor_date", "insurance_status", "disenroll_date",
        ],
    },
    "performance_score": {
        "shown": [
            "patient_id", "ps_id", "ps_yn", "ps_date", "ps_type",
            "ecog_score", "karnofsky_score",
        ],
    },
    "pro": {
        "shown": [
            "patient_id", "pro_id", "pro_yn", "pro_name", "pro_topic",
            "pro_result_quantitative", "pro_result_qualitative", "pro_date",
        ],
    },
    "problem_list": {
        "shown": [
            "patient_id", "problem_list_id", "problem_list_yn", "diagnosis",
            "diagnosis_date", "diagnosis_documented_date",
        ],
    },
    "procedures": {
        "shown": [
            "patient_id", "procedure_id", "procedure_yn", "procedure_provenance",
            "procedure_name", "procedure_location", "tumor_dimension", "tumor_dimension_unit",
            "procedure_start_date", "procedure_stop_date",
        ],
    },
    "quality_of_life_general": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "currently_on_treatment", "overall_quality_of_life_today",
            "overall_quality_of_life_before", "significant_events",
        ],
    },
    "quality_of_life_mental_health": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "overall_mental_emotional_wellbeing_today",
            "overall_mental_wellbeing_compared_to_pre_diagnosis", "feeling_in_control",
            "feeling_depressed", "feeling_happy", "feeling_hopeful", "feeling_anxious",
            "feeling_sad", "stressed_or_overwhelmed", "feeling_useful",
            "trouble_communicating_feelings_to_others", "trouble_with_mood_swings",
            "trouble_coping_with_my_cancer", "trouble_remembering_things",
            "trouble_concentrating", "future_holds", "stress_on_breast_cancer",
            "future_diagnostic_scans_tests", "cancer_metastasis",
            "family_members_getting_cancer", "worries_death", "self_conscious_appearance",
            "unattractive", "sexually_attractive",
        ],
    },
    "quality_of_life_physical_ability": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "everyday_routine_today", "everyday_routine_compared_to_before",
            "bothering_levels_pain", "bothering_levels_fatigue",
            "bothering_levels_lack_of_appetite", "bothering_levels_joint_aches_or_stiffness",
            "bothering_levels_breast_tenderness_or_pain",
            "bothering_levels_swelling_or_pain_hands_arms",
            "bothering_levels_shortness_of_breath", "bothering_levels_hair_loss_or_thinning",
            "bothering_levels_dizziness", "bothering_levels_hot_flashes",
            "bothering_levels_lack_of_quality_sleep",
            "bothering_levels_hallucinations_or_nightmares",
            "bothering_levels_pain_with_sexual_intercourse",
            "bothering_levels_reduced_interest_in_sex", "bothering_levels_vaginal_discharge",
            "bothering_levels_nausea", "bothering_levels_diarrhea",
            "bothering_levels_frequent_urinary_tract_infections",
            "bothering_levels_unintentional_weight_gain_or_loss",
            "bothering_levels_headaches_or_migraines", "bothering_levels_mood_swings",
            "interference_daily_activity_pain", "interference_daily_activity_fatigue",
            "interference_daily_activity_lack_of_appetite",
            "interference_daily_activity_joint_aches_or_stiffness",
            "interference_daily_activity_breast_tenderness_or_pain",
            "interference_daily_activity_swelling_or_pain_hands_arms",
            "interference_daily_activity_shortness_of_breath",
            "interference_daily_activity_hair_loss_or_thinning",
            "interference_daily_activity_dizziness", "interference_daily_activity_hot_flashes",
            "interference_daily_activity_lack_of_quality_sleep",
            "interference_daily_activity_hallucinations_or_nightmares",
            "interference_daily_activity_pain_with_sexual_intercourse",
            "interference_daily_activity_reduced_interest_in_sex",
            "interference_daily_activity_vaginal_discharge",
            "interference_daily_activity_nausea", "interference_daily_activity_diarrhea",
            "interference_daily_activity_urinary_tract_infections",
            "interference_daily_activity_weight_gain_or_loss",
            "interference_daily_activity_headaches_or_migraines",
            "interference_daily_activity_mood_swings",
        ],
    },
    "quality_of_life_social_connectedness": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "connected_to_others", "connected_now_to_before_diagnosis", "living_alone",
            "number_people_living_in_household", "caregivers", "feeling_isolated",
            "avoiding_social_gatherings", "feeling_close_to_friends",
            "feeling_close_to_primary_support_person", "received_needed_support",
            "trouble_meeting_family_needs", "lacking_interest_in_sex", "able_to_work",
            "reluctant_to_start_relationships",
        ],
    },
    "quality_of_life_spirituality": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "religious_or_spiritual", "importance_of_spiritual_activities",
            "spirituality_changes_because_cancer_or_treatment",
            "spirituality_helping_accept_cancer_or_treatment", "last_month_sense_of_purpose",
        ],
    },
    "radiation_therapy": {
        "shown": [
            "patient_id", "radiation_therapy_id", "radiation_therapy_yn",
            "radiation_clinical_trial_yn", "radiation_energy", "radiation_technique",
            "radiation_anatomic_site", "radiation_anatomic_site_side",
            "radiation_total_dose_received_value", "radiation_dose_received_unit",
            "radiation_total_fractions_received", "radiation_discontinuation",
            "radiation_dc_reason", "radiation_start_date", "radiation_end_date",
        ],
    },
    "regimen": {
        "shown": [
            "patient_id", "regimen_id", "regimen_yn", "regimen_clinical_trial_yn",
            "regimen_name", "regimen_drugs", "regimen_route_of_administration",
            "regimen_intent", "regimen_start_date", "regimen_end_date",
            "regimen_discontinuation", "regimen_dc_reason",
        ],
    },
    "social_determinant_social_and_community_context": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "impact_of_support_system_on_decisions",
            "help_received_with_daily_activities_past_12_months",
            "discussing_breast_cancer_with_others",
            "participated_in_cancer_support_groups",
            "felt_lonely_or_isolated_past_12_months",
            "worried_about_self_or_family_past_12_months",
            "felt_discriminated_against_in_daily_life",
            "frequency_felt_discriminated_against_in_daily_life",
            "area_of_experienced_discrimination_past_12_months",
            "received_inferior_service", "frequency_received_inferior_service",
            "area_of_experienced_inferior_service",
        ],
    },
    "social_determinants_economic_stability": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "food_impact", "clothing_impact", "childcare_impact", "housing_impact",
            "internet_impact", "phone_impact", "transportation_impact", "utilities_impact",
            "medications_or_prescriptions_impact", "health_care_impact",
            "experienced_financial_situations", "extent_of_impact_to_financial_stress",
            "applied_for_financial_assistance", "missed_scheduled_doctor_appointments",
            "missed_medications_or_prescriptions", "working_paid_position",
            "type_of_employment", "annual_household_income_before_taxes",
            "lost_or_left_job", "reason_for_leaving_or_losing_job",
        ],
    },
    "social_determinants_education_access_and_quality": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "highest_level_of_school_completed", "unable_to_finish_school_training",
            "needed_help_understanding_diagnosis_or_treatment",
            "received_help_understanding_diagnosis_or_treatment",
            "freq_help_received_understanding_diagnosis_or_treatment",
            "who_helped_understand_diagnosis_or_treatment",
            "help_gained_regarding_diagnosis_or_treatment",
        ],
    },
    "social_determinants_health_care_access_and_quality": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "past_12_months_access_to_medical_care", "past_12_months_avoided_medical_care",
            "past_12_months_experienced_delays_in_medical_care",
            "past_12_months_discussed_with_health_care_team",
            "help_from_counselor_nurse_patient_navigator",
            "freq_of_help_from_counselor_nurse_patient_navigator",
            "took_part_in_clinical_trials", "did_not_participate_in_clinical_trials_reason",
            "type_of_insurance_during_breast_cancer_diagnosis",
            "time_with_no_health_insurance", "total_amount_of_time_with_no_health_insurance",
            "worked_to_keep_health_insurance",
        ],
    },
    "social_determinants_neighborhood_and_built_environment": {
        "shown": [
            "patient_id", "task_id", "task_version", "patient_task_id",
            "description_of_general_area_living_today", "childcare_dependent_care",
            "community_recreation_center", "entertainment", "hospital",
            "primary_physicians_office", "parks_or_green_spaces", "pharmacy_or_drug_store",
            "public_transportation", "place_for_cancer_screenings",
            "place_for_receiving_cancer_care",
            "place_for_rehabilitation_or_physical_therapy",
            "place_for_scans_or_other_testing_during_cancer_treatment", "urgent_care",
            "shops_markets_stores", "living_situation", "concerns_about_living_place",
            "times_moved_from_one_home_to_another_past_12_months",
            "transportation_used_for_medical_appointments",
        ],
    },
    "symptom": {
        "shown": [
            "patient_id", "visit_symptoms_id", "visit_id", "symptom_yn",
            "symptom", "symptom_start_date", "symptom_end_date",
        ],
    },
    "trial": {
        "shown": [
            "patient_id", "trial_id", "clinical_trial_yn", "trial_code",
            "trial_phase", "trial_enroll_date", "trial_complete_date", "clinical_trial_outcome",
        ],
    },
    "tumor_response": {
        "shown": [
            "patient_id", "tumor_response_id", "tumor_response_yn", "response_date",
            "response_result", "response_source_biopsy", "response_source_imaging",
            "response_source_provider",
        ],
    },
    "visit": {
        "shown": [
            "patient_id", "visit_id", "visit_yn", "visit_type", "speciality_dept",
            "presented_date", "leave_date",
        ],
    },
    # Main-dataset only
    "patient_id_map": {
        "shown": ["patient_id", "hashed_patient_id"],
    },
    # Sequencing files table (generated, not sourced from a CSV)
    "sequencing_files": {
        "shown": [
            "participant_id", "cram", "crai", "cram_md5", "gvcf", "gvcf_tbi",
            "vcf", "vcf_md5", "vcf_tbi", "mapping_metrics", "coverage_metrics", "vc_metrics",
        ],
    },
    # Sub-dataset project metadata — dynamic table name (researcher_id_*_project_id_*_metadata).
    # Workspace upload logic matches the pattern and maps it to this entry at upload time.
    "researcher_project_metadata": {
        "shown": [
            "researcher_id", "project_id", "project_name", "date_created",
            "dataset_version", "project_size", "project_filter",
        ],
    },
}

