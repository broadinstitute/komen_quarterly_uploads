"""
Pydantic models for main_dataset CSV files.
These models inherit from sub_models and add main-specific models.
"""

from typing import Optional
from pydantic import BaseModel

# Import all models from sub_models
from .sub_models import (
    Demographics,
    Biomarker,
    BiomarkerTestDetail,
    BiomarkersGenes,
    DiseaseCharacteristic,
    DiseaseCharacteristicsPrimarySite,
    DiseaseCharacteristicsProgression,
    DiseaseCharacteristicsProgressionSite,
    FamilyHistoryBiologicalFather,
    FamilyHistoryBiologicalMother,
    FamilyHistoryBiologicalSiblings,
    FamilyHistoryBiologicalSiblingsIntro,
    FamilyHistoryCancer,
    FamilyHistoryOtherFamilyMembers,
    FamilyHistoryOtherFamilyMembersRelatives,
    FamilyHistoryYou,
    Imaging,
    Lab,
    MedList,
    PatientEnrollmentStatus,
    PatientProfileCancerInfo,
    PatientProfileContactInfo,
    PatientProfileEligibility,
    PatientProfileMoreAboutYou,
    PatientProfileProviderInfo,
    PatientProfileSupplementalAboutYou,
    Payor,
    PerformanceScore,
    Pro,
    ProblemList,
    Procedures,
    QualityOfLifeGeneral,
    QualityOfLifeMentalHealth,
    QualityOfLifePhysicalAbility,
    QualityOfLifeSocialConnectedness,
    QualityOfLifeSpirituality,
    RadiationTherapy,
    Regimen,
    SocialDeterminantSocialAndCommunityContext,
    SocialDeterminantsEconomicStability,
    SocialDeterminantsEducationAccessAndQuality,
    SocialDeterminantsHealthCareAccessAndQuality,
    SocialDeterminantsNeighborhoodAndBuiltEnvironment,
    Symptom,
    Trial,
    TumorResponse,
    Visit,
)


# Main-specific model (only in main_dataset)
class PatientIdMap(BaseModel):
    """Model for patient_id_map.csv - only in main_dataset"""
    patient_id: str
    hashed_patient_id: Optional[str] = None


# Re-export all models for convenience
__all__ = [
    # Main-specific
    "PatientIdMap",
    # From sub_models
    "Demographics",
    "Biomarker",
    "BiomarkerTestDetail",
    "BiomarkersGenes",
    "DiseaseCharacteristic",
    "DiseaseCharacteristicsPrimarySite",
    "DiseaseCharacteristicsProgression",
    "DiseaseCharacteristicsProgressionSite",
    "FamilyHistoryBiologicalFather",
    "FamilyHistoryBiologicalMother",
    "FamilyHistoryBiologicalSiblings",
    "FamilyHistoryBiologicalSiblingsIntro",
    "FamilyHistoryCancer",
    "FamilyHistoryOtherFamilyMembers",
    "FamilyHistoryOtherFamilyMembersRelatives",
    "FamilyHistoryYou",
    "Imaging",
    "Lab",
    "MedList",
    "PatientEnrollmentStatus",
    "PatientProfileCancerInfo",
    "PatientProfileContactInfo",
    "PatientProfileEligibility",
    "PatientProfileMoreAboutYou",
    "PatientProfileProviderInfo",
    "PatientProfileSupplementalAboutYou",
    "Payor",
    "PerformanceScore",
    "Pro",
    "ProblemList",
    "Procedures",
    "QualityOfLifeGeneral",
    "QualityOfLifeMentalHealth",
    "QualityOfLifePhysicalAbility",
    "QualityOfLifeSocialConnectedness",
    "QualityOfLifeSpirituality",
    "RadiationTherapy",
    "Regimen",
    "SocialDeterminantSocialAndCommunityContext",
    "SocialDeterminantsEconomicStability",
    "SocialDeterminantsEducationAccessAndQuality",
    "SocialDeterminantsHealthCareAccessAndQuality",
    "SocialDeterminantsNeighborhoodAndBuiltEnvironment",
    "Symptom",
    "Trial",
    "TumorResponse",
    "Visit",
]

