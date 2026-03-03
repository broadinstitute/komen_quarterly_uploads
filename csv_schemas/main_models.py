"""
Pydantic models for main_dataset CSV files.
These models inherit from sub_models and add main-specific models.
"""

from typing import Optional
from pydantic import BaseModel


# Main-specific model (only in main_dataset)
class PatientIdMap(BaseModel):
    """Model for patient_id_map.csv - only in main_dataset"""
    patient_id: str
    hashed_patient_id: Optional[str] = None
