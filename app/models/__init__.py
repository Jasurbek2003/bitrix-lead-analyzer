"""
Data models and structures
"""

from .lead import Lead, LeadFilter, LeadBatch, LeadActivity, LeadContact
from .analysis_result import (
    LeadAnalysisResult, BatchAnalysisResult, TranscriptionResult,
    AIAnalysisResult, AnalysisAction, AnalysisReason
)

__all__ = [
    'Lead', 'LeadFilter', 'LeadBatch', 'LeadActivity', 'LeadContact',
    'LeadAnalysisResult', 'BatchAnalysisResult', 'TranscriptionResult',
    'AIAnalysisResult', 'AnalysisAction', 'AnalysisReason'
]