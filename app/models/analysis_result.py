"""
Analysis result models for Bitrix24 Lead Analyzer
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum

class AnalysisAction(Enum):
    """Analysis action enumeration"""
    KEEP_STATUS = "keep_status"
    CHANGE_STATUS = "change_status"
    SKIP = "skip"
    ERROR = "error"

class AnalysisReason(Enum):
    """Analysis reason enumeration"""
    SUFFICIENT_CALLS = "sufficient_calls"          # >= 5 unsuccessful calls
    INSUFFICIENT_CALLS = "insufficient_calls"     # < 5 unsuccessful calls
    AI_SUITABLE = "ai_suitable"                   # AI says status is suitable
    AI_NOT_SUITABLE = "ai_not_suitable"          # AI says status is not suitable
    NO_AUDIO_FILES = "no_audio_files"            # No audio files found
    NO_TRANSCRIPTION = "no_transcription"        # Transcription failed
    NOT_TARGET_STATUS = "not_target_status"      # Lead doesn't have target junk status
    API_ERROR = "api_error"                      # API call failed
    VALIDATION_ERROR = "validation_error"        # Data validation failed

@dataclass
class TranscriptionResult:
    """Transcription result"""
    audio_file: str
    transcription: str
    confidence: Optional[float] = None
    duration: Optional[float] = None
    language: Optional[str] = None
    error: Optional[str] = None

    @property
    def is_successful(self) -> bool:
        """Check if transcription was successful"""
        return bool(self.transcription) and not self.error

@dataclass
class AIAnalysisResult:
    """AI analysis result"""
    is_suitable: bool
    confidence: Optional[float] = None
    reasoning: Optional[str] = None
    model_used: Optional[str] = None
    processing_time: Optional[float] = None
    error: Optional[str] = None

    @property
    def is_successful(self) -> bool:
        """Check if AI analysis was successful"""
        return not self.error

@dataclass
class LeadAnalysisResult:
    """Complete lead analysis result"""
    lead_id: str
    original_status: Optional[str] = None
    original_junk_status: Optional[int] = None
    action: Optional[AnalysisAction] = None
    reason: Optional[AnalysisReason] = None
    new_status: Optional[str] = None
    new_junk_status: Optional[int] = None

    # Analysis details
    unsuccessful_calls_count: int = 0
    transcription_results: List[TranscriptionResult] = field(default_factory=list)
    ai_analysis: Optional[AIAnalysisResult] = None

    # Metadata
    analysis_start_time: datetime = field(default_factory=datetime.now)
    analysis_end_time: Optional[datetime] = None
    processing_time: Optional[float] = None
    error_message: Optional[str] = None

    def __post_init__(self):
        if self.analysis_end_time and self.analysis_start_time:
            self.processing_time = (self.analysis_end_time - self.analysis_start_time).total_seconds()

    def mark_completed(self):
        """Mark analysis as completed"""
        self.analysis_end_time = datetime.now()
        self.processing_time = (self.analysis_end_time - self.analysis_start_time).total_seconds()

    def add_transcription_result(self, result: TranscriptionResult):
        """Add transcription result"""
        self.transcription_results.append(result)

    def set_ai_analysis(self, result: AIAnalysisResult):
        """Set AI analysis result"""
        self.ai_analysis = result

    def set_error(self, error_message: str, reason: AnalysisReason = AnalysisReason.API_ERROR):
        """Set error state"""
        self.error_message = error_message
        self.action = AnalysisAction.ERROR
        self.reason = reason
        self.mark_completed()

    def set_action(self, action: AnalysisAction, reason: AnalysisReason,
                   new_status: Optional[str] = None, new_junk_status: Optional[int] = None):
        """Set analysis action and reason"""
        self.action = action
        self.reason = reason
        self.new_status = new_status
        self.new_junk_status = new_junk_status

    @property
    def is_successful(self) -> bool:
        """Check if analysis was successful"""
        return self.action != AnalysisAction.ERROR and not self.error_message

    @property
    def requires_update(self) -> bool:
        """Check if lead requires status update"""
        return self.action == AnalysisAction.CHANGE_STATUS

    @property
    def transcription_success_rate(self) -> float:
        """Calculate transcription success rate"""
        if not self.transcription_results:
            return 0.0
        successful = sum(1 for result in self.transcription_results if result.is_successful)
        return successful / len(self.transcription_results)

    @property
    def total_transcription_text(self) -> str:
        """Get combined transcription text"""
        successful_transcriptions = [
            result.transcription
            for result in self.transcription_results
            if result.is_successful
        ]
        return "\n\n".join(successful_transcriptions)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return {
            'lead_id': self.lead_id,
            'original_status': self.original_status,
            'original_junk_status': self.original_junk_status,
            'action': self.action.value if self.action else None,
            'reason': self.reason.value if self.reason else None,
            'new_status': self.new_status,
            'new_junk_status': self.new_junk_status,
            'unsuccessful_calls_count': self.unsuccessful_calls_count,
            'transcription_results': [
                {
                    'audio_file': tr.audio_file,
                    'transcription': tr.transcription,
                    'confidence': tr.confidence,
                    'duration': tr.duration,
                    'language': tr.language,
                    'error': tr.error,
                    'is_successful': tr.is_successful
                }
                for tr in self.transcription_results
            ],
            'ai_analysis': {
                'is_suitable': self.ai_analysis.is_suitable,
                'confidence': self.ai_analysis.confidence,
                'reasoning': self.ai_analysis.reasoning,
                'model_used': self.ai_analysis.model_used,
                'processing_time': self.ai_analysis.processing_time,
                'error': self.ai_analysis.error,
                'is_successful': self.ai_analysis.is_successful
            } if self.ai_analysis else None,
            'analysis_start_time': self.analysis_start_time.isoformat(),
            'analysis_end_time': self.analysis_end_time.isoformat() if self.analysis_end_time else None,
            'processing_time': self.processing_time,
            'error_message': self.error_message,
            'is_successful': self.is_successful,
            'requires_update': self.requires_update,
            'transcription_success_rate': self.transcription_success_rate
        }

    def __repr__(self) -> str:
        return f"LeadAnalysisResult(lead_id={self.lead_id}, action={self.action}, reason={self.reason})"

@dataclass
class BatchAnalysisResult:
    """Batch analysis result"""
    batch_id: str
    lead_results: List[LeadAnalysisResult] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_processing_time: Optional[float] = None

    def add_result(self, result: LeadAnalysisResult):
        """Add lead analysis result"""
        self.lead_results.append(result)

    def mark_completed(self):
        """Mark batch analysis as completed"""
        self.end_time = datetime.now()
        self.total_processing_time = (self.end_time - self.start_time).total_seconds()

    @property
    def total_leads(self) -> int:
        """Total number of leads analyzed"""
        return len(self.lead_results)

    @property
    def successful_analyses(self) -> int:
        """Number of successful analyses"""
        return sum(1 for result in self.lead_results if result.is_successful)

    @property
    def failed_analyses(self) -> int:
        """Number of failed analyses"""
        return sum(1 for result in self.lead_results if not result.is_successful)

    @property
    def leads_updated(self) -> int:
        """Number of leads that were updated"""
        return sum(1 for result in self.lead_results if result.requires_update)

    @property
    def leads_kept(self) -> int:
        """Number of leads that kept their status"""
        return sum(1 for result in self.lead_results
                  if result.action == AnalysisAction.KEEP_STATUS)

    @property
    def leads_skipped(self) -> int:
        """Number of leads that were skipped"""
        return sum(1 for result in self.lead_results
                  if result.action == AnalysisAction.SKIP)

    @property
    def success_rate(self) -> float:
        """Calculate analysis success rate"""
        if self.total_leads == 0:
            return 0.0
        return self.successful_analyses / self.total_leads

    @property
    def average_processing_time(self) -> float:
        """Calculate average processing time per lead"""
        successful_results = [r for r in self.lead_results if r.processing_time]
        if not successful_results:
            return 0.0
        return sum(r.processing_time for r in successful_results) / len(successful_results)

    def get_results_by_action(self, action: AnalysisAction) -> List[LeadAnalysisResult]:
        """Get results filtered by action"""
        return [result for result in self.lead_results if result.action == action]

    def get_results_by_reason(self, reason: AnalysisReason) -> List[LeadAnalysisResult]:
        """Get results filtered by reason"""
        return [result for result in self.lead_results if result.reason == reason]

    def to_dict(self) -> Dict[str, Any]:
        """Convert batch result to dictionary"""
        return {
            'batch_id': self.batch_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_processing_time': self.total_processing_time,
            'total_leads': self.total_leads,
            'successful_analyses': self.successful_analyses,
            'failed_analyses': self.failed_analyses,
            'leads_updated': self.leads_updated,
            'leads_kept': self.leads_kept,
            'leads_skipped': self.leads_skipped,
            'success_rate': self.success_rate,
            'average_processing_time': self.average_processing_time,
            'lead_results': [result.to_dict() for result in self.lead_results]
        }

    def __repr__(self) -> str:
        return f"BatchAnalysisResult(batch_id={self.batch_id}, total_leads={self.total_leads}, success_rate={self.success_rate:.2f})"