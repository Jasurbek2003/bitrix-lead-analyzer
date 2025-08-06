"""
Custom exceptions for Bitrix24 Lead Analyzer
"""

class LeadAnalyzerError(Exception):
    """Base exception for Lead Analyzer application"""
    pass

class ConfigurationError(LeadAnalyzerError):
    """Raised when there are configuration issues"""
    pass

class ValidationError(LeadAnalyzerError):
    """Raised when data validation fails"""
    pass

class BitrixAPIError(LeadAnalyzerError):
    """Raised when Bitrix24 API operations fail"""
    pass

class TranscriptionError(LeadAnalyzerError):
    """Raised when transcription service operations fail"""
    pass

class AIAnalysisError(LeadAnalyzerError):
    """Raised when AI analysis operations fail"""
    pass

class SchedulerError(LeadAnalyzerError):
    """Raised when scheduler operations fail"""
    pass

class WebhookError(LeadAnalyzerError):
    """Raised when webhook operations fail"""
    pass