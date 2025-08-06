"""
Test suite for Lead Analyzer Service
"""

import pytest
import unittest.mock as mock
from datetime import datetime, timedelta

# Add app to Python path for testing
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.lead_analyzer import LeadAnalyzerService
from app.models.lead import Lead, LeadFilter
from app.models.analysis_result import LeadAnalysisResult, AnalysisAction, AnalysisReason
from app.utils.exceptions import LeadAnalyzerError


class TestLeadAnalyzerService:
    """Test cases for Lead Analyzer Service"""

    def setup_method(self):
        """Setup test fixtures"""
        # Mock the service dependencies
        self.mock_bitrix = mock.MagicMock()
        self.mock_transcription = mock.MagicMock()
        self.mock_gemini = mock.MagicMock()

    @pytest.fixture
    def sample_lead(self):
        """Sample lead for testing"""
        return Lead(
            id="123",
            title="Test Lead",
            status_id="JUNK",
            junk_status=229,  # Ariza qoldirmagan
            date_create=datetime.now()
        )

    @pytest.fixture
    def analyzer_service(self):
        """Lead analyzer service with mocked dependencies"""
        with mock.patch('app.services.lead_analyzer.BitrixService') as mock_bitrix_cls, \
                mock.patch('app.services.lead_analyzer.TranscriptionService') as mock_trans_cls, \
                mock.patch('app.services.lead_analyzer.GeminiService') as mock_gemini_cls:
            mock_bitrix_cls.return_value = self.mock_bitrix
            mock_trans_cls.return_value = self.mock_transcription
            mock_gemini_cls.return_value = self.mock_gemini

            return LeadAnalyzerService()

    def test_analyze_new_leads_success(self, analyzer_service, sample_lead):
        """Test successful analysis of new leads"""
        # Setup mocks
        self.mock_bitrix.get_lead_activities.return_value = activities
        self.mock_bitrix.update_lead_complete.return_value = True

        # Run analysis
        result = analyzer_service._analyze_single_lead(sample_lead, dry_run=False)

        # Assertions
        assert result.action == AnalysisAction.CHANGE_STATUS
        assert result.reason == AnalysisReason.INSUFFICIENT_CALLS
        assert result.unsuccessful_calls_count == 2
        assert result.new_status == "NEW"
        assert result.new_junk_status is None

    def test_analyze_lead_ai_not_suitable(self, analyzer_service, sample_lead):
        """Test AI analysis determining status is not suitable"""
        # Mock audio files and transcription
        self.mock_bitrix.get_lead_audio_files.return_value = ['http://example.com/audio.wav']

        from app.models.analysis_result import TranscriptionResult, AIAnalysisResult
        transcription_result = TranscriptionResult(
            audio_file='http://example.com/audio.wav',
            transcription="Customer submitted application yesterday"
        )
        self.mock_transcription.transcribe_url.return_value = transcription_result

        # AI says status is not suitable
        ai_result = AIAnalysisResult(is_suitable=False, reasoning="Customer has submitted application")
        self.mock_gemini.analyze_lead_status.return_value = ai_result
        self.mock_bitrix.update_lead_complete.return_value = True

        # Run analysis
        result = analyzer_service._analyze_single_lead(sample_lead, dry_run=False)

        # Assertions
        assert result.action == AnalysisAction.CHANGE_STATUS
        assert result.reason == AnalysisReason.AI_NOT_SUITABLE
        assert result.new_status == "NEW"
        assert result.ai_analysis.is_suitable == False

    def test_analyze_lead_no_audio_files(self, analyzer_service, sample_lead):
        """Test analysis when no audio files are found"""
        # Mock no audio files
        self.mock_bitrix.get_lead_audio_files.return_value = []

        # Run analysis
        result = analyzer_service._analyze_single_lead(sample_lead, dry_run=True)

        # Assertions
        assert result.action == AnalysisAction.SKIP
        assert result.reason == AnalysisReason.NO_AUDIO_FILES

    def test_analyze_lead_transcription_failure(self, analyzer_service, sample_lead):
        """Test analysis when transcription fails"""
        # Mock audio files but failed transcription
        self.mock_bitrix.get_lead_audio_files.return_value = ['http://example.com/audio.wav']

        from app.models.analysis_result import TranscriptionResult
        transcription_result = TranscriptionResult(
            audio_file='http://example.com/audio.wav',
            transcription='',
            error='Transcription service unavailable'
        )
        self.mock_transcription.transcribe_url.return_value = transcription_result

        # Run analysis
        result = analyzer_service._analyze_single_lead(sample_lead, dry_run=True)

        # Assertions
        assert result.action == AnalysisAction.SKIP
        assert result.reason == AnalysisReason.NO_TRANSCRIPTION

    def test_analyze_lead_invalid_junk_status(self, analyzer_service, sample_lead):
        """Test analysis with invalid junk status"""
        # Set invalid junk status
        sample_lead.junk_status = 999

        # Run analysis
        result = analyzer_service._analyze_single_lead(sample_lead, dry_run=True)

        # Assertions
        assert result.action == AnalysisAction.SKIP
        assert result.reason == AnalysisReason.NOT_TARGET_STATUS

    def test_health_check_all_healthy(self, analyzer_service):
        """Test health check when all services are healthy"""
        # Mock healthy services
        self.mock_bitrix.test_connection.return_value = True
        self.mock_transcription.test_connection.return_value = True
        self.mock_gemini.test_connection.return_value = True

        # Run health check
        health_status = analyzer_service.check_health()

        # Assertions
        assert health_status['bitrix'] == True
        assert health_status['transcription'] == True
        assert health_status['gemini'] == True
        assert all(health_status.values())

    def test_health_check_service_failure(self, analyzer_service):
        """Test health check when some services fail"""
        # Mock mixed service health
        self.mock_bitrix.test_connection.return_value = True
        self.mock_transcription.test_connection.return_value = False
        self.mock_gemini.test_connection.return_value = True

        # Run health check
        health_status = analyzer_service.check_health()

        # Assertions
        assert health_status['bitrix'] == True
        assert health_status['transcription'] == False
        assert health_status['gemini'] == True
        assert not all(health_status.values())

    def test_analyze_lead_by_id_not_found(self, analyzer_service):
        """Test analyzing specific lead that doesn't exist"""
        # Mock lead not found
        self.mock_bitrix.get_lead_by_id.return_value = None

        # Run analysis
        result = analyzer_service.analyze_lead_by_id("999", dry_run=True)

        # Assertions
        assert result is None

    def test_analyze_lead_by_id_success(self, analyzer_service, sample_lead):
        """Test successful analysis of specific lead"""
        # Mock lead found and successful analysis
        self.mock_bitrix.get_lead_by_id.return_value = sample_lead
        self.mock_bitrix.get_lead_audio_files.return_value = []  # No audio files, will skip

        # Run analysis
        result = analyzer_service.analyze_lead_by_id("123", dry_run=True)

        # Assertions
        assert result is not None
        assert result.lead_id == "123"
        assert result.action == AnalysisAction.SKIP
        assert result.reason == AnalysisReason.NO_AUDIO_FILES

    def test_get_statistics(self, analyzer_service):
        """Test getting system statistics"""
        # Mock service responses
        self.mock_bitrix.test_connection.return_value = True
        self.mock_transcription.test_connection.return_value = True
        self.mock_gemini.test_connection.return_value = True
        self.mock_bitrix.get_junk_leads_count.return_value = 15

        # Get statistics
        stats = analyzer_service.get_statistics()

        # Assertions
        assert 'last_analysis_time' in stats
        assert 'services_health' in stats
        assert 'junk_leads_count' in stats
        assert 'configuration' in stats
        assert stats['junk_leads_count'] == 15
        assert stats['services_health']['bitrix'] == True


class TestLeadAnalyzerIntegration:
    """Integration tests for Lead Analyzer"""

    @pytest.mark.integration
    def test_real_configuration_validation(self):
        """Test with real configuration (requires .env file)"""
        try:
            from app.config import validate_config
            # This will use actual .env file if present
            is_valid = validate_config()
            # Don't assert here as it depends on environment
            print(f"Configuration validation: {is_valid}")
        except Exception as e:
            pytest.skip(f"Configuration test skipped: {e}")

    @pytest.mark.integration
    def test_real_service_health(self):
        """Test with real services (requires actual service URLs)"""
        try:
            analyzer = LeadAnalyzerService()
            health = analyzer.check_health()
            print(f"Service health: {health}")
            # Don't assert as this depends on actual service availability
        except Exception as e:
            pytest.skip(f"Service health test skipped: {e}")


# Test fixtures and utilities
@pytest.fixture
def mock_config():
    """Mock configuration for testing"""
    config_data = {
        'bitrix': {
            'webhook_url': 'https://test.bitrix24.com/rest/1/abc123',
            'timeout_seconds': 30,
            'max_retries': 3
        },
        'transcription': {
            'service_url': 'http://localhost:8101',
            'timeout_seconds': 60,
            'max_retries': 3
        },
        'gemini': {
            'api_key': 'test_api_key',
            'model_name': 'gemini-pro',
            'timeout_seconds': 30,
            'max_retries': 3
        },
        'scheduler': {
            'check_interval_hours': 24,
            'max_concurrent_leads': 10,
            'delay_between_leads': 2.0
        },
        'lead_status': {
            'junk_status_field': 'UF_CRM_1751812306933',
            'main_status_field': 'STATUS_ID',
            'junk_status_value': 'JUNK',
            'active_status_value': 'NEW',
            'junk_statuses': {
                158: "5 marta javob bermadi",
                227: "Notog'ri raqam",
                229: "Ariza qoldirmagan",
                783: "Notog'ri mijoz",
                807: "Yoshi to'g'ri kelmadi"
            }
        }
    }

    with mock.patch('app.config.get_config') as mock_get_config:
        mock_config_obj = mock.MagicMock()
        for key, value in config_data.items():
            setattr(mock_config_obj, key, mock.MagicMock(**value))
        mock_get_config.return_value = mock_config_obj
        yield mock_config_obj


# Performance tests
class TestPerformance:
    """Performance tests for Lead Analyzer"""

    @pytest.mark.performance
    def test_batch_analysis_performance(self):
        """Test performance of batch analysis"""
        import time

        # Create mock leads
        leads = []
        for i in range(100):
            lead = Lead(id=str(i), status_id="JUNK", junk_status=229)
            leads.append(lead)

        # Mock analyzer with fast responses
        with mock.patch('app.services.lead_analyzer.BitrixService'), \
                mock.patch('app.services.lead_analyzer.TranscriptionService'), \
                mock.patch('app.services.lead_analyzer.GeminiService'):

            analyzer = LeadAnalyzerService()

            start_time = time.time()
            # Simulate batch processing (dry run)
            for lead in leads[:10]:  # Test with smaller batch
                result = analyzer._analyze_single_lead(lead, dry_run=True)
            end_time = time.time()

            processing_time = end_time - start_time
            leads_per_second = 10 / processing_time if processing_time > 0 else 0

            print(f"Processed 10 leads in {processing_time:.2f} seconds")
            print(f"Rate: {leads_per_second:.2f} leads/second")

            # Performance assertion (adjust based on requirements)
            assert leads_per_second > 1, "Processing rate should be > 1 lead/second"

