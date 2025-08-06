"""
Core lead analysis service that orchestrates the analysis process
"""

import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from app.config import get_config
from app.logger import LoggerMixin
from app.models.lead import Lead, LeadFilter, LeadBatch
from app.models.analysis_result import (
    LeadAnalysisResult, BatchAnalysisResult, AnalysisAction, AnalysisReason,
    TranscriptionResult, AIAnalysisResult
)
from app.services.bitrix_service import BitrixService
from app.services.transcription_service import TranscriptionService
from app.services.gemini_service import GeminiService
from app.utils.exceptions import LeadAnalyzerError, ValidationError


class LeadAnalyzerService(LoggerMixin):
    """Core service for analyzing leads and updating their statuses"""

    def __init__(self):
        self.config = get_config()

        # Initialize service dependencies
        self.bitrix_service = BitrixService()
        self.transcription_service = TranscriptionService()
        self.gemini_service = GeminiService()

        self.last_analysis_time = datetime.now() - timedelta(hours=self.config.scheduler.check_interval_hours)

        self.log_service_action("LeadAnalyzerService", "init", "Initialized lead analyzer service")

    def analyze_new_leads(self, dry_run: bool = False) -> BatchAnalysisResult:
        """Analyze leads added since last check"""
        batch_id = f"new_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        batch_result = BatchAnalysisResult(batch_id=batch_id)

        try:
            self.logger.info("Starting analysis of new junk leads")

            # Create filter for new junk leads
            lead_filter = LeadFilter(
                status_id=self.config.lead_status.junk_status_value,
                junk_statuses=list(self.config.lead_status.junk_statuses.keys()),
                date_from=self.last_analysis_time,
                limit=self.config.scheduler.max_concurrent_leads
            )

            # Get new junk leads
            leads = self.bitrix_service.get_leads(lead_filter)

            if not leads:
                self.logger.info("No new junk leads found")
                batch_result.mark_completed()
                return batch_result

            self.logger.info(f"Found {len(leads)} new junk leads to analyze")

            # Analyze each lead
            for lead in leads:
                try:
                    result = self._analyze_single_lead(lead, dry_run)
                    batch_result.add_result(result)

                    # Small delay between leads
                    time.sleep(self.config.scheduler.delay_between_leads)

                except Exception as e:
                    self.log_lead_action(lead.id, "analyze_error", f"Error analyzing lead: {e}")
                    error_result = LeadAnalysisResult(
                        lead_id=lead.id,
                        original_status=lead.status_id,
                        original_junk_status=lead.junk_status
                    )
                    error_result.set_error(str(e))
                    batch_result.add_result(error_result)

            # Update last analysis time
            self.last_analysis_time = datetime.now()

            batch_result.mark_completed()

            self.logger.info(f"New leads analysis completed: {batch_result.success_rate:.2f} success rate")
            return batch_result

        except Exception as e:
            self.logger.error(f"Error in new leads analysis: {e}")
            batch_result.mark_completed()
            raise LeadAnalyzerError(f"New leads analysis failed: {e}")

    def analyze_all_junk_leads(self, dry_run: bool = False) -> BatchAnalysisResult:
        """Analyze all existing junk leads"""
        batch_id = f"all_junk_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        batch_result = BatchAnalysisResult(batch_id=batch_id)

        try:
            self.logger.info("Starting analysis of all junk leads")

            # Create filter for all junk leads
            lead_filter = LeadFilter(
                status_id=self.config.lead_status.junk_status_value,
                junk_statuses=list(self.config.lead_status.junk_statuses.keys()),
                limit=100  # Process in batches of 100
            )

            # Get all junk leads
            leads = self.bitrix_service.get_leads(lead_filter)

            if not leads:
                self.logger.info("No junk leads found")
                batch_result.mark_completed()
                return batch_result

            self.logger.info(f"Found {len(leads)} junk leads to analyze")

            # Analyze each lead
            for i, lead in enumerate(leads):
                try:
                    result = self._analyze_single_lead(lead, dry_run)
                    batch_result.add_result(result)

                    # Progress logging
                    if (i + 1) % 10 == 0:
                        self.logger.info(f"Processed {i + 1}/{len(leads)} leads")

                    # Small delay between leads
                    time.sleep(self.config.scheduler.delay_between_leads)

                except Exception as e:
                    self.log_lead_action(lead.id, "analyze_error", f"Error analyzing lead: {e}")
                    error_result = LeadAnalysisResult(
                        lead_id=lead.id,
                        original_status=lead.status_id,
                        original_junk_status=lead.junk_status
                    )
                    error_result.set_error(str(e))
                    batch_result.add_result(error_result)

            batch_result.mark_completed()

            self.logger.info(f"All junk leads analysis completed: {batch_result.success_rate:.2f} success rate")
            return batch_result

        except Exception as e:
            self.logger.error(f"Error in all junk leads analysis: {e}")
            batch_result.mark_completed()
            raise LeadAnalyzerError(f"All junk leads analysis failed: {e}")

    def analyze_lead_by_id(self, lead_id: str, dry_run: bool = False) -> Optional[LeadAnalysisResult]:
        """Analyze a specific lead by ID"""
        try:
            self.log_lead_action(lead_id, "analyze_start", "Starting lead analysis")

            # Get lead data
            lead = self.bitrix_service.get_lead_by_id(lead_id)

            if not lead:
                self.log_lead_action(lead_id, "analyze_error", "Lead not found")
                return None

            # Analyze the lead
            result = self._analyze_single_lead(lead, dry_run)

            self.log_lead_action(lead_id, "analyze_complete",
                                 f"Analysis completed: {result.action.value if result.action else 'unknown'}")
            return result

        except Exception as e:
            self.log_lead_action(lead_id, "analyze_error", f"Error analyzing lead: {e}")
            error_result = LeadAnalysisResult(lead_id=lead_id)
            error_result.set_error(str(e))
            return error_result

    def _analyze_single_lead(self, lead: Lead, dry_run: bool = False) -> LeadAnalysisResult:
        """Analyze a single lead and return result"""
        result = LeadAnalysisResult(
            lead_id=lead.id,
            original_status=lead.status_id,
            original_junk_status=lead.junk_status
        )

        try:
            self.log_lead_action(lead.id, "analyze", f"Analyzing junk status {lead.junk_status}")

            # Check if lead has target junk status
            if not lead.has_target_junk_status:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NOT_TARGET_STATUS)
                result.mark_completed()
                return result

            # Special handling for status 158 (5 marta javob bermadi)
            if lead.junk_status == 158:
                result = self._analyze_unsuccessful_calls(lead, result, dry_run)
            else:
                # For other statuses, use AI analysis
                result = self._analyze_with_ai(lead, result, dry_run)

            result.mark_completed()
            return result

        except Exception as e:
            self.log_lead_action(lead.id, "analyze_error", f"Analysis error: {e}")
            result.set_error(str(e))
            return result

    def _analyze_unsuccessful_calls(self, lead: Lead, result: LeadAnalysisResult, dry_run: bool) -> LeadAnalysisResult:
        """Analyze lead with status 158 (5 marta javob bermadi)"""
        try:
            # Get lead activities
            activities = self.bitrix_service.get_lead_activities(lead.id)
            lead.activities = activities

            # Count unsuccessful calls
            unsuccessful_calls = lead.unsuccessful_calls_count
            result.unsuccessful_calls_count = unsuccessful_calls

            self.log_lead_action(lead.id, "call_analysis", f"Found {unsuccessful_calls} unsuccessful calls")

            if unsuccessful_calls >= 5:
                # Keep current junk status
                result.set_action(AnalysisAction.KEEP_STATUS, AnalysisReason.SUFFICIENT_CALLS)
                self.log_lead_action(lead.id, "decision", "Keeping status - sufficient unsuccessful calls")
            else:
                # Change to active status
                new_status = self.config.lead_status.active_status_value
                result.set_action(
                    AnalysisAction.CHANGE_STATUS,
                    AnalysisReason.INSUFFICIENT_CALLS,
                    new_status=new_status,
                    new_junk_status=None
                )

                self.log_lead_action(lead.id, "decision", "Changing status - insufficient unsuccessful calls")

                # Update lead status if not dry run
                if not dry_run:
                    success = self.bitrix_service.update_lead_complete(lead.id, new_status, None)
                    if not success:
                        result.set_error("Failed to update lead status")

            return result

        except Exception as e:
            result.set_error(f"Error analyzing unsuccessful calls: {e}")
            return result

    def _analyze_with_ai(self, lead: Lead, result: LeadAnalysisResult, dry_run: bool) -> LeadAnalysisResult:
        """Analyze lead using AI transcription analysis"""
        try:
            # Get audio files for the lead
            audio_files = self.bitrix_service.get_lead_audio_files(lead.id)

            if not audio_files:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NO_AUDIO_FILES)
                self.log_lead_action(lead.id, "ai_analysis", "No audio files found")
                return result

            self.log_lead_action(lead.id, "ai_analysis", f"Found {len(audio_files)} audio files")

            # Transcribe all audio files
            transcription_results = []
            for audio_file in audio_files:
                try:
                    if audio_file.startswith(('http://', 'https://')):
                        transcription_result = self.transcription_service.transcribe_url(audio_file)
                    else:
                        transcription_result = self.transcription_service.transcribe_file(audio_file)

                    transcription_results.append(transcription_result)
                    result.add_transcription_result(transcription_result)

                except Exception as e:
                    self.log_lead_action(lead.id, "transcription_error", f"Error transcribing {audio_file}: {e}")
                    error_transcription = TranscriptionResult(
                        audio_file=audio_file,
                        transcription='',
                        error=str(e)
                    )
                    transcription_results.append(error_transcription)
                    result.add_transcription_result(error_transcription)

            # Check if we have any successful transcriptions
            successful_transcriptions = [tr for tr in transcription_results if tr.is_successful]

            if not successful_transcriptions:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NO_TRANSCRIPTION)
                self.log_lead_action(lead.id, "ai_analysis", "No successful transcriptions")
                return result

            # Combine all transcriptions
            combined_transcription = result.total_transcription_text

            self.log_lead_action(lead.id, "ai_analysis", f"Analyzing {len(successful_transcriptions)} transcriptions")

            # Analyze with Gemini AI
            status_name = self.config.lead_status.junk_statuses.get(lead.junk_status, "Unknown")
            ai_result = self.gemini_service.analyze_lead_status(
                combined_transcription,
                lead.junk_status,
                status_name
            )

            result.set_ai_analysis(ai_result)

            if not ai_result.is_successful:
                result.set_error(f"AI analysis failed: {ai_result.error}")
                return result

            # Make decision based on AI result
            if ai_result.is_suitable:
                # Keep current junk status
                result.set_action(AnalysisAction.KEEP_STATUS, AnalysisReason.AI_SUITABLE)
                self.log_lead_action(lead.id, "decision", "Keeping status - AI says suitable")
            else:
                # Change to active status
                new_status = self.config.lead_status.active_status_value
                result.set_action(
                    AnalysisAction.CHANGE_STATUS,
                    AnalysisReason.AI_NOT_SUITABLE,
                    new_status=new_status,
                    new_junk_status=None
                )

                self.log_lead_action(lead.id, "decision", "Changing status - AI says not suitable")

                # Update lead status if not dry run
                if not dry_run:
                    success = self.bitrix_service.update_lead_complete(lead.id, new_status, None)
                    if not success:
                        result.set_error("Failed to update lead status")

            return result

        except Exception as e:
            result.set_error(f"Error in AI analysis: {e}")
            return result

    def check_health(self) -> Dict[str, bool]:
        """Check health of all services"""
        health_status = {}

        try:
            health_status['bitrix'] = self.bitrix_service.test_connection()
        except Exception:
            health_status['bitrix'] = False

        try:
            health_status['transcription'] = self.transcription_service.test_connection()
        except Exception:
            health_status['transcription'] = False

        try:
            health_status['gemini'] = self.gemini_service.test_connection()
        except Exception:
            health_status['gemini'] = False

        return health_status

    def test_analysis_pipeline(self) -> bool:
        """Test the complete analysis pipeline"""
        try:
            self.logger.info("Testing analysis pipeline...")

            # Test each service individually
            health = self.check_health()

            if not all(health.values()):
                failed_services = [service for service, status in health.items() if not status]
                self.logger.error(f"Service health check failed: {failed_services}")
                return False

            # Test getting junk leads (should not fail even if empty)
            try:
                lead_filter = LeadFilter(
                    status_id=self.config.lead_status.junk_status_value,
                    junk_statuses=list(self.config.lead_status.junk_statuses.keys()),
                    limit=1
                )

                leads = self.bitrix_service.get_leads(lead_filter)
                self.logger.info(f"Junk leads query test: found {len(leads)} leads")

            except Exception as e:
                self.logger.error(f"Junk leads query test failed: {e}")
                return False

            # Test transcription service with dummy data (if supported)
            try:
                service_info = self.transcription_service.get_service_info()
                self.logger.info(f"Transcription service info: {service_info.get('service', 'unknown')}")
            except Exception as e:
                self.logger.warning(f"Could not get transcription service info: {e}")

            # Test Gemini with simple prompt
            try:
                test_transcription = "Customer said hello and asked about services."
                test_result = self.gemini_service.analyze_lead_status(
                    test_transcription, 229, "Ariza qoldirmagan"
                )

                if test_result.is_successful:
                    self.logger.info(f"Gemini test completed: suitable={test_result.is_suitable}")
                else:
                    self.logger.error(f"Gemini test failed: {test_result.error}")
                    return False

            except Exception as e:
                self.logger.error(f"Gemini test failed: {e}")
                return False

            self.logger.info("Analysis pipeline test completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Analysis pipeline test failed: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Get analysis statistics"""
        try:
            stats = {
                'last_analysis_time': self.last_analysis_time.isoformat(),
                'services_health': self.check_health(),
                'junk_leads_count': self.bitrix_service.get_junk_leads_count(),
                'configuration': {
                    'check_interval_hours': self.config.scheduler.check_interval_hours,
                    'max_concurrent_leads': self.config.scheduler.max_concurrent_leads,
                    'delay_between_leads': self.config.scheduler.delay_between_leads,
                    'junk_statuses': self.config.lead_status.junk_statuses
                }
            }

            return stats

        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {'error': str(e)}

    def close(self):
        """Close all services and cleanup resources"""
        try:
            self.bitrix_service.close()
        except Exception as e:
            self.logger.warning(f"Error closing Bitrix service: {e}")

        try:
            self.transcription_service.close()
        except Exception as e:
            self.logger.warning(f"Error closing transcription service: {e}")

        try:
            self.gemini_service.close()
        except Exception as e:
            self.logger.warning(f"Error closing Gemini service: {e}")

        self.log_service_action("LeadAnalyzerService", "close", "Service closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()