"""
Enhanced Lead Analyzer Service for Bitrix24 with improved analysis logic
"""

import time
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from app.config import get_config
from app.logger import LoggerMixin
from app.models.lead import Lead, LeadFilter
from app.models.analysis_result import (
    LeadAnalysisResult, BatchAnalysisResult, AnalysisAction, AnalysisReason,
    TranscriptionResult
)
from app.services.bitrix_service import BitrixService
from app.services.gemini_service import GeminiService
from app.utils.exceptions import LeadAnalyzerError, ValidationError
from enhanced.enhanced_gemini import EnhancedGeminiService


class EnhancedTranscriptionService(LoggerMixin):
    """Enhanced transcription service for audio analysis"""

    def __init__(self):
        self.config = get_config().transcription
        self.session = requests.Session()
        self.session.timeout = self.config.timeout_seconds
        self.log_service_action("EnhancedTranscriptionService", "init", "Initialized enhanced transcription service")

    def analyze_audio(self, audio_url: str, language: str = "uz") -> Dict[str, Any]:
        """
        Send audio to transcription service and get detailed analysis
        """
        try:
            self.logger.info(f"Analyzing audio from URL: {audio_url}")

            # Download audio file first
            audio_response = requests.get(audio_url, timeout=30)
            audio_response.raise_for_status()

            # Prepare the request
            url = f"http://127.0.0.1:8101/analyze?language={language}"
            files = {
                'file': ('audio.wav', audio_response.content, 'audio/wav')
            }

            # Make request to transcription service
            response = self.session.post(url, files=files)
            response.raise_for_status()

            result = response.json()
            self.logger.info(f"Successfully analyzed audio: {audio_url}")

            return result

        except Exception as e:
            self.logger.error(f"Error analyzing audio {audio_url}: {e}")
            return {"error": str(e)}

    def transcribe_url(self, audio_url: str) -> TranscriptionResult:
        """
        Transcribe audio from URL using enhanced service
        """
        try:
            analysis_result = self.analyze_audio(audio_url)

            if "error" in analysis_result:
                return TranscriptionResult(
                    audio_file=audio_url,
                    transcription='',
                    error=analysis_result["error"]
                )

            # Extract transcription from the detailed response
            transcription_parts = []
            if "transcription" in analysis_result:
                for part in analysis_result["transcription"]:
                    transcription_parts.append(f"{part['speaker']}: {part['text']}")

            full_transcription = "\n".join(transcription_parts)

            return TranscriptionResult(
                audio_file=audio_url,
                transcription=full_transcription,
                confidence=analysis_result.get("overall_performance_score", 0) / 100.0 if analysis_result.get(
                    "overall_performance_score") else None,
                language="uz"
            )

        except Exception as e:
            self.logger.error(f"Error transcribing audio {audio_url}: {e}")
            return TranscriptionResult(
                audio_file=audio_url,
                transcription='',
                error=str(e)
            )

    def close(self):
        """Close the service"""
        if hasattr(self, 'session'):
            self.session.close()
        self.log_service_action("EnhancedTranscriptionService", "close", "Service closed")


class EnhancedLeadAnalyzerService(LoggerMixin):
    """Enhanced Lead Analyzer Service with improved analysis logic"""

    def __init__(self):
        self.config = get_config()

        # Initialize service dependencies
        self.bitrix_service = BitrixService()
        self.transcription_service = EnhancedTranscriptionService()
        # self.gemini_service = GeminiService()
        self.gemini_service = EnhancedGeminiService()

        self.last_analysis_time = datetime.now() - timedelta(hours=self.config.scheduler.check_interval_hours)

        # Junk status definitions
        self.junk_statuses = {
            158: "5 marta javob bermadi",
            227: "Notog'ri raqam",
            229: "Ariza qoldirmagan",
            783: "Notog'ri mijoz",
            807: "Yoshi to'g'ri kelmadi"
        }

        self.log_service_action("EnhancedLeadAnalyzerService", "init", "Initialized enhanced lead analyzer service")

    def analyze_new_leads(self, dry_run: bool = False) -> BatchAnalysisResult:
        """Analyze leads added since last check"""
        batch_id = f"new_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        batch_result = BatchAnalysisResult(batch_id=batch_id)

        try:
            self.logger.info("Starting analysis of new junk leads")

            # Create filter for new junk leads added since last analysis
            lead_filter = LeadFilter(
                status_id=self.config.lead_status.junk_status_value,
                junk_statuses=list(self.junk_statuses.keys()),
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

                    # Small delay between leads to avoid overwhelming services
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

    def _analyze_single_lead(self, lead: Lead, dry_run: bool = False) -> LeadAnalysisResult:
        """Analyze a single lead based on junk status"""
        result = LeadAnalysisResult(
            lead_id=lead.id,
            original_status=lead.status_id,
            original_junk_status=lead.junk_status
        )

        try:
            self.log_lead_action(lead.id, "analyze", f"Analyzing junk status {lead.junk_status}")

            # Check if lead has valid junk status
            if lead.junk_status not in self.junk_statuses:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NOT_TARGET_STATUS)
                result.mark_completed()
                return result

            # Handle different junk statuses
            if lead.junk_status == 158:
                # Status 158: "5 marta javob bermadi" - check unsuccessful calls
                result = self._analyze_unsuccessful_calls(lead, result, dry_run)
            else:
                # Other statuses: use AI analysis with transcription
                result = self._analyze_with_ai_transcription(lead, result, dry_run)

            result.mark_completed()
            return result

        except Exception as e:
            self.log_lead_action(lead.id, "analyze_error", f"Analysis error: {e}")
            result.set_error(str(e))
            return result

    def _analyze_unsuccessful_calls(self, lead: Lead, result: LeadAnalysisResult, dry_run: bool) -> LeadAnalysisResult:
        """Analyze lead with status 158 by checking unsuccessful calls"""
        try:
            # Get lead activities
            activities = self.bitrix_service.get_lead_activities(lead.id)
            lead.activities = activities

            # Count unsuccessful calls
            unsuccessful_calls = 0
            for activity in activities:
                if activity.is_unsuccessful_call:
                    unsuccessful_calls += 1

            result.unsuccessful_calls_count = unsuccessful_calls

            self.log_lead_action(lead.id, "call_analysis", f"Found {unsuccessful_calls} unsuccessful calls")

            if unsuccessful_calls >= 5:
                # Keep current junk status - sufficient unsuccessful calls
                result.set_action(AnalysisAction.KEEP_STATUS, AnalysisReason.SUFFICIENT_CALLS)
                self.log_lead_action(lead.id, "decision", "Keeping status - sufficient unsuccessful calls")
            else:
                # Change to active status - insufficient unsuccessful calls
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

    def _analyze_with_ai_transcription(self, lead: Lead, result: LeadAnalysisResult,
                                       dry_run: bool) -> LeadAnalysisResult:
        """Analyze lead using AI with enhanced transcription and alternative status checking"""
        try:
            # Get audio files from Voximplant
            voximplant_data = self.bitrix_service.get_voximplant_call_data(lead.id)

            audio_files = []
            for call_data in voximplant_data:
                # Extract audio file URL from call data
                if 'CALL_RECORD_URL' in call_data and call_data['CALL_FAILED_CODE'] == "200":
                    audio_files.append(call_data['CALL_RECORD_URL'])

            if not audio_files:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NO_AUDIO_FILES)
                self.log_lead_action(lead.id, "ai_analysis", "No audio files found")
                return result

            self.log_lead_action(lead.id, "ai_analysis", f"Found {len(audio_files)} audio files")

            # Analyze all audio files with enhanced transcription
            transcription_results = []
            all_transcription_text = []

            for audio_file in audio_files:
                try:
                    # Use enhanced transcription service
                    transcription_result = self.transcription_service.transcribe_url(audio_file)
                    transcription_results.append(transcription_result)
                    result.add_transcription_result(transcription_result)

                    if transcription_result.is_successful:
                        all_transcription_text.append(transcription_result.transcription)

                except Exception as e:
                    self.log_lead_action(lead.id, "transcription_error", f"Error transcribing {audio_file}: {e}")
                    error_transcription = TranscriptionResult(
                        audio_file=audio_file,
                        transcription='',
                        error=str(e)
                    )
                    transcription_results.append(error_transcription)
                    result.add_transcription_result(error_transcription)

            # Check if we have successful transcriptions
            successful_transcriptions = [tr for tr in transcription_results if tr.is_successful]

            if not successful_transcriptions:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NO_TRANSCRIPTION)
                self.log_lead_action(lead.id, "ai_analysis", "No successful transcriptions")
                return result

            # Combine all transcriptions
            combined_transcription = "\n\n".join(all_transcription_text)

            self.log_lead_action(lead.id, "ai_analysis", f"Analyzing {len(successful_transcriptions)} transcriptions")

            # Analyze with Gemini AI
            status_name = self.junk_statuses.get(lead.junk_status, "Unknown")
            ai_result = self.gemini_service.analyze_lead_status(
                combined_transcription,
                lead.junk_status,
                status_name
            )

            result.set_ai_analysis(ai_result)

            if not ai_result.is_successful:
                result.set_error(f"AI analysis failed: {ai_result.error}")
                return result

            # Enhanced decision logic with alternative status handling
            if ai_result.is_suitable:
                if ai_result.has_alternative_status:
                    # Current status not suitable, but alternative status is suitable
                    alternative_status = ai_result.alternative_status
                    alternative_name = self.junk_statuses.get(alternative_status, f"Status {alternative_status}")

                    result.set_action(
                        AnalysisAction.CHANGE_STATUS,
                        AnalysisReason.AI_NOT_SUITABLE,
                        new_status=self.config.lead_status.junk_status_value,  # Keep as JUNK
                        new_junk_status=alternative_status  # Change to alternative status
                    )

                    self.log_lead_action(
                        lead.id,
                        "decision",
                        f"Changing junk status from {lead.junk_status} to {alternative_status} ({alternative_name})"
                    )

                    # Log AI reasoning
                    if ai_result.reasoning:
                        self.log_lead_action(lead.id, "ai_reasoning", f"AI Decision Details:\n{ai_result.reasoning}")

                    # Update lead status if not dry run
                    if not dry_run:
                        # Update both main status (keep as JUNK) and junk status (change to alternative)
                        success = self.bitrix_service.update_lead_complete(
                            lead.id,
                            self.config.lead_status.junk_status_value,  # Keep as JUNK
                            alternative_status  # New junk status
                        )
                        if not success:
                            result.set_error("Failed to update lead status")
                else:
                    # Keep current junk status - AI says it's suitable
                    result.set_action(AnalysisAction.KEEP_STATUS, AnalysisReason.AI_SUITABLE)
                    self.log_lead_action(lead.id, "decision", "Keeping status - AI says suitable")

                    # Log AI reasoning if available
                    if ai_result.reasoning:
                        self.log_lead_action(lead.id, "ai_reasoning", f"AI Decision Details:\n{ai_result.reasoning}")

            else:
                # Change to active status - AI says lead is not junk at all
                new_status = self.config.lead_status.active_status_value
                result.set_action(
                    AnalysisAction.CHANGE_STATUS,
                    AnalysisReason.AI_NOT_SUITABLE,
                    new_status=new_status,
                    new_junk_status=None
                )

                self.log_lead_action(lead.id, "decision", "Changing status to NEW - AI says not junk")

                # Log detailed reasoning for false results
                if ai_result.reasoning:
                    self.log_lead_action(lead.id, "ai_reasoning", f"AI Decision Details:\n{ai_result.reasoning}")
                else:
                    self.log_lead_action(
                        lead.id,
                        "ai_reasoning",
                        "AI determined lead should not be junk (no detailed reasoning provided)"
                    )

                # Update lead status if not dry run
                if not dry_run:
                    success = self.bitrix_service.update_lead_complete(lead.id, new_status, None)
                    if not success:
                        result.set_error("Failed to update lead status")

            return result

        except Exception as e:
            self.logger.error(f"Error in AI analysis: {e}")
            result.set_error(f"Error in AI analysis: {e}")
            return result

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

    def check_health(self) -> Dict[str, bool]:
        """Check health of all services"""
        health_status = {}

        try:
            health_status['bitrix'] = self.bitrix_service.test_connection()
        except Exception:
            health_status['bitrix'] = False

        try:
            # Test enhanced transcription service
            test_url = "http://127.0.0.1:8101/analyze?language=uz"
            response = requests.get("http://127.0.0.1:8101", timeout=5)
            health_status['transcription'] = response.status_code == 200
        except Exception:
            health_status['transcription'] = False

        try:
            health_status['gemini'] = self.gemini_service.test_connection()
        except Exception:
            health_status['gemini'] = False

        return health_status

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

        self.log_service_action("EnhancedLeadAnalyzerService", "close", "Service closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()