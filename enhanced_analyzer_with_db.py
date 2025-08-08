"""
Enhanced Lead Analyzer Service with Database Caching
"""

import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session

from database_models import (
    Lead, Transcription, AnalysisHistory, SchedulerState,
    db_manager, get_db
)
from app.config import get_config
from app.logger import LoggerMixin
from app.models.lead import LeadFilter
from app.models.analysis_result import (
    LeadAnalysisResult, BatchAnalysisResult, AnalysisAction, AnalysisReason,
    TranscriptionResult, AIAnalysisResult
)
from app.services.bitrix_service import BitrixService
from enhanced.enhanced_gemini import EnhancedGeminiService
from app.utils.exceptions import LeadAnalyzerError
import requests


class CachedTranscriptionService(LoggerMixin):
    """Transcription service with database caching"""

    def __init__(self):
        self.config = get_config().transcription
        self.session = requests.Session()
        self.session.timeout = self.config.timeout_seconds
        self.log_service_action("CachedTranscriptionService", "init", "Initialized with database caching")

    def _get_audio_hash(self, audio_url: str) -> str:
        """Generate hash for audio URL"""
        return hashlib.sha256(audio_url.encode()).hexdigest()

    def _get_cached_transcription(self, db: Session, audio_url: str) -> Optional[Transcription]:
        """Get cached transcription from database"""
        audio_hash = self._get_audio_hash(audio_url)
        return db.query(Transcription).filter(Transcription.audio_hash == audio_hash).first()

    def _save_transcription_to_cache(self, db: Session, lead_id: str, audio_url: str,
                                     transcription_data: Dict[str, Any]) -> Transcription:
        """Save transcription to database cache"""
        audio_hash = self._get_audio_hash(audio_url)

        transcription = Transcription(
            lead_id=lead_id,
            audio_url=audio_url,
            audio_hash=audio_hash,
            transcription_text=transcription_data.get('transcription', ''),
            confidence=transcription_data.get('confidence'),
            duration=transcription_data.get('duration'),
            language=transcription_data.get('language', 'uz'),
            is_successful=transcription_data.get('is_successful', False),
            error_message=transcription_data.get('error'),
            processing_time=transcription_data.get('processing_time'),
            transcription_service='docker_service',
            service_version='1.0'
        )

        db.add(transcription)
        db.commit()
        db.refresh(transcription)

        return transcription

    def analyze_audio_with_cache(self, lead_id: str, audio_url: str, language: str = "uz") -> Dict[str, Any]:
        """Analyze audio with database caching"""
        with next(get_db()) as db:
            # Check cache first
            cached = self._get_cached_transcription(db, audio_url)
            if cached:
                self.logger.info(f"Using cached transcription for: {audio_url}")
                return {
                    'transcription': cached.transcription_text,
                    'confidence': cached.confidence,
                    'duration': cached.duration,
                    'language': cached.language,
                    'is_successful': cached.is_successful,
                    'error': cached.error_message,
                    'from_cache': True
                }

            # Not in cache, analyze with service
            self.logger.info(f"Analyzing new audio: {audio_url}")
            start_time = time.time()

            try:
                # Download audio file
                audio_response = requests.get(audio_url, timeout=30)
                audio_response.raise_for_status()

                # Send to transcription service
                url = f"http://127.0.0.1:8101/analyze?language={language}"
                files = {'file': ('audio.wav', audio_response.content, 'audio/wav')}

                response = self.session.post(url, files=files)
                response.raise_for_status()

                result = response.json()
                processing_time = time.time() - start_time

                # Process transcription result
                transcription_parts = []
                if "transcription" in result:
                    for part in result["transcription"]:
                        transcription_parts.append(f"{part['speaker']}: {part['text']}")

                full_transcription = "\n".join(transcription_parts)

                # Prepare data for caching
                cache_data = {
                    'transcription': full_transcription,
                    'confidence': result.get("overall_performance_score", 0) / 100.0 if result.get(
                        "overall_performance_score") else None,
                    'duration': result.get("duration"),
                    'language': language,
                    'is_successful': bool(full_transcription),
                    'processing_time': processing_time,
                    'from_cache': False
                }

                # Save to cache
                self._save_transcription_to_cache(db, lead_id, audio_url, cache_data)

                self.logger.info(f"Successfully analyzed and cached audio: {audio_url}")
                return cache_data

            except Exception as e:
                self.logger.error(f"Error analyzing audio {audio_url}: {e}")

                # Save error to cache to avoid retrying
                error_data = {
                    'transcription': '',
                    'error': str(e),
                    'is_successful': False,
                    'processing_time': time.time() - start_time,
                    'from_cache': False
                }

                self._save_transcription_to_cache(db, lead_id, audio_url, error_data)
                return error_data

    def transcribe_url(self, lead_id: str, audio_url: str) -> TranscriptionResult:
        """Transcribe URL with caching"""
        analysis_result = self.analyze_audio_with_cache(lead_id, audio_url)

        return TranscriptionResult(
            audio_file=audio_url,
            transcription=analysis_result.get('transcription', ''),
            confidence=analysis_result.get('confidence'),
            duration=analysis_result.get('duration'),
            language=analysis_result.get('language', 'uz'),
            error=analysis_result.get('error')
        )

    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get transcription cache statistics"""
        with next(get_db()) as db:
            total_transcriptions = db.query(Transcription).count()
            successful_transcriptions = db.query(Transcription).filter(Transcription.is_successful == True).count()
            failed_transcriptions = db.query(Transcription).filter(Transcription.is_successful == False).count()

            return {
                'total_transcriptions': total_transcriptions,
                'successful_transcriptions': successful_transcriptions,
                'failed_transcriptions': failed_transcriptions,
                'success_rate': successful_transcriptions / total_transcriptions if total_transcriptions > 0 else 0.0
            }


class EnhancedLeadAnalyzerWithDB(LoggerMixin):
    """Enhanced Lead Analyzer with Database Integration"""

    def __init__(self):
        self.config = get_config()

        # Initialize services
        self.bitrix_service = BitrixService()
        self.transcription_service = CachedTranscriptionService()
        self.gemini_service = EnhancedGeminiService()

        # Junk status definitions
        self.junk_statuses = {
            158: "5 marta javob bermadi",
            227: "Notog'ri raqam",
            229: "Ariza qoldirmagan",
            783: "Notog'ri mijoz",
            807: "Yoshi to'g'ri kelmadi"
        }

        self.log_service_action("EnhancedLeadAnalyzerWithDB", "init", "Initialized with database integration")

    def _save_lead_to_db(self, db: Session, lead_data: Dict[str, Any]) -> Lead:
        """Save or update lead in database"""
        lead_id = str(lead_data['ID'])

        # Check if lead exists
        existing_lead = db.query(Lead).filter(Lead.id == lead_id).first()

        if existing_lead:
            # Update existing lead
            existing_lead.title = lead_data.get('TITLE')
            existing_lead.status_id = lead_data.get('STATUS_ID')
            existing_lead.junk_status = lead_data.get(self.config.lead_status.junk_status_field)
            existing_lead.junk_status_name = self.junk_statuses.get(existing_lead.junk_status)
            existing_lead.raw_data = lead_data
            existing_lead.updated_at = datetime.utcnow()

            lead = existing_lead
        else:
            # Create new lead
            date_create = None
            if lead_data.get('DATE_CREATE'):
                try:
                    date_create = datetime.fromisoformat(lead_data['DATE_CREATE'].replace('Z', '+00:00'))
                except ValueError:
                    pass

            junk_status = lead_data.get(self.config.lead_status.junk_status_field)
            if junk_status is not None:
                try:
                    junk_status = int(junk_status)
                except (ValueError, TypeError):
                    junk_status = None

            lead = Lead(
                id=lead_id,
                title=lead_data.get('TITLE'),
                status_id=lead_data.get('STATUS_ID'),
                junk_status=junk_status,
                junk_status_name=self.junk_statuses.get(junk_status),
                date_create=date_create,
                phone=lead_data.get('PHONE'),
                email=lead_data.get('EMAIL'),
                name=lead_data.get('NAME'),
                raw_data=lead_data
            )

            db.add(lead)

        db.commit()
        db.refresh(lead)
        return lead

    def _save_analysis_to_db(self, db: Session, lead_id: str, result: LeadAnalysisResult):
        """Save analysis result to database"""
        analysis = AnalysisHistory(
            lead_id=lead_id,
            original_status=result.original_status,
            original_junk_status=result.original_junk_status,
            action=result.action.value if result.action else None,
            reason=result.reason.value if result.reason else None,
            new_status=result.new_status,
            new_junk_status=result.new_junk_status,
            unsuccessful_calls_count=result.unsuccessful_calls_count,
            transcription_success_rate=result.transcription_success_rate,
            total_processing_time=result.processing_time,
            is_successful=result.is_successful,
            requires_update=result.requires_update,
            error_message=result.error_message,
            dry_run=False  # Set based on actual run mode
        )

        # Add AI analysis data if available
        if result.ai_analysis:
            analysis.ai_suitable = result.ai_analysis.is_suitable
            analysis.ai_confidence = result.ai_analysis.confidence
            analysis.ai_reasoning = result.ai_analysis.reasoning
            analysis.ai_alternative_status = getattr(result.ai_analysis, 'alternative_status', None)
            analysis.ai_processing_time = result.ai_analysis.processing_time
            analysis.ai_model_used = result.ai_analysis.model_used

        db.add(analysis)

        # Update lead's analysis tracking
        lead = db.query(Lead).filter(Lead.id == lead_id).first()
        if lead:
            lead.last_analyzed = datetime.utcnow()
            lead.analysis_count = (lead.analysis_count or 0) + 1
            lead.last_analysis_result = result.action.value if result.action else None
            lead.last_analysis_reason = result.reason.value if result.reason else None
            lead.unsuccessful_calls_count = result.unsuccessful_calls_count

        db.commit()

    def get_new_leads_since_last_analysis(self) -> List[Dict[str, Any]]:
        """Get new leads since last analysis with database tracking"""
        with next(get_db()) as db:
            # Get last analysis time from database
            last_analysis_time_str = db_manager.get_config_value('last_analysis_time')

            if last_analysis_time_str:
                try:
                    last_analysis_time = datetime.fromisoformat(last_analysis_time_str)
                except ValueError:
                    last_analysis_time = datetime.now() - timedelta(days=1)
            else:
                last_analysis_time = datetime.now() - timedelta(days=1)

            self.logger.info(f"Checking for new leads since: {last_analysis_time}")

            # Create filter for new junk leads
            lead_filter = LeadFilter(
                status_id=self.config.lead_status.junk_status_value,
                junk_statuses=list(self.junk_statuses.keys()),
                date_from=last_analysis_time,
                limit=50
            )

            # Get leads from Bitrix24
            leads = self.bitrix_service.get_leads(lead_filter)

            # Convert to dict format and save to database
            leads_data = []
            for lead in leads:
                lead_dict = lead.raw_data

                # Save to database
                self._save_lead_to_db(db, lead_dict)
                leads_data.append(lead_dict)

            self.logger.info(f"Found {len(leads_data)} new leads")
            return leads_data

    def analyze_new_leads(self, dry_run: bool = False) -> BatchAnalysisResult:
        """Analyze new leads with database integration"""
        batch_id = f"new_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        batch_result = BatchAnalysisResult(batch_id=batch_id)

        with next(get_db()) as db:
            # Record scheduler state
            scheduler_state = SchedulerState(
                last_analysis_time=datetime.utcnow(),
                status='running',
                started_at=datetime.utcnow()
            )
            db.add(scheduler_state)
            db.commit()

            try:
                self.logger.info("Starting analysis of new leads with database caching")

                # Get new leads
                leads_data = self.get_new_leads_since_last_analysis()

                if not leads_data:
                    self.logger.info("No new leads found")
                    scheduler_state.status = 'completed'
                    scheduler_state.completed_at = datetime.utcnow()
                    db.commit()
                    batch_result.mark_completed()
                    return batch_result

                # Analyze each lead
                for lead_data in leads_data:
                    try:
                        lead_id = str(lead_data['ID'])
                        result = self._analyze_single_lead_with_db(lead_data, dry_run)
                        batch_result.add_result(result)

                        # Save analysis to database
                        self._save_analysis_to_db(db, lead_id, result)

                        # Small delay between leads
                        time.sleep(self.config.scheduler.delay_between_leads)

                    except Exception as e:
                        self.log_lead_action(lead_data['ID'], "analyze_error", f"Error: {e}")
                        error_result = LeadAnalysisResult(
                            lead_id=str(lead_data['ID']),
                            original_status=lead_data.get('STATUS_ID'),
                            original_junk_status=lead_data.get(self.config.lead_status.junk_status_field)
                        )
                        error_result.set_error(str(e))
                        batch_result.add_result(error_result)

                # Update last analysis time
                db_manager.set_config_value('last_analysis_time', datetime.utcnow().isoformat())

                # Update scheduler state
                scheduler_state.status = 'completed'
                scheduler_state.completed_at = datetime.utcnow()
                scheduler_state.leads_processed = batch_result.total_leads
                scheduler_state.leads_updated = batch_result.leads_updated
                scheduler_state.success_rate = batch_result.success_rate
                scheduler_state.processing_time = batch_result.total_processing_time

                db.commit()
                batch_result.mark_completed()

                self.logger.info(f"Analysis completed: {batch_result.success_rate:.2f} success rate")
                return batch_result

            except Exception as e:
                self.logger.error(f"Error in lead analysis: {e}")
                scheduler_state.status = 'failed'
                scheduler_state.error_message = str(e)
                scheduler_state.completed_at = datetime.utcnow()
                db.commit()
                raise LeadAnalyzerError(f"Lead analysis failed: {e}")

    def _analyze_single_lead_with_db(self, lead_data: Dict[str, Any], dry_run: bool = False) -> LeadAnalysisResult:
        """Analyze single lead with database integration"""
        lead_id = str(lead_data['ID'])
        junk_status = lead_data.get(self.config.lead_status.junk_status_field)

        # Convert junk status to int
        if junk_status is not None:
            try:
                junk_status = int(junk_status)
            except (ValueError, TypeError):
                junk_status = None

        result = LeadAnalysisResult(
            lead_id=lead_id,
            original_status=lead_data.get('STATUS_ID'),
            original_junk_status=junk_status
        )

        try:
            self.log_lead_action(lead_id, "analyze", f"Analyzing junk status {junk_status}")

            # Check if valid junk status
            if junk_status not in self.junk_statuses:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NOT_TARGET_STATUS)
                result.mark_completed()
                return result

            # Handle status 158 (5 unsuccessful calls)
            if junk_status == 158:
                result = self._analyze_unsuccessful_calls_with_db(lead_id, result, dry_run)
            else:
                # Other statuses: use AI analysis
                result = self._analyze_with_ai_and_db(lead_id, result, dry_run)

            result.mark_completed()
            return result

        except Exception as e:
            self.log_lead_action(lead_id, "analyze_error", f"Analysis error: {e}")
            result.set_error(str(e))
            return result

    def _analyze_unsuccessful_calls_with_db(self, lead_id: str, result: LeadAnalysisResult,
                                            dry_run: bool) -> LeadAnalysisResult:
        """Analyze unsuccessful calls for status 158"""
        try:
            # Get call statistics from Voximplant
            call_stats = self.bitrix_service.get_lead_call_statistics(lead_id)
            unsuccessful_calls = call_stats['unsuccessful_calls']

            result.unsuccessful_calls_count = unsuccessful_calls
            self.log_lead_action(lead_id, "call_analysis", f"Found {unsuccessful_calls} unsuccessful calls")

            if unsuccessful_calls >= 5:
                # Keep current junk status
                result.set_action(AnalysisAction.KEEP_STATUS, AnalysisReason.SUFFICIENT_CALLS)
                self.log_lead_action(lead_id, "decision", "Keeping status - sufficient calls")
            else:
                # Change to active status
                new_status = self.config.lead_status.active_status_value
                result.set_action(
                    AnalysisAction.CHANGE_STATUS,
                    AnalysisReason.INSUFFICIENT_CALLS,
                    new_status=new_status,
                    new_junk_status=None
                )

                self.log_lead_action(lead_id, "decision", "Changing status - insufficient calls")

                # Update lead status if not dry run
                if not dry_run:
                    success = self.bitrix_service.update_lead_complete(lead_id, new_status, None)
                    if not success:
                        result.set_error("Failed to update lead status")

            return result

        except Exception as e:
            result.set_error(f"Error analyzing calls: {e}")
            return result

    def _analyze_with_ai_and_db(self, lead_id: str, result: LeadAnalysisResult,
                                dry_run: bool) -> LeadAnalysisResult:
        """Analyze lead using AI with database caching"""
        try:
            # Get audio files from Voximplant
            voximplant_data = self.bitrix_service.get_voximplant_call_data(lead_id)

            audio_files = []
            for call_data in voximplant_data:
                if 'CALL_RECORD_URL' in call_data and call_data['CALL_FAILED_CODE'] == "200":
                    audio_files.append(call_data['CALL_RECORD_URL'])

            if not audio_files:
                result.set_action(AnalysisAction.SKIP, AnalysisReason.NO_AUDIO_FILES)
                self.log_lead_action(lead_id, "ai_analysis", "No audio files found")
                return result

            self.log_lead_action(lead_id, "ai_analysis", f"Found {len(audio_files)} audio files")

            # Analyze all audio files with caching
            transcription_results = []
            all_transcription_text = []

            for audio_file in audio_files:
                try:
                    # Use cached transcription service
                    transcription_result = self.transcription_service.transcribe_url(lead_id, audio_file)
                    transcription_results.append(transcription_result)
                    result.add_transcription_result(transcription_result)

                    if transcription_result.is_successful:
                        all_transcription_text.append(transcription_result.transcription)

                except Exception as e:
                    self.log_lead_action(lead_id, "transcription_error", f"Error transcribing {audio_file}: {e}")
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
                self.log_lead_action(lead_id, "ai_analysis", "No successful transcriptions")
                return result

            # Combine all transcriptions
            combined_transcription = "\n\n".join(all_transcription_text)

            self.log_lead_action(lead_id, "ai_analysis", f"Analyzing {len(successful_transcriptions)} transcriptions")

            # Analyze with Enhanced Gemini AI
            status_name = self.junk_statuses.get(result.original_junk_status, "Unknown")
            ai_result = self.gemini_service.analyze_lead_status(
                combined_transcription,
                result.original_junk_status,
                status_name
            )

            result.set_ai_analysis(ai_result)

            if not ai_result.is_successful:
                result.set_error(f"AI analysis failed: {ai_result.error}")
                return result

            # Enhanced decision logic with alternative status handling
            if ai_result.is_suitable:
                if hasattr(ai_result, 'alternative_status') and ai_result.alternative_status:
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
                        lead_id,
                        "decision",
                        f"Changing junk status from {result.original_junk_status} to {alternative_status} ({alternative_name})"
                    )

                    # Update lead status if not dry run
                    if not dry_run:
                        success = self.bitrix_service.update_lead_complete(
                            lead_id,
                            self.config.lead_status.junk_status_value,
                            alternative_status
                        )
                        if not success:
                            result.set_error("Failed to update lead status")
                else:
                    # Keep current junk status - AI says it's suitable
                    result.set_action(AnalysisAction.KEEP_STATUS, AnalysisReason.AI_SUITABLE)
                    self.log_lead_action(lead_id, "decision", "Keeping status - AI says suitable")
            else:
                # Change to active status - AI says lead is not junk at all
                new_status = self.config.lead_status.active_status_value
                result.set_action(
                    AnalysisAction.CHANGE_STATUS,
                    AnalysisReason.AI_NOT_SUITABLE,
                    new_status=new_status,
                    new_junk_status=None
                )

                self.log_lead_action(lead_id, "decision", "Changing status to NEW - AI says not junk")

                # Update lead status if not dry run
                if not dry_run:
                    success = self.bitrix_service.update_lead_complete(lead_id, new_status, None)
                    if not success:
                        result.set_error("Failed to update lead status")

            # Log AI reasoning
            if ai_result.reasoning:
                self.log_lead_action(lead_id, "ai_reasoning", f"AI Decision Details:\n{ai_result.reasoning}")

            return result

        except Exception as e:
            self.logger.error(f"Error in AI analysis: {e}")
            result.set_error(f"Error in AI analysis: {e}")
            return result

    def analyze_lead_by_id(self, lead_id: str, dry_run: bool = False) -> Optional[LeadAnalysisResult]:
        """Analyze a specific lead by ID with database integration"""
        try:
            self.log_lead_action(lead_id, "analyze_start", "Starting lead analysis")

            # Get lead data from Bitrix24
            lead = self.bitrix_service.get_lead_by_id(lead_id)

            if not lead:
                self.log_lead_action(lead_id, "analyze_error", "Lead not found")
                return None

            # Save lead to database
            with next(get_db()) as db:
                self._save_lead_to_db(db, lead.raw_data)

            # Analyze the lead
            result = self._analyze_single_lead_with_db(lead.raw_data, dry_run)

            # Save analysis to database
            with next(get_db()) as db:
                self._save_analysis_to_db(db, lead_id, result)

            self.log_lead_action(lead_id, "analyze_complete",
                                 f"Analysis completed: {result.action.value if result.action else 'unknown'}")
            return result

        except Exception as e:
            self.log_lead_action(lead_id, "analyze_error", f"Error analyzing lead: {e}")
            error_result = LeadAnalysisResult(lead_id=lead_id)
            error_result.set_error(str(e))
            return error_result

    def get_analysis_statistics(self) -> Dict[str, Any]:
        """Get comprehensive analysis statistics from database"""
        with next(get_db()) as db:
            # Lead statistics
            total_leads = db.query(Lead).count()
            analyzed_leads = db.query(Lead).filter(Lead.last_analyzed.isnot(None)).count()

            # Analysis history statistics
            total_analyses = db.query(AnalysisHistory).count()
            successful_analyses = db.query(AnalysisHistory).filter(AnalysisHistory.is_successful == True).count()
            leads_updated = db.query(AnalysisHistory).filter(AnalysisHistory.requires_update == True).count()

            # Transcription cache statistics
            transcription_stats = self.transcription_service.get_cache_statistics()

            # Recent scheduler runs
            recent_runs = db.query(SchedulerState).order_by(SchedulerState.created_at.desc()).limit(5).all()

            return {
                'leads': {
                    'total': total_leads,
                    'analyzed': analyzed_leads,
                    'analysis_coverage': analyzed_leads / total_leads if total_leads > 0 else 0.0
                },
                'analyses': {
                    'total': total_analyses,
                    'successful': successful_analyses,
                    'leads_updated': leads_updated,
                    'success_rate': successful_analyses / total_analyses if total_analyses > 0 else 0.0
                },
                'transcription_cache': transcription_stats,
                'recent_scheduler_runs': [
                    {
                        'started_at': run.started_at.isoformat() if run.started_at else None,
                        'completed_at': run.completed_at.isoformat() if run.completed_at else None,
                        'status': run.status,
                        'leads_processed': run.leads_processed,
                        'leads_updated': run.leads_updated,
                        'success_rate': run.success_rate
                    }
                    for run in recent_runs
                ]
            }

    def get_lead_history(self, lead_id: str) -> Dict[str, Any]:
        """Get complete history for a specific lead"""
        with next(get_db()) as db:
            # Get lead info
            lead = db.query(Lead).filter(Lead.id == lead_id).first()
            if not lead:
                return {'error': 'Lead not found'}

            # Get analysis history
            analyses = db.query(AnalysisHistory).filter(
                AnalysisHistory.lead_id == lead_id
            ).order_by(AnalysisHistory.analysis_date.desc()).all()

            # Get transcriptions
            transcriptions = db.query(Transcription).filter(
                Transcription.lead_id == lead_id
            ).order_by(Transcription.created_at.desc()).all()

            return {
                'lead': {
                    'id': lead.id,
                    'title': lead.title,
                    'status_id': lead.status_id,
                    'junk_status': lead.junk_status,
                    'junk_status_name': lead.junk_status_name,
                    'date_create': lead.date_create.isoformat() if lead.date_create else None,
                    'last_analyzed': lead.last_analyzed.isoformat() if lead.last_analyzed else None,
                    'analysis_count': lead.analysis_count,
                    'last_analysis_result': lead.last_analysis_result,
                    'unsuccessful_calls_count': lead.unsuccessful_calls_count
                },
                'analysis_history': [
                    {
                        'analysis_date': analysis.analysis_date.isoformat(),
                        'action': analysis.action,
                        'reason': analysis.reason,
                        'original_junk_status': analysis.original_junk_status,
                        'new_junk_status': analysis.new_junk_status,
                        'ai_suitable': analysis.ai_suitable,
                        'ai_reasoning': analysis.ai_reasoning,
                        'ai_alternative_status': analysis.ai_alternative_status,
                        'processing_time': analysis.total_processing_time,
                        'is_successful': analysis.is_successful
                    }
                    for analysis in analyses
                ],
                'transcriptions': [
                    {
                        'audio_url': trans.audio_url,
                        'transcription_text': trans.transcription_text[:200] + '...' if len(
                            trans.transcription_text) > 200 else trans.transcription_text,
                        'confidence': trans.confidence,
                        'is_successful': trans.is_successful,
                        'created_at': trans.created_at.isoformat()
                    }
                    for trans in transcriptions
                ]
            }

    def cleanup_old_data(self, days: int = 30):
        """Clean up old analysis and transcription data"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        with next(get_db()) as db:
            # Clean old analysis history
            old_analyses = db.query(AnalysisHistory).filter(
                AnalysisHistory.analysis_date < cutoff_date
            ).count()

            db.query(AnalysisHistory).filter(
                AnalysisHistory.analysis_date < cutoff_date
            ).delete()

            # Clean old scheduler states
            old_scheduler_states = db.query(SchedulerState).filter(
                SchedulerState.created_at < cutoff_date
            ).count()

            db.query(SchedulerState).filter(
                SchedulerState.created_at < cutoff_date
            ).delete()

            # Keep transcriptions as they are valuable cache

            db.commit()

            self.logger.info(f"Cleaned up {old_analyses} old analyses and {old_scheduler_states} old scheduler states")

    def check_health(self) -> Dict[str, bool]:
        """Check health of all services including database"""
        health_status = {}

        # Check Bitrix service
        try:
            health_status['bitrix'] = self.bitrix_service.test_connection()
        except Exception:
            health_status['bitrix'] = False

        # Check transcription service
        try:
            response = requests.get("http://127.0.0.1:8101", timeout=5)
            health_status['transcription'] = response.status_code == 200
        except Exception:
            health_status['transcription'] = False

        # Check Gemini service
        try:
            health_status['gemini'] = self.gemini_service.test_connection()
        except Exception:
            health_status['gemini'] = False

        # Check database
        try:
            with next(get_db()) as db:
                db.execute("SELECT 1")
                health_status['database'] = True
        except Exception:
            health_status['database'] = False

        return health_status

    def close(self):
        """Close all services and cleanup resources"""
        try:
            self.bitrix_service.close()
        except Exception as e:
            self.logger.warning(f"Error closing Bitrix service: {e}")

        try:
            self.transcription_service.session.close()
        except Exception as e:
            self.logger.warning(f"Error closing transcription service: {e}")

        try:
            self.gemini_service.close()
        except Exception as e:
            self.logger.warning(f"Error closing Gemini service: {e}")

        self.log_service_action("EnhancedLeadAnalyzerWithDB", "close", "Service closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()