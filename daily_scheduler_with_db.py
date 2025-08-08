"""
Enhanced Daily Scheduler with Database Integration
"""

import time
import schedule
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from database_models import SchedulerState, db_manager, get_db
from app.config import get_config
from app.logger import LoggerMixin
from app.utils.exceptions import SchedulerError


class EnhancedDailySchedulerWithDB(LoggerMixin):
    """Enhanced daily scheduler with database state tracking"""

    def __init__(self, analyzer_service=None):
        self.config = get_config().scheduler

        # Import here to avoid circular imports
        if analyzer_service is None:
            from enhanced_analyzer_with_db import EnhancedLeadAnalyzerWithDB
            self.analyzer = EnhancedLeadAnalyzerWithDB()
        else:
            self.analyzer = analyzer_service

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Initialize database
        db_manager.init_system_config()

        # Setup default schedule
        schedule.every().day.at("09:00").do(self._scheduled_analysis)

        self.log_service_action("EnhancedDailySchedulerWithDB", "init",
                                "Initialized scheduler with database integration")

    def start(self):
        """Start the scheduler"""
        if self._running:
            self.logger.warning("Scheduler is already running")
            return

        self.logger.info("Starting enhanced daily scheduler with database integration...")

        self._running = True
        self._stop_event.clear()

        # Start scheduler thread
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._thread.start()

        self.log_service_action("EnhancedDailySchedulerWithDB", "start", "Scheduler started successfully")

    def stop(self, timeout: float = 30.0):
        """Stop the scheduler"""
        if not self._running:
            return

        self.logger.info("Stopping enhanced scheduler...")

        self._running = False
        self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

            if self._thread.is_alive():
                self.logger.warning("Scheduler thread did not stop within timeout")

        # Clear scheduled jobs
        schedule.clear()

        self.log_service_action("EnhancedDailySchedulerWithDB", "stop", "Scheduler stopped")

    def _scheduler_loop(self):
        """Main scheduler loop with database state tracking"""
        self.logger.info("Scheduler loop started with database integration")

        while self._running and not self._stop_event.is_set():
            try:
                # Run pending scheduled jobs
                schedule.run_pending()

                # Sleep for 1 minute before checking again
                if not self._stop_event.wait(60):
                    continue
                else:
                    break

            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")

                # Log error to database
                with next(get_db()) as db:
                    error_state = SchedulerState(
                        last_analysis_time=datetime.utcnow(),
                        status='failed',
                        error_message=str(e),
                        started_at=datetime.utcnow(),
                        completed_at=datetime.utcnow()
                    )
                    db.add(error_state)
                    db.commit()

                # Wait 5 minutes before retrying
                if self._stop_event.wait(300):
                    break

        self.logger.info("Enhanced scheduler loop ended")

    def _scheduled_analysis(self):
        """Scheduled analysis job with database state tracking"""
        try:
            self.logger.info("Starting scheduled lead analysis with database integration")
            start_time = datetime.utcnow()

            # Check for new leads and run analysis
            batch_result = self.analyzer.analyze_new_leads()

            end_time = datetime.utcnow()
            processing_time = (end_time - start_time).total_seconds()

            # Log comprehensive results
            self._log_analysis_results(batch_result, processing_time)

            # Update system configuration
            db_manager.set_config_value('last_scheduled_run', end_time.isoformat())

            # Clean up old data periodically (every 7 days)
            last_cleanup = db_manager.get_config_value('last_cleanup')
            if not last_cleanup or (datetime.utcnow() - datetime.fromisoformat(last_cleanup)).days >= 7:
                self.logger.info("Running periodic data cleanup")
                self.analyzer.cleanup_old_data(days=30)
                db_manager.set_config_value('last_cleanup', datetime.utcnow().isoformat())

        except Exception as e:
            self.logger.error(f"Scheduled analysis failed: {e}")
            raise SchedulerError(f"Scheduled analysis failed: {e}")

    def _log_analysis_results(self, batch_result, processing_time: float):
        """Log detailed analysis results with database integration"""
        self.logger.info(f"Scheduled analysis completed in {processing_time:.2f} seconds")
        self.logger.info(f"Total leads processed: {batch_result.total_leads}")
        self.logger.info(f"Success rate: {batch_result.success_rate:.2f}")
        self.logger.info(f"Leads updated: {batch_result.leads_updated}")
        self.logger.info(f"Leads kept: {batch_result.leads_kept}")
        self.logger.info(f"Leads skipped: {batch_result.leads_skipped}")

        if batch_result.failed_analyses > 0:
            self.logger.warning(f"Failed analyses: {batch_result.failed_analyses}")

        # Log action breakdown
        from app.models.analysis_result import AnalysisAction
        action_summary = {}
        for action in AnalysisAction:
            count = len(batch_result.get_results_by_action(action))
            if count > 0:
                action_summary[action.value] = count

        if action_summary:
            self.logger.info(f"Action breakdown: {action_summary}")

        # Update system statistics
        total_processed = int(db_manager.get_config_value('total_leads_processed', '0')) + batch_result.total_leads
        db_manager.set_config_value('total_leads_processed', str(total_processed))

    def force_run(self):
        """Force an immediate analysis run"""
        if not self._running:
            raise SchedulerError("Scheduler is not running")

        self.logger.info("Forcing immediate analysis run")

        try:
            self._scheduled_analysis()
        except Exception as e:
            self.logger.error(f"Forced analysis run failed: {e}")
            raise

    def get_status(self) -> Dict[str, Any]:
        """Get detailed scheduler status from database"""
        with next(get_db()) as db:
            # Get recent scheduler states
            recent_states = db.query(SchedulerState).order_by(
                SchedulerState.created_at.desc()
            ).limit(5).all()

            # Get system configuration
            last_analysis = db_manager.get_config_value('last_analysis_time')
            last_scheduled_run = db_manager.get_config_value('last_scheduled_run')
            total_processed = db_manager.get_config_value('total_leads_processed', '0')

            return {
                'running': self._running,
                'thread_alive': self._thread.is_alive() if self._thread else False,
                'scheduled_jobs': len(schedule.jobs),
                'next_scheduled_job': schedule.next_run().isoformat() if schedule.next_run() else None,
                'last_analysis_time': last_analysis,
                'last_scheduled_run': last_scheduled_run,
                'total_leads_processed': int(total_processed),
                'recent_runs': [
                    {
                        'started_at': state.started_at.isoformat() if state.started_at else None,
                        'completed_at': state.completed_at.isoformat() if state.completed_at else None,
                        'status': state.status,
                        'leads_processed': state.leads_processed,
                        'leads_updated': state.leads_updated,
                        'success_rate': state.success_rate,
                        'processing_time': state.processing_time,
                        'error_message': state.error_message
                    }
                    for state in recent_states
                ]
            }

    def add_custom_schedule(self, time_str: str):
        """Add custom schedule time"""
        schedule.every().day.at(time_str).do(self._scheduled_analysis)
        self.logger.info(f"Added custom schedule at {time_str}")

    def set_interval_schedule(self, hours: int):
        """Set interval-based scheduling instead of daily"""
        schedule.clear()
        schedule.every(hours).hours.do(self._scheduled_analysis)
        self.logger.info(f"Set interval schedule: every {hours} hours")

    def get_analytics_dashboard_data(self) -> Dict[str, Any]:
        """Get data for analytics dashboard"""
        with next(get_db()) as db:
            # Import here to avoid circular imports
            from database_models import Lead, AnalysisHistory, Transcription

            # Lead statistics by junk status
            junk_status_stats = {}
            junk_statuses = {
                158: "5 marta javob bermadi",
                227: "Notog'ri raqam",
                229: "Ariza qoldirmagan",
                783: "Notog'ri mijoz",
                807: "Yoshi to'g'ri kelmadi"
            }

            for status_code, status_name in junk_statuses.items():
                count = db.query(Lead).filter(Lead.junk_status == status_code).count()
                junk_status_stats[status_name] = count

            # Analysis results over time (last 30 days)
            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
            daily_analysis_counts = db.query(AnalysisHistory).filter(
                AnalysisHistory.analysis_date >= thirty_days_ago
            ).count()

            # Success rates by analysis reason
            from app.models.analysis_result import AnalysisReason
            reason_stats = {}
            for reason in AnalysisReason:
                count = db.query(AnalysisHistory).filter(
                    AnalysisHistory.reason == reason.value
                ).count()
                if count > 0:
                    reason_stats[reason.value] = count

            # Transcription cache efficiency
            total_transcriptions = db.query(Transcription).count()
            successful_transcriptions = db.query(Transcription).filter(
                Transcription.is_successful == True
            ).count()

            # Recent performance metrics
            recent_runs = db.query(SchedulerState).filter(
                SchedulerState.status == 'completed'
            ).order_by(SchedulerState.completed_at.desc()).limit(10).all()

            avg_processing_time = 0
            avg_success_rate = 0
            if recent_runs:
                avg_processing_time = sum(run.processing_time or 0 for run in recent_runs) / len(recent_runs)
                avg_success_rate = sum(run.success_rate or 0 for run in recent_runs) / len(recent_runs)

            return {
                'overview': {
                    'total_leads': db.query(Lead).count(),
                    'total_analyses': db.query(AnalysisHistory).count(),
                    'total_transcriptions': total_transcriptions,
                    'cache_success_rate': successful_transcriptions / total_transcriptions if total_transcriptions > 0 else 0.0,
                    'avg_processing_time': avg_processing_time,
                    'avg_success_rate': avg_success_rate
                },
                'junk_status_distribution': junk_status_stats,
                'analysis_reasons': reason_stats,
                'recent_performance': [
                    {
                        'date': run.completed_at.date().isoformat() if run.completed_at else None,
                        'leads_processed': run.leads_processed,
                        'success_rate': run.success_rate,
                        'processing_time': run.processing_time
                    }
                    for run in recent_runs
                ]
            }

    def export_analysis_data(self, days: int = 30, format: str = 'json') -> Dict[str, Any]:
        """Export analysis data for reporting"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        with next(get_db()) as db:
            # Import here to avoid circular imports
            from database_models import Lead, AnalysisHistory, Transcription

            # Get analysis data
            analyses = db.query(AnalysisHistory).filter(
                AnalysisHistory.analysis_date >= cutoff_date
            ).all()

            # Get leads data
            analyzed_lead_ids = [analysis.lead_id for analysis in analyses]
            leads = db.query(Lead).filter(Lead.id.in_(analyzed_lead_ids)).all()

            # Format data
            export_data = {
                'export_date': datetime.utcnow().isoformat(),
                'period_days': days,
                'total_analyses': len(analyses),
                'leads': [
                    {
                        'id': lead.id,
                        'title': lead.title,
                        'junk_status': lead.junk_status,
                        'junk_status_name': lead.junk_status_name,
                        'analysis_count': lead.analysis_count,
                        'last_analyzed': lead.last_analyzed.isoformat() if lead.last_analyzed else None,
                        'last_result': lead.last_analysis_result
                    }
                    for lead in leads
                ],
                'analyses': [
                    {
                        'lead_id': analysis.lead_id,
                        'analysis_date': analysis.analysis_date.isoformat(),
                        'action': analysis.action,
                        'reason': analysis.reason,
                        'original_junk_status': analysis.original_junk_status,
                        'new_junk_status': analysis.new_junk_status,
                        'ai_suitable': analysis.ai_suitable,
                        'ai_reasoning': analysis.ai_reasoning[:500] if analysis.ai_reasoning else None,
                        # Truncate for export
                        'processing_time': analysis.total_processing_time,
                        'is_successful': analysis.is_successful
                    }
                    for analysis in analyses
                ]
            }

            return export_data

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()


# Standalone function for cron job or external scheduling
def run_daily_analysis_with_db():
    """
    Standalone function that can be called from cron or external scheduler
    Usage in cron: 0 9 * * * /usr/bin/python3 /path/to/scheduler.py
    """
    import sys
    import os

    # Add project root to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from enhanced_analyzer_with_db import EnhancedLeadAnalyzerWithDB
    from app.logger import get_logger

    logger = get_logger('CronAnalysis')

    try:
        logger.info("Starting cron-triggered lead analysis with database integration")

        with EnhancedLeadAnalyzerWithDB() as analyzer:
            batch_result = analyzer.analyze_new_leads()

            logger.info(f"Cron analysis completed: {batch_result.total_leads} leads processed")
            logger.info(f"Success rate: {batch_result.success_rate:.2f}")
            logger.info(f"Leads updated: {batch_result.leads_updated}")

            return 0  # Success

    except Exception as e:
        logger.error(f"Cron analysis failed: {e}")
        return 1  # Error


if __name__ == "__main__":
    """Run as standalone script"""
    import sys

    sys.exit(run_daily_analysis_with_db())