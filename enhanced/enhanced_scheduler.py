"""
Enhanced Daily Scheduler for Bitrix24 Lead Analysis
"""

import time
import schedule
import threading
from datetime import datetime, timedelta
from typing import Optional, Callable

from app.config import get_config
from app.logger import LoggerMixin
from app.utils.exceptions import SchedulerError


class EnhancedDailyScheduler(LoggerMixin):
    """Enhanced scheduler for daily lead analysis with flexible scheduling"""

    def __init__(self, analyzer_service=None):
        self.config = get_config().scheduler

        # Import here to avoid circular imports
        if analyzer_service is None:
            from enhanced_lead_analyzer import EnhancedLeadAnalyzerService
            self.analyzer = EnhancedLeadAnalyzerService()
        else:
            self.analyzer = analyzer_service

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self.last_run_time: Optional[datetime] = None
        self.next_run_time: Optional[datetime] = None

        # Setup schedule
        schedule.every().day.at("09:00").do(self._scheduled_analysis)  # Run at 9 AM daily
        # Alternative: schedule.every(self.config.check_interval_hours).hours.do(self._scheduled_analysis)

        self.log_service_action("EnhancedDailyScheduler", "init",
                                f"Initialized scheduler to run daily at 09:00")

    def start(self):
        """Start the scheduler"""
        if self._running:
            self.logger.warning("Scheduler is already running")
            return

        self.logger.info("Starting enhanced daily scheduler...")

        self._running = True
        self._stop_event.clear()

        # Calculate next run time
        self._calculate_next_run_time()

        # Start scheduler thread
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._thread.start()

        self.log_service_action("EnhancedDailyScheduler", "start", "Scheduler started successfully")

    def stop(self, timeout: float = 30.0):
        """Stop the scheduler"""
        if not self._running:
            return

        self.logger.info("Stopping scheduler...")

        self._running = False
        self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

            if self._thread.is_alive():
                self.logger.warning("Scheduler thread did not stop within timeout")

        # Clear scheduled jobs
        schedule.clear()

        self.log_service_action("EnhancedDailyScheduler", "stop", "Scheduler stopped")

    def _scheduler_loop(self):
        """Main scheduler loop using schedule library"""
        self.logger.info(f"Scheduler loop started. Next run: {self.next_run_time}")

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
                # Wait 5 minutes before retrying
                if self._stop_event.wait(300):
                    break

        self.logger.info("Scheduler loop ended")

    def _scheduled_analysis(self):
        """Scheduled analysis job"""
        try:
            self.logger.info("Starting scheduled lead analysis")
            start_time = datetime.now()
            self.last_run_time = start_time

            # Check for new leads and run analysis
            batch_result = self.analyzer.analyze_new_leads()

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Log comprehensive results
            self._log_analysis_results(batch_result, processing_time)

            # Calculate next run time
            self._calculate_next_run_time()

        except Exception as e:
            self.logger.error(f"Scheduled analysis failed: {e}")
            raise SchedulerError(f"Scheduled analysis failed: {e}")

    def _log_analysis_results(self, batch_result, processing_time: float):
        """Log detailed analysis results"""
        self.logger.info(f"Scheduled analysis completed in {processing_time:.2f} seconds")
        self.logger.info(f"Total leads processed: {batch_result.total_leads}")
        self.logger.info(f"Success rate: {batch_result.success_rate:.2f}")
        self.logger.info(f"Leads updated: {batch_result.leads_updated}")
        self.logger.info(f"Leads kept: {batch_result.leads_kept}")
        self.logger.info(f"Leads skipped: {batch_result.leads_skipped}")

        if batch_result.failed_analyses > 0:
            self.logger.warning(f"Failed analyses: {batch_result.failed_analyses}")

        # Log breakdown by action
        from app.models.analysis_result import AnalysisAction
        action_summary = {}
        for action in AnalysisAction:
            count = len(batch_result.get_results_by_action(action))
            if count > 0:
                action_summary[action.value] = count

        if action_summary:
            self.logger.info(f"Action breakdown: {action_summary}")

    def _calculate_next_run_time(self):
        """Calculate the next scheduled run time"""
        # Get next scheduled job time
        next_job = schedule.next_run()
        if next_job:
            self.next_run_time = next_job
            self.logger.info(f"Next scheduled run: {self.next_run_time}")
        else:
            # Fallback: calculate based on interval
            if self.last_run_time:
                self.next_run_time = self.last_run_time + timedelta(hours=self.config.check_interval_hours)
            else:
                self.next_run_time = datetime.now() + timedelta(hours=self.config.check_interval_hours)

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

    def get_status(self) -> dict:
        """Get detailed scheduler status"""
        return {
            'running': self._running,
            'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
            'next_run_time': self.next_run_time.isoformat() if self.next_run_time else None,
            'check_interval_hours': self.config.check_interval_hours,
            'thread_alive': self._thread.is_alive() if self._thread else False,
            'scheduled_jobs': len(schedule.jobs),
            'next_scheduled_job': schedule.next_run().isoformat() if schedule.next_run() else None
        }

    def add_custom_schedule(self, time_str: str, job_func: Callable = None):
        """Add custom schedule time"""
        if job_func is None:
            job_func = self._scheduled_analysis

        schedule.every().day.at(time_str).do(job_func)
        self.logger.info(f"Added custom schedule at {time_str}")

    def set_interval_schedule(self, hours: int):
        """Set interval-based scheduling instead of daily"""
        schedule.clear()
        schedule.every(hours).hours.do(self._scheduled_analysis)
        self.logger.info(f"Set interval schedule: every {hours} hours")

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()


# Standalone function for cron job or external scheduling
def run_daily_analysis():
    """
    Standalone function that can be called from cron or external scheduler
    Usage in cron: 0 9 * * * /usr/bin/python3 /path/to/scheduler.py
    """
    import sys
    import os

    # Add project root to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from enhanced_lead_analyzer import EnhancedLeadAnalyzerService
    from app.logger import get_logger

    logger = get_logger('CronAnalysis')

    try:
        logger.info("Starting cron-triggered lead analysis")

        with EnhancedLeadAnalyzerService() as analyzer:
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

    sys.exit(run_daily_analysis())