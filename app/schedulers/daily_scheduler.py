"""
Daily scheduler for automated lead analysis
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Optional, Callable

from app.config import get_config
from app.logger import LoggerMixin
from app.services.lead_analyzer import LeadAnalyzerService
from app.utils.exceptions import SchedulerError


class DailyScheduler(LoggerMixin):
    """Scheduler for running daily lead analysis"""

    def __init__(self):
        self.config = get_config().scheduler
        self.analyzer = LeadAnalyzerService()

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self.last_run_time: Optional[datetime] = None
        self.next_run_time: Optional[datetime] = None

        self.log_service_action("DailyScheduler", "init",
                                f"Initialized scheduler with {self.config.check_interval_hours}h interval")

    def start(self):
        """Start the scheduler"""
        if self._running:
            self.logger.warning("Scheduler is already running")
            return

        self.logger.info("Starting daily scheduler...")

        self._running = True
        self._stop_event.clear()

        # Calculate next run time
        self._calculate_next_run_time()

        # Start scheduler thread
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._thread.start()

        self.log_service_action("DailyScheduler", "start", "Scheduler started successfully")

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

        self.log_service_action("DailyScheduler", "stop", "Scheduler stopped")

    def _scheduler_loop(self):
        """Main scheduler loop"""
        self.logger.info(f"Scheduler loop started. Next run: {self.next_run_time}")

        while self._running and not self._stop_event.is_set():
            try:
                # Check if it's time to run
                current_time = datetime.now()

                if current_time >= self.next_run_time:
                    self._run_analysis()
                    self._calculate_next_run_time()

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

    def _run_analysis(self):
        """Run the lead analysis"""
        try:
            self.logger.info("Starting scheduled lead analysis")

            start_time = datetime.now()
            self.last_run_time = start_time

            # Run analysis of new leads
            batch_result = self.analyzer.analyze_new_leads()

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Log results
            self.logger.info(f"Scheduled analysis completed in {processing_time:.2f} seconds")
            self.logger.info(f"Processed {batch_result.total_leads} leads")
            self.logger.info(f"Success rate: {batch_result.success_rate:.2f}")
            self.logger.info(f"Leads updated: {batch_result.leads_updated}")

            # Log detailed statistics
            self._log_analysis_statistics(batch_result)

        except Exception as e:
            self.logger.error(f"Scheduled analysis failed: {e}")
            raise SchedulerError(f"Scheduled analysis failed: {e}")

    def _calculate_next_run_time(self):
        """Calculate the next run time"""
        if self.last_run_time:
            self.next_run_time = self.last_run_time + timedelta(hours=self.config.check_interval_hours)
        else:
            # First run - start immediately or after a short delay
            self.next_run_time = datetime.now() + timedelta(minutes=1)

        self.logger.info(f"Next scheduled run: {self.next_run_time}")

    def _log_analysis_statistics(self, batch_result):
        """Log detailed analysis statistics"""
        from app.models.analysis_result import AnalysisAction, AnalysisReason

        # Count results by action
        action_counts = {}
        for action in AnalysisAction:
            count = len(batch_result.get_results_by_action(action))
            if count > 0:
                action_counts[action.value] = count

        if action_counts:
            self.logger.info(f"Actions taken: {action_counts}")

        # Count results by reason
        reason_counts = {}
        for reason in AnalysisReason:
            count = len(batch_result.get_results_by_reason(reason))
            if count > 0:
                reason_counts[reason.value] = count

        if reason_counts:
            self.logger.info(f"Analysis reasons: {reason_counts}")

        # Log processing time statistics
        if batch_result.average_processing_time > 0:
            self.logger.info(f"Average processing time per lead: {batch_result.average_processing_time:.2f}s")

    def force_run(self):
        """Force an immediate analysis run"""
        if not self._running:
            raise SchedulerError("Scheduler is not running")

        self.logger.info("Forcing immediate analysis run")

        try:
            self._run_analysis()
            self._calculate_next_run_time()
        except Exception as e:
            self.logger.error(f"Forced analysis run failed: {e}")
            raise

    def get_status(self) -> dict:
        """Get scheduler status"""
        return {
            'running': self._running,
            'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
            'next_run_time': self.next_run_time.isoformat() if self.next_run_time else None,
            'check_interval_hours': self.config.check_interval_hours,
            'thread_alive': self._thread.is_alive() if self._thread else False
        }

    def update_interval(self, new_interval_hours: int):
        """Update the check interval"""
        if new_interval_hours <= 0:
            raise ValueError("Interval must be positive")

        old_interval = self.config.check_interval_hours
        self.config.check_interval_hours = new_interval_hours

        # Recalculate next run time
        self._calculate_next_run_time()

        self.logger.info(f"Updated check interval from {old_interval}h to {new_interval_hours}h")
        self.logger.info(f"Next run rescheduled to: {self.next_run_time}")

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()