"""
Main entry point for Bitrix24 Lead Analyzer application
"""

import argparse
import sys
import time
from datetime import datetime
from typing import Optional

from app.config import get_config, validate_config
from app.logger import get_logger, setup_logging
from app.services.lead_analyzer import LeadAnalyzerService
from app.schedulers.daily_scheduler import DailyScheduler
from app.utils.exceptions import LeadAnalyzerError


def setup_argument_parser() -> argparse.ArgumentParser:
    """Setup command line argument parser"""
    parser = argparse.ArgumentParser(
        description='Bitrix24 Lead Analyzer - Automated junk lead analysis and status updates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode scheduled                    # Run continuous scheduled analysis
  %(prog)s --mode single                       # Run single analysis cycle
  %(prog)s --mode all-junk                     # Analyze all existing junk leads
  %(prog)s --mode single --lead-id 123         # Analyze specific lead
  %(prog)s --config-test                       # Test configuration
  %(prog)s --health-check                      # Check service health
        """
    )

    parser.add_argument(
        '--mode',
        choices=['scheduled', 'single', 'all-junk', 'test'],
        default='scheduled',
        help='Run mode (default: scheduled)'
    )

    parser.add_argument(
        '--lead-id',
        type=str,
        help='Specific lead ID to analyze (for single mode)'
    )

    parser.add_argument(
        '--config-test',
        action='store_true',
        help='Test configuration and exit'
    )

    parser.add_argument(
        '--health-check',
        action='store_true',
        help='Check health of all services and exit'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of leads to process in parallel (default: 10)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform analysis without updating lead statuses'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--log-file',
        type=str,
        help='Override log file path'
    )

    return parser


def test_configuration() -> bool:
    """Test application configuration"""
    logger = get_logger('ConfigTest')

    logger.info("Testing application configuration...")

    try:
        # Validate configuration
        config = get_config()
        if not validate_config():
            logger.error("Configuration validation failed")
            return False

        logger.info("✅ Configuration validation passed")

        # Display configuration summary
        logger.info("Configuration summary:")
        logger.info(f"  - Bitrix webhook URL: {config.bitrix.webhook_url}")
        logger.info(f"  - Transcription service: {config.transcription.service_url}")
        logger.info(f"  - Gemini model: {config.gemini.model_name}")
        logger.info(f"  - Check interval: {config.scheduler.check_interval_hours} hours")
        logger.info(f"  - Junk status field: {config.lead_status.junk_status_field}")

        return True

    except Exception as e:
        logger.error(f"Configuration test failed: {e}")
        return False


def health_check() -> bool:
    """Check health of all services"""
    logger = get_logger('HealthCheck')

    logger.info("Performing health check...")

    try:
        analyzer = LeadAnalyzerService()
        health_status = analyzer.check_health()

        logger.info("Health check results:")
        for service, status in health_status.items():
            status_icon = "✅" if status else "❌"
            logger.info(f"  {status_icon} {service}: {'OK' if status else 'FAILED'}")

        overall_health = all(health_status.values())

        if overall_health:
            logger.info("🎉 All services are healthy!")
        else:
            logger.warning("⚠️  Some services are not healthy")

        return overall_health

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


def run_single_analysis(lead_id: Optional[str] = None, dry_run: bool = False) -> bool:
    """Run single analysis cycle"""
    logger = get_logger('SingleAnalysis')

    try:
        analyzer = LeadAnalyzerService()

        if lead_id:
            logger.info(f"Analyzing specific lead: {lead_id}")
            result = analyzer.analyze_lead_by_id(lead_id, dry_run=dry_run)

            if result:
                logger.info(f"Analysis completed: {result.action.value if result.action else 'unknown'}")
                return result.is_successful
            else:
                logger.error("Lead not found or analysis failed")
                return False
        else:
            logger.info("Running single analysis cycle for new leads")
            batch_result = analyzer.analyze_new_leads(dry_run=dry_run)

            logger.info(f"Analysis completed: {batch_result.total_leads} leads processed")
            logger.info(f"Success rate: {batch_result.success_rate:.2f}")
            logger.info(f"Leads updated: {batch_result.leads_updated}")

            return batch_result.success_rate > 0.5  # 50% success rate threshold

    except Exception as e:
        logger.error(f"Single analysis failed: {e}")
        return False


def run_all_junk_analysis(dry_run: bool = False) -> bool:
    """Analyze all existing junk leads"""
    logger = get_logger('AllJunkAnalysis')

    try:
        analyzer = LeadAnalyzerService()

        logger.info("Starting analysis of all junk leads...")
        batch_result = analyzer.analyze_all_junk_leads(dry_run=dry_run)

        logger.info(f"Analysis completed: {batch_result.total_leads} leads processed")
        logger.info(f"Success rate: {batch_result.success_rate:.2f}")
        logger.info(f"Leads updated: {batch_result.leads_updated}")
        logger.info(f"Processing time: {batch_result.total_processing_time:.2f} seconds")

        return batch_result.success_rate > 0.5

    except Exception as e:
        logger.error(f"All junk analysis failed: {e}")
        return False


def run_scheduled_mode() -> None:
    """Run continuous scheduled analysis"""
    logger = get_logger('ScheduledMode')

    try:
        logger.info("Starting scheduled mode...")

        # Create and start scheduler
        scheduler = DailyScheduler()
        scheduler.start()

        logger.info("Scheduler started successfully")

        # Keep running until interrupted
        try:
            while True:
                time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            logger.info("Shutdown requested by user")

        finally:
            logger.info("Stopping scheduler...")
            scheduler.stop()
            logger.info("Scheduler stopped")

    except Exception as e:
        logger.error(f"Scheduled mode failed: {e}")
        raise


def run_test_mode() -> bool:
    """Run test mode - check all services"""
    logger = get_logger('TestMode')

    logger.info("Running comprehensive test mode...")

    # Test configuration
    config_ok = test_configuration()

    # Test service health
    health_ok = health_check()

    # Test single lead analysis (dry run)
    try:
        analyzer = LeadAnalyzerService()
        test_result = analyzer.test_analysis_pipeline()

        if test_result:
            logger.info("✅ Analysis pipeline test passed")
        else:
            logger.error("❌ Analysis pipeline test failed")

    except Exception as e:
        logger.error(f"❌ Analysis pipeline test error: {e}")
        test_result = False

    overall_success = config_ok and health_ok and test_result

    if overall_success:
        logger.info("🎉 All tests passed! System is ready for production.")
    else:
        logger.warning("⚠️  Some tests failed. Please review the issues above.")

    return overall_success


def main():
    """Main application entry point"""
    # Setup logging first
    setup_logging()
    logger = get_logger('Main')

    # Parse command line arguments
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Override log level if verbose
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting Bitrix24 Lead Analyzer")
    logger.info(f"Mode: {args.mode}")

    try:
        # Handle different modes
        if args.config_test:
            success = test_configuration()
            sys.exit(0 if success else 1)

        elif args.health_check:
            success = health_check()
            sys.exit(0 if success else 1)

        elif args.mode == 'test':
            success = run_test_mode()
            sys.exit(0 if success else 1)

        elif args.mode == 'single':
            success = run_single_analysis(
                lead_id=args.lead_id,
                dry_run=args.dry_run
            )
            sys.exit(0 if success else 1)

        elif args.mode == 'all-junk':
            success = run_all_junk_analysis(dry_run=args.dry_run)
            sys.exit(0 if success else 1)

        elif args.mode == 'scheduled':
            run_scheduled_mode()

        else:
            logger.error(f"Unknown mode: {args.mode}")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        sys.exit(0)

    except LeadAnalyzerError as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()