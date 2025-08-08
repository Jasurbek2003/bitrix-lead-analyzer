"""
Main application script for Enhanced Bitrix24 Lead Analyzer
"""

import argparse
import sys
import time
from datetime import datetime
from typing import Optional

from app.config import get_config, validate_config
from app.logger import get_logger, setup_logging
from enhanced.enhanced_lead_analyzer import EnhancedLeadAnalyzerService
from enhanced.enhanced_scheduler import EnhancedDailyScheduler
from app.utils.exceptions import LeadAnalyzerError


def setup_argument_parser() -> argparse.ArgumentParser:
    """Setup command line argument parser"""
    parser = argparse.ArgumentParser(
        description='Enhanced Bitrix24 Lead Analyzer - Advanced junk lead analysis with AI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode scheduled                    # Run continuous scheduled analysis (daily at 9 AM)
  %(prog)s --mode single                       # Run single analysis cycle for new leads
  %(prog)s --mode all-junk                     # Analyze all existing junk leads
  %(prog)s --mode single --lead-id 123         # Analyze specific lead by ID
  %(prog)s --config-test                       # Test configuration and services
  %(prog)s --health-check                      # Check all service health
  %(prog)s --mode test                         # Run comprehensive test mode
  %(prog)s --force-analysis                    # Force immediate analysis (for cron)
        """
    )

    parser.add_argument(
        '--mode',
        choices=['scheduled', 'single', 'all-junk', 'test', 'daemon'],
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
        '--force-analysis',
        action='store_true',
        help='Force immediate analysis run (useful for cron jobs)'
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
        '--schedule-time',
        type=str,
        default="09:00",
        help='Daily schedule time in HH:MM format (default: 09:00)'
    )

    parser.add_argument(
        '--interval-hours',
        type=int,
        help='Use interval scheduling instead of daily (hours)'
    )

    return parser


def test_configuration() -> bool:
    """Test application configuration"""
    logger = get_logger('ConfigTest')

    logger.info("Testing enhanced application configuration...")

    try:
        # Validate configuration
        config = get_config()
        if not validate_config():
            logger.error("Configuration validation failed")
            return False

        logger.info("✅ Configuration validation passed")

        # Display configuration summary
        logger.info("Enhanced configuration summary:")
        logger.info(f"  - Bitrix webhook URL: {config.bitrix.webhook_url[:50]}...")
        logger.info(f"  - Transcription service: http://127.0.0.1:8101")
        logger.info(f"  - Gemini model: {config.gemini.model_name}")
        logger.info(f"  - Check interval: {config.scheduler.check_interval_hours} hours")
        logger.info(f"  - Junk status field: {config.lead_status.junk_status_field}")

        # Test junk status mappings
        junk_statuses = {
            158: "5 marta javob bermadi",
            227: "Notog'ri raqam",
            229: "Ariza qoldirmagan",
            783: "Notog'ri mijoz",
            807: "Yoshi to'g'ri kelmadi"
        }
        logger.info(f"  - Junk statuses configured: {len(junk_statuses)}")

        return True

    except Exception as e:
        logger.error(f"Configuration test failed: {e}")
        return False


def health_check() -> bool:
    """Check health of all services"""
    logger = get_logger('HealthCheck')

    logger.info("Performing enhanced health check...")

    try:
        analyzer = EnhancedLeadAnalyzerService()
        health_status = analyzer.check_health()

        logger.info("Enhanced health check results:")
        for service, status in health_status.items():
            status_icon = "✅" if status else "❌"
            logger.info(f"  {status_icon} {service.title()} Service")

        overall_health = all(health_status.values())

        if overall_health:
            logger.info("🎉 All services are healthy!")
        else:
            logger.warning("⚠️  Some services are not healthy")

            # Specific service guidance
            if not health_status.get('transcription', False):
                logger.warning("   📋 Transcription service not responding. Please ensure:")
                logger.warning("      - Docker container is running: docker ps")
                logger.warning("      - Service is accessible at http://127.0.0.1:8101")

            if not health_status.get('gemini', False):
                logger.warning("   🤖 Gemini AI service not responding. Please check:")
                logger.warning("      - GEMINI_API_KEY is set correctly")
                logger.warning("      - Internet connection is available")

        analyzer.close()
        return overall_health

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


def run_single_analysis(lead_id: Optional[str] = None, dry_run: bool = False) -> bool:
    """Run single analysis cycle"""
    logger = get_logger('SingleAnalysis')

    try:
        with EnhancedLeadAnalyzerService() as analyzer:
            if lead_id:
                logger.info(f"Analyzing specific lead: {lead_id}")
                result = analyzer.analyze_lead_by_id(lead_id, dry_run=dry_run)

                if result:
                    logger.info(f"Analysis completed for lead {lead_id}:")
                    logger.info(f"  Action: {result.action.value if result.action else 'unknown'}")
                    logger.info(f"  Reason: {result.reason.value if result.reason else 'unknown'}")
                    if result.ai_analysis:
                        logger.info(f"  AI Decision: {'Suitable' if result.ai_analysis.is_suitable else 'Not suitable'}")
                        if result.ai_analysis.reasoning:
                            logger.info(f"  AI Detailed Reasoning:\n{result.ai_analysis.reasoning}")
                        if result.ai_analysis.processing_time:
                            logger.info(f"  AI Processing Time: {result.ai_analysis.processing_time:.2f}s")
                    if result.unsuccessful_calls_count > 0:
                        logger.info(f"  Unsuccessful Calls: {result.unsuccessful_calls_count}")
                    return result.is_successful
                else:
                    logger.error("Lead not found or analysis failed")
                    return False
            else:
                logger.info("Running single analysis cycle for new leads")
                batch_result = analyzer.analyze_new_leads(dry_run=dry_run)

                logger.info(f"Enhanced analysis completed:")
                logger.info(f"  Total leads processed: {batch_result.total_leads}")
                logger.info(f"  Success rate: {batch_result.success_rate:.2f}")
                logger.info(f"  Leads updated: {batch_result.leads_updated}")
                logger.info(f"  Leads kept: {batch_result.leads_kept}")
                logger.info(f"  Leads skipped: {batch_result.leads_skipped}")
                logger.info(f"  Processing time: {batch_result.total_processing_time:.2f}s")

                return batch_result.success_rate > 0.5

    except Exception as e:
        logger.error(f"Single analysis failed: {e}")
        return False


def run_all_junk_analysis(dry_run: bool = False) -> bool:
    """Analyze all existing junk leads"""
    logger = get_logger('AllJunkAnalysis')

    try:
        with EnhancedLeadAnalyzerService() as analyzer:
            logger.info("Starting enhanced analysis of all junk leads...")
            batch_result = analyzer.analyze_new_leads(dry_run=dry_run)  # Will process all recent junk leads

            logger.info(f"Enhanced all-junk analysis completed:")
            logger.info(f"  Total leads processed: {batch_result.total_leads}")
            logger.info(f"  Success rate: {batch_result.success_rate:.2f}")
            logger.info(f"  Leads updated: {batch_result.leads_updated}")
            logger.info(f"  Leads kept: {batch_result.leads_kept}")
            logger.info(f"  Leads skipped: {batch_result.leads_skipped}")
            logger.info(f"  Processing time: {batch_result.total_processing_time:.2f} seconds")

            return batch_result.success_rate > 0.5

    except Exception as e:
        logger.error(f"All junk analysis failed: {e}")
        return False


def run_scheduled_mode(schedule_time: str = "09:00", interval_hours: Optional[int] = None) -> None:
    """Run continuous scheduled analysis"""
    logger = get_logger('ScheduledMode')

    try:
        logger.info("Starting enhanced scheduled mode...")

        # Create analyzer and scheduler
        analyzer = EnhancedLeadAnalyzerService()
        scheduler = EnhancedDailyScheduler(analyzer)

        # Configure scheduling
        if interval_hours:
            scheduler.set_interval_schedule(interval_hours)
            logger.info(f"Using interval scheduling: every {interval_hours} hours")
        else:
            scheduler.add_custom_schedule(schedule_time)
            logger.info(f"Using daily scheduling: {schedule_time} daily")

        # Start scheduler
        scheduler.start()

        logger.info("Enhanced scheduler started successfully")
        logger.info("Schedule status:", scheduler.get_status())

        # Keep running until interrupted
        try:
            while True:
                time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            logger.info("Shutdown requested by user")

        finally:
            logger.info("Stopping enhanced scheduler...")
            scheduler.stop()
            analyzer.close()
            logger.info("Enhanced scheduler stopped")

    except Exception as e:
        logger.error(f"Scheduled mode failed: {e}")
        raise


def run_test_mode() -> bool:
    """Run comprehensive test mode"""
    logger = get_logger('TestMode')

    logger.info("Running enhanced comprehensive test mode...")

    # Test configuration
    config_ok = test_configuration()

    # Test service health
    health_ok = health_check()

    # Test analysis pipeline
    try:
        with EnhancedLeadAnalyzerService() as analyzer:
            logger.info("Testing enhanced analysis pipeline...")

            # Test with a small batch
            batch_result = analyzer.analyze_new_leads(dry_run=True)

            pipeline_ok = True
            logger.info(f"✅ Enhanced analysis pipeline test completed")
            logger.info(f"   Processed {batch_result.total_leads} leads in test mode")

    except Exception as e:
        logger.error(f"❌ Enhanced analysis pipeline test failed: {e}")
        pipeline_ok = False

    overall_success = config_ok and health_ok and pipeline_ok

    if overall_success:
        logger.info("🎉 All enhanced tests passed! System is ready for production.")
        logger.info("💡 You can now run:")
        logger.info("   python main_app.py --mode scheduled  # Start daily analysis")
        logger.info("   python main_app.py --mode single     # Run one-time analysis")
    else:
        logger.warning("⚠️  Some tests failed. Please review the issues above.")

    return overall_success


def force_immediate_analysis() -> bool:
    """Force immediate analysis (useful for cron jobs)"""
    logger = get_logger('ForceAnalysis')

    try:
        logger.info("Starting forced immediate analysis...")

        with EnhancedLeadAnalyzerService() as analyzer:
            batch_result = analyzer.analyze_new_leads()

            logger.info(f"Forced analysis completed:")
            logger.info(f"  Leads processed: {batch_result.total_leads}")
            logger.info(f"  Success rate: {batch_result.success_rate:.2f}")
            logger.info(f"  Leads updated: {batch_result.leads_updated}")

            return batch_result.success_rate > 0.5

    except Exception as e:
        logger.error(f"Forced analysis failed: {e}")
        return False


def main():
    """Main application entry point"""
    # Setup logging first
    setup_logging()
    logger = get_logger('EnhancedMain')

    # Parse command line arguments
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Override log level if verbose
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting Enhanced Bitrix24 Lead Analyzer")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")

    try:
        # Handle different modes
        if args.config_test:
            success = test_configuration()
            sys.exit(0 if success else 1)

        elif args.health_check:
            success = health_check()
            sys.exit(0 if success else 1)

        elif args.force_analysis:
            success = force_immediate_analysis()
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

        elif args.mode == 'scheduled' or args.mode == 'daemon':
            run_scheduled_mode(
                schedule_time=args.schedule_time,
                interval_hours=args.interval_hours
            )

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