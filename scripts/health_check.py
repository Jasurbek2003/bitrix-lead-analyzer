"""
Comprehensive health check script
"""

import sys
import json
from datetime import datetime

# Add app to Python path
sys.path.insert(0, '.')

from app.services.lead_analyzer import LeadAnalyzerService
from app.config import get_config, validate_config
from app.logger import get_logger


def main():
    """Run comprehensive health check"""
    logger = get_logger('HealthCheck')

    print("🏥 Bitrix24 Lead Analyzer - Health Check")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()

    # Check configuration
    print("⚙️ Configuration Check:")
    try:
        config = get_config()
        if validate_config():
            print("  ✅ Configuration valid")
        else:
            print("  ❌ Configuration invalid")
            return 1
    except Exception as e:
        print(f"  ❌ Configuration error: {e}")
        return 1

    # Check services
    print("\n🔍 Service Health Check:")
    try:
        with LeadAnalyzerService() as analyzer:
            health_status = analyzer.check_health()

            for service, status in health_status.items():
                status_icon = "✅" if status else "❌"
                print(f"  {status_icon} {service.title()} Service")

            all_healthy = all(health_status.values())

            # Get statistics
            print("\n📊 System Statistics:")
            try:
                stats = analyzer.get_statistics()
                print(f"  • Last analysis: {stats.get('last_analysis_time', 'Never')}")
                print(f"  • Junk leads count: {stats.get('junk_leads_count', 0)}")
                print(f"  • Check interval: {stats.get('configuration', {}).get('check_interval_hours', 'N/A')} hours")
            except Exception as e:
                print(f"  ⚠️ Could not get statistics: {e}")

            # Test analysis pipeline
            print("\n🧪 Pipeline Test:")
            try:
                pipeline_ok = analyzer.test_analysis_pipeline()
                if pipeline_ok:
                    print("  ✅ Analysis pipeline working")
                else:
                    print("  ❌ Analysis pipeline failed")
                    all_healthy = False
            except Exception as e:
                print(f"  ❌ Pipeline test error: {e}")
                all_healthy = False

            print("\n" + "=" * 50)

            if all_healthy:
                print("🎉 All systems healthy!")
                return 0
            else:
                print("⚠️ Some issues detected")
                return 1

    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
