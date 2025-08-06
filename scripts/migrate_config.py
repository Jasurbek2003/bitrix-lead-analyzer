
# scripts/migrate_config.py
"""
Configuration migration utility
"""

import os
import json
import shutil
from pathlib import Path
from datetime import datetime


def backup_current_config():
    """Backup current configuration"""
    if os.path.exists('.env'):
        backup_name = f".env.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy('.env', backup_name)
        print(f"✅ Backed up current config to {backup_name}")
        return backup_name
    return None


def migrate_to_v2():
    """Migrate configuration to version 2 format"""
    print("🔄 Migrating configuration to v2.0...")

    # Add new configuration options
    new_options = """
# Version 2.0 additions
WEBHOOK_LOG_FILE=logs/webhook.log
LOG_MAX_FILE_SIZE_MB=10
LOG_BACKUP_COUNT=5

# Performance tuning
BATCH_PROCESSING_SIZE=20
CONNECTION_POOL_SIZE=10
"""

    if os.path.exists('.env'):
        with open('.env', 'a') as f:
            f.write(new_options)
        print("✅ Configuration updated to v2.0")
    else:
        print("❌ .env file not found")


def main():
    """Main migration function"""
    print("🔄 Configuration Migration Utility")
    print("=" * 40)

    # Backup current config
    backup_file = backup_current_config()

    # Run migration
    migrate_to_v2()

    print("\n✅ Migration completed")
    if backup_file:
        print(f"💾 Backup saved as: {backup_file}")


if __name__ == "__main__":
    main()