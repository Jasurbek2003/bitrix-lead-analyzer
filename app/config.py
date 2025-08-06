"""
Configuration management for Bitrix24 Lead Analyzer
"""

import os
from typing import Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class BitrixConfig:
    """Bitrix24 API configuration"""
    webhook_url: str
    timeout_seconds: int = 30
    max_retries: int = 3

    def __post_init__(self):
        if not self.webhook_url:
            raise ValueError("BITRIX_WEBHOOK_URL is required")


@dataclass
class TranscriptionConfig:
    """Transcription service configuration"""
    service_url: str
    timeout_seconds: int = 60
    max_retries: int = 3

    def __post_init__(self):
        if not self.service_url:
            raise ValueError("TRANSCRIPTION_SERVICE_URL is required")


@dataclass
class GeminiConfig:
    """Gemini AI configuration"""
    api_key: str
    model_name: str = "gemini-2.0-flash"
    timeout_seconds: int = 30
    max_retries: int = 3

    def __post_init__(self):
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY is required")


@dataclass
class SchedulerConfig:
    """Scheduler configuration"""
    check_interval_hours: int = 24
    max_concurrent_leads: int = 10
    delay_between_leads: float = 2.0  # seconds


@dataclass
class LoggingConfig:
    """Logging configuration"""
    log_level: str = "INFO"
    log_file: str = "logs/app.log"
    error_log_file: str = "logs/error.log"
    webhook_log_file: str = "logs/webhook.log"
    max_file_size_mb: int = 10
    backup_count: int = 5


@dataclass
class LeadStatusConfig:
    """Lead status configuration"""
    junk_status_field: str = "UF_CRM_1751812306933"
    main_status_field: str = "STATUS_ID"
    junk_status_value: str = "JUNK"
    active_status_value: str = "NEW"

    # Junk status mappings
    junk_statuses: Dict[int, str] = None

    def __post_init__(self):
        if self.junk_statuses is None:
            self.junk_statuses = {
                158: "5 marta javob bermadi",
                227: "Notog'ri raqam",
                229: "Ariza qoldirmagan",
                783: "Notog'ri mijoz",
                807: "Yoshi to'g'ri kelmadi"
            }


class Config:
    """Main configuration class"""

    def __init__(self):
        self.bitrix = BitrixConfig(
            webhook_url=os.getenv('BITRIX_WEBHOOK_URL', ''),
            timeout_seconds=int(os.getenv('BITRIX_TIMEOUT_SECONDS', '30')),
            max_retries=int(os.getenv('BITRIX_MAX_RETRIES', '3'))
        )

        self.transcription = TranscriptionConfig(
            service_url=os.getenv('TRANSCRIPTION_SERVICE_URL', ''),
            timeout_seconds=int(os.getenv('TRANSCRIPTION_TIMEOUT_SECONDS', '60')),
            max_retries=int(os.getenv('TRANSCRIPTION_MAX_RETRIES', '3'))
        )

        self.gemini = GeminiConfig(
            api_key=os.getenv('GEMINI_API_KEY', ''),
            model_name=os.getenv('GEMINI_MODEL_NAME', 'gemini-2.0-flash'),
            timeout_seconds=int(os.getenv('GEMINI_TIMEOUT_SECONDS', '30')),
            max_retries=int(os.getenv('GEMINI_MAX_RETRIES', '3'))
        )

        self.scheduler = SchedulerConfig(
            check_interval_hours=int(os.getenv('CHECK_INTERVAL_HOURS', '24')),
            max_concurrent_leads=int(os.getenv('MAX_CONCURRENT_LEADS', '10')),
            delay_between_leads=float(os.getenv('DELAY_BETWEEN_LEADS', '2.0'))
        )

        self.logging = LoggingConfig(
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            log_file=os.getenv('LOG_FILE', 'logs/app.log'),
            error_log_file=os.getenv('ERROR_LOG_FILE', 'logs/error.log'),
            webhook_log_file=os.getenv('WEBHOOK_LOG_FILE', 'logs/webhook.log'),
            max_file_size_mb=int(os.getenv('LOG_MAX_FILE_SIZE_MB', '10')),
            backup_count=int(os.getenv('LOG_BACKUP_COUNT', '5'))
        )

        self.lead_status = LeadStatusConfig(
            junk_status_field=os.getenv('JUNK_STATUS_FIELD', 'UF_CRM_1751812306933'),
            main_status_field=os.getenv('MAIN_STATUS_FIELD', 'STATUS_ID'),
            junk_status_value=os.getenv('JUNK_STATUS_VALUE', 'JUNK'),
            active_status_value=os.getenv('ACTIVE_STATUS_VALUE', 'NEW')
        )

    def validate(self) -> bool:
        """Validate all configurations"""
        try:
            # Validate required configurations
            required_configs = [
                self.bitrix.webhook_url,
                self.transcription.service_url,
                self.gemini.api_key
            ]

            if not all(required_configs):
                missing = []
                if not self.bitrix.webhook_url:
                    missing.append("BITRIX_WEBHOOK_URL")
                if not self.transcription.service_url:
                    missing.append("TRANSCRIPTION_SERVICE_URL")
                if not self.gemini.api_key:
                    missing.append("GEMINI_API_KEY")

                raise ValueError(f"Missing required configuration: {', '.join(missing)}")

            # Validate URLs
            if not (self.bitrix.webhook_url.startswith('http:') or
                    self.bitrix.webhook_url.startswith('https:')):
                raise ValueError("BITRIX_WEBHOOK_URL must start with http or https")

            if not (self.transcription.service_url.startswith('http:') or
                    self.transcription.service_url.startswith('https:')):
                raise ValueError("TRANSCRIPTION_SERVICE_URL must start with http or https")

            # Validate numeric values
            if self.scheduler.check_interval_hours <= 0:
                raise ValueError("CHECK_INTERVAL_HOURS must be positive")

            if self.scheduler.max_concurrent_leads <= 0:
                raise ValueError("MAX_CONCURRENT_LEADS must be positive")

            return True

        except Exception as e:
            print(f"Configuration validation error: {e}")
            return False

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'bitrix': {
                'webhook_url': self.bitrix.webhook_url,
                'timeout_seconds': self.bitrix.timeout_seconds,
                'max_retries': self.bitrix.max_retries
            },
            'transcription': {
                'service_url': self.transcription.service_url,
                'timeout_seconds': self.transcription.timeout_seconds,
                'max_retries': self.transcription.max_retries
            },
            'gemini': {
                'model_name': self.gemini.model_name,
                'timeout_seconds': self.gemini.timeout_seconds,
                'max_retries': self.gemini.max_retries,
                'api_key_set': bool(self.gemini.api_key)
            },
            'scheduler': {
                'check_interval_hours': self.scheduler.check_interval_hours,
                'max_concurrent_leads': self.scheduler.max_concurrent_leads,
                'delay_between_leads': self.scheduler.delay_between_leads
            },
            'lead_status': {
                'junk_status_field': self.lead_status.junk_status_field,
                'main_status_field': self.lead_status.main_status_field,
                'junk_status_value': self.lead_status.junk_status_value,
                'active_status_value': self.lead_status.active_status_value,
                'junk_statuses': self.lead_status.junk_statuses
            }
        }


# Global configuration instance
config = Config()


def get_config() -> Config:
    """Get the global configuration instance"""
    return config


def validate_config() -> bool:
    """Validate the global configuration"""
    return config.validate()