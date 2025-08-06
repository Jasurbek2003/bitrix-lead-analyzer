"""
Logging configuration for Bitrix24 Lead Analyzer
"""

import logging
import logging.handlers
import os
import sys
from typing import Dict, Optional
from datetime import datetime
from app.config import get_config


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output"""

    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',  # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',  # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'  # Reset
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        record.levelname = f"{log_color}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)


class ContextFilter(logging.Filter):
    """Add context information to log records"""

    def filter(self, record):
        # Add timestamp
        record.timestamp = datetime.now().isoformat()

        # Add process/thread info
        record.process_id = os.getpid()

        # Add custom context if available
        if hasattr(record, 'lead_id'):
            record.context = f"[Lead:{record.lead_id}]"
        elif hasattr(record, 'service'):
            record.context = f"[{record.service}]"
        else:
            record.context = ""

        return True


class LeadAnalyzerLogger:
    """Custom logger for the Lead Analyzer application"""

    def __init__(self):
        self.config = get_config().logging
        self.loggers: Dict[str, logging.Logger] = {}
        self._setup_logging()

    def _ensure_log_directory(self):
        """Ensure log directory exists"""
        log_files = [
            self.config.log_file,
            self.config.error_log_file,
            self.config.webhook_log_file
        ]

        for log_file in log_files:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)

    def _setup_logging(self):
        """Setup logging configuration"""
        self._ensure_log_directory()

        # Set root logger level
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.log_level.upper()))

        # Clear existing handlers
        root_logger.handlers.clear()

        # Create formatters
        file_formatter = logging.Formatter(
            fmt='%(timestamp)s - %(name)s - %(levelname)s - %(context)s%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_formatter = ColoredFormatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(context)s%(message)s',
            datefmt='%H:%M:%S'
        )

        # Add context filter
        context_filter = ContextFilter()

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.addFilter(context_filter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)

        # Main log file handler
        main_handler = logging.handlers.RotatingFileHandler(
            filename=self.config.log_file,
            maxBytes=self.config.max_file_size_mb * 1024 * 1024,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        main_handler.setFormatter(file_formatter)
        main_handler.addFilter(context_filter)
        root_logger.addHandler(main_handler)

        # Error log file handler
        error_handler = logging.handlers.RotatingFileHandler(
            filename=self.config.error_log_file,
            maxBytes=self.config.max_file_size_mb * 1024 * 1024,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        error_handler.setFormatter(file_formatter)
        error_handler.addFilter(context_filter)
        error_handler.setLevel(logging.ERROR)
        root_logger.addHandler(error_handler)

    def get_logger(self, name: str) -> logging.Logger:
        """Get a logger instance"""
        if name not in self.loggers:
            logger = logging.getLogger(name)
            self.loggers[name] = logger
        return self.loggers[name]

    def get_webhook_logger(self) -> logging.Logger:
        """Get webhook-specific logger"""
        logger_name = "webhook"

        if logger_name not in self.loggers:
            logger = logging.getLogger(logger_name)

            # Add webhook-specific file handler
            webhook_handler = logging.handlers.RotatingFileHandler(
                filename=self.config.webhook_log_file,
                maxBytes=self.config.max_file_size_mb * 1024 * 1024,
                backupCount=self.config.backup_count,
                encoding='utf-8'
            )

            webhook_formatter = logging.Formatter(
                fmt='%(timestamp)s - WEBHOOK - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            webhook_handler.setFormatter(webhook_formatter)
            webhook_handler.addFilter(ContextFilter())

            logger.addHandler(webhook_handler)
            self.loggers[logger_name] = logger

        return self.loggers[logger_name]


# Global logger instance
_logger_instance: Optional[LeadAnalyzerLogger] = None


def setup_logging():
    """Initialize logging system"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = LeadAnalyzerLogger()


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance"""
    global _logger_instance
    if _logger_instance is None:
        setup_logging()
    return _logger_instance.get_logger(name)


def get_webhook_logger() -> logging.Logger:
    """Get webhook logger instance"""
    global _logger_instance
    if _logger_instance is None:
        setup_logging()
    return _logger_instance.get_webhook_logger()


class LoggerMixin:
    """Mixin class to add logging capabilities to any class"""

    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class"""
        return get_logger(self.__class__.__name__)

    def log_with_context(self, level: int, message: str, **context):
        """Log message with additional context"""
        extra = {}
        for key, value in context.items():
            extra[key] = value

        self.logger.log(level, message, extra=extra)

    def log_lead_action(self, lead_id: str, action: str, message: str, level: int = logging.INFO):
        """Log action related to a specific lead"""
        self.log_with_context(level, f"{action}: {message}", lead_id=lead_id)

    def log_service_action(self, service: str, action: str, message: str, level: int = logging.INFO):
        """Log action related to a specific service"""
        self.log_with_context(level, f"{action}: {message}", service=service)


# Initialize logging on import
setup_logging()