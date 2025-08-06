"""
Validation utilities for Bitrix24 Lead Analyzer
"""

import re
from typing import Optional, List
from urllib.parse import urlparse
from pathlib import Path


def validate_webhook_url(url: str) -> bool:
    """Validate Bitrix24 webhook URL format"""
    if not url:
        return False

    try:
        parsed = urlparse(url)

        # Must have scheme and netloc
        if not parsed.scheme or not parsed.netloc:
            return False

        # Must be HTTP or HTTPS
        if parsed.scheme not in ['http', 'https']:
            return False

        # Should contain 'bitrix24' and 'rest' in the URL
        if 'bitrix24' not in url.lower() or 'rest' not in url.lower():
            return False

        return True

    except Exception:
        return False


def validate_lead_id(lead_id: str) -> bool:
    """Validate lead ID format"""
    if not lead_id:
        return False

    # Lead ID should be numeric string
    return lead_id.isdigit() and len(lead_id) > 0


def validate_junk_status(junk_status: int, valid_statuses: Optional[List[int]] = None) -> bool:
    """Validate junk status code"""
    if valid_statuses is None:
        valid_statuses = [158, 227, 229, 783, 807]

    return junk_status in valid_statuses


def validate_phone_number(phone: str) -> bool:
    """Validate phone number format"""
    if not phone:
        return False

    # Remove common separators
    cleaned = re.sub(r'[\s\-\(\)\+]', '', phone)

    # Should be 7-15 digits
    return cleaned.isdigit() and 7 <= len(cleaned) <= 15


def validate_email(email: str) -> bool:
    """Validate email format"""
    if not email:
        return False

    # Simple email validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.match(email_pattern, email) is not None


def validate_audio_file(file_path: str) -> bool:
    """Validate audio file format and existence"""
    if not file_path:
        return False

    # Check if it's a URL or file path
    if file_path.startswith(('http://', 'https://')):
        # For URLs, just check basic format
        return validate_url(file_path)

    # For file paths, check extension
    path = Path(file_path)
    valid_extensions = {'.wav', '.mp3', '.m4a', '.aac', '.ogg', '.flac', '.wma'}

    return path.suffix.lower() in valid_extensions


def validate_url(url: str) -> bool:
    """Validate URL format"""
    if not url:
        return False

    try:
        parsed = urlparse(url)
        return parsed.scheme in ['http', 'https'] and parsed.netloc
    except Exception:
        return False


def validate_api_key(api_key: str) -> bool:
    """Validate API key format"""
    if not api_key:
        return False

    # API key should be at least 20 characters and contain alphanumeric characters
    return len(api_key) >= 20 and re.match(r'^[a-zA-Z0-9_-]+', api_key)


def validate_config_value(value: str, value_type: str) -> bool:
    """Validate configuration values based on type"""
    if not value:
        return False

    if value_type == 'url':
        return validate_url(value)
    elif value_type == 'webhook_url':
        return validate_webhook_url(value)
    elif value_type == 'api_key':
        return validate_api_key(value)
    elif value_type == 'positive_int':
        try:
            return int(value) > 0
        except ValueError:
            return False
    elif value_type == 'positive_float':
        try:
            return float(value) > 0
        except ValueError:
            return False
    elif value_type == 'log_level':
        return value.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    else:
        return True


def validate_transcription_text(text: str) -> bool:
    """Validate transcription text"""
    if not text:
        return False

    # Should have minimum length and contain actual words
    words = text.split()
    return len(words) >= 3 and len(text.strip()) >= 10


def validate_file_path(file_path: str, check_exists: bool = False) -> bool:
    """Validate file path format and optionally check existence"""
    if not file_path:
        return False

    try:
        path = Path(file_path)

        # Check if path is valid
        if not path.name:
            return False

        # Check if file exists is requested
        if check_exists and not path.exists():
            return False

        return True

    except Exception:
        return False


def validate_directory_path(dir_path: str, create_if_missing: bool = False) -> bool:
    """Validate directory path and optionally create if missing"""
    if not dir_path:
        return False

    try:
        path = Path(dir_path)

        if path.exists() and path.is_dir():
            return True

        if create_if_missing:
            path.mkdir(parents=True, exist_ok=True)
            return True

        return False

    except Exception:
        return False


def validate_lead_data(lead_data: dict) -> tuple[bool, List[str]]:
    """Validate lead data structure and return validation errors"""
    errors = []

    # Required fields
    required_fields = ['ID']
    for field in required_fields:
        if field not in lead_data:
            errors.append(f"Missing required field: {field}")

    # Validate ID
    if 'ID' in lead_data and not validate_lead_id(str(lead_data['ID'])):
        errors.append("Invalid lead ID format")

    # Validate phone if present
    if 'PHONE' in lead_data and lead_data['PHONE']:
        phone_value = lead_data['PHONE']
        if isinstance(phone_value, dict):
            # Bitrix24 phone format: {'0': {'VALUE': '+1234567890'}}
            phone_entries = phone_value.values()
            for entry in phone_entries:
                if isinstance(entry, dict) and 'VALUE' in entry:
                    if not validate_phone_number(entry['VALUE']):
                        errors.append(f"Invalid phone number: {entry['VALUE']}")
        elif isinstance(phone_value, str):
            if not validate_phone_number(phone_value):
                errors.append(f"Invalid phone number: {phone_value}")

    # Validate email if present
    if 'EMAIL' in lead_data and lead_data['EMAIL']:
        email_value = lead_data['EMAIL']
        if isinstance(email_value, dict):
            # Bitrix24 email format similar to phone
            email_entries = email_value.values()
            for entry in email_entries:
                if isinstance(entry, dict) and 'VALUE' in entry:
                    if not validate_email(entry['VALUE']):
                        errors.append(f"Invalid email: {entry['VALUE']}")
        elif isinstance(email_value, str):
            if not validate_email(email_value):
                errors.append(f"Invalid email: {email_value}")

    return len(errors) == 0, errors


def validate_activity_data(activity_data: dict) -> tuple[bool, List[str]]:
    """Validate activity data structure"""
    errors = []

    # Required fields
    required_fields = ['ID', 'TYPE_ID']
    for field in required_fields:
        if field not in activity_data:
            errors.append(f"Missing required field: {field}")

    # Validate ID
    if 'ID' in activity_data and not str(activity_data['ID']).isdigit():
        errors.append("Invalid activity ID format")

    # Validate TYPE_ID
    if 'TYPE_ID' in activity_data and not str(activity_data['TYPE_ID']).isdigit():
        errors.append("Invalid activity type ID format")

    return len(errors) == 0, errors


def sanitize_input(input_text: str, max_length: int = 1000) -> str:
    """Sanitize user input text"""
    if not input_text:
        return ""

    # Remove null bytes and control characters
    sanitized = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', input_text)

    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]

    # Strip whitespace
    return sanitized.strip()


def validate_batch_size(batch_size: int, max_size: int = 100) -> bool:
    """Validate batch processing size"""
    return 1 <= batch_size <= max_size


def validate_time_interval(interval_hours: int) -> bool:
    """Validate time interval for scheduling"""
    # Should be between 1 hour and 7 days
    return 1 <= interval_hours <= 168