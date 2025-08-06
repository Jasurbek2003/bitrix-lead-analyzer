"""
Lead data models for Bitrix24 Lead Analyzer
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum


class LeadStatus(Enum):
    """Lead status enumeration"""
    JUNK = "JUNK"
    NEW = "NEW"
    IN_PROCESS = "IN_PROCESS"
    PROCESSED = "PROCESSED"
    CONVERTED = "CONVERTED"


class JunkStatusCode(Enum):
    """Junk status code enumeration"""
    FIVE_NO_RESPONSE = 158  # "5 marta javob bermadi"
    WRONG_NUMBER = 227  # "Notog'ri raqam"
    NO_APPLICATION = 229  # "Ariza qoldirmagan"
    WRONG_CLIENT = 783  # "Notog'ri mijoz"
    WRONG_AGE = 807  # "Yoshi to'g'ri kelmadi"


@dataclass
class LeadContact:
    """Lead contact information"""
    phone: Optional[str] = None
    email: Optional[str] = None
    name: Optional[str] = None

    def __post_init__(self):
        # Clean phone number
        if self.phone:
            self.phone = self.phone.strip().replace(' ', '').replace('-', '')


@dataclass
class LeadActivity:
    """Lead activity data"""
    id: str
    type_id: str
    direction: str
    result: Optional[str] = None
    description: Optional[str] = None
    date: Optional[datetime] = None
    audio_file: Optional[str] = None

    @property
    def is_call(self) -> bool:
        """Check if activity is a call"""
        return self.type_id == "2"

    @property
    def is_unsuccessful_call(self) -> bool:
        """Check if call was unsuccessful"""
        if not self.is_call:
            return False

        unsuccessful_results = ['UNSUCCESSFUL', 'NO_ANSWER', 'BUSY', 'FAILED']
        return self.result in unsuccessful_results


@dataclass
class Lead:
    """Main lead data model"""
    id: str
    title: Optional[str] = None
    status_id: Optional[str] = None
    junk_status: Optional[int] = None
    date_create: Optional[datetime] = None
    contact: LeadContact = field(default_factory=LeadContact)
    activities: List[LeadActivity] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Ensure contact is LeadContact instance
        if isinstance(self.contact, dict):
            self.contact = LeadContact(**self.contact)
        elif self.contact is None:
            self.contact = LeadContact()

    @classmethod
    def from_bitrix_data(cls, data: Dict[str, Any], junk_status_field: str = "UF_CRM_1751812306933") -> 'Lead':
        """Create Lead instance from Bitrix24 API data"""
        # Parse date
        date_create = None
        if data.get('DATE_CREATE'):
            try:
                date_create = datetime.fromisoformat(data['DATE_CREATE'].replace('Z', '+00:00'))
            except ValueError:
                pass


        # Parse junk status
        junk_status = data.get(junk_status_field)
        if junk_status is not None:
            try:
                junk_status = int(junk_status)
            except (ValueError, TypeError):
                junk_status = None

        # Create contact info
        contact = LeadContact(
            name=data.get('NAME') or data.get('TITLE')
        )

        return cls(
            id=str(data['ID']),
            title=data.get('TITLE'),
            status_id=data.get('STATUS_ID'),
            junk_status=junk_status,
            date_create=date_create,
            contact=contact,
            raw_data=data
        )

    @property
    def is_junk(self) -> bool:
        """Check if lead has junk status"""
        return self.status_id == LeadStatus.JUNK.value

    @property
    def junk_status_name(self) -> Optional[str]:
        """Get junk status name"""
        junk_status_names = {
            158: "5 marta javob bermadi",
            227: "Notog'ri raqam",
            229: "Ariza qoldirmagan",
            783: "Notog'ri mijoz",
            807: "Yoshi to'g'ri kelmadi"
        }
        return junk_status_names.get(self.junk_status)

    @property
    def has_target_junk_status(self) -> bool:
        """Check if lead has one of the target junk statuses"""
        target_statuses = [158, 227, 229, 783, 807]
        return self.junk_status in target_statuses

    @property
    def unsuccessful_calls_count(self) -> int:
        """Count unsuccessful calls"""
        return sum(1 for activity in self.activities if activity.is_unsuccessful_call)

    @property
    def audio_files(self) -> List[str]:
        """Get list of audio files from activities"""
        files = []
        for activity in self.activities:
            if activity.audio_file:
                files.append(activity.audio_file)
        return files

    def add_activity(self, activity_data: Dict[str, Any]):
        """Add activity to the lead"""
        # Parse date if provided
        date = None
        if activity_data.get('DATE'):
            try:
                date = datetime.fromisoformat(activity_data['DATE'].replace('Z', '+00:00'))
            except ValueError:
                pass

        activity = LeadActivity(
            id=str(activity_data['ID']),
            type_id=str(activity_data.get('TYPE_ID', '')),
            direction=str(activity_data.get('DIRECTION', '')),
            result=activity_data.get('RESULT'),
            description=activity_data.get('DESCRIPTION'),
            date=date,
            audio_file=activity_data.get('AUDIO_FILE')
        )

        self.activities.append(activity)

    def to_dict(self) -> Dict[str, Any]:
        """Convert lead to dictionary"""
        return {
            'id': self.id,
            'title': self.title,
            'status_id': self.status_id,
            'junk_status': self.junk_status,
            'junk_status_name': self.junk_status_name,
            'date_create': self.date_create.isoformat() if self.date_create else None,
            'contact': {
                'phone': self.contact.phone,
                'email': self.contact.email,
                'name': self.contact.name
            },
            'activities_count': len(self.activities),
            'unsuccessful_calls_count': self.unsuccessful_calls_count,
            'audio_files_count': len(self.audio_files),
            'is_junk': self.is_junk,
            'has_target_junk_status': self.has_target_junk_status
        }

    def __repr__(self) -> str:
        return f"Lead(id={self.id}, title='{self.title}', junk_status={self.junk_status})"


@dataclass
class LeadFilter:
    """Lead filtering criteria"""
    status_id: Optional[str] = None
    junk_statuses: Optional[List[int]] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    limit: int = 50

    def to_bitrix_filter(self, junk_status_field: str = "UF_CRM_1751812306933") -> Dict[str, Any]:
        """Convert to Bitrix24 API filter format"""
        filter_params = {}

        if self.status_id:
            filter_params['STATUS_ID'] = self.status_id

        if self.junk_statuses:
            filter_params[junk_status_field] = self.junk_statuses

        if self.date_from:
            filter_params['>=DATE_CREATE'] = self.date_from.strftime('%Y-%m-%dT%H:%M:%S')

        if self.date_to:
            filter_params['<=DATE_CREATE'] = self.date_to.strftime('%Y-%m-%dT%H:%M:%S')

        return filter_params


@dataclass
class LeadBatch:
    """Batch of leads for processing"""
    leads: List[Lead] = field(default_factory=list)
    total_count: int = 0
    processed_count: int = 0
    success_count: int = 0
    error_count: int = 0

    def add_lead(self, lead: Lead):
        """Add lead to batch"""
        self.leads.append(lead)
        self.total_count += 1

    def mark_processed(self, lead_id: str, success: bool):
        """Mark a lead as processed"""
        self.processed_count += 1
        if success:
            self.success_count += 1
        else:
            self.error_count += 1

    @property
    def is_complete(self) -> bool:
        """Check if batch processing is complete"""
        return self.processed_count >= self.total_count

    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.processed_count == 0:
            return 0.0
        return self.success_count / self.processed_count

    def to_dict(self) -> Dict[str, Any]:
        """Convert batch to dictionary"""
        return {
            'total_count': self.total_count,
            'processed_count': self.processed_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate': self.success_rate,
            'is_complete': self.is_complete,
            'leads': [lead.to_dict() for lead in self.leads]
        }