"""
Bitrix24 API service for lead management
"""
import logging

import requests
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.config import get_config
from app.logger import LoggerMixin
from app.models.lead import Lead, LeadFilter, LeadActivity
from app.utils.exceptions import BitrixAPIError, ValidationError
from app.utils.validators import validate_lead_id, validate_webhook_url


class BitrixService(LoggerMixin):
    """Service for interacting with Bitrix24 API"""

    def __init__(self):
        self.config = get_config().bitrix
        self.lead_config = get_config().lead_status

        # Validate configuration
        if not validate_webhook_url(self.config.webhook_url):
            raise ValidationError("Invalid Bitrix24 webhook URL")

        self.session = requests.Session()
        self.session.timeout = self.config.timeout_seconds

        self.log_service_action("BitrixService", "init", "Initialized Bitrix24 service")

    def _make_request(self, endpoint: str, data: Dict[str, Any], method: str = "POST") -> Dict[str, Any]:
        """Make API request to Bitrix24"""
        url = f"{self.config.webhook_url}/{endpoint}"

        for attempt in range(self.config.max_retries):
            try:
                self.logger.debug(f"Making request to {endpoint}, attempt {attempt + 1}")

                if method.upper() == "POST":
                    response = self.session.post(url, json=data)
                else:
                    response = self.session.get(url, params=data)

                response.raise_for_status()

                result = response.json()

                # Check for Bitrix24 API errors
                if 'error' in result:
                    error_msg = result['error_description'] if 'error_description' in result else result['error']
                    raise BitrixAPIError(f"Bitrix24 API error: {error_msg}")

                self.logger.debug(f"Request to {endpoint} successful")
                return result

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.config.max_retries - 1:
                    raise BitrixAPIError(f"Failed to connect to Bitrix24 after {self.config.max_retries} attempts: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff

            except Exception as e:
                self.logger.error(f"Unexpected error in request to {endpoint}: {e}")
                raise BitrixAPIError(f"Unexpected error: {e}")

    def get_leads(self, lead_filter: LeadFilter) -> List[Lead]:
        """Get leads based on filter criteria"""
        try:
            print("Fetching leads with params:", )
            filter_params = lead_filter.to_bitrix_filter(self.lead_config.junk_status_field)

            params = {
                'filter': filter_params,
                'select': [
                    'ID', 'TITLE', 'STATUS_ID', self.lead_config.junk_status_field,
                    'DATE_CREATE', 'PHONE', 'EMAIL', 'NAME'
                ],
                'start': 0,
                'rows': lead_filter.limit
            }
            print("Fetching leads with params:", params)

            self.log_service_action("BitrixService", "get_leads", f"Fetching leads with filter: {filter_params}")

            result = self._make_request("crm.lead.list.json", params)
            leads_data = result.get('result', [])

            leads = []
            for lead_data in leads_data:
                try:
                    lead = Lead.from_bitrix_data(lead_data, self.lead_config.junk_status_field)
                    leads.append(lead)
                except Exception as e:
                    self.logger.warning(f"Failed to parse lead {lead_data.get('ID', 'unknown')}: {e}")

            self.log_service_action("BitrixService", "get_leads", f"Successfully fetched {len(leads)} leads")
            return leads

        except Exception as e:
            self.logger.error(f"Error fetching leads: {e}")
            raise

    def get_lead_by_id(self, lead_id: str) -> Optional[Lead]:
        """Get a specific lead by ID"""
        if not validate_lead_id(lead_id):
            raise ValidationError(f"Invalid lead ID: {lead_id}")

        try:
            params = {
                'ID': lead_id,
                'select': [
                    'ID', 'TITLE', 'STATUS_ID', self.lead_config.junk_status_field,
                    'DATE_CREATE', 'PHONE', 'EMAIL', 'NAME'
                ]
            }

            self.log_lead_action(lead_id, "get_lead", "Fetching lead details")
            result = self._make_request("crm.lead.get.json", params)
            lead_data = result.get('result')

            if not lead_data:
                self.log_lead_action(lead_id, "get_lead", "Lead not found")
                return None

            lead = Lead.from_bitrix_data(lead_data, self.lead_config.junk_status_field)
            self.log_lead_action(lead_id, "get_lead", "Successfully fetched lead")
            return lead

        except Exception as e:
            self.log_lead_action(lead_id, "get_lead", f"Error fetching lead: {e}")
            raise

    def get_data_from_voximplant(self, lead_id):
        if not validate_lead_id(lead_id):
            raise ValidationError(f"Invalid lead ID: {lead_id}")

        task = {"filter": {"CRM_ENTITY_ID": lead_id}, "sort": "ID", "order": "DESC"}
        # data = bx24.call(f"voximplant.statistic.get?start={start}", [task])
        data = self._make_request("voximplant.statistic.get", task, method="GET").get('result', [])
        print("Fetching data from voximplant.statistic.get")
        print(data)
        return data

    def get_lead_activities(self, lead_id: str) -> List[LeadActivity]:
        """Get activities for a specific lead"""
        if not validate_lead_id(lead_id):
            raise ValidationError(f"Invalid lead ID: {lead_id}")

        try:
            params = {
                'filter': {
                    'OWNER_ID': lead_id,
                    'OWNER_TYPE_ID': 1  # Lead type
                },
                # 'select': ['ID', 'TYPE_ID', 'DIRECTION', 'RESULT', 'DESCRIPTION', 'DATE', 'FILES']
            }

            self.log_lead_action(lead_id, "get_activities", "Fetching lead activities")

            result = self._make_request("crm.activity.list.json", params)
            activities_data = result.get('result', [])

            activities = []
            for activity_data in activities_data:
                try:
                    # Parse date
                    date = None
                    if activity_data.get('DATE'):
                        try:
                            date = datetime.fromisoformat(activity_data['DATE'].replace('Z', '+00:00'))
                        except ValueError:
                            pass

                    # Extract audio file if available
                    audio_file = None
                    files = activity_data.get('FILES', [])
                    if files:
                        for file_info in files:
                            if isinstance(file_info, dict) and file_info.get('url', '').startswith('https://'):
                                audio_file = file_info.get('url') or file_info.get('path')
                                audio_id = file_info.get('id')
                                print("Fetching audio file", audio_id)
                                if audio_id:
                                    audio_file = f"{self.config.webhook_url}crm.file.get?ID={audio_id}"
                                else:
                                    self.logger.warning(f"Audio file ID not found in activity {activity_data.get('ID', 'unknown')}")
                                print("Fetching audio file", audio_file)
                                break

                    activity = LeadActivity(
                        id=str(activity_data['ID']),
                        type_id=str(activity_data.get('TYPE_ID', '')),
                        direction=str(activity_data.get('DIRECTION', '')),
                        result=activity_data.get('RESULT'),
                        description=activity_data.get('DESCRIPTION'),
                        date=date,
                        audio_file=audio_file
                    )

                    activities.append(activity)

                except Exception as e:
                    self.logger.warning(f"Failed to parse activity {activity_data.get('ID', 'unknown')}: {e}")

            self.log_lead_action(lead_id, "get_activities", f"Successfully fetched {len(activities)} activities")
            return activities

        except Exception as e:
            self.log_lead_action(lead_id, "get_activities", f"Error fetching activities: {e}")
            raise

    def update_lead_status(self, lead_id: str, new_status: str) -> bool:
        """Update lead main status"""
        if not validate_lead_id(lead_id):
            raise ValidationError(f"Invalid lead ID: {lead_id}")

        try:
            params = {
                'ID': lead_id,
                'fields': {
                    self.lead_config.main_status_field: new_status
                }
            }

            self.log_lead_action(lead_id, "update_status", f"Updating status to {new_status}")

            result = self._make_request("crm.lead.update.json", params)
            success = result.get('result', False)

            if success:
                self.log_lead_action(lead_id, "update_status", f"Successfully updated status to {new_status}")
            else:
                self.log_lead_action(lead_id, "update_status", f"Failed to update status", level=logging.ERROR)

            return bool(success)

        except Exception as e:
            self.log_lead_action(lead_id, "update_status", f"Error updating status: {e}")
            raise

    def update_lead_junk_status(self, lead_id: str, junk_status: Optional[int]) -> bool:
        """Update lead junk status"""
        if not validate_lead_id(lead_id):
            raise ValidationError(f"Invalid lead ID: {lead_id}")

        try:
            params = {
                'ID': lead_id,
                'fields': {
                    self.lead_config.junk_status_field: junk_status
                }
            }

            action_desc = f"Clearing junk status" if junk_status is None else f"Setting junk status to {junk_status}"
            self.log_lead_action(lead_id, "update_junk_status", action_desc)

            result = self._make_request("crm.lead.update.json", params)
            success = result.get('result', False)

            if success:
                self.log_lead_action(lead_id, "update_junk_status", f"Successfully updated junk status")
            else:
                self.log_lead_action(lead_id, "update_junk_status", f"Failed to update junk status",
                                     level=logging.ERROR)

            return bool(success)

        except Exception as e:
            self.log_lead_action(lead_id, "update_junk_status", f"Error updating junk status: {e}")
            raise

    def update_lead_complete(self, lead_id: str, new_status: str, new_junk_status: Optional[int] = None) -> bool:
        """Update both main status and junk status in one call"""
        if not validate_lead_id(lead_id):
            raise ValidationError(f"Invalid lead ID: {lead_id}")

        try:
            fields = {
                self.lead_config.main_status_field: new_status
            }

            if new_junk_status is not None:
                fields[self.lead_config.junk_status_field] = new_junk_status
            else:
                fields[self.lead_config.junk_status_field] = None

            params = {
                'ID': lead_id,
                'fields': fields
            }

            action_desc = f"Updating status to {new_status}"
            if new_junk_status is None:
                action_desc += " and clearing junk status"
            else:
                action_desc += f" and setting junk status to {new_junk_status}"

            self.log_lead_action(lead_id, "update_complete", action_desc)

            result = self._make_request("crm.lead.update.json", params)
            success = result.get('result', False)

            if success:
                self.log_lead_action(lead_id, "update_complete", "Successfully updated lead")
            else:
                self.log_lead_action(lead_id, "update_complete", "Failed to update lead", level=logging.ERROR)

            return bool(success)

        except Exception as e:
            self.log_lead_action(lead_id, "update_complete", f"Error updating lead: {e}")
            raise

    def test_connection(self) -> bool:
        """Test connection to Bitrix24 API"""
        try:
            self.log_service_action("BitrixService", "test_connection", "Testing connection")

            # Simple test request
            params = {
                'start': 0,
                'rows': 1,
                'select': ['ID']
            }

            result = self._make_request("crm.lead.list.json", params)

            if 'result' in result:
                self.log_service_action("BitrixService", "test_connection", "Connection successful")
                return True
            else:
                self.log_service_action("BitrixService", "test_connection", "Connection failed - no result",
                                        level=logging.ERROR)
                return False

        except Exception as e:
            self.log_service_action("BitrixService", "test_connection", f"Connection failed: {e}", level=logging.ERROR)
            return False

    def get_junk_leads_count(self) -> int:
        """Get count of leads with junk status"""
        try:
            lead_filter = LeadFilter(
                status_id=self.lead_config.junk_status_value,
                junk_statuses=list(self.lead_config.junk_statuses.keys()),
                limit=1
            )

            filter_params = lead_filter.to_bitrix_filter(self.lead_config.junk_status_field)

            params = {
                'filter': filter_params,
                'select': ['ID']
            }

            result = self._make_request("crm.lead.list.json", params)
            total = result.get('total', 0)

            self.log_service_action("BitrixService", "get_junk_count", f"Found {total} junk leads")
            return int(total)

        except Exception as e:
            self.logger.error(f"Error getting junk leads count: {e}")
            return 0

    def get_lead_audio_files(self, lead_id: str) -> List[str]:
        """Get audio files associated with a lead"""
        if not validate_lead_id(lead_id):
            raise ValidationError(f"Invalid lead ID: {lead_id}")

        # activities = self.get_lead_activities(lead_id)
        activities = self.get_data_from_voximplant(lead_id)
        audio_files = []
        print(audio_files, "activities:", activities)

        for activity in activities:
            if activity.audio_file:
                audio_files.append(activity.audio_file)

        self.log_lead_action(lead_id, "get_audio_files", f"Found {len(audio_files)} audio files")
        return audio_files

    def close(self):
        """Close the service and cleanup resources"""
        if hasattr(self, 'session'):
            self.session.close()
        self.log_service_action("BitrixService", "close", "Service closed")
