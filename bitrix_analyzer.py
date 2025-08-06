import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
import argparse
from dataclasses import dataclass
import google.generativeai as genai
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bitrix_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class LeadStatus:
    """Lead status configuration"""
    FIVE_MARCH_NO_RESPONSE = 158  # "5 marta javob bermadi"
    WRONG_NUMBER = 227  # "Notog'ri raqam"
    NO_APPLICATION = 229  # "Ariza qoldirmagan"
    WRONG_CLIENT = 783  # "Notog'ri mijoz"
    WRONG_AGE = 807  # "Yoshi to'g'ri kelmadi"


class BitrixLeadAnalyzer:
    def __init__(self, bitrix_webhook_url: str = None, transcription_service_url: str = None,
                 gemini_api_key: str = None):
        # Load configuration from environment variables
        self.bitrix_webhook_url = bitrix_webhook_url or os.getenv('BITRIX_WEBHOOK_URL')
        self.transcription_service_url = transcription_service_url or os.getenv('TRANSCRIPTION_SERVICE_URL')
        self.gemini_api_key = gemini_api_key or os.getenv('GEMINI_API_KEY')
        self.check_interval_hours = int(os.getenv('CHECK_INTERVAL_HOURS', '24'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.timeout_seconds = int(os.getenv('TIMEOUT_SECONDS', '30'))

        # Validate required configurations
        if not all([self.bitrix_webhook_url, self.transcription_service_url, self.gemini_api_key]):
            raise ValueError(
                "Missing required configuration. Check BITRIX_WEBHOOK_URL, TRANSCRIPTION_SERVICE_URL, and GEMINI_API_KEY")

        self.last_check_time = datetime.now() - timedelta(hours=self.check_interval_hours)
        self.junk_statuses = {
            158: "5 marta javob bermadi",
            227: "Notog'ri raqam",
            229: "Ariza qoldirmagan",
            783: "Notog'ri mijoz",
            807: "Yoshi to'g'ri kelmadi"
        }

        # Configure Gemini AI
        try:
            genai.configure(api_key=self.gemini_api_key)
            self.gemini_model = genai.GenerativeModel('gemini-pro')
            logger.info("Gemini AI configured successfully")
        except Exception as e:
            logger.error(f"Failed to configure Gemini AI: {e}")
            raise

    def get_new_leads(self) -> List[Dict[str, Any]]:
        """Get leads added since last check"""
        try:
            # Convert datetime to Bitrix24 format
            time_filter = self.last_check_time.strftime('%Y-%m-%dT%H:%M:%S')

            params = {
                'filter': {
                    '>=DATE_CREATE': time_filter,
                    'STATUS_ID': "JUNK",
                    'UF_CRM_1751812306933': list(self.junk_statuses.keys())
                },
                'select': ['ID', 'TITLE', 'STATUS_ID', 'UF_CRM_1751812306933', 'DATE_CREATE', 'PHONE', 'EMAIL']
            }

            response = requests.post(
                f"{self.bitrix_webhook_url}/crm.lead.list.json",
                json=params,
                timeout=self.timeout_seconds
            )
            response.raise_for_status()

            result = response.json()
            leads = result.get('result', [])

            logger.info(f"Found {len(leads)} new junk leads")
            return leads

        except Exception as e:
            logger.error(f"Error fetching new leads: {e}")
            return []

    def get_lead_activities(self, lead_id: str) -> List[Dict[str, Any]]:
        """Get activities for a specific lead"""
        try:
            params = {
                'filter': {
                    'OWNER_ID': lead_id,
                    'OWNER_TYPE_ID': 1  # Lead type
                },
                'select': ['ID', 'TYPE_ID', 'DIRECTION', 'RESULT', 'DESCRIPTION']
            }

            response = requests.post(
                f"{self.bitrix_webhook_url}/crm.activity.list.json",
                json=params
            )
            response.raise_for_status()

            result = response.json()
            activities = result.get('result', [])

            return activities

        except Exception as e:
            logger.error(f"Error fetching activities for lead {lead_id}: {e}")
            return []

    def count_unsuccessful_calls(self, activities: List[Dict[str, Any]]) -> int:
        """Count unsuccessful calls from activities"""
        unsuccessful_calls = 0

        for activity in activities:
            # Check if it's a call activity (TYPE_ID = 2 usually means call)
            if activity.get('TYPE_ID') == '2' and activity.get('DIRECTION') == '2':
                # Check if call was unsuccessful (you may need to adjust this logic)
                result = activity.get('RESULT', '')
                if result in ['UNSUCCESSFUL', 'NO_ANSWER', 'BUSY']:
                    unsuccessful_calls += 1

        return unsuccessful_calls

    def get_lead_audio_files(self, lead_id: str) -> List[str]:
        """Get audio files associated with a lead"""
        try:
            # This would depend on how Bitrix24 stores audio files
            # You might need to get them from activities or attachments
            activities = self.get_lead_activities(lead_id)
            audio_files = []

            for activity in activities:
                # Check if activity has audio attachments
                # This is a placeholder - you'll need to implement based on your Bitrix24 setup
                if 'AUDIO_FILE' in activity:
                    audio_files.append(activity['AUDIO_FILE'])

            return audio_files

        except Exception as e:
            logger.error(f"Error getting audio files for lead {lead_id}: {e}")
            return []

    def transcribe_audio(self, audio_file_path: str) -> str:
        """Send audio to transcription service"""
        try:
            with open(audio_file_path, 'rb') as audio_file:
                files = {'audio': audio_file}
                response = requests.post(
                    f"{self.transcription_service_url}/transcribe",
                    files=files
                )
                response.raise_for_status()

                result = response.json()
                return result.get('transcription', '')

        except Exception as e:
            logger.error(f"Error transcribing audio {audio_file_path}: {e}")
            return ""

    def analyze_with_gemini(self, transcription: str, current_status: str, status_name: str) -> bool:
        """Analyze transcription with Gemini AI to determine if status is suitable"""
        try:
            prompt = f"""
            Analyze the following call transcription and determine if the lead status "{status_name}" is suitable.

            Current Status: {status_name}
            Status Code: {current_status}

            Call Transcription: {transcription}

            Status Definitions:
            - "Notog'ri raqam" (227): Wrong phone number - use when number is incorrect or doesn't belong to target person
            - "Ariza qoldirmagan" (229): No application submitted - use when person hasn't submitted any application
            - "Notog'ri mijoz" (783): Wrong client - use when person is not the target client/customer
            - "Yoshi to'g'ri kelmadi" (807): Wrong age - use when person's age doesn't meet requirements

            Based on the transcription, determine if the current status is appropriate for this lead.

            Respond with only "true" if the status is suitable, or "false" if it's not suitable.
            """

            response = self.gemini_model.generate_content(prompt)
            result = response.text.strip().lower()

            return result == "true"

        except Exception as e:
            logger.error(f"Error analyzing with Gemini: {e}")
            return False

    def update_lead_status(self, lead_id: str, new_status: str):
        return
        """Update lead status in Bitrix24"""
        try:
            params = {
                'ID': lead_id,
                'fields': {
                    'STATUS_ID': new_status
                }
            }

            response = requests.post(
                f"{self.bitrix_webhook_url}/crm.lead.update.json",
                json=params,
                timeout=self.timeout_seconds
            )
            response.raise_for_status()

            logger.info(f"Updated lead {lead_id} status to {new_status}")

        except Exception as e:
            logger.error(f"Error updating lead {lead_id} status: {e}")

    def update_lead_junk_status(self, lead_id: str, junk_status_value):
        """Update lead junk status field in Bitrix24"""
        try:
            params = {
                'ID': lead_id,
                'fields': {
                    'UF_CRM_1751812306933': junk_status_value
                }
            }

            response = requests.post(
                f"{self.bitrix_webhook_url}/crm.lead.update.json",
                json=params,
                timeout=self.timeout_seconds
            )
            response.raise_for_status()

            logger.info(f"Updated lead {lead_id} junk status to {junk_status_value}")

        except Exception as e:
            logger.error(f"Error updating lead {lead_id} junk status: {e}")

    def analyze_lead(self, lead: Dict[str, Any]):
        """Main analysis function for a lead"""
        try:
            lead_id = lead['ID']
            current_junk_status = lead.get('UF_CRM_1751812306933')

            # Convert to integer if it's a string
            if isinstance(current_junk_status, str):
                try:
                    current_junk_status = int(current_junk_status)
                except ValueError:
                    logger.warning(f"Lead {lead_id} has invalid junk status: {current_junk_status}")
                    return

            logger.info(f"Analyzing lead {lead_id} with junk status {current_junk_status}")

            # Check if lead has a junk status we handle
            if current_junk_status not in self.junk_statuses:
                logger.info(f"Lead {lead_id} doesn't have a target junk status, skipping")
                return

            status_name = self.junk_statuses[current_junk_status]

            # 4.2. Special handling for status 158 (5 marta javob bermadi)
            if current_junk_status == LeadStatus.FIVE_MARCH_NO_RESPONSE:
                activities = self.get_lead_activities(lead_id)
                unsuccessful_calls = self.count_unsuccessful_calls(activities)

                if unsuccessful_calls >= 5:
                    logger.info(f"Lead {lead_id} has {unsuccessful_calls} unsuccessful calls, keeping status")
                    return
                else:
                    logger.info(f"Lead {lead_id} has only {unsuccessful_calls} unsuccessful calls, changing status")
                    # Change back to active status
                    self.update_lead_status(lead_id, "NEW")
                    return

            # 4.3. For other statuses, analyze with AI
            audio_files = self.get_lead_audio_files(lead_id)

            if not audio_files:
                logger.info(f"No audio files found for lead {lead_id}")
                return

            # Transcribe all audio files
            all_transcriptions = []
            for audio_file in audio_files:
                transcription = self.transcribe_audio(audio_file)
                if transcription:
                    all_transcriptions.append(transcription)

            if not all_transcriptions:
                logger.info(f"No transcriptions available for lead {lead_id}")
                return

            # Combine all transcriptions
            combined_transcription = "\n\n".join(all_transcriptions)

            # 4.4. Analyze with Gemini
            is_suitable = self.analyze_with_gemini(combined_transcription, str(current_junk_status), status_name)

            if is_suitable:
                logger.info(f"Junk status is suitable for lead {lead_id}, keeping current status")
            else:
                logger.info(f"Junk status is not suitable for lead {lead_id}, changing status")
                # Change back to active status and clear junk status
                self.update_lead_status(lead_id, "NEW")
                self.update_lead_junk_status(lead_id, None)

        except Exception as e:
            logger.error(f"Error analyzing lead {lead['ID']}: {e}")

    def run_analysis_cycle(self):
        """Run one cycle of lead analysis"""
        logger.info("Starting lead analysis cycle")

        # Get new leads
        new_leads = self.get_new_leads()

        # Analyze each lead
        for lead in new_leads:
            self.analyze_lead(lead)

        # Update last check time
        self.last_check_time = datetime.now()
        logger.info(f"Analysis cycle completed. Next check at {self.last_check_time}")

    def get_all_junk_leads(self) -> List[Dict[str, Any]]:
        """Get all leads with JUNK status and target junk status values"""
        try:
            params = {
                'filter': {
                    'STATUS_ID': "JUNK",
                    'UF_CRM_1751812306933': list(self.junk_statuses.keys())
                },
                'select': ['ID', 'TITLE', 'STATUS_ID', 'UF_CRM_1751812306933', 'DATE_CREATE', 'PHONE', 'EMAIL']
            }

            response = requests.post(
                f"{self.bitrix_webhook_url}/crm.lead.list.json",
                json=params,
                timeout=self.timeout_seconds
            )
            response.raise_for_status()

            result = response.json()
            leads = result.get('result', [])

            logger.info(f"Found {len(leads)} total junk leads")
            return leads

        except Exception as e:
            logger.error(f"Error fetching all junk leads: {e}")
            return []
        """Get a specific lead by ID"""
        try:
            params = {
                'ID': lead_id,
                'select': ['ID', 'TITLE', 'STATUS_ID', 'UF_CRM_1751812306933', 'DATE_CREATE', 'PHONE', 'EMAIL']
            }

            response = requests.post(
                f"{self.bitrix_webhook_url}/crm.lead.get.json",
                json=params,
                timeout=self.timeout_seconds
            )
            response.raise_for_status()

            result = response.json()
            return result.get('result', {})

        except Exception as e:
            logger.error(f"Error fetching lead {lead_id}: {e}")
            return {}

    def test_connections(self):
        """Test all service connections"""
        logger.info("Testing connections...")

        # Test Bitrix24 connection
        try:
            response = requests.post(
                f"{self.bitrix_webhook_url}/crm.lead.list.json",
                json={'start': 0, 'rows': 1},
                timeout=self.timeout_seconds
            )
            response.raise_for_status()
            logger.info("✓ Bitrix24 connection successful")
        except Exception as e:
            logger.error(f"✗ Bitrix24 connection failed: {e}")

        # Test transcription service
        try:
            response = requests.get(
                f"{self.transcription_service_url}/health",
                timeout=self.timeout_seconds
            )
            response.raise_for_status()
            logger.info("✓ Transcription service connection successful")
        except Exception as e:
            logger.error(f"✗ Transcription service connection failed: {e}")

        # Test Gemini AI
        try:
            test_response = self.gemini_model.generate_content("Test message")
            logger.info("✓ Gemini AI connection successful")
        except Exception as e:
            logger.error(f"✗ Gemini AI connection failed: {e}")

    def start_scheduler(self):
        """Start the scheduled analysis (runs every 24 hours)"""
        logger.info(f"Starting Bitrix24 Lead Analyzer scheduler (check interval: {self.check_interval_hours} hours)")

        while True:
            try:
                self.run_analysis_cycle()

                # Wait for specified interval
                sleep_seconds = self.check_interval_hours * 3600
                logger.info(f"Waiting {self.check_interval_hours} hours for next cycle...")
                time.sleep(sleep_seconds)

            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                # Wait 5 minutes before retrying
                time.sleep(300)


def main():
    parser = argparse.ArgumentParser(description='Bitrix24 Lead Analyzer')
    parser.add_argument('--mode', choices=['scheduled', 'single', 'all-junk', 'test'],
                        default='scheduled',
                        help='Run mode: scheduled (continuous), single (new leads), all-junk (all junk leads), or test')
    parser.add_argument('--lead-id', help='Specific lead ID to analyze (for single mode)')
    parser.add_argument('--config-test', action='store_true', help='Test configuration and exit')

    args = parser.parse_args()

    try:
        # Initialize the analyzer
        analyzer = BitrixLeadAnalyzer()

        if args.config_test:
            logger.info("Configuration test passed!")
            return

        if args.mode == 'scheduled':
            logger.info("Starting scheduled mode...")
            analyzer.start_scheduler()
        elif args.mode == 'single':
            logger.info("Running single analysis cycle...")
            if args.lead_id:
                # Analyze specific lead
                lead_data = analyzer.get_lead_by_id(args.lead_id)
                if lead_data:
                    analyzer.analyze_lead(lead_data)
                else:
                    logger.error(f"Lead {args.lead_id} not found")
            else:
                # Run full cycle once (new leads only)
                analyzer.run_analysis_cycle()
        elif args.mode == 'all-junk':
            logger.info("Analyzing all existing junk leads...")
            all_junk_leads = analyzer.get_all_junk_leads()
            logger.info(f"Found {len(all_junk_leads)} junk leads to analyze")

            for lead in all_junk_leads:
                analyzer.analyze_lead(lead)
                # Small delay between leads to avoid overwhelming the APIs
                time.sleep(2)

            logger.info("Completed analysis of all junk leads")
        elif args.mode == 'test':
            logger.info("Running test mode...")
            analyzer.test_connections()

    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


if __name__ == "__main__":
    main()