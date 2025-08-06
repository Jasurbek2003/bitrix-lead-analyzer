"""
Bitrix24 Lead Analyzer Application
Analyzes and filters leads based on status and audio transcriptions
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List
import schedule
import asyncio
import aiohttp
from dataclasses import dataclass
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lead_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Lead:
    id: str
    status_id: str
    title: str
    contact_id: str
    company_id: str
    created_date: str
    modified_date: str
    assigned_by_id: str


class BitrixAPI:
    """Bitrix24 API wrapper"""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url.rstrip('/')
        self.session = requests.Session()

    def _make_request(self, method: str, params: Dict = None) -> Dict:
        """Make API request to Bitrix24"""
        url = f"{self.webhook_url}/{method}"
        try:
            response = self.session.post(url, json=params or {})
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Bitrix API error: {e}")
            raise

    def get_leads(self, filter_params: Dict = None, select: List[str] = None) -> List[Dict]:
        """Get leads from Bitrix24"""
        params = {
            'filter': filter_params or {},
            'select': select or ['*']
        }
        result = self._make_request('crm.lead.list', params)
        return result.get('result', [])

    def get_lead_activities(self, lead_id: str) -> List[Dict]:
        """Get activities for a specific lead"""
        params = {
            'filter': {
                'OWNER_ID': lead_id,
                'OWNER_TYPE_ID': 1  # Lead type
            }
        }
        result = self._make_request('crm.activity.list', params)
        return result.get('result', [])

    def update_lead_status(self, lead_id: str, status_id: str) -> bool:
        """Update lead status"""
        params = {
            'id': lead_id,
            'fields': {
                'STATUS_ID': status_id
            }
        }
        result = self._make_request('crm.lead.update', params)
        return result.get('result', False)

    def get_call_records(self, activity_id: str) -> List[Dict]:
        """Get call records for an activity"""
        params = {
            'CALL_ID': activity_id
        }
        result = self._make_request('telephony.externalcall.searchCrmEntities', params)
        return result.get('result', [])


class AudioTranscriptionService:
    """Service to handle audio transcription via Docker service"""

    def __init__(self, service_url: str):
        self.service_url = service_url.rstrip('/')

    async def transcribe_audio(self, audio_url: str) -> str:
        """Send audio to transcription service and get text"""
        async with aiohttp.ClientSession() as session:
            payload = {'audio_url': audio_url}
            try:
                async with session.post(f"{self.service_url}/transcribe", json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get('transcription', '')
                    else:
                        logger.error(f"Transcription service error: {response.status}")
                        return ''
            except Exception as e:
                logger.error(f"Error calling transcription service: {e}")
                return ''


class GeminiAnalyzer:
    """Gemini AI analyzer for lead status validation"""

    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')

    def analyze_lead_status(self, transcription: str, current_status: str, status_description: str) -> bool:
        """Analyze if the status is suitable for the lead based on transcription"""
        prompt = f"""
        Analyze the following phone call transcription and determine if the lead status is appropriate.

        Current Status: {current_status}
        Status Description: {status_description}

        Call Transcription: {transcription}

        Instructions:
        - Analyze the conversation content
        - Check if the assigned status accurately reflects what happened in the call
        - Consider the context and customer's responses
        - Return TRUE if the status is suitable, FALSE if it needs to be changed

        Examples:
        - If status is "Notog'ri raqam" (Wrong number) but the conversation shows a valid customer interaction, return FALSE
        - If status is "Ariza qoldirmagan" (No application) but customer shows interest, return FALSE  
        - If status matches the actual conversation content, return TRUE

        Respond with only TRUE or FALSE.
        """

        try:
            response = self.model.generate_content(prompt)
            result = response.text.strip().upper()
            return result == 'TRUE'
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return False  # Default to changing status if AI fails


class LeadAnalyzer:
    """Main lead analyzer class"""

    def __init__(self):
        self.bitrix = BitrixAPI("https://b24-l2y8pg.bitrix24.kz/rest/49/9hufhxdf41qvrfv1/")
        self.transcription_service = AudioTranscriptionService("http://localhost:8101")  # Docker service URL
        self.gemini = GeminiAnalyzer("AIzaSyDw23UtouGoQcr_v6Ug4XRd1E72qvTmgJw")

        # Status mappings
        self.junk_statuses = {
            "158": "5 marta javob bermadi",
            "227": "Notog'ri raqam",
            "229": "Ariza qoldirmagan",
            "783": "Notog'ri mijoz",
            "807": "Yoshi to'g'ri kelmadi"
        }

        # Track processed leads to avoid reprocessing
        self.processed_leads_file = 'processed_leads.json'
        self.processed_leads = self.load_processed_leads()

    def load_processed_leads(self) -> set:
        """Load previously processed lead IDs"""
        try:
            if os.path.exists(self.processed_leads_file):
                with open(self.processed_leads_file, 'r') as f:
                    return set(json.load(f))
        except Exception as e:
            logger.error(f"Error loading processed leads: {e}")
        return set()

    def save_processed_leads(self):
        """Save processed lead IDs to file"""
        try:
            with open(self.processed_leads_file, 'w') as f:
                json.dump(list(self.processed_leads), f)
        except Exception as e:
            logger.error(f"Error saving processed leads: {e}")

    def get_new_leads(self) -> List[Dict]:
        """Get new leads added since last check"""
        # Get leads from last 24 hours with junk statuses
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')

        filter_params = {
            'STATUS_ID': "JUNK",
            'UF_CRM_1751812306933': list(self.junk_statuses.keys())
        }

        all_leads = self.bitrix.get_leads(filter_params)

        # Filter out already processed leads
        new_leads = [lead for lead in all_leads if lead['ID'] not in self.processed_leads]

        logger.info(f"Found {len(new_leads)} new leads to analyze")
        return new_leads

    @staticmethod
    def count_unsuccessful_calls(activities: List[Dict]) -> int:
        """Count unsuccessful calls in activities"""
        unsuccessful_count = 0
        for activity in activities:
            if activity.get('TYPE_ID') == '2' and activity.get('COMPLETED') == 'N':  # Call type
                unsuccessful_count += 1
        return unsuccessful_count

    async def analyze_lead(self, lead: Dict):
        """Analyze a single lead"""
        lead_id = lead['ID']
        status_id = lead['STATUS_ID']

        logger.info(f"Analyzing lead {lead_id} with status {status_id}")

        try:
            if status_id == "158":  # "5 marta javob bermadi"
                activities = self.bitrix.get_lead_activities(lead_id)
                unsuccessful_calls = self.count_unsuccessful_calls(activities)

                if unsuccessful_calls >= 5:
                    logger.info(f"Lead {lead_id} has {unsuccessful_calls} unsuccessful calls, keeping status")
                else:
                    logger.info(f"Lead {lead_id} has only {unsuccessful_calls} unsuccessful calls, changing status")
                    # Change to a different status (you may want to define what status to change to)
                    self.bitrix.update_lead_status(lead_id, "NEW")  # Example: change to NEW

            else:  # Other junk statuses
                activities = self.bitrix.get_lead_activities(lead_id)
                transcriptions = []

                # Get all call recordings and transcribe them
                for activity in activities:
                    if activity.get('TYPE_ID') == '2':  # Call type
                        call_records = self.bitrix.get_call_records(activity['ID'])
                        for record in call_records:
                            if 'RECORD_URL' in record:
                                transcription = await self.transcription_service.transcribe_audio(record['RECORD_URL'])
                                if transcription:
                                    transcriptions.append(transcription)

                if transcriptions:
                    # Combine all transcriptions
                    full_transcription = " ".join(transcriptions)
                    status_description = self.junk_statuses.get(status_id, "Unknown status")

                    # Analyze with Gemini
                    is_suitable = self.gemini.analyze_lead_status(full_transcription, status_id, status_description)

                    if not is_suitable:
                        logger.info(f"Lead {lead_id} status {status_id} is not suitable, changing status")
                        self.bitrix.update_lead_status(lead_id, "NEW")  # Example: change to NEW
                    else:
                        logger.info(f"Lead {lead_id} status {status_id} is suitable, keeping status")
                else:
                    logger.warning(f"No audio transcriptions found for lead {lead_id}")

            # Mark lead as processed
            self.processed_leads.add(lead_id)
            self.save_processed_leads()

        except Exception as e:
            logger.error(f"Error analyzing lead {lead_id}: {e}")

    async def analyze_all_new_leads(self):
        """Analyze all new leads"""
        logger.info("Starting lead analysis cycle")

        new_leads = self.get_new_leads()

        if not new_leads:
            logger.info("No new leads to analyze")
            return

        # Analyze leads concurrently
        tasks = [self.analyze_lead(lead) for lead in new_leads]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(f"Completed analysis of {len(new_leads)} leads")


def run_analyzer():
    """Run the analyzer (called by scheduler)"""
    analyzer = LeadAnalyzer()
    asyncio.run(analyzer.analyze_all_new_leads())


def main():
    """Main application entry point"""
    logger.info("Starting Bitrix24 Lead Analyzer")

    # Schedule the analyzer to run every day
    schedule.every(1).days.do(run_analyzer)

    # Run immediately on startup
    run_analyzer()

    logger.info("Lead analyzer scheduled. Running every 24 hours.")

    # Keep the application running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()