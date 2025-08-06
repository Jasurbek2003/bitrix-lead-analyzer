"""
Gemini AI service for lead analysis
"""

import time
from typing import Optional, Dict, Any
import google.generativeai as genai

from app.config import get_config
from app.logger import LoggerMixin
from app.models.analysis_result import AIAnalysisResult
from app.utils.exceptions import AIAnalysisError, ValidationError


class GeminiService(LoggerMixin):
    """Service for interacting with Google Gemini AI"""

    def __init__(self):
        self.config = get_config().gemini
        self.lead_config = get_config().lead_status

        if not self.config.api_key:
            raise ValidationError("Gemini API key is required")

        try:
            # Configure Gemini AI
            genai.configure(api_key=self.config.api_key)
            self.model = genai.GenerativeModel(self.config.model_name)

            self.log_service_action("GeminiService", "init",
                                    f"Initialized Gemini AI with model {self.config.model_name}")

        except Exception as e:
            raise AIAnalysisError(f"Failed to initialize Gemini AI: {e}")

    def analyze_lead_status(self, transcription: str, current_junk_status: int,
                            status_name: str) -> AIAnalysisResult:
        """Analyze if junk status is suitable based on transcription"""
        try:
            if not transcription.strip():
                return AIAnalysisResult(
                    is_suitable=False,
                    error="Empty transcription provided"
                )

            if current_junk_status not in self.lead_config.junk_statuses:
                return AIAnalysisResult(
                    is_suitable=False,
                    error=f"Unknown junk status: {current_junk_status}"
                )

            start_time = time.time()

            prompt = self._build_analysis_prompt(transcription, current_junk_status, status_name)

            self.logger.debug(f"Analyzing junk status {current_junk_status} with Gemini AI")

            # Make request to Gemini with retry logic
            response = None
            for attempt in range(self.config.max_retries):
                try:
                    response = self.model.generate_content(prompt)
                    break
                except Exception as e:
                    self.logger.warning(f"Gemini API attempt {attempt + 1} failed: {e}")
                    if attempt == self.config.max_retries - 1:
                        raise
                    time.sleep(2 ** attempt)

            if not response or not response.text:
                return AIAnalysisResult(
                    is_suitable=False,
                    error="No response from Gemini AI"
                )

            processing_time = time.time() - start_time

            # Parse response
            result_text = response.text.strip().lower()

            # Extract boolean result
            is_suitable = self._parse_suitability_response(result_text)

            # Try to extract reasoning if available
            reasoning = self._extract_reasoning(response.text)

            self.logger.info(f"Gemini analysis completed in {processing_time:.2f}s: suitable={is_suitable}")

            return AIAnalysisResult(
                is_suitable=is_suitable,
                reasoning=reasoning,
                model_used=self.config.model_name,
                processing_time=processing_time
            )

        except Exception as e:
            self.logger.error(f"Error in Gemini analysis: {e}")
            return AIAnalysisResult(
                is_suitable=False,
                error=str(e)
            )

    def _build_analysis_prompt(self, transcription: str, junk_status: int, status_name: str) -> str:
        """Build prompt for junk status analysis"""

        # Get all status definitions
        status_definitions = []
        for code, name in self.lead_config.junk_statuses.items():
            if code == 158:
                status_definitions.append(
                    f'- "{name}" ({code}): Use when the customer has not responded after 5 or more call attempts')
            elif code == 227:
                status_definitions.append(
                    f'- "{name}" ({code}): Use when phone number is incorrect or doesn\'t belong to target person')
            elif code == 229:
                status_definitions.append(
                    f'- "{name}" ({code}): Use when person hasn\'t submitted any application or request')
            elif code == 783:
                status_definitions.append(
                    f'- "{name}" ({code}): Use when person is not the target client/customer type')
            elif code == 807:
                status_definitions.append(f'- "{name}" ({code}): Use when person\'s age doesn\'t meet the requirements')

        prompt = f"""
Analyze the following phone call transcription and determine if the current junk status is appropriate.

CURRENT STATUS: "{status_name}" (Code: {junk_status})

CALL TRANSCRIPTION:
{transcription}

JUNK STATUS DEFINITIONS:
{chr(10).join(status_definitions)}

ANALYSIS INSTRUCTIONS:
1. Read the transcription carefully to understand what happened during the call
2. Determine if the current junk status "{status_name}" accurately reflects the situation
3. Consider if the conversation supports this classification or if it should be changed

IMPORTANT:
- Only respond with "true" if the current status is suitable and accurate
- Respond with "false" if the current status is incorrect or doesn't match the conversation
- Base your decision solely on the content of the transcription
- Be strict in your evaluation - when in doubt, respond "false"

RESPONSE FORMAT:
Respond with only "true" or "false" (no other text, explanations, or punctuation).
"""

        return prompt.strip()

    def _parse_suitability_response(self, response_text: str) -> bool:
        """Parse AI response to extract suitability decision"""
        # Clean the response
        cleaned_response = response_text.strip().lower()

        # Remove any punctuation
        import re
        cleaned_response = re.sub(r'[^\w\s]', '', cleaned_response)

        # Check for true/false indicators
        if 'true' in cleaned_response and 'false' not in cleaned_response:
            return True
        elif 'false' in cleaned_response and 'true' not in cleaned_response:
            return False
        elif cleaned_response in ['yes', 'suitable', 'appropriate', 'correct']:
            return True
        elif cleaned_response in ['no', 'unsuitable', 'inappropriate', 'incorrect']:
            return False
        else:
            # Default to false if unclear
            self.logger.warning(f"Unclear AI response: '{response_text}', defaulting to False")
            return False

    def _extract_reasoning(self, full_response: str) -> Optional[str]:
        """Extract reasoning from AI response if available"""
        # If response is just true/false, no reasoning available
        if len(full_response.strip()) <= 10:
            return None

        # Look for reasoning patterns
        lines = full_response.split('\n')
        reasoning_lines = []

        for line in lines:
            line = line.strip()
            if line and line.lower() not in ['true', 'false']:
                # Skip the true/false line and collect reasoning
                reasoning_lines.append(line)

        if reasoning_lines:
            return ' '.join(reasoning_lines)

        return None

    def test_connection(self) -> bool:
        """Test connection to Gemini AI"""
        try:
            self.log_service_action("GeminiService", "test_connection", "Testing Gemini AI connection")

            test_prompt = "Respond with exactly 'test successful' (without quotes)"
            response = self.model.generate_content(test_prompt)

            if response and response.text:
                self.log_service_action("GeminiService", "test_connection", "Connection successful")
                return True
            else:
                self.log_service_action("GeminiService", "test_connection", "No response from Gemini",
                                        level=logging.ERROR)
                return False

        except Exception as e:
            self.log_service_action("GeminiService", "test_connection", f"Connection failed: {e}", level=logging.ERROR)
            return False

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the AI model"""
        try:
            # Try to get model info from Gemini API
            models = list(genai.list_models())
            current_model_info = None

            for model in models:
                if self.config.model_name in model.name:
                    current_model_info = {
                        'name': model.name,
                        'display_name': getattr(model, 'display_name', 'Unknown'),
                        'description': getattr(model, 'description', 'No description available')
                    }
                    break

            return {
                'service': 'Google Gemini AI',
                'model_name': self.config.model_name,
                'model_info': current_model_info,
                'timeout_seconds': self.config.timeout_seconds,
                'max_retries': self.config.max_retries
            }

        except Exception as e:
            return {
                'service': 'Google Gemini AI',
                'model_name': self.config.model_name,
                'model_info': None,
                'error': str(e)
            }

    def analyze_batch(self, transcriptions_and_statuses: list) -> list:
        """Analyze multiple transcriptions in batch"""
        results = []

        self.logger.info(f"Starting batch analysis of {len(transcriptions_and_statuses)} items")

        for i, (transcription, junk_status, status_name) in enumerate(transcriptions_and_statuses):
            try:
                result = self.analyze_lead_status(transcription, junk_status, status_name)
                results.append(result)

                # Small delay between requests to respect rate limits
                time.sleep(0.1)

                if (i + 1) % 10 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(transcriptions_and_statuses)} analyses")

            except Exception as e:
                self.logger.error(f"Error in batch analysis item {i}: {e}")
                results.append(AIAnalysisResult(
                    is_suitable=False,
                    error=str(e)
                ))

        successful = sum(1 for r in results if r.is_successful)
        self.logger.info(f"Batch analysis completed: {successful}/{len(results)} successful")

        return results

    def close(self):
        """Close the service and cleanup resources"""
        # Gemini client doesn't need explicit cleanup
        self.log_service_action("GeminiService", "close", "Service closed")