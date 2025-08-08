"""
Enhanced Gemini AI service for detailed lead analysis
"""
import logging
import time
from typing import Optional, Dict, Any, List
import google.generativeai as genai

from app.config import get_config
from app.logger import LoggerMixin
from app.models.analysis_result import AIAnalysisResult
from app.utils.exceptions import AIAnalysisError, ValidationError


class EnhancedGeminiService(LoggerMixin):
    """Enhanced service for interacting with Google Gemini AI for lead analysis"""

    def __init__(self):
        self.config = get_config().gemini
        self.lead_config = get_config().lead_status

        if not self.config.api_key:
            raise ValidationError("Gemini API key is required")

        try:
            # Configure Gemini AI
            genai.configure(api_key=self.config.api_key)
            self.model = genai.GenerativeModel(self.config.model_name)

            self.log_service_action("EnhancedGeminiService", "init",
                                    f"Initialized Enhanced Gemini AI with model {self.config.model_name}")

        except Exception as e:
            raise AIAnalysisError(f"Failed to initialize Gemini AI: {e}")

    def analyze_lead_status(self, transcription: str, current_junk_status: int,
                            status_name: str) -> AIAnalysisResult:
        """Analyze if junk status is suitable based on transcription with enhanced prompting"""
        try:
            if not transcription.strip():
                return AIAnalysisResult(
                    is_suitable=False,
                    error="Empty transcription provided"
                )

            # Validate junk status
            valid_statuses = {
                158: "5 marta javob bermadi",
                227: "Notog'ri raqam",
                229: "Ariza qoldirmagan",
                783: "Notog'ri mijoz",
                807: "Yoshi to'g'ri kelmadi"
            }

            if current_junk_status not in valid_statuses:
                return AIAnalysisResult(
                    is_suitable=False,
                    error=f"Unknown junk status: {current_junk_status}"
                )

            start_time = time.time()

            # Build enhanced prompt based on status
            prompt = self._build_enhanced_analysis_prompt(transcription, current_junk_status, status_name)

            self.logger.debug(f"Analyzing junk status {current_junk_status} with Enhanced Gemini AI")

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

            # Parse response with enhanced logic
            result_text = response.text.strip()
            is_suitable, reasoning = self._parse_enhanced_response(result_text)

            self.logger.info(f"Enhanced Gemini analysis completed in {processing_time:.2f}s: suitable={is_suitable}")

            return AIAnalysisResult(
                is_suitable=is_suitable,
                reasoning=reasoning,
                model_used=self.config.model_name,
                processing_time=processing_time
            )

        except Exception as e:
            self.logger.error(f"Error in Enhanced Gemini analysis: {e}")
            return AIAnalysisResult(
                is_suitable=False,
                error=str(e)
            )

    def _build_enhanced_analysis_prompt(self, transcription: str, junk_status: int, status_name: str) -> str:
        """Build enhanced analysis prompt based on specific junk status with detailed reasoning"""

        # Base context about the system
        base_context = f"""
Sen Bitrix24 CRM tizimida ishlayotgan mijozlar bilan qo'ng'iroqlarni tahlil qiluvchi AI assistantisan.

HOZIRGI HOLAT: "{status_name}" (Kod: {junk_status})

QO'NG'IROQ YOZUVI:
{transcription}

"""

        # Status-specific instructions
        if junk_status == 227:  # "Notog'ri raqam"
            specific_prompt = """
VAZIFA: Bu qo'ng'iroq yozuviga asoslanib, "Notog'ri raqam" holati to'g'ri yoki noto'g'ri ekanligini aniqlang.

"Notog'ri raqam" holati QO'LLANILISHI KERAK agar:
- Qo'ng'iroq noto'g'ri odamga yetgan bo'lsa
- Telefon raqami boshqa kishiga tegishli bo'lsa
- Mijoz "men bu xizmatga yozilmaganman" yoki "noto'g'ri raqam" desa
- Qo'ng'iroq qabul qilgan kishi hech narsa bilmasa

"Notog'ri raqam" holati QO'LLANILMASLIGI KERAK agar:
- Mijoz xizmat haqida bilsa va qiziqsa
- Mijoz oldin ariza berganini tasdiqlasa
- Mijoz xizmatdan foydalanmoqchi bo'lsa
- Mijoz savollar bersa va javoblarni kutsa
"""

        elif junk_status == 229:  # "Ariza qoldirmagan"
            specific_prompt = """
VAZIFA: Bu qo'ng'iroq yozuviga asoslanib, "Ariza qoldirmagan" holati to'g'ri yoki noto'g'ri ekanligini aniqlang.

"Ariza qoldirmagan" holati QO'LLANILISHI KERAK agar:
- Mijoz hech qachon ariza bermaganini aytsa
- Mijoz xizmat haqida bilmasa
- Mijoz "men bunday narsaga yozilmaganman" desa
- Mijoz umuman qiziqmasa va rad etsa

"Ariza qoldirmagan" holati QO'LLANILMASLIGI KERAK agar:
- Mijoz ariza berganini tasdiqlasa
- Mijoz xizmat haqida bilsa
- Mijoz qiziqsa va savollar bersa
- Mijoz keyinroq aloqaga chiqishni xohlasa
"""

        elif junk_status == 783:  # "Notog'ri mijoz"
            specific_prompt = """
VAZIFA: Bu qo'ng'iroq yozuviga asoslanib, "Notog'ri mijoz" holati to'g'ri yoki noto'g'ri ekanligini aniqlang.

"Notog'ri mijoz" holati QO'LLANILISHI KERAK agar:
- Mijoz xizmat uchun mos kelmasligini aytsa
- Mijoz boshqa mamlakatda yashasa (xizmat faqat ma'lum hududlar uchun bo'lsa)
- Mijoz talablarga javob bermasa
- Mijoz umuman boshqa xizmat kerak ekanini aytsa

"Notog'ri mijoz" holati QO'LLANILMASLIGI KERAK agar:
- Mijoz xizmat uchun mos kelsa
- Mijoz qiziqsa va mos talablarga javob bersa
- Mijoz to'g'ri hududda yashasa
- Mijoz xizmatni xohlasa
"""

        elif junk_status == 807:  # "Yoshi to'g'ri kelmadi"
            specific_prompt = """
VAZIFA: Bu qo'ng'iroq yozuviga asoslanib, "Yoshi to'g'ri kelmadi" holati to'g'ri yoki noto'g'ri ekanligini aniqlang.

"Yoshi to'g'ri kelmadi" holati QO'LLANILISHI KERAK agar:
- Mijoz yoshi xizmat uchun kichik(16 yoshdan kichik) bo'lsa
- Mijoz yosh chegarasiga to'g'ri kelmasligini aytsa
- Operator yosh talabi haqida eslatsa va mijoz mos kelmasligini aytsa

"Yoshi to'g'ri kelmadi" holati QO'LLANILMASLIGI KERAK agar:
- Mijoz yoshi mos kelsa
- Yosh haqida gaplar bo'lmasa
- Mijoz boshqa sabablar tufayli rad etsa
"""

        else:  # Default for other statuses
            specific_prompt = f"""
VAZIFA: Bu qo'ng'iroq yozuviga asoslanib, "{status_name}" holati to'g'ri yoki noto'g'ri ekanligini aniqlang.

Qo'ng'iroq mazmuniga asoslanib, hozirgi holat mijozning haqiqiy ahvoliga mos keladimi yoki yo'qmi deb baholang.
"""

        # Enhanced final instructions with detailed reasoning request
        final_instructions = """
JAVOB FORMATI:
Javobingizni quyidagi formatda bering:

QAROR: [true yoki false]

SABABLARI:
- [Birinchi sabab]
- [Ikkinchi sabab]
- [Uchinchi sabab]
- [To'rtinchi sabab (agar kerak bo'lsa)]

TUSHUNTIRISH:
[Qisqa xulosangiz]

QOIDALAR:
- "true" = hozirgi holat to'g'ri va saqlanishi kerak
- "false" = hozirgi holat noto'g'ri va o'zgartirilishi kerak
- Faqat qo'ng'iroq yozuviga asoslanib javob bering
- Shubha bo'lsa, "true" deb javob bering
- Mijoz qiziqsa yoki savol bersa, odatda "false" deb javob bering
- Mijoz rad etsa yoki mos kelmasa, "true" deb javob bering
- Har bir sababni aniq va qisqa yozing
- Sabablar qo'ng'iroq yozuviga asoslangan bo'lishi kerak
"""

        return base_context + specific_prompt + final_instructions

    def _parse_enhanced_response(self, response_text: str) -> tuple[bool, Optional[str]]:
        """Parse enhanced AI response to extract decision and detailed reasoning in bullet points"""
        lines = response_text.strip().split('\n')

        # Initialize variables
        is_suitable = None
        decision_reasons = []
        explanation = ""
        current_section = None

        # Parse the structured response
        for line in lines:
            line_stripped = line.strip()
            line_lower = line_stripped.lower()

            # Skip empty lines
            if not line_stripped:
                continue

            # Identify sections
            if line_lower.startswith('qaror:') or line_lower.startswith('decision:'):
                # Extract decision from this line
                decision_part = line_stripped.split(':', 1)[1].strip().lower()
                if 'true' in decision_part and 'false' not in decision_part:
                    is_suitable = True
                elif 'false' in decision_part and 'true' not in decision_part:
                    is_suitable = False
                current_section = 'decision'

            elif line_lower.startswith('sabablari:') or line_lower.startswith('reasons:'):
                current_section = 'reasons'

            elif line_lower.startswith('tushuntirish:') or line_lower.startswith('explanation:'):
                current_section = 'explanation'

            # Process content based on current section
            elif current_section == 'reasons':
                # Look for bullet points
                if line_stripped.startswith('-') or line_stripped.startswith('•') or line_stripped.startswith('*'):
                    reason = line_stripped[1:].strip()
                    if reason:
                        decision_reasons.append(reason)

            elif current_section == 'explanation':
                if not line_lower.startswith('qaror:') and not line_lower.startswith('sabablari:'):
                    explanation += line_stripped + " "

            # Fallback: look for standalone true/false
            elif line_lower == 'true':
                is_suitable = True
            elif line_lower == 'false':
                is_suitable = False

        # If no structured format found, try simple parsing
        if is_suitable is None:
            response_clean = response_text.strip().lower()
            if 'true' in response_clean and 'false' not in response_clean:
                is_suitable = True
            elif 'false' in response_clean and 'true' not in response_clean:
                is_suitable = False
            else:
                # Default to false if unclear
                self.logger.warning(f"Unclear AI response: '{response_text}', defaulting to False")
                is_suitable = False

        # Build detailed reasoning string
        detailed_reasoning = ""

        if not is_suitable and decision_reasons:
            # Format reasons as bullet points
            detailed_reasoning = "Holat noto'g'ri deb topilgan sabablari:\n"
            for i, reason in enumerate(decision_reasons, 1):
                detailed_reasoning += f"• {reason}\n"

            if explanation.strip():
                detailed_reasoning += f"\nQo'shimcha tushuntirish: {explanation.strip()}"

        elif not is_suitable and not decision_reasons:
            # No specific reasons provided, create a generic explanation
            detailed_reasoning = "Holat noto'g'ri deb topildi, lekin batafsil sabab ko'rsatilmagan."

        elif is_suitable and (decision_reasons or explanation.strip()):
            # Status is suitable, but we have some reasoning
            detailed_reasoning = "Holat to'g'ri deb tasdiqlandi."
            if decision_reasons:
                detailed_reasoning += "\nTasdiqlovchi dalillar:\n"
                for reason in decision_reasons:
                    detailed_reasoning += f"• {reason}\n"
            if explanation.strip():
                detailed_reasoning += f"\nTushuntirish: {explanation.strip()}"

        return is_suitable, detailed_reasoning if detailed_reasoning else None

    def analyze_batch_leads(self, lead_transcriptions: List[Dict]) -> List[AIAnalysisResult]:
        """Analyze multiple leads in batch with rate limiting"""
        results = []

        self.logger.info(f"Starting batch analysis of {len(lead_transcriptions)} leads")

        for i, lead_data in enumerate(lead_transcriptions):
            try:
                transcription = lead_data.get('transcription', '')
                junk_status = lead_data.get('junk_status')
                status_name = lead_data.get('status_name', 'Unknown')

                result = self.analyze_lead_status(transcription, junk_status, status_name)
                results.append(result)

                # Rate limiting between requests
                time.sleep(0.5)

                if (i + 1) % 10 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(lead_transcriptions)} leads")

            except Exception as e:
                self.logger.error(f"Error in batch analysis item {i}: {e}")
                results.append(AIAnalysisResult(
                    is_suitable=False,
                    error=str(e)
                ))

        successful = sum(1 for r in results if r.is_successful)
        self.logger.info(f"Batch analysis completed: {successful}/{len(results)} successful")

        return results

    def test_connection(self) -> bool:
        """Test connection to Gemini AI with enhanced test"""
        try:
            self.log_service_action("EnhancedGeminiService", "test_connection", "Testing Enhanced Gemini AI connection")

            test_prompt = """
Bu test so'rovidir. Iltimos, faqat "test successful" deb javob bering.
"""
            response = self.model.generate_content(test_prompt)

            if response and response.text:
                response_clean = response.text.strip().lower()
                if 'test successful' in response_clean or 'test' in response_clean:
                    self.log_service_action("EnhancedGeminiService", "test_connection", "Connection successful")
                    return True
                else:
                    self.log_service_action("EnhancedGeminiService", "test_connection",
                                          f"Unexpected response: {response.text}", level=logging.WARNING)
                    return True  # Still working, just unexpected response
            else:
                self.log_service_action("EnhancedGeminiService", "test_connection", "No response from Gemini",
                                        level=logging.ERROR)
                return False

        except Exception as e:
            self.log_service_action("EnhancedGeminiService", "test_connection", f"Connection failed: {e}",
                                  level=logging.ERROR)
            return False

    def get_analysis_statistics(self) -> Dict[str, Any]:
        """Get analysis statistics and model information"""
        try:
            # Try to get model info from Gemini API
            models = list(genai.list_models())
            current_model_info = None

            for model in models:
                if self.config.model_name in model.name:
                    current_model_info = {
                        'name': model.name,
                        'display_name': getattr(model, 'display_name', 'Unknown'),
                        'description': getattr(model, 'description', 'No description available'),
                        'supported_generation_methods': getattr(model, 'supported_generation_methods', [])
                    }
                    break

            return {
                'service': 'Enhanced Google Gemini AI',
                'model_name': self.config.model_name,
                'model_info': current_model_info,
                'timeout_seconds': self.config.timeout_seconds,
                'max_retries': self.config.max_retries,
                'supported_statuses': {
                    158: "5 marta javob bermadi",
                    227: "Notog'ri raqam",
                    229: "Ariza qoldirmagan",
                    783: "Notog'ri mijoz",
                    807: "Yoshi to'g'ri kelmadi"
                }
            }

        except Exception as e:
            return {
                'service': 'Enhanced Google Gemini AI',
                'model_name': self.config.model_name,
                'model_info': None,
                'error': str(e)
            }

    def close(self):
        """Close the service and cleanup resources"""
        # Gemini client doesn't need explicit cleanup
        self.log_service_action("EnhancedGeminiService", "close", "Enhanced service closed")