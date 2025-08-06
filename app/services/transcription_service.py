"""
Transcription service client for audio processing
"""
import logging

import requests
import time
import os
from typing import Optional, Dict, Any, List
from pathlib import Path

from app.config import get_config
from app.logger import LoggerMixin
from app.models.analysis_result import TranscriptionResult
from app.utils.exceptions import TranscriptionError, ValidationError
from app.utils.validators import validate_audio_file


class TranscriptionService(LoggerMixin):
    """Service for transcribing audio files"""

    def __init__(self):
        self.config = get_config().transcription

        # Validate configuration
        if not self.config.service_url:
            raise ValidationError("Transcription service URL is required")

        self.session = requests.Session()
        self.session.timeout = self.config.timeout_seconds

        self.log_service_action("TranscriptionService", "init", "Initialized transcription service")

    def _make_request(self, endpoint: str, files: Optional[Dict] = None,
                      data: Optional[Dict] = None, method: str = "POST") -> Dict[str, Any]:
        """Make request to transcription service"""
        url = f"{self.config.service_url.rstrip('/')}/{endpoint.lstrip('/')}"

        for attempt in range(self.config.max_retries):
            try:
                self.logger.debug(f"Making request to transcription service, attempt {attempt + 1}")

                if method.upper() == "POST":
                    response = self.session.post(url, files=files, data=data)
                else:
                    response = self.session.get(url, params=data)

                response.raise_for_status()

                # Try to parse JSON response
                try:
                    result = response.json()
                except ValueError:
                    # If not JSON, return text response
                    result = {'text': response.text, 'status_code': response.status_code}

                self.logger.debug(f"Request to transcription service successful")
                return result

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Transcription request attempt {attempt + 1} failed: {e}")
                if attempt == self.config.max_retries - 1:
                    raise TranscriptionError(
                        f"Failed to connect to transcription service after {self.config.max_retries} attempts: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff

            except Exception as e:
                self.logger.error(f"Unexpected error in transcription request: {e}")
                raise TranscriptionError(f"Unexpected error: {e}")

    def test_connection(self) -> bool:
        """Test connection to transcription service"""
        try:
            self.log_service_action("TranscriptionService", "test_connection", "Testing connection")

            # Try health endpoint first
            try:
                result = self._make_request("", method="GET")
                self.log_service_action("TranscriptionService", "test_connection", "Health check successful")
                return True
            except:
                # If health endpoint fails, try status endpoint
                try:
                    result = self._make_request("status", method="GET")
                    self.log_service_action("TranscriptionService", "test_connection", "Status check successful")
                    return True
                except:
                    # If both fail, try root endpoint
                    result = self._make_request("", method="GET")
                    self.log_service_action("TranscriptionService", "test_connection", "Root endpoint accessible")
                    return True

        except Exception as e:
            self.log_service_action("TranscriptionService", "test_connection", f"Connection failed: {e}",
                                    level=logging.ERROR)
            return False

    def transcribe_file(self, file_path: str) -> TranscriptionResult:
        """Transcribe a single audio file"""
        if not validate_audio_file(file_path):
            raise ValidationError(f"Invalid audio file: {file_path}")

        if not os.path.exists(file_path):
            raise ValidationError(f"Audio file not found: {file_path}")

        try:
            file_name = os.path.basename(file_path)
            self.logger.info(f"Transcribing audio file: {file_name}")

            start_time = time.time()

            with open(file_path, 'rb') as audio_file:
                files = {
                    'audio': (file_name, audio_file, self._get_content_type(file_path))
                }

                result = self._make_request("transcribe", files=files)

            processing_time = time.time() - start_time

            # Parse response
            transcription = result.get('transcription', '')
            confidence = result.get('confidence')
            duration = result.get('duration')
            language = result.get('language')
            error = result.get('error')

            if error:
                self.logger.error(f"Transcription service returned error for {file_name}: {error}")
                return TranscriptionResult(
                    audio_file=file_path,
                    transcription='',
                    error=error
                )

            if not transcription:
                self.logger.warning(f"No transcription returned for {file_name}")
                return TranscriptionResult(
                    audio_file=file_path,
                    transcription='',
                    error="No transcription returned"
                )

            self.logger.info(f"Successfully transcribed {file_name} in {processing_time:.2f} seconds")

            return TranscriptionResult(
                audio_file=file_path,
                transcription=transcription.strip(),
                confidence=confidence,
                duration=duration,
                language=language
            )

        except Exception as e:
            self.logger.error(f"Error transcribing {file_path}: {e}")
            return TranscriptionResult(
                audio_file=file_path,
                transcription='',
                error=str(e)
            )

    def transcribe_url(self, audio_url: str) -> TranscriptionResult:
        """Transcribe audio from URL"""
        try:
            self.logger.info(f"Transcribing audio from URL: {audio_url}")

            start_time = time.time()

            # Download the file first
            temp_file = self._download_audio_file(audio_url)

            if not temp_file:
                return TranscriptionResult(
                    audio_file=audio_url,
                    transcription='',
                    error="Failed to download audio file"
                )

            try:
                # Transcribe the downloaded file
                result = self.transcribe_file(temp_file)
                result.audio_file = audio_url  # Keep original URL reference
                return result
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_file)
                except:
                    pass

        except Exception as e:
            self.logger.error(f"Error transcribing URL {audio_url}: {e}")
            return TranscriptionResult(
                audio_file=audio_url,
                transcription='',
                error=str(e)
            )

    def transcribe_multiple(self, file_paths: List[str]) -> List[TranscriptionResult]:
        """Transcribe multiple audio files"""
        results = []

        self.logger.info(f"Transcribing {len(file_paths)} audio files")

        for file_path in file_paths:
            try:
                result = self.transcribe_file(file_path)
                results.append(result)

                # Small delay between requests to avoid overwhelming the service
                time.sleep(0.5)

            except Exception as e:
                self.logger.error(f"Error processing {file_path}: {e}")
                results.append(TranscriptionResult(
                    audio_file=file_path,
                    transcription='',
                    error=str(e)
                ))

        successful = sum(1 for r in results if r.is_successful)
        self.logger.info(f"Completed transcription: {successful}/{len(results)} successful")

        return results

    def _download_audio_file(self, url: str) -> Optional[str]:
        """Download audio file from URL to temporary location"""
        try:
            # Create temp directory if it doesn't exist
            temp_dir = Path("data/temp_audio")
            temp_dir.mkdir(parents=True, exist_ok=True)

            # Generate temp filename
            import uuid
            temp_filename = f"temp_audio_{uuid.uuid4().hex[:8]}.wav"
            temp_path = temp_dir / temp_filename

            # Download file
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()

            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return str(temp_path)

        except Exception as e:
            self.logger.error(f"Error downloading audio file from {url}: {e}")
            return None

    def _get_content_type(self, file_path: str) -> str:
        """Get content type for audio file"""
        extension = Path(file_path).suffix.lower()

        content_types = {
            '.wav': 'audio/wav',
            '.mp3': 'audio/mpeg',
            '.m4a': 'audio/m4a',
            '.aac': 'audio/aac',
            '.ogg': 'audio/ogg',
            '.flac': 'audio/flac',
            '.wma': 'audio/x-ms-wma'
        }

        return content_types.get(extension, 'audio/wav')

    def get_supported_formats(self) -> List[str]:
        """Get list of supported audio formats"""
        try:
            result = self._make_request("formats", method="GET")
            return result.get('formats', ['.wav', '.mp3', '.m4a', '.aac'])
        except:
            # Return default formats if service doesn't support this endpoint
            return ['.wav', '.mp3', '.m4a', '.aac', '.ogg', '.flac']

    def get_service_info(self) -> Dict[str, Any]:
        """Get transcription service information"""
        try:
            result = self._make_request("info", method="GET")
            return result
        except:
            return {
                'service': 'transcription_service',
                'version': 'unknown',
                'supported_formats': self.get_supported_formats()
            }

    def close(self):
        """Close the service and cleanup resources"""
        if hasattr(self, 'session'):
            self.session.close()
        self.log_service_action("TranscriptionService", "close", "Service closed")