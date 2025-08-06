"""
Automated setup script for Bitrix24 Lead Analyzer
"""
import os
import subprocess
import sys

from Scripts.activate_this import path


def check_python_version():
    """Check Python version compatibility"""
    if sys.version_info < (3, 11):
        print("❌ Python 3.11 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} is compatible")
    return True


def check_docker():
    """Check if Docker is installed and running"""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker found: {result.stdout.strip()}")
            return True
    except FileNotFoundError:
        pass

    print("❌ Docker not found or not running")
    print("Please install Docker Desktop from https://docker.com")
    return False


def create_directories():
    """Create required directories"""
    directories = [
        'logs',
        'data/temp_audio',
        'data/backups',
        'tests'
    ]

    for dir_path in directories:
        path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {dir_path}")


def create_env_file():
    """Create .env file from template"""
    env_template = """# Bitrix24 Configuration
BITRIX_WEBHOOK_URL=https://your-domain.bitrix24.com/rest/USER_ID/WEBHOOK_CODE
BITRIX_TIMEOUT_SECONDS=30
BITRIX_MAX_RETRIES=3

# Transcription Service Configuration  
TRANSCRIPTION_SERVICE_URL=http://localhost:8101
TRANSCRIPTION_TIMEOUT_SECONDS=60
TRANSCRIPTION_MAX_RETRIES=3

# Google Gemini AI Configuration
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL_NAME=gemini-pro
GEMINI_TIMEOUT_SECONDS=30
GEMINI_MAX_RETRIES=3

# Application Settings
CHECK_INTERVAL_HOURS=24
MAX_CONCURRENT_LEADS=10
DELAY_BETWEEN_LEADS=2.0
LOG_LEVEL=INFO
LOG_FILE=logs/app.log
ERROR_LOG_FILE=logs/error.log
WEBHOOK_LOG_FILE=logs/webhook.log

# Lead Status Configuration
JUNK_STATUS_FIELD=UF_CRM_1751812306933
MAIN_STATUS_FIELD=STATUS_ID
JUNK_STATUS_VALUE=JUNK
ACTIVE_STATUS_VALUE=NEW
"""

    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_template)
        print("✅ Created .env file template")
        print("⚠️  Please edit .env file with your actual configuration values")
    else:
        print("✅ .env file already exists")


def install_dependencies():
    """Install Python dependencies"""
    try:
        print("📦 Installing Python dependencies...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'],
                       check=True)
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False


def run_tests():
    """Run basic configuration tests"""
    try:
        print("🧪 Running configuration tests...")
        result = subprocess.run([sys.executable, '-m', 'app.main', '--config-test'],
                                capture_output=True, text=True)

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        return result.returncode == 0
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def main():
    """Main setup function"""
    print("🚀 Setting up Bitrix24 Lead Analyzer")
    print("=" * 50)

    # Check prerequisites
    if not check_python_version():
        sys.exit(1)

    docker_available = check_docker()

    # Create directories
    create_directories()

    # Create environment file
    create_env_file()

    # Install dependencies
    if not install_dependencies():
        sys.exit(1)

    # Run basic tests (will fail if .env not configured, but that's expected)
    print("\n🧪 Running basic tests...")
    tests_passed = run_tests()

    print("\n" + "=" * 50)
    print("📋 SETUP SUMMARY")
    print("=" * 50)

    print("✅ Python environment ready")
    print("✅ Dependencies installed")
    print("✅ Directories created")
    print("✅ Configuration template created")

    if docker_available:
        print("✅ Docker available")
    else:
        print("⚠️  Docker not available (optional)")

    if tests_passed:
        print("✅ Configuration tests passed")
    else:
        print("⚠️  Configuration tests failed (expected if .env not configured)")

    print("\n🎯 NEXT STEPS:")
    print("1. Edit .env file with your actual API keys and URLs")
    print("2. Test configuration: python -m app.main --config-test")
    print("3. Run health check: python -m app.main --health-check")
    print("4. Test analysis: python -m app.main --mode test")
    print("5. Start application: python -m app.main --mode scheduled")

    if docker_available:
        print("\n🐳 DOCKER COMMANDS:")
        print("- Build and run: docker-compose up -d")
        print("- View logs: docker-compose logs -f")
        print("- Stop services: docker-compose down")


if __name__ == "__main__":
    main()

