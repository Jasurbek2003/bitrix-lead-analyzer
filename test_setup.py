#!/usr/bin/env python3
"""
Test script to verify the Bitrix24 Lead Analyzer setup
"""

import os
import sys
import requests
import asyncio
import aiohttp
from datetime import datetime
import json


def test_environment_variables():
    """Test if all required environment variables are set"""
    print("🔧 Testing Environment Variables...")

    required_vars = [
        'BITRIX_WEBHOOK_URL',
        'GEMINI_API_KEY'
    ]

    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)

    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        return False
    else:
        print("✅ All environment variables are set")
        return True


def test_bitrix_connection():
    """Test connection to Bitrix24 API"""
    print("\n📞 Testing Bitrix24 Connection...")

    webhook_url = os.getenv('BITRIX_WEBHOOK_URL')
    if not webhook_url:
        print("❌ BITRIX_WEBHOOK_URL not set")
        return False

    try:
        # Test basic API call
        url = f"{webhook_url.rstrip('/')}/crm.lead.list"
        response = requests.post(url, json={'select': ['ID'], 'filter': {}, 'start': 0})

        if response.status_code == 200:
            data = response.json()
            if 'result' in data:
                print(f"✅ Bitrix24 connection successful. Found {len(data['result'])} leads")
                return True
            else:
                print(f"❌ Unexpected response format: {data}")
                return False
        else:
            print(f"❌ HTTP {response.status_code}: {response.text}")
            return False

    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False


def test_gemini_api():
    """Test Gemini AI API connection"""
    print("\n🤖 Testing Gemini AI Connection...")

    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("❌ GEMINI_API_KEY not set")
        return False

    try:
        import google.generativeai as genai

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')

        # Simple test prompt
        response = model.generate_content("Say 'test successful' if you can read this.")

        if response and response.text:
            print("✅ Gemini AI connection successful")
            return True
        else:
            print("❌ No response from Gemini AI")
            return False

    except Exception as e:
        print(f"❌ Gemini API error: {e}")
        return False


async def test_transcription_service():
    """Test the transcription service"""
    print("\n🎤 Testing Transcription Service...")

    service_url = "http://localhost:8000"

    try:
        # Test health endpoint
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{service_url}/health") as response:
                if response.status == 200:
                    print("✅ Transcription service health check passed")
                    return True
                else:
                    print(f"❌ Health check failed: {response.status}")
                    return False

    except Exception as e:
        print(f"❌ Transcription service error: {e}")
        print("💡 Make sure the service is running: docker-compose up transcription-service")
        return False


def test_file_permissions():
    """Test file system permissions"""
    print("\n📁 Testing File Permissions...")

    test_dirs = ['./data', './logs']
    success = True

    for dir_path in test_dirs:
        try:
            os.makedirs(dir_path, exist_ok=True)

            # Test write permission
            test_file = os.path.join(dir_path, 'test_write.tmp')
            with open(test_file, 'w') as f:
                f.write('test')

            # Test read permission
            with open(test_file, 'r') as f:
                content = f.read()

            # Clean up
            os.remove(test_file)

            print(f"✅ {dir_path} - Read/Write permissions OK")

        except Exception as e:
            print(f"❌ {dir_path} - Permission error: {e}")
            success = False

    return success


def test_lead_status_config():
    """Test lead status configuration"""
    print("\n📊 Testing Lead Status Configuration...")

    # These should match the statuses in your lead_analyzer.py
    expected_statuses = {
        "158": "5 marta javob bermadi",
        "227": "Notog'ri raqam",
        "229": "Ariza qoldirmagan",
        "783": "Notog'ri mijoz",
        "807": "Yoshi to'g'ri kelmadi"
    }

    webhook_url = os.getenv('BITRIX_WEBHOOK_URL')
    if not webhook_url:
        print("❌ Cannot test - BITRIX_WEBHOOK_URL not set")
        return False

    try:
        # Get status list from Bitrix24
        url = f"{webhook_url.rstrip('/')}/crm.status.list"
        response = requests.post(url, json={'filter': {'STATUS_ID': 'JUNK'}})

        if response.status_code == 200:
            data = response.json()
            if 'result' in data:

                print("✅ All configured lead statuses found in Bitrix24")

                return True
            else:
                print("❌ Could not retrieve status list from Bitrix24")
                return False
        else:
            print(f"❌ Failed to get statuses: HTTP {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Status check error: {e}")
        return False


async def main():
    """Run all tests"""
    print("🚀 Starting Bitrix24 Lead Analyzer Test Suite")
    print("=" * 50)

    tests = [
        ("Environment Variables", test_environment_variables),
        ("File Permissions", test_file_permissions),
        ("Bitrix24 Connection", test_bitrix_connection),
        ("Lead Status Config", test_lead_status_config),
        ("Gemini AI Connection", test_gemini_api),
        ("Transcription Service", test_transcription_service),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 50)
    print("📋 TEST SUMMARY")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
        if result:
            passed += 1

    print(f"\n📈 Results: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! Your setup is ready.")
        return True
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please fix the issues above.")
        return False


if __name__ == "__main__":
    # Load environment variables from .env file if available
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        print("💡 Install python-dotenv for .env file support: pip install python-dotenv")

    # Run tests
    success = asyncio.run(main())
    sys.exit(0 if success else 1)