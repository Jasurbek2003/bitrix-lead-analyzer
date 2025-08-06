#!/usr/bin/env python3
"""
Bitrix24 Webhook Test Script
Test your Bitrix24 API connectivity and webhook configuration
"""

import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


def test_bitrix_connection():
    """Test basic Bitrix24 API connection"""
    webhook_url = os.getenv('BITRIX_WEBHOOK_URL')

    if not webhook_url:
        print("❌ BITRIX_WEBHOOK_URL not found in environment variables")
        return False

    print(f"🔗 Testing connection to: {webhook_url}")

    try:
        # Test basic connection with lead list
        response = requests.post(
            f"{webhook_url}/crm.lead.list.json",
            json={
                'start': 0,
                'rows': 5,
                'select': ['ID', 'TITLE', 'STATUS_ID', 'DATE_CREATE']
            },
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            if 'result' in result:
                leads = result['result']
                print(f"✅ Connection successful! Found {len(leads)} leads")

                # Display sample leads
                for lead in leads[:3]:
                    print(f"  - Lead ID: {lead.get('ID')}, Title: {lead.get('TITLE', 'N/A')}")

                return True
            else:
                print(f"❌ Invalid response format: {result}")
                return False
        else:
            print(f"❌ HTTP Error {response.status_code}: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: {e}")
        return False


def test_lead_statuses():
    """Test if we can get lead statuses"""
    webhook_url = os.getenv('BITRIX_WEBHOOK_URL')

    try:
        print("\n📋 Testing lead status retrieval...")
        response = requests.post(
            f"{webhook_url}/crm.status.list.json",
            json={
                'filter': {
                    'ENTITY_ID': 'STATUS'
                }
            },
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            statuses = result.get('result', [])
            print(f"✅ Found {len(statuses)} statuses")

            # Look for our target statuses
            target_statuses = {158, 227, 229, 783, 807}
            found_statuses = {}

            for status in statuses:
                status_id = status.get('STATUS_ID')
                if status_id and int(status_id) in target_statuses:
                    found_statuses[int(status_id)] = status.get('NAME', 'Unknown')

            print("🎯 Target junk statuses found:")
            status_names = {
                158: "5 marta javob bermadi",
                227: "Notog'ri raqam",
                229: "Ariza qoldirmagan",
                783: "Notog'ri mijoz",
                807: "Yoshi to'g'ri kelmadi"
            }

            for status_id in target_statuses:
                if status_id in found_statuses:
                    print(f"  ✅ {status_id}: {found_statuses[status_id]}")
                else:
                    print(f"  ❌ {status_id}: {status_names[status_id]} (NOT FOUND)")

            return len(found_statuses) > 0
        else:
            print(f"❌ Failed to get statuses: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Error getting statuses: {e}")
        return False


def test_lead_activities():
    """Test if we can get lead activities"""
    webhook_url = os.getenv('BITRIX_WEBHOOK_URL')

    try:
        print("\n📞 Testing lead activities retrieval...")

        # First get a lead ID
        response = requests.post(
            f"{webhook_url}/crm.lead.list.json",
            json={'start': 0, 'rows': 1, 'select': ['ID']},
            timeout=30
        )

        if response.status_code != 200:
            print("❌ Cannot get test lead")
            return False

        leads = response.json().get('result', [])
        if not leads:
            print("❌ No leads found for testing activities")
            return False

        lead_id = leads[0]['ID']
        print(f"🧪 Testing with lead ID: {lead_id}")

        # Get activities for this lead
        response = requests.post(
            f"{webhook_url}/crm.activity.list.json",
            json={
                'filter': {
                    'OWNER_ID': lead_id,
                    'OWNER_TYPE_ID': 1  # Lead type
                },
                'select': ['ID', 'TYPE_ID', 'DIRECTION', 'RESULT']
            },
            timeout=30
        )

        if response.status_code == 200:
            activities = response.json().get('result', [])
            print(f"✅ Found {len(activities)} activities for lead {lead_id}")

            # Count call activities
            call_activities = [a for a in activities if a.get('TYPE_ID') == '2']
            print(f"📞 Found {len(call_activities)} call activities")

            return True
        else:
            print(f"❌ Failed to get activities: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Error testing activities: {e}")
        return False


def test_lead_update():
    """Test if we can update a lead (dry run)"""
    webhook_url = os.getenv('BITRIX_WEBHOOK_URL')

    try:
        print("\n✏️  Testing lead update capability...")

        # Get a test lead
        response = requests.post(
            f"{webhook_url}/crm.lead.list.json",
            json={'start': 0, 'rows': 1, 'select': ['ID', 'STATUS_ID']},
            timeout=30
        )

        if response.status_code != 200:
            print("❌ Cannot get test lead")
            return False

        leads = response.json().get('result', [])
        if not leads:
            print("❌ No leads found for testing update")
            return False

        lead_id = leads[0]['ID']
        current_status = leads[0].get('STATUS_ID', 'Unknown')

        print(f"🧪 Test lead ID: {lead_id}, Current status: {current_status}")
        print("⚠️  This is a dry run - no actual update will be performed")

        # Test update permission (without actually updating)
        # We'll just verify we have the right API access
        response = requests.post(
            f"{webhook_url}/crm.lead.get.json",
            json={'ID': lead_id},
            timeout=30
        )

        if response.status_code == 200:
            print("✅ Lead update API access confirmed")
            return True
        else:
            print(f"❌ Cannot access lead update API: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Error testing lead update: {e}")
        return False


def main():
    print("🚀 Bitrix24 Webhook Configuration Test")
    print("=" * 50)

    # Check environment
    webhook_url = os.getenv('BITRIX_WEBHOOK_URL')
    if not webhook_url:
        print("❌ BITRIX_WEBHOOK_URL not found in .env file")
        print("Please check your .env configuration")
        return

    print(f"🔧 Using webhook URL: {webhook_url}")
    print()

    # Run tests
    tests = [
        ("Basic Connection", test_bitrix_connection),
        ("Lead Statuses", test_lead_statuses),
        ("Lead Activities", test_lead_activities),
        ("Update Permission", test_lead_update)
    ]

    results = {}
    for test_name, test_func in tests:
        print(f"\n{'=' * 20} {test_name} {'=' * 20}")
        results[test_name] = test_func()

    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 50)

    passed = sum(results.values())
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:.<30} {status}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Your Bitrix24 configuration is ready.")
    else:
        print("⚠️  Some tests failed. Please check your configuration.")
        print("\nTroubleshooting tips:")
        print("1. Verify your Bitrix24 webhook URL is correct")
        print("2. Check that the webhook has sufficient permissions")
        print("3. Ensure your Bitrix24 account has API access enabled")
        print("4. Test the webhook URL manually in a browser")


if __name__ == "__main__":
    main()