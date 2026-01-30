#!/usr/bin/env python3
"""
Script to process Excel data and push specific fields to ServiceNow
"""
import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime
from typing import Dict, List, Optional

class ServiceNowSync:
    def __init__(self):
        self.instance = os.environ.get('dev313533')
        self.username = os.environ.get('admin')
        self.password = os.environ.get('eG0rRKK0%-jx')
        self.table = os.environ.get('SERVICENOW_TABLE', 'iot_sensor_log')
        
        if not all([self.instance, self.username, self.password]):
            raise ValueError("Missing required ServiceNow credentials in environment variables")
        
        self.base_url = f"https://{self.instance}.service-now.com/api/now/table/{self.table}"
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
    def read_excel_data(self, file_path: str) -> pd.DataFrame:
        """Read data from Excel file"""
        print(f"Reading Excel file: {file_path}")
        df = pd.read_excel(file_path, engine='openpyxl')
        print(f"Loaded {len(df)} rows from Excel")
        return df
    
    def transform_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Transform Excel data to ServiceNow format
        Customize this method based on your specific columns and requirements
        """
        records = []
        
        for idx, row in df.iterrows():
            # Example transformation - adjust based on your actual columns
            record = {
                # Map your Excel columns to ServiceNow fields
                # Example mappings (customize these):
                'id': str(row.get('alert_id', '')),
                'email': str(row.get('Email', '')),
                'phone': str(row.get('Phone', '')),
                'status': str(row.get('Status', '')),
                'last_contact': str(row.get('Last_Contact', '')),
                'notes': str(row.get('Notes', '')),
                'updated_at': datetime.now().isoformat(),
                # Add more field mappings as needed
            }
            
            # Filter out empty values
            record = {k: v for k, v in record.items() if v and v != 'nan'}
            
            if record:  # Only add if there's actual data
                records.append(record)
        
        print(f"Transformed {len(records)} records for ServiceNow")
        return records
    
    def get_existing_records(self) -> Dict[str, str]:
        """Fetch existing records from ServiceNow to check for duplicates"""
        print("Fetching existing records from ServiceNow...")
        
        try:
            response = requests.get(
                self.base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                params={'sysparm_limit': 10000}  # Adjust as needed
            )
            response.raise_for_status()
            
            existing = {}
            for record in response.json().get('result', []):
                # Use a unique identifier - adjust based on your data
                key = record.get('email') or record.get('name')
                if key:
                    existing[key] = record['sys_id']
            
            print(f"Found {len(existing)} existing records")
            return existing
            
        except Exception as e:
            print(f"Error fetching existing records: {e}")
            return {}
    
    def create_record(self, data: Dict) -> bool:
        """Create a new record in ServiceNow"""
        try:
            response = requests.post(
                self.base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                json=data
            )
            response.raise_for_status()
            print(f"✓ Created record: {data.get('name', 'Unknown')}")
            return True
        except Exception as e:
            print(f"✗ Error creating record {data.get('name', 'Unknown')}: {e}")
            return False
    
    def update_record(self, sys_id: str, data: Dict) -> bool:
        """Update an existing record in ServiceNow"""
        try:
            response = requests.patch(
                f"{self.base_url}/{sys_id}",
                auth=(self.username, self.password),
                headers=self.headers,
                json=data
            )
            response.raise_for_status()
            print(f"✓ Updated record: {data.get('name', 'Unknown')}")
            return True
        except Exception as e:
            print(f"✗ Error updating record {data.get('name', 'Unknown')}: {e}")
            return False
    
    def sync_records(self, records: List[Dict]) -> tuple:
        """Sync records to ServiceNow (create or update)"""
        existing = self.get_existing_records()
        
        created = 0
        updated = 0
        failed = 0
        
        for record in records:
            # Use email or name as unique identifier - adjust as needed
            identifier = record.get('email') or record.get('name')
            
            if not identifier:
                print(f"⚠ Skipping record without identifier")
                failed += 1
                continue
            
            if identifier in existing:
                # Update existing record
                if self.update_record(existing[identifier], record):
                    updated += 1
                else:
                    failed += 1
            else:
                # Create new record
                if self.create_record(record):
                    created += 1
                else:
                    failed += 1
        
        return created, updated, failed

def main():
    print("=" * 60)
    print("Starting ServiceNow Sync Process")
    print("=" * 60)
    
    try:
        # Initialize sync client
        sync = ServiceNowSync()
        
        # Read Excel data
        excel_file = "SeniorConnect_MasterLog.xlsx"
        df = sync.read_excel_data(excel_file)
        
        # Transform data
        records = sync.transform_data(df)
        
        if not records:
            print("⚠ No records to sync")
            return
        
        # Sync to ServiceNow
        created, updated, failed = sync.sync_records(records)
        
        # Print summary
        print("\n" + "=" * 60)
        print("Sync Summary:")
        print(f"  ✓ Created: {created}")
        print(f"  ✓ Updated: {updated}")
        print(f"  ✗ Failed:  {failed}")
        print(f"  📊 Total:   {len(records)}")
        print("=" * 60)
        
        # Exit with error if any failed
        if failed > 0:
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
