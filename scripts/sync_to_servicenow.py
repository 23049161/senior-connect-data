#!/usr/bin/env python3
"""
Script to process Excel data and push specific fields to ServiceNow
Custom requirements:
- Only push FIRST data from 12pm and 8pm
- Read data from ALL sheets
- Alert sheet data → iot_alert_event table
- Other sheets → iot_sensor_record table
"""
import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime, time
from typing import Dict, List, Optional

class ServiceNowSync:
    def __init__(self):
        self.instance = os.environ.get('SERVICENOW_INSTANCE')
        self.username = os.environ.get('SERVICENOW_USERNAME')
        self.password = os.environ.get('SERVICENOW_PASSWORD')
        
        if not all([self.instance, self.username, self.password]):
            raise ValueError("Missing required ServiceNow credentials in environment variables")
        
        # Two different tables
        self.alert_table = "iot_alert_event"
        self.sensor_table = "iot_sensor_record"
        
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
    def read_all_sheets(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """Read all sheets from Excel file"""
        print(f"Reading all sheets from Excel file: {file_path}")
        
        # Read all sheets into a dictionary
        all_sheets = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
        
        print(f"Found {len(all_sheets)} sheets:")
        for sheet_name in all_sheets.keys():
            print(f"  - {sheet_name}: {len(all_sheets[sheet_name])} rows")
        
        return all_sheets
    
    def filter_time_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter to only get FIRST data from 12pm and 8pm
        Assumes there's a timestamp column - adjust column name as needed
        """
        if df.empty:
            return df
        
        # Try to find a timestamp/time column
        # Common column names - adjust based on your actual column names
        time_columns = ['timestamp', 'Timestamp', 'Time', 'time', 'DateTime', 'datetime', 'Date Time']
        time_col = None
        
        for col in time_columns:
            if col in df.columns:
                time_col = col
                break
        
        if time_col is None:
            print(f"⚠ Warning: No timestamp column found. Using all data.")
            print(f"Available columns: {list(df.columns)}")
            return df
        
        print(f"Using timestamp column: {time_col}")
        
        # Convert to datetime
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
        
        # Remove rows with invalid timestamps
        df = df.dropna(subset=[time_col])
        
        if df.empty:
            return df
        
        # Extract time component
        df['_time_only'] = df[time_col].dt.time
        
        # Define target times (12pm and 8pm)
        target_time_12pm = time(12, 0)  # 12:00 PM
        target_time_8pm = time(20, 0)   # 8:00 PM (20:00)
        
        # Group by date and find first occurrence at/after 12pm and 8pm
        df['_date'] = df[time_col].dt.date
        
        filtered_records = []
        
        for date, group in df.groupby('_date'):
            # Sort by time
            group = group.sort_values(time_col)
            
            # Find first record at or after 12pm
            noon_records = group[group['_time_only'] >= target_time_12pm]
            if not noon_records.empty:
                filtered_records.append(noon_records.iloc[0])
            
            # Find first record at or after 8pm
            evening_records = group[group['_time_only'] >= target_time_8pm]
            if not evening_records.empty:
                # Make sure it's not the same record as 12pm
                first_evening = evening_records.iloc[0]
                if filtered_records and not first_evening.equals(filtered_records[-1]):
                    filtered_records.append(first_evening)
                elif not filtered_records:
                    filtered_records.append(first_evening)
        
        # Create new dataframe from filtered records
        if filtered_records:
            result_df = pd.DataFrame(filtered_records)
            # Drop helper columns
            result_df = result_df.drop(['_time_only', '_date'], axis=1, errors='ignore')
            print(f"Filtered to {len(result_df)} records (first at 12pm and 8pm each day)")
            return result_df
        else:
            print("⚠ No records found matching 12pm or 8pm criteria")
            return pd.DataFrame()
    
    def transform_alert_data(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """Transform Alert sheet data for iot_alert_event table"""
        records = []
        
        for idx, row in df.iterrows():
            # Customize these field mappings based on your Alert sheet columns
            record = {
                'sheet_name': sheet_name,
                'alert_type': str(row.get('Alert_Type', row.get('Type', ''))),
                'severity': str(row.get('Severity', '')),
                'description': str(row.get('Description', row.get('Message', ''))),
                'sensor_id': str(row.get('Sensor_ID', row.get('SensorID', ''))),
                'timestamp': str(row.get('Timestamp', row.get('Time', row.get('DateTime', '')))),
                'value': str(row.get('Value', '')),
                'status': str(row.get('Status', '')),
                'updated_at': datetime.now().isoformat(),
            }
            
            # Filter out empty values
            record = {k: v for k, v in record.items() if v and v != 'nan' and v != 'NaT'}
            
            if record and len(record) > 2:  # Has more than just sheet_name and updated_at
                records.append(record)
        
        print(f"Transformed {len(records)} alert records from sheet '{sheet_name}'")
        return records
    
    def transform_sensor_data(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """Transform sensor data for iot_sensor_record table"""
        records = []
        
        for idx, row in df.iterrows():
            # Customize these field mappings based on your sensor sheet columns
            record = {
                'sheet_name': sheet_name,
                'sensor_id': str(row.get('Sensor_ID', row.get('SensorID', row.get('ID', '')))),
                'sensor_type': str(row.get('Sensor_Type', row.get('Type', ''))),
                'timestamp': str(row.get('Timestamp', row.get('Time', row.get('DateTime', '')))),
                'value': str(row.get('Value', row.get('Reading', ''))),
                'unit': str(row.get('Unit', row.get('Units', ''))),
                'location': str(row.get('Location', '')),
                'status': str(row.get('Status', '')),
                'temperature': str(row.get('Temperature', '')),
                'humidity': str(row.get('Humidity', '')),
                'pressure': str(row.get('Pressure', '')),
                'updated_at': datetime.now().isoformat(),
            }
            
            # Filter out empty values
            record = {k: v for k, v in record.items() if v and v != 'nan' and v != 'NaT'}
            
            if record and len(record) > 2:  # Has more than just sheet_name and updated_at
                records.append(record)
        
        print(f"Transformed {len(records)} sensor records from sheet '{sheet_name}'")
        return records
    
    def get_existing_records(self, table: str) -> Dict[str, str]:
        """Fetch existing records from ServiceNow to check for duplicates"""
        print(f"Fetching existing records from {table}...")
        
        base_url = f"https://{self.instance}.service-now.com/api/now/table/{table}"
        
        try:
            response = requests.get(
                base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                params={'sysparm_limit': 10000}
            )
            response.raise_for_status()
            
            existing = {}
            for record in response.json().get('result', []):
                # Use timestamp + sensor_id as unique identifier
                timestamp = record.get('timestamp', '')
                sensor_id = record.get('sensor_id', record.get('alert_type', ''))
                key = f"{timestamp}_{sensor_id}"
                if key:
                    existing[key] = record['sys_id']
            
            print(f"Found {len(existing)} existing records in {table}")
            return existing
            
        except Exception as e:
            print(f"Error fetching existing records from {table}: {e}")
            return {}
    
    def create_record(self, table: str, data: Dict) -> bool:
        """Create a new record in ServiceNow"""
        base_url = f"https://{self.instance}.service-now.com/api/now/table/{table}"
        
        try:
            response = requests.post(
                base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                json=data
            )
            response.raise_for_status()
            identifier = data.get('sensor_id', data.get('alert_type', 'Unknown'))
            print(f"✓ Created record in {table}: {identifier}")
            return True
        except Exception as e:
            identifier = data.get('sensor_id', data.get('alert_type', 'Unknown'))
            print(f"✗ Error creating record in {table} ({identifier}): {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"  Response: {e.response.text}")
            return False
    
    def update_record(self, table: str, sys_id: str, data: Dict) -> bool:
        """Update an existing record in ServiceNow"""
        base_url = f"https://{self.instance}.service-now.com/api/now/table/{table}/{sys_id}"
        
        try:
            response = requests.patch(
                base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                json=data
            )
            response.raise_for_status()
            identifier = data.get('sensor_id', data.get('alert_type', 'Unknown'))
            print(f"✓ Updated record in {table}: {identifier}")
            return True
        except Exception as e:
            identifier = data.get('sensor_id', data.get('alert_type', 'Unknown'))
            print(f"✗ Error updating record in {table} ({identifier}): {e}")
            return False
    
    def sync_records(self, table: str, records: List[Dict]) -> tuple:
        """Sync records to ServiceNow (create or update)"""
        if not records:
            print(f"No records to sync to {table}")
            return 0, 0, 0
        
        existing = self.get_existing_records(table)
        
        created = 0
        updated = 0
        failed = 0
        
        for record in records:
            # Create unique identifier from timestamp + sensor_id/alert_type
            timestamp = record.get('timestamp', '')
            sensor_id = record.get('sensor_id', record.get('alert_type', ''))
            identifier = f"{timestamp}_{sensor_id}"
            
            if not timestamp or not sensor_id:
                print(f"⚠ Skipping record without timestamp or identifier")
                failed += 1
                continue
            
            if identifier in existing:
                # Update existing record
                if self.update_record(table, existing[identifier], record):
                    updated += 1
                else:
                    failed += 1
            else:
                # Create new record
                if self.create_record(table, record):
                    created += 1
                else:
                    failed += 1
        
        return created, updated, failed

def main():
    print("=" * 60)
    print("Starting ServiceNow Sync Process")
    print("Custom Configuration:")
    print("  - Filtering: First data at 12pm and 8pm only")
    print("  - Reading: All sheets from Excel")
    print("  - Alert sheet → iot_alert_event table")
    print("  - Other sheets → iot_sensor_record table")
    print("=" * 60)
    
    try:
        # Initialize sync client
        sync = ServiceNowSync()
        
        # Read all sheets from Excel
        excel_file = "SeniorConnect_MasterLog.xlsx"
        all_sheets = sync.read_all_sheets(excel_file)
        
        if not all_sheets:
            print("⚠ No sheets found in Excel file")
            return
        
        total_created = 0
        total_updated = 0
        total_failed = 0
        
        # Process each sheet
        for sheet_name, df in all_sheets.items():
            print(f"\n{'=' * 60}")
            print(f"Processing sheet: {sheet_name}")
            print(f"{'=' * 60}")
            
            if df.empty:
                print(f"⚠ Sheet '{sheet_name}' is empty, skipping")
                continue
            
            # Filter to get only first data at 12pm and 8pm
            filtered_df = sync.filter_time_data(df)
            
            if filtered_df.empty:
                print(f"⚠ No data matching time criteria in sheet '{sheet_name}'")
                continue
            
            # Check if this is the Alert sheet
            is_alert_sheet = 'alert' in sheet_name.lower()
            
            if is_alert_sheet:
                # Transform and sync to iot_alert_event
                print(f"📢 Identified as Alert sheet → sending to iot_alert_event")
                records = sync.transform_alert_data(filtered_df, sheet_name)
                created, updated, failed = sync.sync_records(sync.alert_table, records)
            else:
                # Transform and sync to iot_sensor_record
                print(f"📊 Identified as Sensor sheet → sending to iot_sensor_record")
                records = sync.transform_sensor_data(filtered_df, sheet_name)
                created, updated, failed = sync.sync_records(sync.sensor_table, records)
            
            total_created += created
            total_updated += updated
            total_failed += failed
            
            print(f"Sheet '{sheet_name}' sync: {created} created, {updated} updated, {failed} failed")
        
        # Print overall summary
        print("\n" + "=" * 60)
        print("Overall Sync Summary:")
        print(f"  ✓ Total Created: {total_created}")
        print(f"  ✓ Total Updated: {total_updated}")
        print(f"  ✗ Total Failed:  {total_failed}")
        print("=" * 60)
        
        # Exit with error if any failed
        if total_failed > 0:
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
