#!/usr/bin/env python3
"""
Script to process Excel data and push to ServiceNow
Requirements:
- Only push FIRST data from 12pm and 8pm
- Read data from ALL sheets
- ALERTS sheet → iot_alert_event table
- All other sheets → iot_sensor_record table

Excel columns:
- Date (Column A)
- Timestamp Hour (Column B)
- Location (Column C)
- Value (Column D)
- Status (Column E)

ServiceNow tables:
- iot_alert_event: alert_id, sensor_type_id, alert_date, alert_time, location, severity, message
- iot_sensor_record: sensor_record_id, sensor_type_id, record_date, record_time, location, numeric_value, text_value, status, is_active
"""
import os
import sys
import pandas as pd
import requests
from datetime import datetime, time
from typing import Dict, List

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
        Uses the 'Date' and 'Timestamp Hour' columns
        """
        if df.empty:
            return df
        
        print(f"Available columns: {list(df.columns)}")
        
        # Check for required columns
        if 'Date' not in df.columns or 'Timestamp Hour' not in df.columns:
            print(f"⚠ Warning: Required columns 'Date' or 'Timestamp Hour' not found. Using all data.")
            return df
        
        # Create a combined datetime column
        df['_datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Timestamp Hour'].astype(str), 
                                         errors='coerce')
        
        # Remove rows with invalid timestamps
        df = df.dropna(subset=['_datetime'])
        
        if df.empty:
            print("⚠ No valid timestamps found after parsing")
            return df
        
        print(f"Parsed {len(df)} rows with valid timestamps")
        
        # Extract time and date components
        df['_time_only'] = df['_datetime'].dt.time
        df['_date'] = df['_datetime'].dt.date
        
        # Define target times (12pm and 8pm)
        target_time_12pm = time(12, 0)  # 12:00 PM
        target_time_8pm = time(20, 0)   # 8:00 PM (20:00)
        
        filtered_records = []
        
        for date, group in df.groupby('_date'):
            # Sort by time
            group = group.sort_values('_datetime')
            
            # Find first record at or after 12pm
            noon_records = group[group['_time_only'] >= target_time_12pm]
            if not noon_records.empty:
                filtered_records.append(noon_records.iloc[0])
                print(f"  ✓ Selected 12pm record for {date}: {noon_records.iloc[0]['_datetime']}")
            
            # Find first record at or after 8pm
            evening_records = group[group['_time_only'] >= target_time_8pm]
            if not evening_records.empty:
                first_evening = evening_records.iloc[0]
                # Make sure it's different from the 12pm record
                if not filtered_records or not first_evening['_datetime'] == filtered_records[-1]['_datetime']:
                    filtered_records.append(first_evening)
                    print(f"  ✓ Selected 8pm record for {date}: {first_evening['_datetime']}")
        
        # Create new dataframe from filtered records
        if filtered_records:
            result_df = pd.DataFrame(filtered_records)
            # Drop helper columns
            result_df = result_df.drop(['_time_only', '_date', '_datetime'], axis=1, errors='ignore')
            print(f"✓ Filtered to {len(result_df)} records (first at 12pm and 8pm each day)")
            return result_df
        else:
            print("⚠ No records found matching 12pm or 8pm criteria")
            return pd.DataFrame()
    
    def transform_alert_data(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """
        Transform ALERTS sheet data for iot_alert_event table
        
        Excel columns → ServiceNow fields:
        - Date → alert_date
        - Timestamp Hour → alert_time
        - Location → location
        - Value → severity (CRITICAL, MINIMAL, MODERATE)
        - Status → message (FALL_DETECTED, etc.)
        """
        records = []
        
        for idx, row in df.iterrows():
            # Extract date and time
            alert_date = str(row.get('Date', ''))
            alert_time = str(row.get('Timestamp Hour', ''))
            
            # Map the data
            record = {
                'sensor_type_id': 'SENSOR 1',  # Based on your ServiceNow screenshot
                'alert_date': alert_date,
                'alert_time': alert_time,
                'location': str(row.get('Location', '')),
                'severity': str(row.get('Value', '')),  # CRITICAL, MINIMAL, MODERATE
                'message': str(row.get('Status', '')),  # FALL_DETECTED, etc.
            }
            
            # Filter out empty values
            record = {k: v for k, v in record.items() if v and v != 'nan' and v != 'NaT'}
            
            if record and len(record) > 2:
                records.append(record)
        
        print(f"Transformed {len(records)} alert records from sheet '{sheet_name}'")
        return records
    
    def transform_sensor_data(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """
        Transform sensor sheet data for iot_sensor_record table
        
        Excel columns → ServiceNow fields:
        - Date → record_date
        - Timestamp Hour → record_time
        - Location → location
        - Value → numeric_value (if number) or text_value (if text)
        - Status → status
        """
        records = []
        
        for idx, row in df.iterrows():
            # Extract date and time
            record_date = str(row.get('Date', ''))
            record_time = str(row.get('Timestamp Hour', ''))
            
            # Get value and determine if it's numeric or text
            value = row.get('Value', '')
            numeric_value = ''
            text_value = ''
            
            try:
                # Try to convert to float
                numeric_value = str(float(value))
            except (ValueError, TypeError):
                # If it fails, treat as text
                text_value = str(value)
            
            # Map the data
            record = {
                'sensor_type_id': 'SENSOR 1',  # Based on your ServiceNow screenshot
                'record_date': record_date,
                'record_time': record_time,
                'location': str(row.get('Location', '')),
                'status': str(row.get('Status', '')),
                'is_active': 'false',  # Default from your screenshot
            }
            
            # Add numeric or text value
            if numeric_value:
                record['numeric_value'] = numeric_value
            if text_value:
                record['text_value'] = text_value
            
            # Filter out empty values
            record = {k: v for k, v in record.items() if v and v != 'nan' and v != 'NaT'}
            
            if record and len(record) > 2:
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
                # Create unique key from date + time + location
                if table == self.alert_table:
                    date = record.get('alert_date', '')
                    time = record.get('alert_time', '')
                else:
                    date = record.get('record_date', '')
                    time = record.get('record_time', '')
                
                location = record.get('location', '')
                key = f"{date}_{time}_{location}"
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
            
            location = data.get('location', 'Unknown')
            if table == self.alert_table:
                identifier = f"{data.get('alert_date')} {data.get('alert_time')} - {location}"
            else:
                identifier = f"{data.get('record_date')} {data.get('record_time')} - {location}"
            
            print(f"✓ Created record in {table}: {identifier}")
            return True
            
        except Exception as e:
            print(f"✗ Error creating record in {table}: {e}")
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
            
            location = data.get('location', 'Unknown')
            print(f"✓ Updated record in {table}: {location}")
            return True
            
        except Exception as e:
            print(f"✗ Error updating record in {table}: {e}")
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
            # Create unique identifier
            if table == self.alert_table:
                date = record.get('alert_date', '')
                time = record.get('alert_time', '')
            else:
                date = record.get('record_date', '')
                time = record.get('record_time', '')
            
            location = record.get('location', '')
            identifier = f"{date}_{time}_{location}"
            
            if not date or not time:
                print(f"⚠ Skipping record without date or time")
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
    print("=" * 80)
    print("Starting ServiceNow Sync Process")
    print("Custom Configuration:")
    print("  - Filtering: First data at 12pm and 8pm only")
    print("  - Reading: All sheets from Excel")
    print("  - ALERTS sheet → iot_alert_event table")
    print("  - Other sheets → iot_sensor_record table")
    print("=" * 80)
    
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
            print(f"\n{'=' * 80}")
            print(f"Processing sheet: {sheet_name}")
            print(f"{'=' * 80}")
            
            if df.empty:
                print(f"⚠ Sheet '{sheet_name}' is empty, skipping")
                continue
            
            # Filter to get only first data at 12pm and 8pm
            filtered_df = sync.filter_time_data(df)
            
            if filtered_df.empty:
                print(f"⚠ No data matching time criteria in sheet '{sheet_name}'")
                continue
            
            # Check if this is the ALERTS sheet
            is_alert_sheet = sheet_name.upper() == 'ALERTS'
            
            if is_alert_sheet:
                # Transform and sync to iot_alert_event
                print(f"📢 Identified as ALERTS sheet → sending to iot_alert_event")
                records = sync.transform_alert_data(filtered_df, sheet_name)
                created, updated, failed = sync.sync_records(sync.alert_table, records)
            else:
                # Transform and sync to iot_sensor_record
                print(f"📊 Identified as sensor sheet → sending to iot_sensor_record")
                records = sync.transform_sensor_data(filtered_df, sheet_name)
                created, updated, failed = sync.sync_records(sync.sensor_table, records)
            
            total_created += created
            total_updated += updated
            total_failed += failed
            
            print(f"Sheet '{sheet_name}' sync: {created} created, {updated} updated, {failed} failed")
        
        # Print overall summary
        print("\n" + "=" * 80)
        print("Overall Sync Summary:")
        print(f"  ✓ Total Created: {total_created}")
        print(f"  ✓ Total Updated: {total_updated}")
        print(f"  ✗ Total Failed:  {total_failed}")
        print("=" * 80)
        
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
