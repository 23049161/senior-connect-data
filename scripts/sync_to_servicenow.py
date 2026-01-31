#!/usr/bin/env python3
"""
Script to process Excel data and push to ServiceNow
Requirements:
- ALERTS sheet: Push ALL data, but only NEW records (no duplicates on re-runs)
- Other sheets: Only push FIRST ROW where Timestamp Hour = 12:00 and FIRST ROW where Timestamp Hour = 20:00

Excel columns:
- Date (Column A)
- Timestamp Hour (Column B) - Format: HH:MM:SS (e.g., 12:00, 18:00, 20:00)
- Location (Column C)
- Value (Column D)
- Status (Column E)

ServiceNow tables:
- iot_alert_event: For ALERTS sheet (all data)
- iot_sensor_record: For other sheets (12pm and 8pm only)
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
    
    def filter_sensor_data_by_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For SENSOR sheets only:
        Filter to get FIRST ROW where Timestamp Hour = 12:00 
        and FIRST ROW where Timestamp Hour = 20:00 (8pm)
        """
        if df.empty:
            return df
        
        print(f"Available columns: {list(df.columns)}")
        
        # Check for required column
        if 'Timestamp Hour' not in df.columns:
            print(f"⚠ Warning: 'Timestamp Hour' column not found. Using all data.")
            return df
        
        # Convert Timestamp Hour to time format for comparison
        # Handle different formats: "18:00", "18:00:00", etc.
        def parse_time_hour(time_str):
            try:
                time_str = str(time_str).strip()
                # Try parsing as time
                if ':' in time_str:
                    parts = time_str.split(':')
                    hour = int(parts[0])
                    minute = int(parts[1]) if len(parts) > 1 else 0
                    return time(hour, minute)
                return None
            except:
                return None
        
        df['_parsed_time'] = df['Timestamp Hour'].apply(parse_time_hour)
        df = df.dropna(subset=['_parsed_time'])
        
        if df.empty:
            print("⚠ No valid time values found")
            return df
        
        # Target times
        target_12pm = time(12, 0)
        target_8pm = time(20, 0)
        
        # Find FIRST row with 12:00
        rows_12pm = df[df['_parsed_time'] == target_12pm]
        
        # Find FIRST row with 20:00
        rows_8pm = df[df['_parsed_time'] == target_8pm]
        
        filtered_records = []
        
        if not rows_12pm.empty:
            first_12pm = rows_12pm.iloc[0]
            filtered_records.append(first_12pm)
            print(f"  ✓ Selected FIRST 12:00 row: Row {rows_12pm.index[0] + 2}")  # +2 for Excel row number
        else:
            print(f"  ⚠ No rows found with Timestamp Hour = 12:00")
        
        if not rows_8pm.empty:
            first_8pm = rows_8pm.iloc[0]
            filtered_records.append(first_8pm)
            print(f"  ✓ Selected FIRST 20:00 row: Row {rows_8pm.index[0] + 2}")  # +2 for Excel row number
        else:
            print(f"  ⚠ No rows found with Timestamp Hour = 20:00")
        
        # Create dataframe from filtered records
        if filtered_records:
            result_df = pd.DataFrame(filtered_records)
            result_df = result_df.drop(['_parsed_time'], axis=1, errors='ignore')
            print(f"✓ Filtered to {len(result_df)} row(s) (first at 12:00 and first at 20:00)")
            return result_df
        else:
            print("⚠ No records found matching 12:00 or 20:00 criteria")
            return pd.DataFrame()
    
    def transform_alert_data(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """
        Transform ALERTS sheet data for iot_alert_event table
        Pushes ALL data from the sheet
        """
        records = []
        
        for idx, row in df.iterrows():
            # Extract data
            alert_date = str(row.get('Date', ''))
            alert_time = str(row.get('Timestamp Hour', ''))
            
            # Map the data
            record = {
                'sensor_type_id': 'SENSOR 1',
                'alert_date': alert_date,
                'alert_time': alert_time,
                'location': str(row.get('Location', '')),
                'severity': str(row.get('Value', '')),
                'message': str(row.get('Status', '')),
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
        Only processes filtered data (12pm and 8pm rows)
        """
        records = []
        
        for idx, row in df.iterrows():
            # Extract data
            record_date = str(row.get('Date', ''))
            record_time = str(row.get('Timestamp Hour', ''))
            
            # Get value and determine if it's numeric or text
            value = row.get('Value', '')
            numeric_value = ''
            text_value = ''
            
            try:
                numeric_value = str(float(value))
            except (ValueError, TypeError):
                text_value = str(value)
            
            # Map the data
            record = {
                'sensor_type_id': 'SENSOR 1',
                'record_date': record_date,
                'record_time': record_time,
                'location': str(row.get('Location', '')),
                'status': str(row.get('Status', '')),
                'is_active': 'false',
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
                    time_val = record.get('alert_time', '')
                    location = record.get('location', '')
                    severity = record.get('severity', '')
                    # For alerts, include severity to handle multiple alerts at same time/location
                    key = f"{date}_{time_val}_{location}_{severity}"
                else:
                    date = record.get('record_date', '')
                    time_val = record.get('record_time', '')
                    location = record.get('location', '')
                    key = f"{date}_{time_val}_{location}"
                
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
        """Sync records to ServiceNow (create new records only, skip duplicates)"""
        if not records:
            print(f"No records to sync to {table}")
            return 0, 0, 0
        
        existing = self.get_existing_records(table)
        
        created = 0
        skipped = 0
        failed = 0
        
        for record in records:
            # Create unique identifier
            if table == self.alert_table:
                date = record.get('alert_date', '')
                time_val = record.get('alert_time', '')
                location = record.get('location', '')
                severity = record.get('severity', '')
                identifier = f"{date}_{time_val}_{location}_{severity}"
            else:
                date = record.get('record_date', '')
                time_val = record.get('record_time', '')
                location = record.get('location', '')
                identifier = f"{date}_{time_val}_{location}"
            
            if not date or not time_val:
                print(f"⚠ Skipping record without date or time")
                failed += 1
                continue
            
            if identifier in existing:
                # Skip - already exists
                skipped += 1
            else:
                # Create new record
                if self.create_record(table, record):
                    created += 1
                else:
                    failed += 1
        
        if skipped > 0:
            print(f"⏭ Skipped {skipped} existing record(s) (no duplicates created)")
        
        return created, skipped, failed

def main():
    print("=" * 80)
    print("Starting ServiceNow Sync Process")
    print("Configuration:")
    print("  - ALERTS sheet: Push ALL data (only NEW records)")
    print("  - Other sheets: Push FIRST row at 12:00 and FIRST row at 20:00 only")
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
        total_skipped = 0
        total_failed = 0
        
        # Process each sheet
        for sheet_name, df in all_sheets.items():
            print(f"\n{'=' * 80}")
            print(f"Processing sheet: {sheet_name}")
            print(f"{'=' * 80}")
            
            if df.empty:
                print(f"⚠ Sheet '{sheet_name}' is empty, skipping")
                continue
            
            # Check if this is the ALERTS sheet
            is_alert_sheet = sheet_name.upper() == 'ALERTS'
            
            if is_alert_sheet:
                # ALERTS: Push ALL data
                print(f"📢 ALERTS sheet → Pushing ALL data to iot_alert_event")
                print(f"Total rows in sheet: {len(df)}")
                records = sync.transform_alert_data(df, sheet_name)
                created, skipped, failed = sync.sync_records(sync.alert_table, records)
            else:
                # OTHER SHEETS: Filter to 12pm and 8pm FIRST ROWS only
                print(f"📊 Sensor sheet → Filtering for 12:00 and 20:00 FIRST rows")
                filtered_df = sync.filter_sensor_data_by_time(df)
                
                if filtered_df.empty:
                    print(f"⚠ No data matching time criteria in sheet '{sheet_name}'")
                    continue
                
                records = sync.transform_sensor_data(filtered_df, sheet_name)
                created, skipped, failed = sync.sync_records(sync.sensor_table, records)
            
            total_created += created
            total_skipped += skipped
            total_failed += failed
            
            print(f"Sheet '{sheet_name}' sync: {created} created, {skipped} skipped, {failed} failed")
        
        # Print overall summary
        print("\n" + "=" * 80)
        print("Overall Sync Summary:")
        print(f"  ✓ Total Created: {total_created}")
        print(f"  ⏭ Total Skipped: {total_skipped} (already existed)")
        print(f"  ✗ Total Failed:  {total_failed}")
        print("=" * 80)
        
        # Exit with error only if all records failed
        if total_failed > 0 and total_created == 0:
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
