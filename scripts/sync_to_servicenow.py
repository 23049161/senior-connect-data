#!/usr/bin/env python3
"""
Script to process Excel data and push to ServiceNow
Requirements:
- ALERTS sheet: Push ALL data, but only NEW records (no duplicates on re-runs)
- Other sheets: Only push FIRST ROW where Hour = 12 and FIRST ROW where Hour = 20

Excel columns (actual structure):
- Date (Column A)
- Timestamp (Column B) 
- Hour (Column C) - Contains hour value like 12, 18, 20
- Location (Column D)
- Value (Column E)
- Status (Column F)

ServiceNow tables:
- x_1855398_elderl_0_iot_alert_event - For ALERTS sheet
- x_1855398_elderl_0_iot_sensor_record - For sensor sheets
- x_1855398_elderl_0_sensor_type - For sensor type mapping
"""
import os
import sys
import pandas as pd
import requests
from datetime import datetime
from typing import Dict, List

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ Loaded environment variables from .env file")
except ImportError:
    print("⚠ python-dotenv not installed. Using system environment variables only.")
    print("  Install with: pip install python-dotenv")

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ Loaded environment variables from .env file")
except ImportError:
    print("⚠ python-dotenv not installed. Using system environment variables only.")
    print("  Install with: pip install python-dotenv")

class ServiceNowSync:
    def __init__(self):
        self.instance = os.environ.get('SERVICENOW_INSTANCE')
        self.username = os.environ.get('SERVICENOW_USERNAME')
        self.password = os.environ.get('SERVICENOW_PASSWORD')
        
        if not all([self.instance, self.username, self.password]):
            raise ValueError("Missing required ServiceNow credentials in environment variables")
        
        # Correct table names with app scope prefix
        self.alert_table = "x_1855398_elderl_0_iot_alert_event"
        self.sensor_table = "x_1855398_elderl_0_iot_sensor_record"
        self.sensor_type_table = "x_1855398_elderl_0_sensor_type"
        
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Cache for sensor types
        self.sensor_types = {}
        
    def get_sensor_types(self) -> Dict[str, Dict]:
        """Fetch sensor types from ServiceNow"""
        print(f"Fetching sensor types from {self.sensor_type_table}...")
        
        base_url = f"https://{self.instance}.service-now.com/api/now/table/{self.sensor_type_table}"
        
        try:
            response = requests.get(
                base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                params={'sysparm_limit': 1000},
                timeout=30
            )
            response.raise_for_status()
            
            sensor_types = {}
            for record in response.json().get('result', []):
                sensor_type_id = record.get('sensor_type_id', '')
                type_name = record.get('type_name', '')
                
                # Map by type_name (PIR, Temperature, Humidity, Proximity, mmWave)
                if type_name:
                    sensor_types[type_name] = {
                        'sensor_type_id': sensor_type_id,
                        'type_name': type_name,
                        'elderly_id': record.get('elderly_id', '')
                    }
                    print(f"  Found sensor: {sensor_type_id} - {type_name}")
            
            print(f"Loaded {len(sensor_types)} sensor types")
            return sensor_types
            
        except requests.exceptions.RequestException as e:
            print(f"⚠ Error fetching sensor types: {e}")
            print(f"⚠ Continuing without sensor type mapping")
            return {}
        
    def read_all_sheets(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """Read all sheets from Excel file"""
        print(f"Reading all sheets from Excel file: {file_path}")
        
        # Read all sheets into a dictionary
        all_sheets = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
        
        print(f"Found {len(all_sheets)} sheets:")
        for sheet_name in all_sheets.keys():
            print(f"  - {sheet_name}: {len(all_sheets[sheet_name])} rows")
        
        return all_sheets
    
    def filter_sensor_data_by_hour(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For SENSOR sheets only:
        Filter to get FIRST ROW with time at 12pm (noon)
        and FIRST ROW with time at 8pm (20:00)
        """
        if df.empty:
            return df
        
        print(f"Available columns: {list(df.columns)}")
        
        # Check for required column
        if 'Timestamp' not in df.columns:
            print(f"⚠ Warning: 'Timestamp' column not found. Using all data.")
            return df
        
        # Convert Timestamp to datetime and extract hour
        df['_timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        df = df.dropna(subset=['_timestamp'])
        
        if df.empty:
            print("⚠ No valid timestamp values found")
            return df
        
        # Extract hour from timestamp
        df['_hour'] = df['_timestamp'].dt.hour
        
        # Find FIRST row with time at 12pm (hour = 12)
        rows_12pm = df[df['_hour'] == 12]
        
        # Find FIRST row with time at 8pm (hour = 20)
        rows_8pm = df[df['_hour'] == 20]
        
        filtered_records = []
        
        if not rows_12pm.empty:
            first_12pm = rows_12pm.iloc[0]
            filtered_records.append(first_12pm)
            print(f"  ✓ Selected FIRST 12pm row: Excel row {rows_12pm.index[0] + 2}, Time: {first_12pm['Timestamp']}")
        else:
            print(f"  ⚠ No rows found with time at 12pm")
        
        if not rows_8pm.empty:
            first_8pm = rows_8pm.iloc[0]
            filtered_records.append(first_8pm)
            print(f"  ✓ Selected FIRST 8pm row: Excel row {rows_8pm.index[0] + 2}, Time: {first_8pm['Timestamp']}")
        else:
            print(f"  ⚠ No rows found with time at 8pm")
        
        # Create dataframe from filtered records
        if filtered_records:
            result_df = pd.DataFrame(filtered_records)
            result_df = result_df.drop(['_timestamp', '_hour'], axis=1, errors='ignore')
            print(f"✓ Filtered to {len(result_df)} row(s)")
            return result_df
        else:
            print("⚠ No records found with time at 12pm or 8pm")
            return pd.DataFrame()
    
    def get_sensor_type_id_for_sheet(self, sheet_name: str) -> str:
        """
        Match Excel sheet name to sensor type from ServiceNow
        Sheet names match type_name field in sensor_type table
        e.g., "Humidity" sheet → find sensor with type_name = "Humidity"
        
        Special case: Any sheet with "mmwave" in name → SENSOR 5 (mmWave)
        """
        # Normalize sheet name for matching
        sheet_name_normalized = sheet_name.strip()
        sheet_name_lower = sheet_name_normalized.lower()
        
        # Special case: Check if sheet name contains "mmwave" (case-insensitive)
        if 'mmwave' in sheet_name_lower:
            # Find the mmWave sensor type
            for type_name, sensor_info in self.sensor_types.items():
                if type_name.lower() == 'mmwave':
                    sensor_type_id = sensor_info['sensor_type_id']
                    print(f"  Matched sheet '{sheet_name}' (contains 'mmwave') → Sensor: {sensor_type_id} ({sensor_info['type_name']})")
                    return sensor_type_id
            # If mmWave sensor type not found in table, use SENSOR 5 as default
            print(f"  Matched sheet '{sheet_name}' (contains 'mmwave') → Using default SENSOR 5")
            return 'SENSOR 5'
        
        # Check if sheet name matches any sensor type_name exactly
        if sheet_name_normalized in self.sensor_types:
            sensor_info = self.sensor_types[sheet_name_normalized]
            sensor_type_id = sensor_info['sensor_type_id']
            print(f"  Matched sheet '{sheet_name}' → Sensor: {sensor_type_id} ({sensor_info['type_name']})")
            return sensor_type_id
        
        # If no exact match, try case-insensitive matching
        for type_name, sensor_info in self.sensor_types.items():
            if type_name.lower() == sheet_name_lower:
                sensor_type_id = sensor_info['sensor_type_id']
                print(f"  Matched sheet '{sheet_name}' → Sensor: {sensor_type_id} ({sensor_info['type_name']})")
                return sensor_type_id
        
        # No match found
        print(f"  ⚠ No sensor type found for sheet '{sheet_name}', using default 'SENSOR 1'")
        return 'SENSOR 1'
    
    def transform_alert_data(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """
        Transform ALERTS sheet data for iot_alert_event table
        Pushes ALL data from the sheet
        
        ServiceNow fields based on screenshot:
        - sensor_type_id
        - alert_date
        - alert_time
        - location
        - severity
        - message
        """
        records = []
        
        # For alerts, use ALERT MONITOR SENSOR 1,3,4
        sensor_type_id = 'ALERT MONITOR SENSOR 1,3,4'
        
        for idx, row in df.iterrows():
            # Combine Date and Timestamp for the date/time fields
            alert_date = str(row.get('Date', ''))
            timestamp = str(row.get('Timestamp', ''))
            
            # Map the data
            record = {
                'sensor_type_id': sensor_type_id,
                'alert_date': alert_date,
                'alert_time': timestamp,
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
        
        ServiceNow fields based on screenshot:
        - sensor_type_id
        - record_date
        - record_time
        - location
        - numeric_value or text_value
        - status
        - is_active
        """
        records = []
        
        # Get the correct sensor_type_id for this sheet
        sensor_type_id = self.get_sensor_type_id_for_sheet(sheet_name)
        
        for idx, row in df.iterrows():
            # Extract data
            record_date = str(row.get('Date', ''))
            timestamp = str(row.get('Timestamp', ''))
            
            # Get value and determine if it's numeric or text
            value = row.get('Value', '')
            numeric_value = ''
            text_value = ''
            
            # Convert value to string for checking
            value_str = str(value).strip()
            
            # Skip if value is NaN, NaT, empty, or looks like a date/time
            if value_str in ['nan', 'NaT', '', 'None']:
                text_value = ''
            # Check if it's a timestamp/date format (contains colons or dashes in date pattern)
            elif ':' in value_str or (len(value_str) >= 8 and value_str.count('-') >= 2):
                # This looks like a time or date, treat as text
                text_value = value_str
            else:
                # Try to parse as numeric (handles integers, floats, percentages, degrees)
                try:
                    # Clean the value - remove common non-numeric suffixes
                    clean_value = value_str.replace('%', '').replace('°C', '').replace('°F', '').replace('°', '').strip()
                    numeric_val = float(clean_value)
                    numeric_value = str(numeric_val)
                except (ValueError, TypeError, AttributeError):
                    # Not numeric, use as text
                    text_value = value_str
            
            # Map the data
            record = {
                'sensor_type_id': sensor_type_id,
                'record_date': record_date,
                'record_time': timestamp,
                'location': str(row.get('Location', '')),
                'status': str(row.get('Status', '')),
                'is_active': 'false',
            }
            
            # Add numeric or text value (prefer numeric if both are set)
            if numeric_value and numeric_value != 'nan':
                record['numeric_value'] = numeric_value
            elif text_value and text_value != 'nan':
                record['text_value'] = text_value
            
            # Filter out empty values
            record = {k: v for k, v in record.items() if v and v != 'nan' and v != 'NaT'}
            
            if record and len(record) > 2:
                records.append(record)
        
        print(f"Transformed {len(records)} sensor records from sheet '{sheet_name}'")
        return records
    
    def normalize_value(self, value) -> str:
        """Normalize values for consistent comparison"""
        if value is None or value == '':
            return ''
        
        value_str = str(value).strip()
        
        # Normalize common variations
        if value_str.lower() in ['nan', 'nat', 'none', 'null']:
            return ''
        
        # For time values, normalize to remove microseconds and timezone info
        # e.g., "20:57:45.000" -> "20:57:45"
        if ':' in value_str and len(value_str) > 5:
            # Remove microseconds if present
            if '.' in value_str:
                value_str = value_str.split('.')[0]
            # Remove timezone info if present
            if '+' in value_str:
                value_str = value_str.split('+')[0].strip()
            if value_str.endswith('Z'):
                value_str = value_str[:-1].strip()
        
        # Normalize numeric values to consistent format
        try:
            # If it's a number, normalize it
            num = float(value_str)
            # Remove trailing zeros and decimal point if integer
            if num == int(num):
                return str(int(num))
            else:
                return str(num)
        except (ValueError, TypeError):
            pass
        
        return value_str
    
    def get_existing_records(self, table: str) -> Dict[str, str]:
        """Fetch existing records from ServiceNow to check for duplicates"""
        print(f"Fetching existing records from {table}...")
        
        base_url = f"https://{self.instance}.service-now.com/api/now/table/{table}"
        
        try:
            response = requests.get(
                base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                params={'sysparm_limit': 10000},
                timeout=30
            )
            response.raise_for_status()
            
            existing = {}
            for record in response.json().get('result', []):
                # Create unique key from ALL relevant fields to detect exact duplicates
                if table == self.alert_table:
                    date = self.normalize_value(record.get('alert_date', ''))
                    time_val = self.normalize_value(record.get('alert_time', ''))
                    location = self.normalize_value(record.get('location', ''))
                    severity = self.normalize_value(record.get('severity', ''))
                    message = self.normalize_value(record.get('message', ''))
                    sensor_id = self.normalize_value(record.get('sensor_type_id', ''))
                    key = f"{date}|{time_val}|{location}|{severity}|{message}|{sensor_id}"
                else:
                    # For sensor records, include ALL fields in key to detect exact duplicates
                    date = self.normalize_value(record.get('record_date', ''))
                    time_val = self.normalize_value(record.get('record_time', ''))
                    location = self.normalize_value(record.get('location', ''))
                    sensor_id = self.normalize_value(record.get('sensor_type_id', ''))
                    status = self.normalize_value(record.get('status', ''))
                    numeric_val = self.normalize_value(record.get('numeric_value', ''))
                    text_val = self.normalize_value(record.get('text_value', ''))
                    is_active = self.normalize_value(record.get('is_active', ''))
                    # Create comprehensive key with ALL columns
                    key = f"{date}|{time_val}|{location}|{sensor_id}|{status}|{numeric_val}|{text_val}|{is_active}"
                
                if key:
                    existing[key] = record['sys_id']
            
            print(f"Found {len(existing)} existing records in {table}")
            return existing
            
        except requests.exceptions.RequestException as e:
            print(f"⚠ Error fetching existing records from {table}: {e}")
            print(f"⚠ Continuing without duplicate check - all records will be created")
            return {}
    
    def create_record(self, table: str, data: Dict) -> bool:
        """Create a new record in ServiceNow"""
        base_url = f"https://{self.instance}.service-now.com/api/now/table/{table}"
        
        try:
            response = requests.post(
                base_url,
                auth=(self.username, self.password),
                headers=self.headers,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            
            location = data.get('location', 'Unknown')
            sensor_id = data.get('sensor_type_id', 'Unknown')
            if table == self.alert_table:
                identifier = f"{data.get('alert_date')} {data.get('alert_time')} - {location} - {sensor_id}"
            else:
                identifier = f"{data.get('record_date')} {data.get('record_time')} - {location} - {sensor_id}"
            
            print(f"✓ Created record in {table}: {identifier}")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error creating record in {table}: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"  Response: {e.response.text[:500]}")  # First 500 chars
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
            # Create unique identifier matching ALL fields with normalization
            if table == self.alert_table:
                date = self.normalize_value(record.get('alert_date', ''))
                time_val = self.normalize_value(record.get('alert_time', ''))
                location = self.normalize_value(record.get('location', ''))
                severity = self.normalize_value(record.get('severity', ''))
                message = self.normalize_value(record.get('message', ''))
                sensor_id = self.normalize_value(record.get('sensor_type_id', ''))
                identifier = f"{date}|{time_val}|{location}|{severity}|{message}|{sensor_id}"
            else:
                # For sensor records, use ALL fields with normalization to detect exact duplicates
                date = self.normalize_value(record.get('record_date', ''))
                time_val = self.normalize_value(record.get('record_time', ''))
                location = self.normalize_value(record.get('location', ''))
                sensor_id = self.normalize_value(record.get('sensor_type_id', ''))
                status = self.normalize_value(record.get('status', ''))
                numeric_val = self.normalize_value(record.get('numeric_value', ''))
                text_val = self.normalize_value(record.get('text_value', ''))
                is_active = self.normalize_value(record.get('is_active', 'false'))
                identifier = f"{date}|{time_val}|{location}|{sensor_id}|{status}|{numeric_val}|{text_val}|{is_active}"
            
            if not date or not time_val:
                print(f"⚠ Skipping record without date or time: {record}")
                failed += 1
                continue
            
            if identifier in existing:
                # Skip - already exists
                print(f"  ⏭ Skipping duplicate: {date} {time_val} - {location} - {sensor_id}")
                skipped += 1
            else:
                # Create new record
                if self.create_record(table, record):
                    created += 1
                else:
                    failed += 1
        
        if skipped > 0:
            print(f"⏭ Skipped {skipped} existing record(s)")
        
        return created, skipped, failed

def main():
    print("=" * 80)
    print("Starting ServiceNow Sync Process")
    print("Configuration:")
    print("  - ALERTS sheet: Push ALL data (only NEW records)")
    print("  - Other sheets: Push FIRST row with time at 12pm and FIRST row with time at 8pm")
    print("  - Using sensor_type table to match sensors with Excel sheets")
    print("=" * 80)
    
    try:
        # Initialize sync client
        sync = ServiceNowSync()
        
        # Load sensor types from ServiceNow
        sync.sensor_types = sync.get_sensor_types()
        
        # Read all sheets from Excel
        # Get excel file from command line argument or use default
        if len(sys.argv) > 1:
            excel_file = sys.argv[1]
        else:
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
                print(f"📢 ALERTS sheet → Pushing ALL data to {sync.alert_table}")
                print(f"Total rows in sheet: {len(df)}")
                records = sync.transform_alert_data(df, sheet_name)
                created, skipped, failed = sync.sync_records(sync.alert_table, records)
            else:
                # OTHER SHEETS: Filter to time at 12pm and 8pm FIRST ROWS only
                print(f"📊 Sensor sheet → Filtering for time at 12pm and 8pm FIRST rows")
                filtered_df = sync.filter_sensor_data_by_hour(df)
                
                if filtered_df.empty:
                    print(f"⚠ No data with time at 12pm or 8pm in sheet '{sheet_name}'")
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
        
        # Don't exit with error if some succeeded
        if total_failed > 0 and total_created == 0:
            print("⚠ All records failed - exiting with error")
            sys.exit(1)
        elif total_failed > 0:
            print("⚠ Some records failed but some succeeded - exiting successfully")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
