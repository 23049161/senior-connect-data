import pandas as pd
import requests
import os

# Your GitHub RAW URL from Step 1
GITHUB_URL = "https://raw.githubusercontent.com/23049161/senior-connect-data/main/SeniorConnect_MasterLog.xlsx"

SERVICENOW_URL = "https://dev313533.service-now.com/api/now/table/x_1855398_elderl_0_iot_alert_event"
SERVICENOW_USER = os.getenv("SERVICENOW_USER", "admin")
SERVICENOW_PASS = os.getenv("SERVICENOW_PASS", "eG0rRKK0%-jx")

FILTER_COLUMN = "Status"
FILTER_VALUE = "Pending"

SERVICENOW_COLUMNS = {
    "Alert_Date": "alert_date",
    "Alert_Timestamp": "alert_timestamp", 
    "Hour": "hour",
    "Location": "location",
    "Severity": "severity",
    "Message": "short_description"
}

print("[INFO] Starting pipeline", flush=True)

# Read Excel directly from GitHub
all_sheets = pd.read_excel(GITHUB_URL, sheet_name=None)
print(f"[INFO] Found sheets: {list(all_sheets.keys())}", flush=True)

sent = 0
for sheet_name, df in all_sheets.items():
    if FILTER_COLUMN not in df.columns:
        print(f"[INFO] Skipping {sheet_name} (no Status column)")
        continue
        
    pending = df[df[FILTER_COLUMN] == FILTER_VALUE]
    print(f"[INFO] {len(pending)} pending rows in {sheet_name}")
    
    for idx, row in pending.iterrows():
        data = {sn_col: str(row.get(excel_col, "")) for excel_col, sn_col in SERVICENOW_COLUMNS.items()}
        resp = requests.post(SERVICENOW_URL, auth=(SERVICENOW_USER, SERVICENOW_PASS),
                           headers={"Content-Type": "application/json"}, json=data)
        
        if resp.status_code in [200, 201]:
            print(f"[INFO] ✅ Sent {sheet_name}:{idx}")
            sent += 1
        else:
            print(f"[ERROR] ❌ {resp.status_code}: {resp.text}")
    
print(f"[INFO] Pipeline complete. Sent {sent} records", flush=True)
