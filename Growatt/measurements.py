import growattServer
import os, json, time
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_PATH = os.path.join(BASE_DIR, "credentials.json")
CSV_FILE = os.path.join(BASE_DIR, "growatt_log.csv")

def write_to_csv(rows):
    """Writes a list of rows to the CSV at once."""
    if not rows:
        return
    file_exists = os.path.isfile(CSV_FILE)
    pd.DataFrame(rows).to_csv(CSV_FILE, mode="a", header=not file_exists, index=False)

def fetch_and_log():
    master_now = datetime.now(ZoneInfo("Europe/Amsterdam")).replace(microsecond=0).isoformat()
    
    with open(CREDS_PATH, "r") as f:
        config = json.load(f)
    
    api = growattServer.OpenApiV1(token=config["api_token"])
    results_buffer = []

    try:
        plants = api.plant_list() # Get available plants
        if not plants.get("plants"):
            print("No plants found.")
            return

        plant_id = plants["plants"][0]["plant_id"] # get plant id
        devices = api.device_list(plant_id) # get devices in plant
        
        print(f"--- Starting Sample Run: {master_now} ---")

        for device in devices.get("devices", []): 
            sn = device["device_sn"]
            if device["type"] != 7:
                print(f"Skipping {sn} (Type {device['type']})")
                continue
            try:
                print(f"Fetching data for {sn}...")
                data = api.min_energy(device_sn=sn)
                row = {
                    "timestamp": master_now, 
                    "device_sn": sn,
                    "ac_power_watts": float(data.get("pac", 0)),
                    "pv_power_watts": float(data.get("ppv", 0)),
                    "soc_percent": int(data.get("bdc1Soc", 0))
                }
                
                results_buffer.append(row)
                print(f"Captured {sn} successfully.")
                
                # Time guard for requests. I get a lot of timeouts
                time.sleep(2) 

            except Exception as e:
                print(f"Failed to capture {sn}: {e}")

        if results_buffer:
            write_to_csv(results_buffer)
            print(f"Successfully saved {len(results_buffer)} device(s) to CSV.")
        else:
            print("No data was captured during this run.")

    except Exception as e:
        print(f"Global Growatt API Error: {e}")

if __name__ == "__main__":
    fetch_and_log()