import http.client
import json, os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd

from EnPhase.site_id import get_site_id
from EnPhase.auth import should_refresh, refresh_access_token, update_tokens_file
from EnPhase.access_token_gen import access_creds


# CONSTANTS
FILE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(FILE_DIR, "inverter_log.csv")
CRED_PATH = os.path.join(FILE_DIR, "credentials.json")
API_PATH = os.path.join(FILE_DIR, "access_gen.json")

def parse_inverter_data(raw_data, user):
    """
    Parses raw Enphase API data and returns structured row + total power.
    """

    try:
        json_data = json.loads(raw_data)

        if not json_data or "micro_inverters" not in json_data[0]:
            return None, None

        inverters = json_data[0]["micro_inverters"]

        iso_timestamp = datetime.fromisoformat(
            inverters[0]["last_report_date"]
        )

        local_timestamp = iso_timestamp.astimezone(
            ZoneInfo("Europe/Amsterdam")
        )

        row = {"timestamp": local_timestamp}
        total_power = 0

        for i, inverter in enumerate(inverters):
            power = inverter.get("power_produced", {}).get("value", 0)
            row[f"inverter_{i+1}"] = power
            total_power += power

        row["total_power"] = total_power
        row["user_value"] = user

        return row, total_power

    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"Parsing error: {e}")
        return None, None

def write_to_csv(row):
    """
    Writes a single row to CSV (creates file if needed).
    """

    file_exists = os.path.isfile(FILE)

    pd.DataFrame([row]).to_csv(
        FILE,
        mode="a",
        header=not file_exists,
        index=False
    )

def main(cache=None):
    # -------------------------------------------------------------------------
    # Load credentials
    # -------------------------------------------------------------------------
    try:
        with open(API_PATH, "r") as f:
            api_creds = json.load(f)
        with open(CRED_PATH, "r") as f:
            creds = json.load(f)
    except FileNotFoundError:
        print("Credentials file missing. Initializing new authorization session...")
        access_creds()
        with open(CRED_PATH, "r") as f:
            creds = json.load(f)

    client_id = api_creds["client_id"]
    client_secret = api_creds["client_secret"]
    api_key = api_creds["api_key"]

    if "expires_at" not in creds:
        print("Missing 'expires_at'. Deriving timestamp from 'expires_in'...")
        seconds = creds.get("expires_in", 86399)
        expiry_date = datetime.now() + timedelta(seconds=seconds)
        creds["expires_at"] = expiry_date.isoformat()
        with open(CRED_PATH, "w") as f:
            json.dump(creds, f, indent=4)

    # -------------------------------------------------------------------------
    # Token Validation & Preventive Refresh (Must Happen FIRST)
    # -------------------------------------------------------------------------
    if should_refresh(creds.get("expires_at")):
        print("Tokens outdated according to timestamp. Executing planned refresh...")
        try:
            new_tokens = refresh_access_token(client_id, client_secret, creds["refresh_token"])
            update_tokens_file(CRED_PATH, new_tokens)
            with open(CRED_PATH, "r") as f:
                creds = json.load(f)
        except Exception as e:
            print(f"Preventive token refresh failed ({e}). Forcing hard reset...")
            if os.path.exists(CRED_PATH):
                os.remove(CRED_PATH)
            access_creds()
            with open(CRED_PATH, "r") as f:
                creds = json.load(f)

    access_token = creds["access_token"]
    token_type = creds["token_type"].title()

    # -------------------------------------------------------------------------
    # Handle Site ID Cache / Fetching (Safe now because tokens are fresh)
    # -------------------------------------------------------------------------
    if cache and cache.get("site_id"):
        site_id = cache["site_id"]
        user = cache["user"]
    else:
        print("Site ID not found in memory cache. Fetching from Enphase API...")
        site_id, user = get_site_id(access_token, api_key, token_type)
        
        #EMERGENCY BACKUP: If the API still rejects us
        if site_id is None:
            print("Token unexpectedly rejected (401). Attempting emergency refresh...")
            try:
                new_tokens = refresh_access_token(client_id, client_secret, creds["refresh_token"])
                update_tokens_file(CRED_PATH, new_tokens)
                with open(CRED_PATH, "r") as f:
                    creds = json.load(f)
                access_token = creds["access_token"]
                token_type = creds["token_type"].title()
                
                print("Retrying Site ID fetch with fresh credentials...")
                site_id, user = get_site_id(access_token, api_key, token_type)
            except Exception as re_err:
                print(f"Refresh token is entirely dead ({re_err}). Resetting credentials file...")
                if os.path.exists(CRED_PATH):
                    os.remove(CRED_PATH)
                access_creds()
                return

        #Commit to memory cache if successfully retrieved
        if cache is not None and site_id:
            cache["site_id"] = site_id
            cache["user"] = user


    if not site_id:
        print("Could not retrieve site_id. Skipping this cycle.")
        return

    # -------------------------------------------------------------------------
    # Execute Inverter Metrics API Request
    # -------------------------------------------------------------------------
    path = f"/api/v4/systems/inverters_summary_by_envoy_or_site?site_id={site_id}"
    headers = {
        "Authorization": f"{token_type} {access_token}",
        "key": api_key
    }

    conn = http.client.HTTPSConnection("api.enphaseenergy.com")
    try:
        conn.request("GET", path, "", headers)
        res = conn.getresponse()
        raw_data = res.read().decode("utf-8")
        if res.status == 200:
            row, total_power = parse_inverter_data(raw_data, user)
            if row is None:
                print("Invalid JSON or missing data")
                return
            write_to_csv(row)
            print(f"Total current system generation: {total_power}W")
        else:
            print(f"API Connection Rejected ({res.status}): {raw_data}")
    finally:
        conn.close()


if __name__ == "__main__":
    main(cache=None)