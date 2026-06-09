import http.client
import json
import base64
from datetime import datetime, timedelta

BASE_URL = "api.enphaseenergy.com"

def update_tokens_file(save_path, new_tokens):
    with open(save_path, "r") as f:
        existing = json.load(f)

    existing["access_token"] = new_tokens.get("access_token")
    existing["token_type"] = new_tokens.get("token_type")
    existing["refresh_token"] = new_tokens.get("refresh_token")
    existing["expires_in"] = new_tokens.get("expires_in")
    existing["expires_at"] = (datetime.now() + timedelta(seconds=new_tokens["expires_in"])).isoformat()

    with open(save_path, "w") as f:
        json.dump(existing, f, indent=4)

def should_refresh(expires_at):
    if not expires_at:
        return True
    expiry_time = datetime.fromisoformat(expires_at)
    refresh_time = expiry_time - timedelta(hours=2)
    return datetime.now() >= refresh_time

def refresh_access_token(client_id, client_secret, refresh_token):
    credentials = f"{client_id}:{client_secret}"
    encoded = base64.b64encode(credentials.encode()).decode()

    conn = http.client.HTTPSConnection(BASE_URL)
    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    endpoint = f"/oauth/token?grant_type=refresh_token&refresh_token={refresh_token}"

    try:
        conn.request("POST", endpoint, "", headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        if res.status != 200:
            raise Exception(f"Token refresh failed: {res.status} {data}")

        return json.loads(data)
    finally:
        conn.close()

if __name__ == "__main__":
    CRED_PATH = "credentials.json"
    API_PATH = "access_gen.json"

    with open(API_PATH, "r") as f1:
        api_creds = json.load(f1)

    with open(CRED_PATH, "r") as f2:
        creds = json.load(f2)

    if should_refresh(creds.get("expires_at")):
        print("Refreshing token...")
        new_tokens = refresh_access_token(
            api_creds["client_id"],
            api_creds["client_secret"],
            creds["refresh_token"]
        )
        update_tokens_file(CRED_PATH, new_tokens)
        print("Token refreshed and saved.")
    else:
        print("Token still valid.")