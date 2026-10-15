import requests
import base64
import json
import os
from datetime import datetime, timedelta

def access_creds():    
    with open("access_gen.json", "r") as f:
        creds = json.load(f)
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]
    redirect_uri = creds["redirect_uri"]
    auth_code = creds["auth_code"]

    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    URL = "https://api.enphaseenergy.com/oauth/token"
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
        "code": auth_code
    }

    print("Requesting tokens from EnPhase...")
    # The auth code is single-use and short-lived: fail rather than hang.
    response = requests.post(URL, headers=headers, data=data, timeout=(10, 30))

    if response.status_code == 200:
        tokens = response.json()
        print("--- SUCCESS! ---")

        expires_in_seconds = tokens.get("expires_in", 86399)
        expires_at = (datetime.now() + timedelta(seconds=expires_in_seconds)).isoformat()

        save_dict = {
            "access_token": tokens.get("access_token"),
            "token_type": tokens.get("token_type"),
            "refresh_token": tokens.get("refresh_token"),
            "expires_in": expires_in_seconds,
            "expires_at": expires_at
        }
        # Owner-only: the refresh token gives month-long access to the
        # customer's data. fchmod also tightens a file left by an earlier run.
        fd = os.open("credentials.json", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(save_dict, f, indent=4)
        print("credentials.json created successfully.")
    else:
        print(f"Failed! Status Code: {response.status_code}")
        # Only the OAuth error fields, not the raw body.
        try:
            body = response.json()
        except ValueError:
            body = {}
        if isinstance(body, dict) and "error" in body:
            print(f"Error: {body['error']}: {body.get('error_description', '')}")

if __name__ == "__main__":
    access_creds()