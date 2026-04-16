import requests, base64, json
from datetime import datetime, timedelta

# Read info given by user
with open("access_gen.json", "r") as f:
    creds = json.load(f)
client_id = creds["client_id"]
client_secret = creds["client_secret"]
redirect_uri = creds["redirect_uri"]
auth_code = creds["auth_code"]

# Payload to make the request
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

# Request authentication in the form of access_token
print("Requesting tokens from EnPhase...")
response = requests.post(URL, headers=headers, data=data)

# Check response and save the response credentials
if response.status_code == 200:
    tokens = response.json()
    print("--- SUCCESS! ---")

    expires_in_seconds = tokens.get("expires_in", 86399)
    expires_at = (datetime.now() + timedelta(seconds=expires_in_seconds)).isoformat()

    save_dict = {
        "access_token": tokens.get("access_token"),
        "token_type": tokens.get("token_type"),
        "refresh_token": expires_in_seconds,
        "expires_in": expires_at
    }
    with open("credentials.json", "w") as f:
        json.dump(save_dict, f, indent=4)
    print("credentials.json created successfully.")
else:
    print(f"Failed! Status Code: {response.status_code}")
    print(f"Response: {response.text}")

# Use this for testing
if __name__ == "main":
    print("Test")