import http.client
import json
import base64
from datetime import datetime, timedelta


BASE_URL = "api.enphaseenergy.com"


def refresh_access_token(client_id, client_secret, refresh_token, save_path):
    """
    Uses refresh_token to generate new access_token and refresh_token.
    Overwrites saved credentials.json file.
    """

    credentials = f"{client_id}:{client_secret}"
    encoded = base64.b64encode(credentials.encode()).decode()

    conn = http.client.HTTPSConnection(BASE_URL)

    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    endpoint = f"/oauth/token?grant_type=refresh_token&refresh_token={refresh_token}"

    conn.request("POST", endpoint, "", headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")

    if res.status != 200:
        raise Exception(f"Token refresh failed: {res.status} {data}")

    tokens = json.loads(data)

    # Save updated tokens
    # with open(save_path, "w") as f:
    #     json.dump(tokens, f, indent=4)

    return tokens

if __name__ == "__main__":
    with open("access_gen.json", "r") as f1:
        creds1 = json.load(f1)
    with open("credentials.json", "r") as f2:
        creds2 = json.load(f2)
    
    returns = refresh_access_token(creds1["client_id"], creds1["client_secret"], creds2["refresh_token"], None)
    print(returns)