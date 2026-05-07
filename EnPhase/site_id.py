import http.client, json

def get_site_id(access_token, API_KEY, token_type="Bearer"):
  conn = http.client.HTTPSConnection("api.enphaseenergy.com")
  headers = {
    'Authorization': f'{token_type} {access_token}'
  }
  endpoint = f"/api/v4/systems?key={API_KEY}"
  try:
      conn.request("GET", endpoint, headers=headers)
      res = conn.getresponse()
      raw_data = res.read().decode("utf-8")      
      if res.status != 200:
          print(f"Failed to get systems: {res.status} {raw_data}")
          return None, None
      data = json.loads(raw_data)      
      if 'systems' in data and len(data['systems']) > 0:
          system_id = data['systems'][0]['system_id']
          user = data['systems'][0]['name']
          return system_id, user
      else:
          print("No systems found in the account.")
          return None, None          
  finally:
      conn.close()
  
if __name__ == "__main__":
    try:
        with open("credentials.json", "r") as f1:
            creds1 = json.load(f1)
        with open("access_gen.json", "r") as f2:
            creds2 = json.load(f2)
            
        sid, name = get_site_id(
            creds1["access_token"], 
            creds2["api_key"], 
            creds1.get("token_type", "Bearer")
        )
        print(f"System ID: {sid}, User: {name}")
    except FileNotFoundError:
        print("Missing JSON files. Run the bootstrap script first!")