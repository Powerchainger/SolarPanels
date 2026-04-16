import growattServer
import time, requests
from datetime import datetime


def fetch_data(api):
    plants = api.plant_list() # See available plants
    time.sleep(1.0)
    plant_id = plants["plants"][0]["plant_id"] # get plant ID
    devices = api.device_list(plant_id) # See devices in that plant
    time.sleep(1.0)
    print(f"\n[{datetime.now()}] Found {len(devices['devices'])} devices in plant {plant_id}")

    for device in devices["devices"]: # Iterate over all devices
        inverter_sn = device["device_sn"]

        if device["type"] == 7: # (MIN/TLX Inverters) 
            print(f"\nProcessing device S/N {inverter_sn}")

            energy_data = api.min_energy(device_sn=inverter_sn) # get latest sample data from that device
            time.sleep(1.0)

            print("Power overview (Watts)")
            print(f'AC Power                {float(energy_data["pac"]):>10.1f}')
            print(f'PV Power                {float(energy_data["ppv"]):>10.1f}')
            print(f'Battery SOC             {int(energy_data["bdc1Soc"]):>10}%')
        else:
            print(f"Device {inverter_sn} not hybrid type")

def main(api_token):
    api = growattServer.OpenApiV1(token=api_token)

    print("----- CONNECTED SUCCESSFULLY -----")

    while True:
        try:
            fetch_data(api)

            print("\nSleeping for 5 minutes...\n")
            time.sleep(300)  # 5 minutes

        except growattServer.GrowattV1ApiError as e:
            print(f"API Error: {e}")

            print("Retrying in 1 minute...\n")
            time.sleep(60)

        except growattServer.GrowattParameterError as e:
            print(f"Parameter Error: {e}")
            print("Retrying in 1 minute...\n")
            time.sleep(60)

        except requests.exceptions.RequestException as e:
            print(f"Network Error: {e}")
            print("Retrying in 1 minute...\n")
            time.sleep(60)

        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Retrying in 1 minute...\n")
            time.sleep(60)


if __name__ == "__main__":
    API_TOKEN = "API_TOKEN_HERE"
    main(API_TOKEN)
