# Growatt Energy Monitor

This script connects to the Growatt Open API and retrieves real-time energy and device data from a configured plant.

---

## What it does

- Connects to Growatt API using `growattServer`
- Retrieves available plants
- Fetches devices in the first plant (Every 5 minutes)
- Filters inverter devices (type = 7, MIN/TLX hybrid inverters)
- Prints real-time power and battery metrics:
  - AC Power
  - PV production
  - Grid import/export
  - Battery charge/discharge
  - State of charge (SOC)
- If request fails, retries after a minute
- Measurements are currently broadcasted in the log screen

---

## Hardware Requirements

Install dependencies:

```bash
pip install growattServer requests

```

This is covered with with installing requirements.txt in the root directory.

---

## User requirements

User needs to provide an API_TOKEN. This needs to be created in the ShinePhone app by them.

Steps: 
- **"Me"(bottom right) > 'account name' > API Token**
- Create and copy the token by pressing the copy icon on the right
- Send it to us so we can run the pipeline using that API_key
- **IN CASE** you have the API_key you insert it in the credentials.json

---

## How to Run

### 1. Run once (manual execution)

Insert API_key in the according variable in the __main__ call of the API_request.py

From project root:

python -m Growatt.API_request

---

### 2. Run scheduler every 5 minutes (recommended)

create a ```bash credentials.json``` file and insert the API_key inside.

example:

```bash
{
  "api_token": "...."
}
```

From project root:

python -m Growatt.run_scheduler

---
