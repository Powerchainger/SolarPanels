# EnPhase Solar Data Pipeline

## Overview

This project connects to the Enphase Energy API, retrieves micro-inverter production data, processes it, and logs it locally for analysis.

It includes:
- OAuth token handling
- Site ID retrieval
- Inverter power aggregation
- CSV data logging
- Automated hourly scheduler
- Structured logging system

---

## Project Structure

SolarPanels/ \
│ \
├── EnPhase/ \
│   ├── `cloud_measurements.py` \
│   ├── `site_id.py` \
│   ├── `run_scheduler.py` \
│   ├── `inverter_log.csv`  \
│   ├── `credentials.json` \
│   ├── `access_gen.json` \
│   ├── logs/ \
│   │   └── `scheduler.log` \
│   └── `__init__.py`

---

## Features

### Data Collection
- Fetches inverter-level power production
- Aggregates total system output
- Converts timestamps to Europe/Amsterdam timezone

### Data Storage
- Appends results to CSV file:
EnPhase/inverter_log.csv

### Scheduling
- Runs automatically every 1 hour
- Uses APScheduler (BackgroundScheduler)
- Safe shutdown with Ctrl+C support

### Logging
- Logs both to console and file:
EnPhase/logs/scheduler.log

### Access
- Token auto-refresh system
- Updates access and refresh token every 22 hours

---

## How to Run

First enable virtual environment and install dependencies.

### 1. Run once (manual execution)

From project root:

python -m EnPhase.cloud_measurements

---

### 2. Run hourly scheduler (recommended)

From project root:

python -m EnPhase.run_scheduler

---

## Scheduler Behavior

- Runs immediately on start
- Repeats every 1 hour
- Prevents overlapping runs
- Logs all executions

---

## Output Data Format (CSV)

Each row contains:

timestamp      → local time (Europe/Amsterdam) \
inverter_1..N  → individual inverter power values \
total_power    → total system output \
user_value     → site/user name 

---

## Authentication

### credentials.json

```bash
{
  "access_token": "...",
  "token_type": "Bearer",
  "refresh_token": "...",
  "expires_in": "...."
}
```
### access_gen.json

```bash
{
  "client_id": "...",
"client_secret": "...",
"redirect_uri": "...",
"auth_code": "...",
"api_key": "..."
}
```
---

## Architecture

```
Scheduler
    ↓
cloud_measurements.main()
    ↓
API Request (Enphase)
    ↓ 
parse_inverter_data()
    ↓
CSV Logging
```
---

## Stop Scheduler

Press:

Ctrl + C

This safely shuts down:
- scheduler
- background jobs
- logging system

---

## Important Notes

- Always run from project root
- Use module execution (-m), not direct file execution
- Do NOT manually edit CSV while scheduler is running
- Ensure credentials are valid before starting

---

## Recommended Usage

Development / Testing:

python -m EnPhase.cloud_measurements

Production (hourly automation):

python -m EnPhase.run_scheduler

---

## Input from user

- User has to follow the procedure until step 7, in this [URL](https://developer-v4.enphase.com/docs/quickstart.html)
- Send us to following information:
```bash
    - "client_id"
    - "client_secret"
    - "redirect_uri"
    - "auth_code"
    - "api_key"
```
- **TESTING**:  
    - place the above in a new file named ```access_gen.json```
    - run ```access_token_gen.py ``` -> copy the log output in a new ```credentials.json```
    - run ```EnPhase.scheduler``` (continuous 1-houer calls) or ```cloud_measurements.py``` (one-time call)

- **IMPORTANT** : 
    - Generate as fast as possible the credentials because ```auth_code``` expires very fast and if it does the user has to repeat the steps for the URL.
    - If ```refresh_token``` expires then the whole process needs to be done from the start (this should happen in case you have credentials but didn't use them for a month)


## Future Improvements


- SQLite instead of CSV storage
- Real-time dashboard (Plotly / Grafana)
- Email/Telegram alerts on failures
- Cloud deployment (Raspberry Pi / VPS)