# Monitoring

There are three places to check that the service is working:

1. **Account health** (`manage_keys.py list`): is each provider answering?
2. **Delivery status** in MongoDB: are records reaching your output API?
3. **Logs**: what happened, and why.

The service has no built-in alerting. The checks below are easy to script
if you want to hook them into your own monitoring.

## Container health

```bash
podman compose ps
```

The `app` container shows as `healthy` while the service is running
normally, and `unhealthy` when it has stopped responding (it then also stops
holding on to its accounts). Restart it with `podman compose restart app` and
check the logs. The health status does **not** cover the providers or your
output API; use the checks below for those.

## Account health

```bash
uv run python manage_keys.py list
```

For each account, check that:

- `last_success` is recent, within a few poll intervals.
- `failures` is absent or low. A short run of failures is normal (provider
  outages, rate limits). A steadily growing number is not.
- There is no `AUTH FAILED`. If there is, see
  [Troubleshooting](troubleshooting.md#an-enphase-account-shows-auth-failed).

This only tells you whether the **provider** answered. Whether the record
then reached your output API is tracked separately, below.

## Delivery status

Every record has a delivery status in the `processed_results` collection:

| Status | Meaning |
|---|---|
| `posted` | Delivered to your output API. |
| `posting` | Being delivered right now, or the last attempt hit a temporary error (timeout, HTTP 5xx or 429). Retried automatically. |
| `failed` | Your API rejected it (HTTP 4xx), or 5 retries were used up. A `failed` record with retries left is retried automatically. |

Delivered records are deleted after `POSTED_TTL_DAYS` (7 days by default),
so `posted` counts only cover that period. Undelivered records are kept until
they are delivered. They are retried every 5 minutes, up to 5 times. After that
they stay `failed` and are not retried any more. See
[Troubleshooting](troubleshooting.md#records-are-not-reaching-the-output-api)
for how to resend them.

To open a database shell (asks for `MONGO_ROOT_PASSWORD`):

```bash
podman compose exec mongo mongosh -u root -p --authenticationDatabase admin solar_scraper
```

Useful queries:

```js
// Count of records per status
db.processed_results.aggregate([{$group: {_id: "$status", n: {$sum: 1}}}])

// Records that will not be retried any more
db.processed_results.find({status: "failed", retry_count: {$gte: 5}},
  {provider: 1, account_id: 1, last_error: 1, created_at: 1})

// Most recent record per account
db.processed_results.aggregate([
  {$sort: {created_at: -1}},
  {$group: {_id: {p: "$provider", a: "$account_id"}, last: {$first: "$created_at"}, status: {$first: "$status"}}}
])
```

`last_error` on a record holds the HTTP status and the start of the response
body from your API, which usually explains a rejection.

## Logs

```bash
podman compose logs -f app            # follow
podman compose logs --since 1h app    # last hour
```

At the default `INFO` level the service logs one line per delivered record
plus anything that goes wrong. Things worth noticing:

| Log line | Meaning |
|---|---|
| `POST … → 201 (status=posted, attempts=1)` | A record was delivered. |
| `POST … → 503 (status=None, attempts=3)` or `→ timeout` | Delivery failed temporarily; the retry job takes it up. |
| `POST … → 400 (status=failed, attempts=1)` | Your API rejected the record. Its response is stored in `last_error`. |
| `fetch failed for growatt::my-home (3 in a row): …` | The provider call failed. Look at the reason after the colon. |
| `… (1/3 before it is skipped)` | EnPhase rejected the credentials; the account is paused at 3. |
| `… account enphase::office is now skipped until re-seeded` ([re-seed](concepts.md#accounts-and-polling)) | An EnPhase account is now paused (`AUTH FAILED`). |
| `skipping enphase::office — needs re-authorization` | Logged every poll while the account is paused. |
| `record … exhausted retries (5), stays failed` | A record will not be retried any more. |
| `flush: retried 12 record(s)` | The [retry job](concepts.md#records-and-delivery) delivered or retried earlier records. |
| `lost lease on …` | Another worker took over this account, or it was deleted. See [Concepts](concepts.md#workers). |

Set `LOG_LEVEL=DEBUG` in `.env` and restart to also see every API call. This
is verbose and meant for short troubleshooting sessions.

## Raw provider responses

The service keeps the unprocessed answer of every provider call for 30 days
(`RAW_TTL_DAYS`) in `raw_responses`. They help when a record looks wrong:

```js
db.raw_responses.find({provider: "enphase", account_id: "office"}).sort({created_at: -1}).limit(1)
```
