# Weather API Caching Proxy

A small FastAPI service that fetches weather from upstream providers, merges/normalizes it, and serves it from a local on-disk cache.

Providers:

- OpenWeather (One Call 3.0)
- Tomorrow.io (Timelines v4) (optional)

## Features

- Periodic background refresh for configured locations
- Per-location cache files under `./weather_cache`
- Simple health endpoint with cache status
- Optional per-request cache-bust via query param
- Configurable daily forecast priority with per-field provenance
- Persistent forecast history, NWS observations and coverage-aware accuracy reports

See [verification operations and limitations](docs/forecast-verification.md) for setup, schema,
retention, statistical rules, evidence boundaries and example responses.

## Requirements

- Python 3.11+ recommended

## Quickstart

1. Create a virtualenv and install deps:

   ```bash
   python -m venv venv
   . venv/bin/activate
   pip install -r requirements.txt
   ```

2. Create your config:

   ```bash
   cp config.yaml.example config.yaml
   ```

   Edit `config.yaml` and set:

   - `openweather_api_key`
   - (optional) `tomorrow_api_key`
   - `locations`

3. Run the API:

   ```bash
   uvicorn main:app --reload --port 8000
   ```

The default `uvicorn` port is `8000`. If you want to run using the `host`/`port` values from `config.yaml`, run:

```bash
python main.py
```

The Docker image also listens on port `8000`.

By default the app reads `config.yaml` from the repo root. You can override the path with:

```bash
export WEATHER_API_CONFIG=/path/to/config.yaml
```

## Endpoints

### `GET /healthz`

Returns uptime, provider key presence, and per-location cache metadata.

### `GET /accuracy/{location_name}?days=30`

Read-only verification report; supports 30, 90 and 365 days. It never fetches upstream
forecasts. Empty history returns insufficient-data warnings. Automatic switching is disabled.

### `GET /weather/{location_name}`

Returns cached merged weather for the configured `location_name`.

`location_name` must match an entry in `config.yaml`.
If it contains spaces or special characters, URL-encode it (e.g. `Eiffel Tower` -> `Eiffel%20Tower`).

Query params:

- `refresh=1` forces an upstream fetch (per-location requests are coalesced with a lock).

## OpenAPI schema

FastAPI serves an OpenAPI schema and interactive docs automatically:

- `GET /openapi.json` returns the OpenAPI schema.
- `GET /docs` serves Swagger UI.
- `GET /redoc` serves ReDoc.

See `OPENAPI.md` for more details.

To export the schema to a file:

```bash
curl -s http://localhost:8000/openapi.json > openapi.json
```

## Configuration

In addition to provider keys and `locations`, `config.yaml` supports:

- `enable_openweather`, `enable_tomorrow`
- `units`: legacy setting, ignored; upstream requests and normalization always use metric
- `tomorrow_forecast_days`: default 5; use 8 only with an eligible plan (range 1–14)
- optional `locations[].timezone`: IANA name, e.g. `America/Chicago`
- `cache_dir`, `cache_expiry_minutes`
- `refresh_interval_minutes`
- `max_calls_per_24h_openweather`, `max_calls_per_24h_tomorrow`
- `host`, `port`

## Forecast contract and provider policy

The API always uses Celsius (`tempC`, `feelsLikeC`, `dewPointC`), km/h
(`windKph`, `windGustKph`), km (`visibilityKm`), hPa, and mm. Configuring the
legacy `units: imperial` does not change these fields: both upstream requests
still use metric. Direct provider fetch helpers reject nonmetric units. Consumers
may convert for display.

Daily entries use the location's **local calendar date**. OpenWeather supplies an
IANA timezone (or UTC offset fallback). An explicit location timezone overrides
it. Tomorrow requests use that timezone (including previously archived IANA metadata
for the same coordinates), or `auto` when unknown, and
`dailyStartHour: 0` for midnight aggregation instead of the default 06:00 boundary.
Configure an explicit IANA timezone, especially for Tomorrow-only use. Without
one, or previously learned IANA metadata, daily Tomorrow records are archived but
not selected: full local-day horizon coverage cannot be established safely.
Astronomical timestamps and hourly timestamps remain UTC instants.

- **Daily:** default priority `[openweather, tomorrow]`, configurable via
  `daily_provider_priority` or `WEATHER_DAILY_PROVIDER_PRIORITY`. Select each valid
  field, keeping zeros and source semantics. The other provider supplies missing
  fields and days. OpenWeather supplies the extended horizon when Tomorrow is
  preferred. Selection excludes stale responses, expired days, mismatched daily
  boundaries and Tomorrow's incomplete final horizon day. Today's remaining
  forecast can be displayed, but is excluded from full-day verification.
  Missing/invalid extrema fall back; mixed extrema must not produce min > max.
  No provider has been demonstrated more accurate for this location.
- **Hourly:** prefer Tomorrow per field, with OpenWeather fallback.
- **Current:** prefer the more recent timestamp, Tomorrow on ties; use the other
  provider for missing fields. This retains the pre-existing timestamp policy;
  Tomorrow's current fallback is its first hourly forecast, not an observation.
- Missing/nonfinite fields fall back; valid zeros do not. Conditions remain atomic
  because the providers use different weather-code namespaces.
- Daily POP is selected independently of other fields. Among eligible values,
  prefer native daily, average interval, maximum interval, then unknown basis.
  Provider order breaks ties. Invalid probabilities stay unavailable, valid zero
  is preserved, and no daily probability is calculated from hourly data.
- Tomorrow's daily intensity aggregates are not precipitation totals. They remain
  available as `extra.precipIntensityMmHr`; its daily `precipMm` is null.
  OpenWeather daily rain + snow is retained when either component is present;
  both absent is null. Components and basis are exposed, and the unestablished
  snow/liquid-equivalence definition prevents using this sum as verification truth.

Tomorrow Timelines accepts relative expressions such as `nowPlus5d`. Its free
plan supports +5 days, typically six daily dates including today, with an endpoint
that can be partially covered and is therefore excluded from daily selection. It cannot supply eight complete days on that plan.
Use `tomorrow_forecast_days: 8` only on a supported plan. A rejected/failed Tomorrow
request gracefully falls back to OpenWeather; it is not automatically retried with
multiple shorter horizons. If OpenWeather fails, the response may have fewer than
seven future days; no forecast days are fabricated.

### Provenance and diagnostics

Existing weather value fields remain. Merged current/hourly/daily records add
`_sources`, mirroring individual selected fields (`tempC.min`, `tempC.max`, etc.
are represented by a nested `tempC` object). Missing values have null sources.
`_src` remains as a summary, but now reports `mixed` when fields come from multiple
providers; clients needing a condition's provider should use `_sources.condition`.
The running family-dashboard bundle inspected on 2026-09-19 does not use `_src`.

Daily `_providers` retains both providers' temperatures, POP, POP basis and
Tomorrow aggregates. `extra.dailyDisagreements` lists high/low differences of at
least 5.5°C or POP differences of at least 0.5 (50 percentage points). These are
comparison diagnostics, not calibrated ensemble statistics; POP bases can differ.
They neither modify selected values nor fail requests, and avoid noisy production
logging. `extra.providerStatus` distinguishes `ok`, `failed`, `disabled`, `rate_limited`,
and `quota_unavailable` when the quota store fails after another provider succeeds.
`extra.historyStatus` reports archive success/failure separately from provider success.
`extra.selectionPolicy` exposes the configured priority and disabled automatic switching.
Daily `_providers[].period` and `.eligible` explain horizon exclusions.

### Cache times and failure behavior

- `extra.providerFetchedAt`: when each successful upstream response was obtained;
  not the provider's forecast-model issue time. Null for unavailable providers.
- `generatedAt`: when normalization/merging produced this response.
- `updatedAt`: backward-compatible alias of `generatedAt`.
- `_cache.writtenAt`: when this merged payload was persisted.
- `_cache.ageSeconds`, `_cache.stale`: age since that write and expiry status at
  serving time. `_cache.refreshFailed` marks a failed/busy refresh fallback, even
  when the old payload has not yet expired.

Failed refreshes do not rewrite cached data or advance its timestamps. Empty
provider payloads count as failures. Atomic cache replacement prevents HTTP/health
readers seeing half-written JSON. Older cache files with naive UTC timestamps
remain readable. No old provider payload is mixed into a newly generated response.

All application fetch paths share SQLite-backed rolling request budgets, including
failed attempts, HTTP refreshes and geocoding. Limits survive restarts. Tomorrow
also has hourly and per-second limits. Quota reservation fails closed if SQLite is
unavailable; cached responses remain usable. After a successful fetch, a failed
request outcome or archive write is logged and the weather response is still used.
Archive failures also appear in `extra.historyStatus`. Other apps
using the same provider account are not counted. Scheduled and HTTP refreshes can
still overlap; both count against these budgets. Avoid polling `refresh=1`.

### Validation and deployment

```bash
python -m unittest discover -s tests -v
python -m py_compile main.py cache.py scheduler.py weather.py history.py observations.py verification.py timeutils.py
```

The captured fixture in `tests/fixtures/forecast-2026-09-19.json` contains current
provider data, not the September 2 incident payload. Integration tests replay it
through normalization, merging, cache persistence and the HTTP handler without
calling providers. See `docs/forecast-investigation-2026-09-19.md` for evidence,
limitations, before/after examples and deployment verification.

On the inspected svc-01 deployment the Git repository is the `app` directory;
`../compose.yml` builds that directory. Code is copied into the image, not bind
mounted. Back up the cache directory and configure observation settings and environment
variables as described in [the runbook](docs/forecast-verification.md) before using the
existing stack's deployment mechanism:

```bash
cd /opt/stacks/weather-api
docker compose build weather-api
docker compose up -d --no-deps weather-api
```

Editing Git files alone does not update the running container. The scheduler
refreshes on startup. Preserve the cache before diagnosing a live forced refresh.
