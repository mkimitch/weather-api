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
it. Tomorrow requests use that timezone, or `auto` when unknown, and
`dailyStartHour: 0` for midnight aggregation instead of the default 06:00 boundary.
If Tomorrow is used alone without an explicit IANA zone, its returned offset is
preserved when assigning dates, and the top-level timezone may be null.
Astronomical timestamps and hourly timestamps remain UTC instants.

- **Daily:** prefer OpenWeather for each usable field, including min/max, POP,
  condition and astronomy; fall back to Tomorrow per field. OpenWeather's eight
  daily entries normally provide today plus the next seven days. This consistent
  preference avoids changing temperature providers just because Tomorrow's
  horizon ends. It does not claim that OpenWeather is always more accurate.
- **Hourly:** prefer Tomorrow per field, with OpenWeather fallback.
- **Current:** prefer the more recent timestamp, Tomorrow on ties; use the other
  provider for missing fields. This retains the pre-existing timestamp policy;
  Tomorrow's current fallback is its first hourly forecast, not an observation.
- Missing/nonfinite fields fall back; valid zeros do not. Conditions remain atomic
  because the providers use different weather-code namespaces.
- Probabilities are never averaged or combined with `max()`. OpenWeather's native
  daily `pop` is already 0–1. Tomorrow percentages are divided by 100; its daily
  fallback prefers `precipitationProbabilityAvg`, then `precipitationProbability`,
  then `precipitationProbabilityMax`. These are provider aggregates, not equivalent
  to OpenWeather's daily event probability. `popBasis` identifies the selected
  quantity. Invalid probabilities become missing, rather than being clamped.
- Tomorrow's daily intensity aggregates are not precipitation totals. They remain
  available as `extra.precipIntensityMmHr`; its daily `precipMm` is null.
  OpenWeather daily rain + snow supplies a true daily mm accumulation when present.

Tomorrow Timelines accepts relative expressions such as `nowPlus5d`. Its free
plan supports +5 days, typically six daily dates including today, with an endpoint
that can be partially covered. It cannot supply eight complete days on that plan.
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
logging. `extra.providerStatus` distinguishes `ok`, `failed`, and `disabled`.

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

Scheduler limits count attempts, including failures, and disable exhausted
providers while allowing the other provider to continue. These are in-memory
scheduler budgets: they reset on restart and do not count direct HTTP refreshes or
city-geocoding calls. The per-location HTTP lock does not cover scheduled refreshes;
forced refreshes can also run sequentially. Avoid polling `refresh=1`.

### Validation and deployment

```bash
python -m unittest discover -s tests -v
python -m py_compile main.py cache.py scheduler.py weather.py
```

The captured fixture in `tests/fixtures/forecast-2026-09-19.json` contains current
provider data, not the September 2 incident payload. Integration tests replay it
through normalization, merging, cache persistence and the HTTP handler without
calling providers. See `docs/forecast-investigation-2026-09-19.md` for evidence,
limitations, before/after examples and deployment verification.

On the inspected svc-01 deployment the Git repository is the `app` directory;
`../compose.yml` builds that directory. Code is copied into the image, not bind
mounted. After passing checks, use the existing stack's deployment mechanism:

```bash
cd /opt/stacks/weather-api
docker compose build weather-api
docker compose up -d --no-deps weather-api
```

Editing Git files alone does not update the running container. The scheduler
refreshes on startup. Preserve the cache before diagnosing a live forced refresh.
