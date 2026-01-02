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
   uvicorn main:app --reload
   ```

By default the app reads `config.yaml` from the repo root. You can override the path with:

```bash
export WEATHER_API_CONFIG=/path/to/config.yaml
```

## Endpoints

### `GET /healthz`

Returns uptime, provider key presence, and per-location cache metadata.

### `GET /weather/{location_name}`

Returns cached merged weather for the configured `location_name`.

Query params:

- `refresh=1` forces an upstream fetch (per-location requests are coalesced with a lock).
