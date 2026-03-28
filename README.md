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
- `units` (`metric` or `imperial`)
- `cache_dir`, `cache_expiry_minutes`
- `refresh_interval_minutes`
- `max_calls_per_24h_openweather`, `max_calls_per_24h_tomorrow`
- `host`, `port`
