# Weather forecast investigation — September 19, 2026

## Evidence boundaries

The reported 103°F / repeated 100% forecast was observed September 2, 2026.
Those values are user-supplied historical evidence, not today's forecast.
Only the rolling `weather_cache/home.json` was present; no historical provider
payloads were found. Retained Docker logs span August 18–September 19 and include
September 2, but do not log forecast values. At 08:26:14 UTC on September 2,
Tomorrow returned HTTP 500 and the service continued with OpenWeather. That event
establishes a fallback occurred, not which payload the screenshot displayed.
There is no retained evidence establishing a historical normalized 39.4°C value.
A labeled synthetic regression reproduces 39.444…°C → 103°F through the merge and
display formula; it is not a reconstruction of the upstream incident payload.

## Deployment and observed data path

The task runs on svc-01. `/opt/stacks/weather-api` is the Compose stack, while
`/opt/stacks/weather-api/app` is the Git repository. Initial branch `main`, HEAD
`98d1a8873f7c5e58a7b37c2863c650fe74c4c111`, clean working tree. Changes are on
`fix/forecast-data-contract`. No existing changes were overwritten. The deployed
`config.yaml` is outside the Git repo and was not modified; its actual settings
were metric, both providers enabled, 5-minute refresh, 30-minute cache expiry,
Lakeville coordinates, and provider scheduler budgets of 900/500 per day.

Existing Compose maps host port 8787 to container port 8000. The running image
copied code at build time and did not include Git's previous daily-POP aggregate
or zero-celestial-timestamp fixes. This explains why merely editing or pulling
repository code had not applied those fixes to the running service.

Baseline cache and `/weather/home` were saved before exactly one
`/weather/home?refresh=1` call. Raw representative provider responses were captured
separately because production does not retain them. Tomorrow was queried once
with the old request, once at +8d to check entitlement, and once at the verified
+5d limit. No repeated forced-refresh loop was used.

The live dashboard `/api/weather` at `http://vaio.home.arpa:3000` was captured.
Its daily records match the weather service/cache; it does not introduce the
observed values. SSH authentication from svc-01 was unavailable and no forwarded
agent socket existed. No private key was copied. Consequently, the dashboard Git
status/HEAD, server route source and environment could not be inspected directly.
The public, currently running compiled page module was inspected instead. It:

- fetches `/api/weather` and uses `response.data || response`;
- converts `tempC.min/max` with `Math.round(C * 9 / 5 + 32)`;
- converts normalized POP with `Math.round(pop * 100)`;
- displays `daily.slice(1, 8)`;
- uses local date parsing and an America/Chicago display timezone;
- has no `_src` references.

The bundle also treats null POP as 0%; this pre-existing display limitation is
unrelated to repeated 100% POP and was not modified. Dashboard asset hashes
changed during the investigation; the current matching entrypoint and module
were fetched together. No dashboard files or services were modified by this task.

## Confirmed causes and first faulty stages

1. **Provider request:** Tomorrow's `nowPlus1d` request returned two daily dates
   (September 19–20) and 25 hourly entries. +8d returned HTTP 403/code 403003:
   this account restricts endTime to at most five days ahead. +5d succeeded with
   six dates (September 19–24) and 121 hourly entries. OpenWeather returned eight
   daily dates (September 19–26). The old temperature preference therefore changed
   providers after two entries, without adequately exposing that transition.
2. **Normalization:** daily dates were derived from UTC string truncation, with
   boundary errors reproducible for Chicago and positive UTC offsets. Tomorrow's
   default daily aggregation started at 06:00 local; the new request explicitly
   starts daily aggregation at midnight. The legacy configurable unit path could
   label Fahrenheit as `tempC` and treat mph as m/s; actual deployment was metric,
   so this is a verified latent bug, not a proven cause of the historical values.
   Tomorrow daily intensity in mm/hour was also mislabeled as a daily mm total.
3. **Merge:** cross-provider maximum POP was a pessimistic selection without a
   defined ensemble interpretation. Tomorrow record presence suppressed valid
   OpenWeather fields when individual values were null; truthiness also discarded
   valid zeros. `_src: tomorrow` misattributed mixed data. Tests reproduce these
   faults and the eight-day/two-day provider boundary.
4. **Runtime/cache:** the running image lagged Git. Cache writes were non-atomic;
   August 27 logs include a health endpoint JSONDecodeError during a cache read.
   Old stale fallback preserved `updatedAt` correctly, but did not expose stale
   status in the weather response. Empty successful provider responses could
   masquerade as freshly fetched useful data.
5. **Scheduler:** exhausted Tomorrow quota produced a log but did not actually
   disable its request; OpenWeather exhaustion prevented Tomorrow-only work.
   Provider gating and attempt counting now reflect the actual scheduled call.

No current raw provider payload reproduces the historical extreme temperatures.
The first demonstrable contract faults are request construction/normalization,
followed by merge selection. In the synthetic 103°F case, 39.444…°C first exists in
the fixture's provider input, passes through the later OpenWeather-only day, and
is correctly converted by the UI. The first stage producing the *historical*
103°F value remains unknown; neither the screenshot nor retained logs proves it.

## Selection policy and API changes

OpenWeather is preferred per daily field across its eight-day horizon, including
min/max temperatures, daily POP, condition, astronomy and accumulation. Tomorrow
is fallback only when the selected field is unusable. This avoids a horizon-driven
provider switch without assuming that an extreme temperature is necessarily wrong.
For OWM=39°C vs Tomorrow=30°C, selection is 39°C with a 9°C disagreement diagnostic.
For OWM POP=1 vs Tomorrow=.25, selection is 1 by policy; for OWM=.25 vs Tomorrow=1,
selection is .25. Thus the rule is preference, not a maximum or average.

Tomorrow's average daily POP is retained as an explicitly identified fallback;
its average, raw and maximum probabilities remain available for inspection.
These aggregates are not interchangeable with OpenWeather's daily probability.
No temperature or probability is clamped to make a forecast look plausible.

Existing value fields remain. New metadata:

- `_sources`: individual selected field sources, including nested `tempC` keys.
- `_providers`: both daily alternatives and Tomorrow's probability/intensity fields.
- `_src`: existing summary key, now `mixed` when appropriate.
- `popBasis`: `daily` or the specific Tomorrow probability field.
- `extra.dailyDisagreements`: >=5.5°C high/low or >=.5 POP differences.
- `extra.providerStatus` and `extra.providerFetchedAt`.
- `generatedAt`, with `updatedAt` retained as its alias.
- `_cache.writtenAt`, `ageSeconds`, `stale`, `refreshFailed` at response time.

Providers are always requested in metric; legacy `units: imperial` is ignored.
Nonmetric inputs are rejected by provider fetch helpers. Tomorrow daily intensity
is preserved in explicitly named metadata; `precipMm` is null rather than a false
daily accumulation. Cache fallback preserves provider-fetch and merge timestamps;
cache files are replaced atomically. Unusable empty provider responses cannot
replace a useful cache with an empty forecast stamped as fresh.

## Representative current before/after (same captured inputs)

September 20 is a matched day. Captured OpenWeather: high 18.81°C, low 11.82°C,
POP .23. Original Tomorrow: high 19.08°C, low 10.90°C, POP 0; this is the
06:00–06:00 aggregate. Corrected midnight Tomorrow: high 19.08°C, low 12.44°C,
average/raw/max POP all 0. The old production response/cache and dashboard proxy
contained the former merged selection. Compact value/provenance projections:

```json
{
  "before": {
    "date": "2026-09-20",
    "tempC": {"max": 19.08, "min": 10.9},
    "pop": 0.23,
    "_src": "tomorrow"
  },
  "after": {
    "date": "2026-09-20",
    "tempC": {"max": 18.81, "min": 11.82},
    "pop": 0.23,
    "popBasis": "daily",
    "_sources": {
      "tempC": {"max": "openweather", "min": "openweather"},
      "pop": "openweather",
      "condition": "openweather",
      "sunrise": "openweather"
    }
  }
}
```

The real response retains additional temperature subfields, astronomy and metadata.
The dashboard formula changes the display from 66°F / 52°F, 23% to
66°F / 53°F, 23%. The corrected selection reaches the cache and HTTP handler
unchanged in the captured-payload integration test.

Full captured forecast replay (high/low °C; Tomorrow POP is the average aggregate):

| Local date | OWM high/low | OWM POP | Tomorrow high/low | Tomorrow POP | Selected high/low | Display °F / POP |
| --- | --- | --- | --- | --- | --- | --- |
| Sep 19 | 15.59 / 14.01 | 1 | 15.61 / 13.67 | .095 | 15.59 / 14.01 | 60 / 57, 100% |
| Sep 20 | 18.81 / 11.82 | .23 | 19.08 / 12.44 | 0 | 18.81 / 11.82 | 66 / 53, 23% |
| Sep 21 | 15.76 / 10.17 | 0 | 15.77 / 10.15 | 0 | 15.76 / 10.17 | 60 / 50, 0% |
| Sep 22 | 15.98 / 7.53 | 0 | 16.00 / 7.27 | 0 | 15.98 / 7.53 | 61 / 46, 0% |
| Sep 23 | 16.73 / 6.60 | 0 | 15.88 / 7.68 | .006 | 16.73 / 6.60 | 62 / 44, 0% |
| Sep 24 | 17.16 / 9.47 | .27 | 16.57 / 10.50 | .213 | 17.16 / 9.47 | 63 / 49, 27% |
| Sep 25 | 19.74 / 13.03 | .20 | unavailable | unavailable | 19.74 / 13.03 | 68 / 55, 20% |
| Sep 26 | 23.05 / 13.51 | 0 | unavailable | unavailable | 23.05 / 13.51 | 73 / 56, 0% |

All eight dates are unique, consecutive and local; minima <= maxima; probabilities
are in [0,1]. Every selected daily temperature and POP here comes from OpenWeather,
including the two dates beyond Tomorrow coverage. Today's .905 POP difference is
visible in diagnostics, with the differing POP bases retained. The seven future
days are not fabricated or duplicated.

## Tests, checks and remaining limits

Tests cover provider request horizons/timezones, eight-day and short-horizon merges,
39°C vs 30°C disagreement, both directions of POP disagreement, null/nonfinite/zero
fallback, both provider outages, metric invariants with imperial legacy config,
Chicago UTC boundaries and DST, field provenance, captured multi-day replay,
atomic cache writes and stale timestamp preservation, empty provider responses,
and scheduler provider limits. CI now runs the unit/integration suite in addition
to its existing syntax check. No new dependency was introduced.

Remaining limits: a provider preference is not a meteorological accuracy guarantee;
Tomorrow's plan cannot cover the last two dates; its POP aggregates have different
semantics; no September 2 raw forecast was retained; dashboard server-side source
and Git state remain unverified because SSH access was unavailable. Scheduler
limits remain in-memory and exclude direct HTTP/geocoding requests, as documented.

## Provider references

- [Tomorrow Timelines request semantics](https://docs.tomorrow.io/reference/post-timelines)
- [Tomorrow plan horizons and aggregate suffixes](https://docs.tomorrow.io/reference/weather-data-layers)
- [Tomorrow core units and precipitation fields](https://docs.tomorrow.io/reference/data-layers-core)
- [OpenWeather One Call daily POP and units](https://openweathermap.org/api/one-call-3)

## Completed deployment and live verification

The existing Compose build/recreate completed successfully on September 19.
The previous image is retained locally as
`weather-api-weather-api:before-forecast-fix-20260919`; the deployed container
reports healthy with zero failing health checks. No configuration or secrets were
changed. The repository remains on `fix/forecast-data-contract` with uncommitted,
reviewable changes. No dashboard deployment was performed by this task.

The existing Dockerfile and requirements use floating versions. Rebuilding
therefore refreshed the base/runtime packages (including FastAPI 0.141.1,
Uvicorn 0.53.0, Requests 2.34.2 and PyYAML 6.0.3); no dependency declaration was
changed. All 31 tests passed both with the old deployed dependencies and inside
the newly built image. Syntax checks and `git diff --check` passed. There is no
separate lint command configured in this repository.

Startup refresh provenance:

- OpenWeather response obtained: `2026-09-19T16:11:04.121705+00:00`.
- Tomorrow response obtained: `2026-09-19T16:11:04.312126+00:00`.
- Merge generated / `updatedAt`: `2026-09-19T16:11:04.314658+00:00`.
- Cache written: `2026-09-19T16:11:04.314668+00:00`.
- At verification: cache age 30 seconds, `stale: false`, `refreshFailed: false`;
  both provider statuses `ok`.

The post-deployment cache, `/weather/home`, and running dashboard `/api/weather`
returned identical daily arrays and `updatedAt`. All eight local dates, min/max
ordering, normalized probability bounds and selected field provenance were
verified. Tomorrow covered six dates; OpenWeather covered all eight. Today's
Tomorrow average POP had changed from .095 in the earlier captured fixture to
.084 by startup; the live .916 disagreement against OWM=1 was correctly exposed.
This is a newer provider response, not a mutation of the captured fixture.

An existing local Playwright/Chromium installation was used because the
agent-browser CLI was unavailable; nothing was installed. A headless browser
loaded the live dashboard and asserted that each of its seven displayed rows
matched the proxied daily values using the actual conversion formulas:

| Displayed day | High / low | POP |
| --- | --- | --- |
| Sun Sep 20 | 66°F / 53°F | 23% |
| Mon Sep 21 | 60°F / 50°F | 0% |
| Tue Sep 22 | 61°F / 46°F | 0% |
| Wed Sep 23 | 62°F / 44°F | 0% |
| Thu Sep 24 | 63°F / 49°F | 27% |
| Fri Sep 25 | 68°F / 55°F | 20% |
| Sat Sep 26 | 73°F / 56°F | 0% |

Temporary investigation artifacts, including baseline/refreshed/post-deployment
responses, raw provider captures, old deployed code, logs, the browser verification
result and weather-card screenshot, are in `/tmp/weather-investigation`. They are
not committed. The sanitized captured-provider fixture is retained in the tests.

## Files changed

- `weather.py`: canonical units, supported Tomorrow horizon/midnight aggregation,
  local dates, explicit per-field policies, probability semantics, provenance,
  disagreement diagnostics, provider status/timing, useful-payload validation.
- `cache.py`: atomic replacement, safe legacy reads, distinct cache timing/staleness.
- `main.py`: honest stale/busy fallback metadata, sanitized upstream failure response.
- `scheduler.py`: effective independent provider limits and failed-attempt counting.
- `config.yaml.example`, `README.md`: contract, units, horizon, timezone and deployment documentation.
- `tests/test_weather.py`, `tests/test_cache.py`, `tests/test_api.py`,
  `tests/test_scheduler.py`, `tests/fixtures/forecast-2026-09-19.json`: regression and replay coverage.
- `.github/workflows/ci.yml`: run the suite in CI.
- `docs/forecast-investigation-2026-09-19.md`: this evidence and verification report.
