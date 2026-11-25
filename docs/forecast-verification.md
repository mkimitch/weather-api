# Forecast selection and verification — implementation / operations

## Audit and architecture

The Git repository is the deployment's `app` directory. The stack Compose file and
live configuration live outside Git. See [the previous forecast investigation](forecast-investigation-2026-09-19.md)
for the original policy and captured provider behavior.

HTTP misses, forced refreshes and scheduled refreshes use shared durable request
reservations. Provider forecasts are normalized and archived independently before
selection and atomic cache replacement. The scheduler also collects NWS observations.
The accuracy endpoint uses retained forecasts and the latest corrected observations.

## Phase 1: selection and contract

`weather.py`: configurable daily order with `[openweather, tomorrow]` as the default; field fallback;
OWM for unavailable days and beyond Tomorrow's complete horizon. Order validation
runs before billable calls. A zero is valid, missing/nonfinite/out-of-range probability
is null. Temperature extrema are numeric, finite, plausibly bounded and ordered;
invalid values are treated as missing, never clamped. If independently falling back
creates min > max, use one provider's valid pair, or mark both missing.

Tomorrow requests still use `dailyStartHour: 0`, `now` to `nowPlus5d`. The last
partially covered calendar day is not selected, even if an aggregate was returned;
it is still archived. Today's remaining forecast is displayed, but is never scored
as a complete day. Dates, units, returned daily boundary and request horizon are
checked independently. `daily_max_age_minutes` bounds response age; this cannot detect
an upstream silently serving an old model because model issuance is unavailable.
Previously learned IANA metadata is reused only at identical coordinates. Without a
known IANA timezone, Tomorrow daily periods are unsuitable for selection/verification.

Current conditions retain the fresher-timestamp policy, Tomorrow on ties. Tomorrow's
current field is an hourly forecast fallback, not a measured observation. No provider
forecast is ingested as independent truth. Native condition codes stay atomic, with
an additive broad `condition.category`; category normalization alone does not make a
representative daily code comparable to an observed daily event.

The public `daily[].tempC`, `pop`, `popBasis`, `_src`, `_sources` and existing top-level
fields remain. New metadata: `_providers[].period`, `_providers[].eligible`,
`extra.selectionPolicy`, `extra.historyStatus`, `precipBasis`, `precipComponents`.
`providerStatus` additionally supports `rate_limited` and `quota_unavailable`.
Missing OWM daily precipitation
now returns null rather than an invented zero; an explicitly reported zero is preserved.

Replay tests cover local date ordering, eight-day OWM coverage, °C/°F conversion,
0–1 POP and identical cached/API daily arrays. The Daymark weather bundle served
on November 24, 2026 maps null POP to 0% and ignores `popBasis`. Keep OpenWeather
first by default until Daymark can render unknown values and label the basis. For Tomorrow use “daily average probability” or “daily maximum
probability” as appropriate; these must not be presented as equivalent to OWM daily
POP. Condition consumers should use `_sources.condition` and its native namespace,
or the additive category, rather than assuming `_src` identifies every field.

## Probability and accumulation semantics

Tomorrow percentages normalize to 0–1. Its daily selection still prefers
`precipitationProbabilityAvg`, then the unsuffixed field, then `Max`. All three
alternatives are preserved. No cross-provider maximum, average or ensemble is used.
Tomorrow describes probability for >0.254 mm liquid-equivalent precipitation over a
period and area around a point; Avg/Max aggregate probabilities are not the probability
of the union of hourly precipitation events. See [core fields](https://docs.tomorrow.io/reference/data-layers-core)
and [aggregate operators / free horizon](https://docs.tomorrow.io/reference/weather-data-layers).

OWM documents a daily probability between 0 and 1, but its public One Call field
definition does not establish the same measurable threshold and aggregation as
Tomorrow. Its rain/snow volumes are retained with their original component labels.
[OWM field definitions](https://openweathermap.org/api/one-call-3).

Tomorrow daily intensities remain labeled mm/hour, not daily totals. Documentation
for accumulation products and individual API snow fields has differing aggregation
qualifications; no new total is manufactured from these fields. See
[accumulation guidance](https://support.tomorrow.io/hc/en-us/articles/38448998562068-Accumulation-Weather-Parameters-Rain-Snow-Sleet-Core-Layer-Included)
and the core field reference above. Daily Brier/amount comparisons remain blocked
until exact event, liquid-equivalence and valid-period definitions are established.

## Phase 2: archive, schema and observations

`history.py` initializes new databases at schema version 2 and upgrades version 1
in place. The migration preserves snapshots and quota records. Existing JSON caches
are untouched. A newer schema version is rejected. SQLite WAL, foreign keys, a ten-second busy timeout and short
transactions support scheduler/HTTP concurrency. Schema:

| Table | Key | Contents |
| --- | --- | --- |
| `responses` | SHA-256 of location identity, provider, retrieval instant | UTC retrieval, nullable issue time, source/version, coordinates, IANA zone, compressed JSON of raw daily/hourly forecast fields, normalized values and units |
| `forecasts` | response ID + target date | local date, UTC interval, lead hours to local midnight, normalized daily fields, explicit noon hourly wind projection |
| `attempts` | integer ID | provider, location, request type, configured point identity, UTC attempt time, outcome, daily/incomplete record counts |
| `observations` | station + UTC valid time | retrieval instant, original units/QC, measured temperature/wind/hourly precipitation, station coordinates |
| `jobs` | job name | attempt/success timestamps and sanitized collection/retention status |

All received successful provider forecasts are stored, including unselected and
partial endpoint days and their raw hourly values. No past forecasts are reconstructed.
A retrieval at a new time produces a new snapshot even when predictions are identical.
Re-ingesting the same provider/location/retrieval is idempotent. Issue time is null,
never fabricated from retrieval. Raw provider error bodies, credentials, request URLs
and headers are not archived. Nonfinite JSON numbers serialize as null.

Retention defaults to **400 days from retrieval** for forecasts, observation valid time
for observations, and attempt time for budgets/reliability. The existing scheduler
claims retention work once per 24 hours. Expired archive rows cascade to daily rows;
JSON caches and other files are not pruned. SQLite reuses freed pages instead of
vacuuming the live database. If the service is stopped, no collection or pruning runs.
Keep at least the report window plus the maximum forecast lead time. At five-minute
refreshes there are up to 576 provider snapshots and roughly 4,000 daily index rows per
location/day; compressed payload and SQLite size depend on upstream responses.
Plan for disk monitoring, potentially several GB at 400 days, or reduce refresh cadence.
This is a rolling archive, not indefinite history. Changing the DB path starts a new
archive/budget unless the existing database is migrated safely.

Independent ingestion (`observations.py`) uses NWS `/stations/KLVN/observations`,
60-minute default cadence, rolling three-day overlap for delayed/corrected records,
bounded pagination (12 pages) and 20-second request timeouts. Failed/empty/partial
runs are visible in `/healthz`; failure messages contain exception types, never request
contents. A persisted job claim prevents repeated retries on each five-minute weather
refresh. It also survives restarts. Requests are made only for explicitly configured
stations, once per station even if multiple locations use it. A contact User-Agent is
required. [NWS service guidance](https://www.weather.gov/documentation/services-web-api).

Accepted readings have numeric finite values, supported units and QC flag V, C or S
(verified, coarse-checked or screened; not all are final quality controlled).
Other QC flags, unknown units, negative precipitation and missing values are excluded.
Newer ingestions may replace a station reading with an upstream correction; forecasts
remain immutable. Original measurement fields and QC are retained. These are preliminary
measurements; daily extrema are derived from samples, not official climate extremes.
No modeled observations or gap interpolation are introduced.

### KLVN evidence and alternate sources

One public batch inspected November 13, 2026 contained 391 records from November 6
21:15 UTC through November 13 20:35 UTC. Temperature was non-null in 388, wind in 385;
hourly precipitation and 24-hour max/min temperature were absent in all 391. These
counts are a point-in-time availability check, not evidence about provider accuracy.
The configured collector's QC rules can further reduce valid coverage. See the
[NWS KLVN viewer](https://www.weather.gov/wrh/timeseries?site=KLVN).

KLVN is nearby airport exposure, not the home: elevation, open runways, instrument
height, surface, snow cover and localized precipitation can differ. MAE includes
this spatial mismatch. An apparent provider advantage against KLVN might not transfer
to the home or to another season.

Additional sources evaluated, not silently integrated:

- [NOAA GHCN Daily](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily)
  supplies measured climate elements, with quality/source flags and revisions. Station
  suitability, observation time and calendar definition must be established first;
  daily climate totals are not automatically a Chicago civil-day total.
- [Iowa Environmental Mesonet METAR archive](https://mesonet.agron.iastate.edu/request/download.phtml?network=IA_ASOS)
  can recover station reports, but missing/trace values require care and an archive
  cannot restore a measurement never reported by KLVN. No duplicate ingestion feed
  was added without demonstrating that it fills the missing variable.
- [Minnesota CoCoRaHS](https://www.cocorahs.org/Content.aspx?page=welcomeMN) is a possible
  independent precipitation gauge source. Typical morning-to-morning totals cannot
  be assigned to midnight-to-midnight forecast days. A compatible recording gauge
  at the home, or a verified nearby source with interval data, is preferable.

## Phase 3: verification and reporting

`GET /accuracy/home?days=30` (also 90 or 365) returns the target-day window, definitions,
provider counts, paired and all-available metrics, observation coverage, failure rates,
lead bins, exclusions and limitations. It reads retained data without upstream calls.
Reports use the configured point identity (name/coordinates/query/zone) and station;
changing coordinates or timezone does not pool old forecasts into the new point.
Reports may be expensive over large archives; keep this a low-frequency diagnostic
endpoint. No heavyweight dashboard or new caching/score invalidation service was added.

Rules:

1. Match Chicago local calendar periods using IANA timezone rules: UTC internally,
   23/24/25-hour days. Only completed days, with six hours for delayed observations.
2. Reject forecasts retrieved after the target day began. Their observations may
   already have been available. Lead bins are [0,24), [24,48), [48,72), etc. hours
   before target-day midnight, including for the explicitly labeled noon wind metric.
3. Use the latest snapshot per provider/target day/lead bin. Frequent refreshes do
   not become independent statistical samples. Match provider retrievals within
   30 minutes; otherwise retain individual metrics but exclude the pair.
4. Sampled high/low require measurements in at least 90% of the actual UTC hours
   of that local day, with no gap (including edges) exceeding 90 minutes. Missing
   readings fail the coverage test; they are not filled with forecasts.
5. Wind uses the forecast's **hourly local-noon** value and the closest valid measured
   reading within ten minutes. Daily provider wind fields are not assumed equivalent
   means. Missing noon hourly coverage is explicitly unscored.
6. Temperature/wind MAE = mean absolute error; signed bias = forecast minus observation.
   Coverage reports counts of eligible forecast events and verified events.
7. Rain ground truth would require exact, nonoverlapping one-hour liquid measurements
   tiling the entire local day. Overlapping hourly totals are never double counted;
   missing totals are never zero. Threshold **>0.254 mm** defines measurable precipitation
   in the calculation kernel. Trace/below-threshold amounts are not an event. The
   endpoint reports daily probability/amount/condition metrics as unverifiable because
   current forecast semantics do not support defensible matching, even if a station
   day has complete rain measurements.

`verification.scores` implements MAE, bias, Brier score/probability bias, amount MAE
plus hits/misses/false alarms/correct negatives/CSI, and categorical accuracy. Tests
verify these formulas; the reporting layer gates their use by available comparable
data. Brier score functionality does **not** imply an operational daily precipitation
comparison exists. No conclusion about overforecasting POP can currently be made.

Forecast reliability covers requests for the configured point, including failures
and unfinished `pending` attempts. The point identity includes the location name,
resolved coordinates, city query if set, and configured timezone. Geocoding and
legacy attempts without recorded point and request type still count toward quota
but are excluded from forecast reliability. `incompleteDailyRecords` counts missing high/low/POP
or partial intervals; `returnedDailyRecords` is its denominator. Absent days are not
counted as failed records because advertised horizons differ. A quota-skipped call is
not an attempted upstream request; it appears in current `providerStatus`, not failure
rate. Account use outside this application is invisible to these counters.

## Phase 4: recommendations, not automatic switching

The report exposes per-variable/per-lead recommendations; the selection function
currently reads explicit configuration only. There is no switch that silently enables
adaptive selection. Archive identity, period/semantic metadata, reliability/freshness
and paired metrics are the small extension points for a future policy.

Evidence needs at least 30 matched target dates across eight calendar weeks. A fixed
seed, 2,000-resample calendar-week cluster bootstrap yields an exploratory 95% interval
for Tomorrow absolute error minus OWM absolute error. Resampling weeks reduces the
false confidence of treating every refresh/day as independent. A recommendation is
only a candidate for manual review when the entire interval exceeds a practical
margin (0.5°C temperature, 2 km/h wind). Otherwise it is inconclusive/insufficient.
Intervals are not adjusted for multiple testing and do not prove future accuracy or
transfer across seasons. Do not enable automatic switching based solely on them.

## Configuration and deployment (operator approval required)

New/changed YAML defaults:

| Setting | Default |
| --- | --- |
| `daily_provider_priority` | `[openweather, tomorrow]` |
| `daily_max_age_minutes` | `90` |
| `history_enabled` | `true` |
| `history_db` | `<cache_dir>/history.sqlite3` |
| `history_retention_days` | `400`, minimum 2 |
| `observations_enabled` | `true`, only configured stations collected |
| `observation_interval_minutes` | `60`, startup validation minimum 10 |
| `observation_lookback_days` | `3`, bounded to 1–7 |
| `nws_user_agent` | empty; collection records failure until supplied |
| `locations[].observation_station` | unset; use `KLVN` for Lakeville |
| `locations[].timezone` | unset; set `America/Chicago` for Lakeville |
| `max_calls_per_24h_openweather` | `900` |
| `max_calls_per_24h_tomorrow` | `500` |
| `max_calls_per_hour_tomorrow` | `25` |
| `max_calls_per_second_tomorrow` | `3` |

Environment overrides (none required if corresponding nonsecret YAML is set):
`WEATHER_DAILY_PROVIDER_PRIORITY=tomorrow,openweather`, `WEATHER_HISTORY_DB`,
`NWS_USER_AGENT`. `OPENWEATHER_API_KEY` and `TOMORROW_API_KEY` override legacy YAML
credentials; their default is the existing YAML value or absent. Existing
`WEATHER_API_CONFIG` behavior remains. Compose must explicitly pass these environment
variables into the container; setting them only on the host is insufficient.

Tomorrow's documented free quotas are 500/day, 25/hour, 3/second. Local rolling limits
are intentionally conservative compared with provider fixed reset boundaries.
[Tomorrow quota reference](https://support.tomorrow.io/hc/en-us/articles/20273728362644-Free-API-Plan-Rate-Limits).
One Call's documented free allocation is 1,000 calls/day; account caps/billing settings
must be checked by the operator. No account portal or actual remaining quota was inspected.
Existing five-minute refresh is ~288 calls/provider/day/location before HTTP requests.
The example's three locations at 30-minute cadence fit the defaults; increasing
locations/cadence can exhaust shared budgets.

Before deployment:

1. Back up the existing cache. For future live SQLite backups use SQLite's backup API,
   or stop the service before copying DB/WAL files together; copying only a live main
   DB file can miss recent transactions. Keep secrets outside Git and image context.
2. Add `timezone: America/Chicago` and `observation_station: KLVN` to the existing
   `home` entry. Supply `NWS_USER_AGENT` with operator contact info. Move credentials
   to environment variables if desired; legacy keys continue working.
3. Keep the existing `./weather_cache:/app/weather_cache` bind mount; it covers the
   default DB and WAL files. Ensure sufficient disk space and service write permissions.
4. Review and approve building/recreating the service through the existing deployment
   process. The schema is initialized or migrated automatically; no data import or
   forecast backfill is performed. Collection starts inside the existing scheduler.
5. Check `/healthz` verification job status, normal `/weather/home` provenance, and
   `/accuracy/home?days=30`. Expect insufficient data initially; wait for future target
   days and adequate independent observations. Check at least one DST-boundary day
   when it occurs, and monitor disk growth and collection failures.

For rollback, use an image compatible with schema version 2. If the previous image
requires an older schema, preserve the migrated database and restore a pre-migration
backup for that image. Backups can lag recent forecasts and quota reservations. The
JSON cache envelope is unchanged.

## Example output

[Forecast provenance](examples/forecast-provenance.json) is generated through the actual
HTTP handler using the captured September fixture, a fixed clock and an explicit
Tomorrow-first override. It shows Tomorrow selected for supported days and keeps both providers' alternatives and period eligibility.
[Accuracy response](examples/accuracy-synthetic.json) uses synthetic deterministic
forecast/observation fixtures, with one paired date and an insufficient-evidence result.
These are illustrative outputs, not historical accuracy findings. The live service will
start with no forecast verification samples.

## Validation and remaining work

Run the full offline suite and compile all runtime modules:

```sh
python -m unittest discover -s tests -v
python -m py_compile main.py cache.py scheduler.py weather.py history.py observations.py verification.py timeutils.py
git diff --check
```

Tests use deterministic inputs and mocked provider/NWS HTTP requests. The
September provider data is a replay fixture. Synthetic observation and accuracy
examples are excluded from production history.

Still required for complete precipitation/condition verification: a trustworthy local
precipitation measurement feed with compatible periods, provider-confirmed event and
liquid-equivalence definitions, a documented daily condition event mapping, and adapters
with fixtures. For true daily extrema, investigate official period-aligned climate
records or a recording home station rather than claiming sampled extrema are exact.
A future hourly precipitation study could use the retained hourly forecasts once
valid periods and observation coverage are established. Adaptive switching additionally
requires sufficient paired data, seasonal coverage, multiple-comparison controls,
station-proxy validation, reliability/freshness safeguards and explicit approval.
