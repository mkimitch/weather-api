# Tomorrow.io rich condition acquisition

## Catalog and contract

The field inventory is pinned in [catalog metadata](../catalogs/tomorrow.json) with the public reference URLs, retrieval date, and a SHA-256 of the structured metric enums. The Weather Codes table and both structured API references agree on the field assignments. The label conflict for the `6200` family is recorded separately. [Weather Codes](https://docs.tomorrow.io/reference/data-layers-weather-codes) · [Metric field descriptors](https://docs.tomorrow.io/reference/weather-data-layers#field-descriptors)

| Timelines field | Catalog coverage before, including the sentinel | Verified field/code bindings now | Known canonical classifications |
| --- | ---: | ---: | ---: |
| `weatherCode` | 24 / 24 | 24 / 24 | 23 |
| `weatherCodeFullDay` | 25 / 94 | 94 / 94 | 92 |
| `weatherCodeDay` | 94 / 94 | 94 / 94 | 92 |
| `weatherCodeNight` | 60 / 94 | 94 / 94 | 92 |

The prior counts are numeric matches under the old catalog. Most were implicit field matches. This reconciliation adds 69 full-day and 34 nighttime bindings and corrects 22 precipitation classifications. The four `0` sentinels remain unknown. The three disputed rich codes are bound to their documented fields and remain unknown with native evidence. Generic `6200` retains its existing light freezing rain interpretation. The Imperial-only generic codes `3000`, `3001`, and `3002` remain unverified for metric data and resolve to unknown if observed.

The card continues to show current and full-day conditions using the existing provider preference. Rich day and night data is available by context through V1. The contract retains atomic source selection, legacy values, POP value/basis/period/provenance, metric units and valid zero values. Solar endpoints remain null. SQLite remains at schema V2 and previously stored forecasts and caches remain readable. [Weather contract](weather-condition-contract.md)

## Scheduled request and acquisition gate

The default request is the existing 20-field point POST to `/v4/timelines`. A local activation appends `weatherCodeFullDay`, `weatherCodeDay`, and `weatherCodeNight` to that same request. Both modes use the existing `1h` and `1d` timesteps, metric units, point, timezone and horizon, starting at `now` with `dailyStartHour: 0`. The client uses the same headers and 12-second timeout. A 3xx is treated as a failed attempt in both modes, preventing a second POST. A scheduled cycle reserves one attempt in the existing rolling SQLite budget. A rejected expanded request uses the existing provider and cache fallback for that slot. It consumes its reservation and returns to the generic body on subsequent slots for that activation. [Timelines request reference](https://docs.tomorrow.io/reference/post-timelines)

The configured location needs an effective IANA timezone and a recent usable Tomorrow.io forecast in history. The acquisition gate examines comparable timestamps and baseline fields from the newest stored response for that location, including the generic code and each available POP field. It checks both timelines, contiguous hourly timestamps through the requested horizon, and complete interior local dates using DST-aware day boundaries. The prior forecast supplies baseline field coverage even when the archived forecast includes rich values. A missing or unreadable reference keeps acquisition generic. The gate requires enough horizon for at least one complete interior local day before it can publish.

Each hourly row and complete interior daily row must also contain usable generic weather data, including at timestamps absent from the previous forecast. A known generic code, valid POP, or valid temperature data is sufficient. The independent minimum leaves incomplete daily boundary rows subject to the existing overlap checks.

For each field, the diagnostics report received and missing values, nulls, sentinels, invalid types, unknown codes, documented disputes, and known values. They show separate counts for each timestep and complete interior days. Publication requires each field to have a known value on at least one complete interior day and documented nonzero codes across the complete interior dates. A disputed code can be retained as unknown on individual days. Unknown, missing, invalid, or sentinel-only interior coverage blocks publication. Hourly rich codes never determine current or hourly presentation.

The `tomorrow_acquisition` logger records timeline counts and bounds, coverage failures, per-field counts, and up to twelve warning samples. Samples contain allowlisted codes, fields, timestep, range, and scope. An explicitly hourly timestep warning for a rich field can pass when the baseline and daily enrichments pass. Other or unscoped warnings block rich publication. A malformed or degraded baseline response enters the provider failure path. An optional-field failure preserves the generic result and POP. The diagnostic excludes arbitrary provider text, response bodies and headers. [Warnings and errors](https://docs.tomorrow.io/reference/api-errors) · [Field availability](https://docs.tomorrow.io/reference/weather-data-layers#field-availability)

## Isolated production verification

The probe was held. Active account billing and remaining quota could not be established, and this workspace has no verified access to the live ledger or production key. There was no external provider call or production reservation attributable to the implementation. All field availability and payload measurements below are from offline replay.

If the prerequisites are later established, use the exact request above for the existing production point and effective request parameters. Use the existing production credential source and reserve one `acquisition_probe` attempt in the existing production SQLite ledger immediately before the POST. Run outside the scheduled worker with no redirects, retries, live control call, OpenWeather call, or cache/publication writes. Compare the response with the most recent stored **generic** forecast. Count a timeout or refusal as an attempt, record the outcome, and stop. The approved probe would contribute at most one provider call, for a nominal 289 in the affected 24-hour window. The result should include the durable ledger entry and actual provider usage when available. An HTTP 200 without field-level and baseline coverage evidence is inconclusive. [Provider metering](https://support.tomorrow.io/hc/en-us/articles/5188003525908-How-an-API-Call-Is-Counted)

## Canary and rollback after a passing probe

1. Follow the [Weather API Compose runbook](weather-release-runbook.md) to retain the production image, cache, SQLite-consistent backup, and existing mounts. Review and deploy the Weather API with the control absent. Keep the existing cadence and provider order. Align the service startup fetch to its normal slot and keep one active scheduler. The Daymark change is a shared fixture and documentation update.
2. For one production location, configure `tomorrow_rich_conditions_control_file` as an absolute `.control` path. Use a dedicated directory under the shared cache mount with trusted ancestors. The service accepts a regular, single-link file and a parent directory owned by root or the service user. Both must deny group and other writes, and the path must resolve without symlinks. The service needs read access. Keep the marker absent until the canary is approved.
3. After a healthy generic cycle, atomically install `observe <token>`, using a fresh token of 32 lowercase hexadecimal characters. The service locks the parent directory to claim the token in the durable ledger and releases that lock before the POST. A single scheduled attempt consumes an observation token across restarts and file replacement. Its generic data can still be cached and archived. Its rich results are available in bounded diagnostics. HTTP refreshes use the generic body and share the scheduler's per-location refresh lock for controlled locations. An ordinary cache miss can reuse the scheduler's result.
4. Review the observation, provider logs, request counts, coverage, and storage budget. A separately approved `publish <new token>` activation uses the same procedure. The service locks the control directory, rechecks the marker, archives the response, publishes the cache, and only then releases that attempt's claim. It publishes usable generic data if the rich gate or archive fails. Failed, abandoned, and observation claims are retained, so rewriting an old marker with the same token cannot restart them. Confirm at least one full quota cycle of request counts and both providers' health before expanding.
5. Remove or replace the marker while holding the same directory lock. An operator edit completed during the POST takes effect before publication, and an edit that acquires the lock after publication takes effect on the next slot. The service leaves the last valid cache available. If a code rollback is needed, restore the previous Weather API image and keep the live cache, SQLite ledger, and WAL mounted. Verify the next normal generic cycle using cached data.

Use the host-side path on the same mount for operator edits. Keep the control directory itself in place so the host and service lock the same inode. Use a local Linux filesystem with shared `flock` semantics and keep one scheduler process. For an approved observation:

```sh
control="${WEATHER_TOMORROW_CONTROL_FILE:?set the host-side control path}"
control_dir="$(dirname -- "$control")"
(
  set -eu
  flock -x 9
  token="$(openssl rand -hex 16)"
  marker="$(mktemp "$control.XXXXXX")"
  trap 'rm -f -- "$marker"' EXIT
  printf "%s %s\n" observe "$token" > "$marker"
  mv -f -- "$marker" "$control"
) 9<"$control_dir"
```

Run the command as the service user or arrange for it to read the mode-0600 marker. To disarm:

```sh
flock -x "$control_dir" rm -f -- "$control"
```

## Offline operational measurements

The replay used the September capture for both providers, expanded its two recorded Tomorrow hourly patterns to 120 rows, and added documented rich values to six daily rows. Measurements used the real scheduled path, FastAPI JSON encoder, cache writer and SQLite archive. Each replay made one simulated Tomorrow request and matched current, hourly, and daily POP behavior against the generic replay. Run `python scripts/measure_tomorrow_acquisition.py` to reproduce the results. [Machine-readable measurements](examples/tomorrow-acquisition-measurements.json)

| Increase per response or snapshot | Daily rich keys only | Hourly rich keys also returned |
| --- | ---: | ---: |
| Provider JSON | 444 B | 9,084–9,324 B |
| Estimated provider gzip | 89 B | 163–245 B |
| API and local cache JSON | 22,913 B each | 22,913 B each |
| Estimated API gzip | About 0.84 kB | About 0.84 kB |
| Logical SQLite archive | 10,921 B | 11,086–11,268 B |

At 288 successful snapshots per day the modeled storage increase is about 3.15–3.25 MB/day, or 1.26–1.30 GB over 400 days. The per-day JSON rows account for 10,196 B of the increase per snapshot. The estimates exclude SQLite page allocation and WAL, filesystem and backup overhead. Confirm actual compression, latency, response size, disk growth and retention on the canary.

The implementation is ready for review and offline release verification. Production acquisition readiness depends on the probe and the one-slot canary.

## Implementation validation

- Weather API passed 118 tests on the workspace interpreter and in the Python 3.11 image with a disabled container network. Python syntax and diff checks passed. The ordinary cloud Docker build hit a package-proxy certificate error. A temporary validation Dockerfile mounted the session CA as a BuildKit secret and built successfully.
- The default-off response and request endpoint, body, headers and timeout matched the deployed Weather API revision `fd6c366c364517ac2b462f3d7de0af3ecc5787e9` byte for byte in offline replay. The single-POST transport tests separately cover rejection of redirects.
- JSON Schema V1 validated all 364 exported fixture rows and the generic and three expanded replay responses. The fixture matches both repositories and the exporter byte for byte. Its SHA-256 is `3c4355ad2c8be6f7195808c541682c90bfb072d276d377b53396dd4e127599a8`.
- Daymark passed 179 tests, the production build and CSS lint. The built app passed V1 and legacy proxy round-trips against a local fixture feed, and the inspector returned 404. Svelte check passed with eight existing CSS warnings in two files. Repository-wide lint remains red. The stock Prettier invocation could not resolve its Svelte plugin under Plug'n'Play. With the existing wrapper, Prettier flagged 12 unrelated files. Direct ESLint found 56 pre-existing errors in 14 source files whose content matches the deployed revision.
- The hand-authored tests cover mixed cloud classifications, all 22 corrected precipitation classifications, exact field binding, rich and generic handling of `6200`, and the Imperial wind codes. The acquisition tests cover both DST transitions, baseline coverage loss, quota exhaustion and reservation order, warning sanitization, concurrent refresh, operator edits in flight, crash recovery, and archive, cache and rearming order. The official metric enums and Weather Codes JSON independently match all 306 bindings and the pinned digest.
