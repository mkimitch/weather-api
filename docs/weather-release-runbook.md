# Weather release and rollback runbook

Deploy Daymark first against the existing Weather API. Deploy Weather API after the Daymark checks pass. This runbook uses the documented production service names, working directories, and loopback ports. Confirm them locally with the existing service definitions before starting. Make each local release commit reachable on its deployment host through the approved release handoff.

## 1. Daymark on the Daymark host

Set `DAYMARK_DIR` to the installed adapter-node checkout and `DAYMARK_SERVICE` to its systemd unit. The commands use the documented default loopback ports. Preserve the service's existing environment file and Yarn Plug'n'Play configuration. This weather release changes no database schema.

### Backup and deploy

In one shell on the Daymark host:

```bash
set -euo pipefail
umask 077
cd "${DAYMARK_DIR:?set the Daymark checkout}"
test -z "$(git status --porcelain --untracked-files=all)"
release="${DAYMARK_RELEASE_SHA:?set the Daymark PR tip}"
git cat-file -e "$release^{commit}"
sudo systemctl show "${DAYMARK_SERVICE:?set the systemd unit}" -p WorkingDirectory -p ExecStart
backup="$HOME/daymark-release-backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup"
git rev-parse HEAD > "$backup/revision"
tar -czf "$backup/adapter-node.tar.gz" build .pnp.cjs .pnp.loader.mjs

printf '%s\n' "$backup"
```

For a file-backed countdown database, set `COUNTDOWN_DB_FILE` to its active path if the service overrides the documented default. Run this online SQLite backup in the same shell. Use the configured database's snapshot procedure if it is external.

```bash
python3 - "${COUNTDOWN_DB_FILE:-data/family-dashboard.db}" "$backup/countdowns.sqlite3" <<'PY'
import sqlite3
import sys
from pathlib import Path

source = Path(sys.argv[1]).resolve(strict=True).as_uri() + "?mode=ro"
with sqlite3.connect(source, uri=True) as live, sqlite3.connect(sys.argv[2]) as backup:
    live.backup(backup)
    assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
PY
```

Deploy the selected commit and keep the service configuration already in use.

```bash
git switch --detach "$release"
yarn install --immutable
yarn build
sudo systemctl restart "$DAYMARK_SERVICE"
sudo systemctl is-active "$DAYMARK_SERVICE"
```

### Verify

Check Weather API's `/healthz` on the Weather API host as shown below before sampling the proxy, so the weather request reads a fresh cache. On the Daymark host:

```bash
sudo journalctl -u "$DAYMARK_SERVICE" -n 50 --no-pager
curl -fsS http://127.0.0.1:3000/ -o /dev/null
curl -fsS http://127.0.0.1:3000/api/weather |
  jq -e '(.data // .) | .daily | type == "array"'
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:3000/dev/weather-conditions)" = 404
```

Open the kiosk in its normal portrait configuration. Check that weather still renders from the previous API, that a real zero shows as `0%`, and that unavailable values show as `—` whenever those inputs are present. Confirm the legend remains readable when displayed.

### Roll back Daymark

Set `backup` to the directory printed at deployment. Restore the original checkout and saved build, keeping the displaced build for inspection.

```bash
set -euo pipefail
cd "${DAYMARK_DIR:?set the Daymark checkout}"
test -f "$backup/revision"
test -f "$backup/adapter-node.tar.gz"
test -z "$(git status --porcelain --untracked-files=all)"
sudo systemctl stop "$DAYMARK_SERVICE"
git switch --detach "$(cat "$backup/revision")"
yarn install --immutable
mv build "$backup/displaced-build"
tar -xzf "$backup/adapter-node.tar.gz"
sudo systemctl start "$DAYMARK_SERVICE"
sudo systemctl is-active "$DAYMARK_SERVICE"
curl -fsS http://127.0.0.1:3000/ -o /dev/null
```

A build rollback can keep the live countdown database and existing environment. The backup is available for separate data recovery.

## 2. Weather API on the Weather API host

Set `WEATHER_STACK_DIR` to the Compose project directory. The commands assume the documented `./app` build context, `weather-api` service name, and loopback port 8787. Keep the existing configuration, bind mounts, provider fields, cadence, and secrets.

### Backup

In one shell on the Weather API host:

```bash
set -euo pipefail
umask 077
cd "${WEATHER_STACK_DIR:?set the Compose project}"
test -z "$(git -C app status --porcelain --untracked-files=all)"
release="${WEATHER_RELEASE_SHA:?set the Weather API PR tip}"
git -C app cat-file -e "$release^{commit}"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup="$HOME/weather-api-release-backups/$stamp"
mkdir -p "$backup/cache"
git -C app rev-parse HEAD > "$backup/revision"
container="$(docker compose ps -q weather-api)"
test -n "$container"
docker inspect --format '{{.Image}}' "$container" > "$backup/image-id"
rollback_image="weather-api-local-rollback:$stamp"
docker image tag "$(cat "$backup/image-id")" "$rollback_image"
printf 'services:\n  weather-api:\n    image: %s\n' "$rollback_image" > "$backup/rollback.compose.yml"
cp -p compose.yml "$backup/compose.yml"
cache_source="$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/app/weather_cache"}}{{.Source}}{{end}}{{end}}' "$container")"
test -d "$cache_source"
find "$cache_source" -maxdepth 1 -type f -name '*.json' -exec cp -p -t "$backup/cache" {} +
printf '%s\n' "$backup"
```

Use SQLite's online backup API inside the existing container to read the configured database, including committed WAL data. The output is a standalone backup file in the private backup directory. If the deployment has no archive yet, the script reports that and the output file is empty.

```bash
docker compose exec -T weather-api python - > "$backup/history.sqlite3" <<'PY'
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

import yaml

with open(os.getenv("WEATHER_API_CONFIG", "config.yaml"), encoding="utf-8") as stream:
    config = yaml.safe_load(stream)

source = Path(os.getenv("WEATHER_HISTORY_DB") or config.get("history_db") or
              Path(config.get("cache_dir", "./weather_cache")) / "history.sqlite3")
if not source.is_file():
    print("No archived SQLite history present", file=sys.stderr)
    sys.exit(0)

with tempfile.TemporaryDirectory() as scratch:
    saved = Path(scratch) / "history.sqlite3"
    with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True) as live, \
         sqlite3.connect(saved) as backup:
        live.backup(backup)
        assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with saved.open("rb") as archive:
        shutil.copyfileobj(archive, sys.stdout.buffer)
PY
```

Retain the active configuration files used by Compose in the same private backup directory, including any mounted Weather API YAML or Compose environment overrides. Store backups privately. Runtime settings and credentials belong on their deployment hosts.

### Deploy and verify

```bash
cd "${WEATHER_STACK_DIR:?set the Compose project}"
git -C app switch --detach "$release"
docker compose build weather-api
docker compose up -d --no-deps weather-api
docker compose ps weather-api
docker compose logs --tail 50 weather-api
```

Allow a normal scheduler cycle to populate a fresh cache. Poll health first. Only request weather once the configured location cache is present and unexpired. The commands use no forced refresh.

```bash
location="${WEATHER_LOCATION:?set the configured location slug}"
curl -fsS http://127.0.0.1:8787/healthz |
  jq -e --arg location "$location" '.status == "ok" and any(.locations[]; .name == $location and .cache.exists and (.cache.expired | not))'

curl -fsS "http://127.0.0.1:8787/weather/$location" |
  jq -e '
    .conditionSchemaVersion == 1
    and ._cache.stale == false
    and all(.hourly[]; .conditions | has("hourly"))
    and all(.daily[]; .conditions | has("fullDay"))
    and all((.current, .hourly[], .daily[]) | select(. != null);
      .pop == null or (.pop | type == "number" and . >= 0 and . <= 1))'

curl -fsS "http://127.0.0.1:8787/accuracy/$location?days=30" |
  jq -e --arg location "$location" '.location == $location and .windowDays == 30'
```

On the Daymark host, while the cache is fresh:

```bash
curl -fsS http://127.0.0.1:3000/api/weather |
  jq -e '(.data // .) | .conditionSchemaVersion == 1'
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:3000/dev/weather-conditions)" = 404
```

Check the kiosk and review upstream status locally. The existing request asks for `weatherCode`. Daytime and nighttime conditions may remain unavailable until a separately approved acquisition change.

### Roll back Weather API

Set `backup` to the directory printed at deployment. Use the saved image with the same Compose project and mounts.

```bash
set -euo pipefail
cd "${WEATHER_STACK_DIR:?set the Compose project}"
test -f "$backup/rollback.compose.yml"
docker compose -f compose.yml -f "$backup/rollback.compose.yml" \
  up -d --no-build --no-deps --force-recreate weather-api
docker compose ps weather-api
curl -fsS http://127.0.0.1:8787/healthz | jq -e '.status == "ok"'
```

After the rollback is healthy, the deployment checkout can be returned to the recorded revision with `git -C app switch --detach "$(cat "$backup/revision")"` from a clean tree. Both tested versions read either cache and archive format with SQLite schema version 2, so a routine code rollback keeps the live data. If data recovery is required, stop the service, retain its current cache and history including WAL sidecars, restore the saved files at their configured paths with the original ownership, and verify SQLite integrity before starting. A rollback artifact from an older deployment should be checked for support of the current archive schema before reuse.
