"""Export deterministic, offline V1 condition examples for Daymark."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conditions import (CATALOGS, SCHEMA_VERSION, TOMORROW_SOLAR_FIELDS, coverage,
                        interval_coverage, openweather_condition, tomorrow_condition)

TIME = "2026-11-01T05:00:00+00:00"
DATE = "2026-11-01"


def rows():
    for code in sorted(map(int, CATALOGS["openweather"]["conditions"])):
        yield openweather_condition({"weather": [{"id": code}]}, "hourly", TIME, TIME)
    for field, codes in CATALOGS["tomorrow"]["conditionsByField"].items():
        for code in sorted(map(int, codes)):
            if field in TOMORROW_SOLAR_FIELDS:
                context, basis = TOMORROW_SOLAR_FIELDS[field]
                yield tomorrow_condition({field: code}, field, context, coverage(basis, local_date=DATE),
                                         TIME, TIME, "1d")
            else:
                yield tomorrow_condition({field: code}, field, "hourly", interval_coverage(TIME), TIME, TIME, "1h")
    yield openweather_condition({"weather": [{"id": 511}, {"id": 615}, {"id": 741}]},
                               "current", TIME, TIME)
    yield openweather_condition({"weather": [{"id": 801}, {"id": 802}]},
                               "fullDay", TIME, TIME, local_date=DATE)
    yield tomorrow_condition({"weatherCode": 99999}, "weatherCode", "hourly",
                             interval_coverage(TIME), TIME, TIME, "1h")


def contents():
    body = ",\n".join("    " + json.dumps(row, ensure_ascii=False) for row in rows())
    return f'{{\n  "conditionSchemaVersion": {SCHEMA_VERSION},\n  "rows": [\n{body}\n  ]\n}}\n'


if __name__ == "__main__":
    print(contents(), end="")
