import datetime as dt
import json
from typing import TypedDict

import dagster as dg


def _to_date(s: str) -> dt.date:
    if len(s):
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    return dt.datetime.strptime(s, "%Y-%m").date()


class _CursorData_str(TypedDict):
    last_storage_ids: dict[str, int]
    month_data: dict[str, list[str]]


class CursorData(TypedDict):
    last_storage_ids: dict[dg.AssetKey, int]
    month_data: dict[str, set[dt.date]]


def _mthdata_to_strdict(d: dict[str, set[dt.date]]) -> dict[str, list[str]]:
    res: dict[str, list[str]] = {}
    for key, value in d.items():
        res[key] = [f"{dat.year}-{dat.month:02}-{dat.day:02}" for dat in value]
    return res


def _mthdata_from_strdict(d: dict[str, list[str]]) -> dict[str, set[dt.date]]:
    res: dict[str, set[dt.date]] = {}
    for key, value in d.items():
        res[key] = set(_to_date(date_str) for date_str in value)
    return res


def cursordata_to_json(cdata: CursorData) -> str:
    cdata_str: _CursorData_str = {
        "last_storage_ids": {
            ak.to_user_string(): eid for ak, eid in cdata["last_storage_ids"].items()
        },
        "month_data": _mthdata_to_strdict(cdata["month_data"]),
    }
    return json.dumps(cdata_str)


def cursordata_from_json(s: str) -> CursorData:
    cdata_str: _CursorData_str = json.loads(s)
    return {
        "last_storage_ids": {
            dg.AssetKey.from_user_string(akstr): eid
            for akstr, eid in cdata_str["last_storage_ids"].items()
        },
        "month_data": _mthdata_from_strdict(cdata_str["month_data"]),
    }


__all__ = ["CursorData", "cursordata_from_json", "cursordata_to_json"]
