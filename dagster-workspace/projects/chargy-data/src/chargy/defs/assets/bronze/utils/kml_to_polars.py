"""
Parse a KML file containing EV charging stations into three Polars DataFrames:
- stations (one row per Placemark)
- charging devices (one row per Data name="chargingdevice" inside a Placemark)
- charging points (one row per connector described in the device JSON)

Expose `parse_path_to_polars(path_str: str)` which returns
(stations_df, devices_df, points_df).

The parser is defensive about:
- missing tags
- XML namespaces
- JSON that may be embedded or contain surrounding text
"""

import datetime as dt
import json
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

log = logging.getLogger(__name__)


def _extract_timestamp_from_filename(file: Path) -> dt.datetime:
    """Tries to extract a timestamp from the filename. Returns the modification time if not."""
    log.debug(f"Extracting timestamp from file name of {file.name}.")
    match = re.search(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|\+\d{2}:\d{2}))", file.name
    )
    if match is not None:
        log.debug(f"Found timestamp {match.group(1)} in file name.")
        return dt.datetime.fromisoformat(match.group(1))
    mtime = dt.datetime.fromtimestamp(file.stat().st_mtime)
    log.debug(
        f'No timestamp in file name"{file.name}". Returning the file modification time {mtime}.'
    )
    return mtime


def _ns_tag(tag: str, ns: Optional[str]) -> str:
    """Return namespaced tag for ElementTree if namespace present."""
    return f"{{{ns}}}{tag}" if ns else tag


def _text_of(elem: Optional[ET.Element]) -> Optional[str]:
    """Return stripped text of an element or None."""
    if elem is None or elem.text is None:
        return None
    txt = elem.text.strip()
    return txt if txt != "" else None


def _get_namespace(root: ET.Element) -> Optional[str]:
    """Extract namespace from ElementTree root tag like '{ns}kml' or return None."""
    m = re.match(r"\{(.+?)\}", root.tag)
    return m.group(1) if m else None


def _safe_int(val: Any, default: int = 0) -> int:
    """Return an int if the tag can be converted to int. Returns the default if not."""
    try:
        if val is None:
            return default
        return int(val)
    except Exception:
        try:
            return int(float(str(val)))
        except Exception:
            return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Return float if the tag can be converted to float. Returns the default if not."""
    try:
        if val is None:
            return default
        return float(val)
    except Exception:
        try:
            # sometimes values come as strings with extra characters
            s = re.sub(r"[^\d.+-eE]", "", str(val))
            return float(s) if s != "" else default
        except Exception:
            return default


def _extract_json_from_text(s: str) -> Optional[Dict[str, Any]]:
    """Try to extract a JSON object from a string. Return dict or None."""
    if not s:
        return None
    s = s.strip()
    # If it looks like direct JSON already
    try:
        return json.loads(s)
    except Exception:
        pass
    # Try to find first {...} block
    m = re.search(r"(\{.*\})", s, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    # Try to find first [...] (some payloads might be an array)
    m2 = re.search(r"(\[.*\])", s, flags=re.DOTALL)
    if m2:
        try:
            v = json.loads(m2.group(1))
            # wrap into dict if it's an array of connectors or devices
            return {"_array": v}
        except Exception:
            pass
    return None


def _parse_kml_to_list(
    kml_path: Path,
    stations: List[Dict[str, Any]] | None = None,
    charging_devices: List[Dict[str, Any]] | None = None,
    charging_points: List[Dict[str, Any]] | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Parse the KML at `kml_path` and return 3 List (stations, charging_devices, charging_points).

    Stations DataFrame columns, from Placemark tags:
      - timestamp (from the file's download date)
      - name (str)
      - visibility (bool)
      - address (str)
      - description (str)
      - status (str) from <styleUrl>
      - cpnum (int) from ExtendedData/Data[@name="CPnum"]/value
      - coordinates (str) from coordinates

    Charging devices DataFrame columns, from json in Placemark/ExtendedData/Data[@name="chargingdevice"]:
      - timestamp (from the file's download date)
      - coordinates (str) (station coords)
      - station_name (str)
      - device_id (str)
      - name (str)
      - n_connectors (int)

    Charging points DataFrame columns, from connectors field in the same json as charging devices:
      - timestamp (from the file's download date)
      - coordinates (str) (station coords)
      - station_name (str)
      - device_id (str)
      - id (str)
      - name (str)
      - maxchspeed (float)
      - connector (int)
      - description (str)
    """
    log.debug(f"Processing KML file: {kml_path}")
    tree = ET.parse(kml_path)
    root = tree.getroot()
    ns = _get_namespace(root)

    document_tag = _ns_tag("Document", ns)
    placemark_tag = _ns_tag("Placemark", ns)
    name_tag = _ns_tag("name", ns)
    visibility_tag = _ns_tag("visibility", ns)
    address_tag = _ns_tag("address", ns)
    description_tag = _ns_tag("description", ns)
    style_url_tag = _ns_tag("styleUrl", ns)
    point_tag = _ns_tag("Point", ns)
    coord_tag = _ns_tag("coordinates", ns)
    extended_data_tag = _ns_tag("ExtendedData", ns)
    data_tag = _ns_tag("Data", ns)
    value_tag = _ns_tag("value", ns)

    stations_rows: List[Dict[str, Any]] = stations if stations is not None else []
    devices_rows: List[Dict[str, Any]] = (
        charging_devices if charging_devices is not None else []
    )
    points_rows: List[Dict[str, Any]] = (
        charging_points if charging_points is not None else []
    )
    ts = _extract_timestamp_from_filename(kml_path)

    for pm in root.findall(f"./{document_tag}/{placemark_tag}"):
        # Station basic fields
        name = _text_of(pm.find(name_tag)) or ""
        visibility_text = _text_of(pm.find(visibility_tag))
        visibility: bool = (
            visibility_text is not None
            and len(visibility_text) > 0
            and (visibility_text.strip().lower() in ("1", "true", "yes"))
        )
        address = _text_of(pm.find(address_tag)) or ""
        description = _text_of(pm.find(description_tag)) or ""
        status = _text_of(pm.find(style_url_tag)) or ""

        # Coordinates (first coordinates descendant)
        coords = ""
        coord_elem = pm.find(f"./{point_tag}/{coord_tag}")
        if coord_elem is not None and coord_elem.text:
            coords = coord_elem.text.strip()

        # cpnum from ExtendedData/Data[@name="CPnum"]/value
        cpnum = 0
        ext = pm.find(extended_data_tag)
        if ext is not None:
            data = ext.find(f"{data_tag}[@name='CPnum']")
            if data is not None:
                cpnum = _safe_int(_text_of(data.find(value_tag)), 0)

        stations_rows.append(
            {
                "timestamp": ts,
                "name": name,
                "visibility": visibility,
                "address": address,
                "description": description,
                "status": status,
                "cpnum": cpnum,
                "coordinates": coords,
            }
        )

        # Charging devices: one or many <Data name="chargingdevice"><value>...</value></Data>
        if ext is not None:
            for data in ext.findall(f"{data_tag}[@name='chargingdevice']"):
                val_text = _text_of(data.find(value_tag)) or ""
                device_json = _extract_json_from_text(val_text) or {}

                # If extraction wrapped an array into {"_array": ...}, handle that by treating array entries as devices
                if "_array" in device_json and isinstance(device_json["_array"], list):
                    # Each item is a device-like dict
                    for dev_item in device_json["_array"]:
                        if not isinstance(dev_item, dict):
                            continue
                        _handle_device(
                            dev_item, coords, name, devices_rows, points_rows, ts
                        )
                else:
                    # device_json may be a dict describing a single device
                    if isinstance(device_json, dict) and device_json:
                        _handle_device(
                            device_json, coords, name, devices_rows, points_rows, ts
                        )
                    else:
                        # fallback: try to parse the raw value as a simple text id
                        raw = val_text.strip()
                        if raw:
                            devices_rows.append(
                                {
                                    "timestamp": ts,
                                    "coordinates": coords,
                                    "station_name": name,
                                    "device_id": raw,
                                    "name": "",
                                    "n_connectors": 0,
                                }
                            )
    log.debug("Processed KML file.")
    return stations_rows, devices_rows, points_rows


def _list_to_polars(
    station_rows: List[Dict[str, Any]] | None = None,
    charging_device_rows: List[Dict[str, Any]] | None = None,
    charging_point_rows: List[Dict[str, Any]] | None = None,
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    # Build Polars DataFrames with explicit schemas when empty
    stations_df = (
        pl.DataFrame(station_rows)
        if station_rows
        else pl.DataFrame(
            [],
            schema=[
                ("timestamp", pl.Datetime(time_zone="UTC")),
                ("name", pl.Utf8),
                ("visibility", pl.Boolean),
                ("address", pl.Utf8),
                ("description", pl.Utf8),
                ("status", pl.Utf8),
                ("cpnum", pl.Int64),
                ("coordinates", pl.Utf8),
            ],
        )
    )

    devices_df = (
        pl.DataFrame(charging_device_rows)
        if charging_device_rows
        else pl.DataFrame(
            [],
            schema=[
                ("timestamp", pl.Datetime(time_zone="UTC")),
                ("coordinates", pl.Utf8),
                ("station_name", pl.Utf8),
                ("device_id", pl.Utf8),
                ("name", pl.Utf8),
                ("n_connectors", pl.Int64),
            ],
        )
    )

    points_df = (
        pl.DataFrame(charging_point_rows)
        if charging_point_rows
        else pl.DataFrame(
            [],
            schema=[
                ("timestamp", pl.Datetime(time_zone="UTC")),
                ("coordinates", pl.Utf8),
                ("station_name", pl.Utf8),
                ("device_id", pl.Utf8),
                ("id", pl.Utf8),
                ("name", pl.Utf8),
                ("maxchspeed", pl.Float64),
                ("connector", pl.Int64),
                ("description", pl.Utf8),
            ],
        )
    )

    return stations_df, devices_df, points_df


def _parse_kml_to_polars(
    kml_path: Path,
    stations: List[Dict[str, Any]] | None = None,
    charging_devices: List[Dict[str, Any]] | None = None,
    charging_points: List[Dict[str, Any]] | None = None,
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    station_rows: List[Dict[str, Any]]
    charging_device_rows: List[Dict[str, Any]]
    charging_point_rows: List[Dict[str, Any]]

    station_rows, charging_device_rows, charging_point_rows = _parse_kml_to_list(
        kml_path, stations, charging_devices, charging_points
    )

    return _list_to_polars(station_rows, charging_device_rows, charging_point_rows)


def _parse_folder_to_polar(
    folder_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    station_list: List[Dict[str, Any]] = []
    charging_device_list: List[Dict[str, Any]] = []
    charging_point_list: List[Dict[str, Any]] = []

    for file in folder_path.rglob("*.kml"):
        try:
            station_list, charging_device_list, charging_point_list = (
                _parse_kml_to_list(
                    file, station_list, charging_device_list, charging_point_list
                )
            )
        except ET.ParseError as e:
            log.error(f"Error parsing {file.name}: {e}")

    return _list_to_polars(station_list, charging_device_list, charging_point_list)


def parse_path_to_polar(
    path_str: str,
    multi_partition: bool = False,
    partition_column_name: str | None = None,
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    """Will parse the path given to produce a tuple of 3 polar Dataframes.
    If passed a folder, it will parse all kml files in it.
    If passed a file, it will just parse the file.

    When a folder, when encountering a file not parseable, it will just log the error.
    When a file, the error thrown by parse_folder is not caught and will be thrown up the stack.
    """
    path = Path(path_str)
    if multi_partition:
        list_1: list[pl.LazyFrame] = []
        list_2: list[pl.LazyFrame] = []
        list_3: list[pl.LazyFrame] = []
        if not path.is_dir():
            raise ValueError(
                f"This run is a multi partition run, but {path_str} is not a directory"
            )
        for f in path.iterdir():
            tmp_df_1, tmp_df_2, tmp_df_3 = parse_path_to_polar(
                str(f),
                multi_partition=False,
                partition_column_name=partition_column_name,
            )
            list_1.append(tmp_df_1)
            list_2.append(tmp_df_2)
            list_3.append(tmp_df_3)
        res_1 = pl.concat(list_1, how="vertical")
        res_2 = pl.concat(list_2, how="vertical")
        res_3 = pl.concat(list_3, how="vertical")
        return res_1, res_2, res_3

    if path.is_dir():
        log.debug(f'{path_str} is a directory, will look for ".kml" in it.')
        tmp_df_1, tmp_df_2, tmp_df_3 = _parse_folder_to_polar(path)
    elif path.is_file():
        log.debug(f"{path_str} is a file, will parse it.")
        tmp_df_1, tmp_df_2, tmp_df_3 = _parse_kml_to_polars(path)
    else:
        log.warning(
            f"{path_str} is neither a directory nor a file, will produce empty dataframes"
        )
        tmp_df_1, tmp_df_2, tmp_df_3 = _list_to_polars([], [], [])

    res_lf_1 = tmp_df_1.lazy()
    res_lf_2 = tmp_df_2.lazy()
    res_lf_3 = tmp_df_3.lazy()
    if partition_column_name:
        if partition_column_name not in tmp_df_1.columns:
            res_lf_1 = res_lf_1.with_columns(
                pl.lit(path.name).alias(partition_column_name)
            )
            res_lf_2 = res_lf_2.with_columns(
                pl.lit(path.name).alias(partition_column_name)
            )
            res_lf_3 = res_lf_3.with_columns(
                pl.lit(path.name).alias(partition_column_name)
            )
    return res_lf_1, res_lf_2, res_lf_3


# Helper defined below to keep the main parse function readable.
def _handle_device(
    device_json: Dict[str, Any],
    coords: str,
    station_name: str,
    devices_rows: List[Dict[str, Any]],
    points_rows: List[Dict[str, Any]],
    ts: dt.datetime,
) -> None:
    """
    Extract device-level fields and append to devices_rows, and expand connectors into points_rows.
    Accepts multiple possible key names for fields (robust extraction).
    """
    # device id/name heuristics
    device_id = str(
        device_json.get("id")
        or device_json.get("deviceId")
        or device_json.get("uid")
        or device_json.get("identifier")
        or ""
    )
    device_name = (
        device_json.get("name")
        or device_json.get("label")
        or device_json.get("title")
        or ""
    )

    # connectors may be in various keys or formats
    connectors = (
        device_json.get("connectors")
        or device_json.get("connectorsList")
        or device_json.get("ports")
        or device_json.get("plugs")
        or None
    )
    if connectors is None:
        # sometimes the device JSON contains a dict of connector objects keyed by something
        # look for any value that's a list under common keys
        for k in ("connectorsArray", "connections", "sockets"):
            if k in device_json and isinstance(device_json[k], list):
                connectors = device_json[k]
                break

    if isinstance(connectors, dict):
        # normalize dict-of-objects to list
        connectors = list(connectors.values())

    if isinstance(connectors, list):
        n_connectors = len(connectors)
    else:
        # fallback heuristics
        n_connectors = _safe_int(
            device_json.get("n_connectors")
            or device_json.get("numConnectors")
            or device_json.get("connectorCount"),
            0,
        )

    devices_rows.append(
        {
            "timestamp": ts,
            "coordinates": coords,
            "station_name": station_name,
            "device_id": device_id,
            "name": device_name,
            "n_connectors": n_connectors,
        }
    )

    # Expand connectors into charging points
    if isinstance(connectors, list):
        for conn in connectors:
            if not isinstance(conn, dict):
                # If entry is primitive, store minimal row
                points_rows.append(
                    {
                        "timestamp": ts,
                        "coordinates": coords,
                        "station_name": station_name,
                        "device_id": device_id,
                        "id": str(conn),
                        "name": "",
                        "maxchspeed": 0.0,
                        "connector": 0,
                        "description": "",
                    }
                )
                continue

            p_id = str(
                conn.get("id")
                or conn.get("connectorId")
                or conn.get("uid")
                or conn.get("identifier")
                or ""
            )
            p_name = conn.get("name") or conn.get("label") or ""
            # common keys for max charge speed/power
            max_keys = [
                "maxchspeed",
                "maxChSpeed",
                "maxChargingSpeed",
                "max_power",
                "power",
                "ratedPower",
            ]
            p_max = 0.0
            for k in max_keys:
                if k in conn and conn[k] is not None:
                    p_max = _safe_float(conn[k], 0.0)
                    break

            connector_idx = _safe_int(
                conn.get("connector")
                or conn.get("index")
                or conn.get("slot")
                or conn.get("type")
                or conn.get("port"),
                0,
            )
            p_desc = (
                conn.get("description") or conn.get("info") or conn.get("details") or ""
            )

            points_rows.append(
                {
                    "timestamp": ts,
                    "coordinates": coords,
                    "station_name": station_name,
                    "device_id": device_id,
                    "id": p_id,
                    "name": p_name,
                    "maxchspeed": p_max,
                    "connector": connector_idx,
                    "description": p_desc,
                }
            )
