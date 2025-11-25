# __init__.py — Azure Function (TimerTrigger, every 10 min or hourly)
# Pipeline:
#   1) Ingest raw -> append per-sensor CSVs (data_per_sensor_clean/*) incrementally
#   2) Build a WIDE frame over a small overlap window
#   3) Run FULL pipeline (step1_df_full, clean_full, datasets, common_t0, online rollup)

# air2 = scripts.air_DataProcessingML
# air = scripts.air_RealTimeDataProcessing
# anomaly_pipeline = scripts.anomaly_pipeline

import os
import re
import json
import ast
from io import StringIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContainerClient

# ---------- Logging ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
for n in ["azure", "azure.storage", "azure.core",
          "azure.core.pipeline.policies.http_logging_policy"]:
    logging.getLogger(n).setLevel(logging.WARNING)
    logging.getLogger(n).propagate = False
os.environ["AZURE_LOG_LEVEL"] = "WARNING"
log = logging.getLogger("func")

def _is_on(name: str) -> bool:
    return (os.getenv(name, "0") or "0").lower() in ("1","true","yes","y")

# ---------- I/O mode ----------
def _out_mode() -> str:
    # Online default = azure
    return (os.getenv("OUTPUT_MODE", "azure") or "azure").lower()

def _local_root() -> Path:
    return Path(os.getenv("LOCAL_OUTPUT_ROOT", "./_local_io")).resolve()

def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _bsc() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])

def _cc(bsc: BlobServiceClient) -> ContainerClient:
    return bsc.get_container_client(os.environ["AZURE_STORAGE_CONTAINER_NAME"])

def write_csv_anywhere(path_like: str, df: pd.DataFrame):
    if _out_mode() == "local":
        p = _local_root() / path_like
        _ensure_parent(p)
        df.to_csv(p, index=False)
        return
    _cc(_bsc()).upload_blob(path_like, df.to_csv(index=False), overwrite=True)

def read_csv_anywhere(path_like: str) -> pd.DataFrame:
    if _out_mode() == "local":
        p = _local_root() / path_like
        return pd.read_csv(p) if p.exists() else pd.DataFrame()
    return read_df_from_blob(_cc(_bsc()), path_like, kind="csv")

def write_text_anywhere(path_like: str, text: str):
    if _out_mode() == "local":
        p = _local_root() / path_like
        _ensure_parent(p)
        p.write_text(text, encoding="utf-8")
        return
    _cc(_bsc()).upload_blob(path_like, text.encode("utf-8"), overwrite=True)

def read_text_anywhere(path_like: str) -> str | None:
    if _out_mode() == "local":
        p = _local_root() / path_like
        return p.read_text(encoding="utf-8") if p.exists() else None
    try:
        return read_blob_text(_cc(_bsc()), path_like)
    except Exception:
        return None

def list_csv_anywhere(prefix: str) -> list[str]:
    if _out_mode() == "local":
        base = _local_root() / (prefix if prefix.endswith("/") else prefix + "/")
        if not base.exists():
            return []
        return [str(p.relative_to(_local_root()).as_posix()) for p in base.rglob("*.csv")]
    paths = []
    cont = _cc(_bsc())
    for b in cont.list_blobs(name_starts_with=prefix if prefix.endswith("/") else prefix + "/"):
        if b.name.endswith(".csv"):
            paths.append(b.name)
    return paths

# ---------- Blob helpers ----------
def read_blob_text(container: ContainerClient, name: str) -> str:
    return container.get_blob_client(name).download_blob().readall().decode("utf-8")

def read_df_from_blob(container: ContainerClient, path: str, kind: str) -> pd.DataFrame:
    try:
        raw = read_blob_text(container, path)
        if kind == "csv":
            return pd.read_csv(StringIO(raw))
        if kind == "json":
            return pd.read_json(StringIO(raw), lines=True)
        return pd.DataFrame()
    except Exception as e:
        log.warning(f"[blob-read] {path}: {e}")
        return pd.DataFrame()

def any_json_under(container: ContainerClient, prefix: str) -> bool:
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    for b in container.list_blobs(name_starts_with=prefix):
        if b.name.endswith(".json"):
            return True
    return False

def list_json_under(container: ContainerClient, prefix: str):
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    for b in container.list_blobs(name_starts_with=prefix):
        if b.name.endswith(".json"):
            yield b.name

# ---------- Your libs ----------
from shared import constants  # noqa: F401
from shared import br_uncompress  # noqa: F401
from scripts.air_DataProcessingML import clean_air_df_v2
from scripts.anomaly_pipeline import run_pipeline_full  # FULL pipeline

# ---------- Sidecars for last_ts per device ----------
_SIDE_PREFIX = "data_per_sensor_clean/state"
def _last_ts_sidecar_path(device: str) -> str:
    return f"{_SIDE_PREFIX}/{device}_last_ts.txt"

def _update_last_ts_sidecar(container: ContainerClient, device: str, ts: pd.Timestamp):
    write_text_anywhere(_last_ts_sidecar_path(device), pd.to_datetime(ts, utc=True).isoformat() + "\n")

def _fast_last_ts(container: ContainerClient, csv_path: str, device: str) -> pd.Timestamp:
    # 1) sidecar
    try:
        txt = read_text_anywhere(_last_ts_sidecar_path(device))
        if txt:
            ts = pd.to_datetime(txt.strip(), errors="coerce", utc=True)
            if pd.notna(ts):
                return ts
    except Exception:
        pass
    # 2) tail-read last timestamp from blob
    try:
        bc = container.get_blob_client(csv_path)
        props = bc.get_blob_properties()
        size = getattr(props, "size", 0) or 0
        if size > 0:
            chunk = min(size, 128 * 1024)
            tail = bc.download_blob(offset=size - chunk, length=chunk).readall().decode("utf-8", errors="ignore")
            for ln in reversed([ln for ln in tail.splitlines() if ln.strip()]):
                ts = pd.to_datetime(ln.split(",", 1)[0], errors="coerce", utc=True)
                if pd.notna(ts):
                    _update_last_ts_sidecar(container, device, ts)
                    return ts
    except Exception:
        pass
    # 3) slow fallback
    try:
        df = read_csv_anywhere(csv_path)
        if "timestamp" not in df.columns:
            return pd.Timestamp("1970-01-01", tz="UTC")
        ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).max()
        ts = ts if pd.notna(ts) else pd.Timestamp("1970-01-01", tz="UTC")
        _update_last_ts_sidecar(container, device, ts)
        return ts
    except Exception:
        return pd.Timestamp("1970-01-01", tz="UTC")

def _get_last_ts_from_csv(container: ContainerClient, csv_path: str, device: str) -> pd.Timestamp:
    return _fast_last_ts(container, csv_path, device)

# ---------- Ingestion helpers ----------
def get_data_path(day: str, hour: int) -> str:
    return f"rawsensordata/{day}/{hour:02d}"

def read_all_json_under(container: ContainerClient, prefix: str) -> pd.DataFrame:
    dfs = []
    for name in list_json_under(container, prefix):
        try:
            raw = read_blob_text(container, name)
            dfs.append(pd.read_json(StringIO(raw), lines=True))
        except Exception as e:
            log.warning(f"[json-read] {name}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def _construct_device_csv(device: str, day_str: str) -> str:
    return f"data_per_sensor_clean/{day_str[:4]}/{device}.csv"

def _clean_occupancy(df: pd.DataFrame) -> pd.DataFrame:
    tcol = None
    for c in ("EventEnqueuedUtcTime", "ReceivedTimeStamp", "HandledTimeStamp"):
        if c in df.columns:
            tcol = c; break
    out = df.copy()
    if tcol:
        out[tcol] = pd.to_datetime(out[tcol], errors="coerce", utc=True)
        out["timestamp"] = out[tcol].dt.floor("10min")
    else:
        out["timestamp"] = pd.NaT
    keep = [c for c in ["timestamp","values","raw","device","metadata"] if c in out.columns]
    return out[keep].dropna(subset=["timestamp"])

def _ensure_dict(x):
    if isinstance(x, dict): return x
    if x is None or (isinstance(x, float) and pd.isna(x)): return {}
    if isinstance(x, str):
        for conv in (json.loads, ast.literal_eval):
            try:
                d = conv(x); return d if isinstance(d, dict) else {}
            except Exception: pass
    return {}

def _expand_values_metadata(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "values" in out.columns:
        vdf = pd.json_normalize(out["values"].map(_ensure_dict), max_level=1).add_prefix("values_")
        out = pd.concat([out.drop(columns=["values"]), vdf], axis=1)
    if "metadata" in out.columns:
        mdf = pd.json_normalize(out["metadata"].map(_ensure_dict), max_level=1).add_prefix("metadata_")
        out = pd.concat([out.drop(columns=["metadata"]), mdf], axis=1)
    return out

def _coerce_values_dict(x):
    d = _ensure_dict(x)
    return {str(k).lower(): v for k, v in d.items()}

def _merge_bucket_rows(df_bucket: pd.DataFrame) -> dict:
    merged = {}
    for _, r in df_bucket.iterrows():
        for k, val in _coerce_values_dict(r.get("values")).items():
            if val is not None:
                merged[k] = val
    raw_any = df_bucket.get("raw").dropna().iloc[-1] if "raw" in df_bucket and df_bucket["raw"].notna().any() else None
    meta_any = df_bucket.get("metadata").dropna().iloc[-1] if "metadata" in df_bucket and df_bucket["metadata"].notna().any() else None
    return {"values": merged, "raw": raw_any, "metadata": meta_any}

def _clean_sensor_batch(device: str, df: pd.DataFrame, is_air: bool, current_hour_iso: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    cols = [c for c in ["device","values","raw","timestamp","metadata"] if c in df.columns]
    df = df[cols]
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    if is_air:
        df2 = clean_air_df_v2(df.copy(), current_hour_iso)
        if df2.empty:
            return df2
        df2["timestamp"] = pd.to_datetime(df2["timestamp"], errors="coerce", utc=True)
        return _expand_values_metadata(df2)

    # merge rows within each (device,timestamp)
    rows = []
    for (_, ts), grp in df.groupby(["device","timestamp"], dropna=False):
        rows.append({"device": device, "timestamp": ts, **_merge_bucket_rows(grp)})
    return _expand_values_metadata(pd.DataFrame(rows))

def _align_new_to_existing_schema(existing: pd.DataFrame, out: pd.DataFrame) -> pd.DataFrame:
    if out is None or out.empty:
        return out
    def to_values(c): return c if c in ("timestamp",) or c.startswith("values_") else f"values_{c}"
    ren = {c: to_values(c) for c in out.columns if c != "timestamp"}
    out = out.rename(columns=ren)
    return out

def _append_per_sensor_df(container: ContainerClient, csv_path: str, new_df: pd.DataFrame):
    if new_df is None or new_df.empty:
        return
    out = new_df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    out = (out.dropna(subset=["timestamp"])
               .drop_duplicates(subset=["timestamp"], keep="last")
               .sort_values("timestamp")
               .reset_index(drop=True))
    drop_cols = ["raw","deveui","battery","vdd","light_status","pir_status","region_count"]
    out = out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")

    try:
        existing = read_csv_anywhere(csv_path)
    except Exception:
        existing = pd.DataFrame()

    out = _align_new_to_existing_schema(existing, out)

    if existing is None or existing.empty:
        write_csv_anywhere(csv_path, out)
        return

    existing["timestamp"] = pd.to_datetime(existing["timestamp"], errors="coerce", utc=True)
    existing = (existing.dropna(subset=["timestamp"])
                        .drop_duplicates(subset=["timestamp"], keep="last")
                        .set_index("timestamp"))
    up = out.set_index("timestamp")

    # align columns
    for c in up.columns:
        if c not in existing.columns:
            existing[c] = pd.NA
    for c in existing.columns:
        if c not in up.columns:
            up[c] = pd.NA

    # prefer existing non-null; update with non-null new
    existing.update(up)

    # append brand-new timestamps
    new_idx = up.index.difference(existing.index)
    if len(new_idx) > 0:
        existing = pd.concat([existing, up.loc[new_idx]], axis=0)

    final = (existing.sort_index()
                    .reset_index()
                    .drop_duplicates(subset=["timestamp"], keep="last")
                    .sort_values("timestamp")
                    .reset_index(drop=True))
    write_csv_anywhere(csv_path, final)

# ---------- Wide builder ----------
def _rename_keep_columns(df: pd.DataFrame, device: str) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    CANON = {
        "occupancy":"occupancy","temperature":"temperature","humidity":"humidity",
        "people_count_all":"people_count_all","people_count_max":"people_count_max",
        "lmax":"lmax","leq":"leq","spl":"spl","presentvalue":"presentvalue",
        "co2":"CO2","cov":"COV","h":"H","t":"T",
    }
    dev_prefix_lc = (device + "_").lower()
    EXCL = {"vdd","battery","light_status","pir_status","region_count"}

    def strip_values(c): return c[7:] if c.startswith("values_") else c

    rename = {}
    keep = ["timestamp"]
    for col in df.columns:
        if col == "timestamp": continue
        base = strip_values(col)
        base_lc = base.lower()
        if base_lc in EXCL: continue
        if base_lc.startswith(dev_prefix_lc):
            rename[col] = base; keep.append(base); continue
        tok = CANON.get(base_lc)
        if tok is not None:
            out_name = f"{device}_{tok}"
            rename[col] = out_name; keep.append(out_name); continue
    if rename:
        df = df.rename(columns=rename)
    keep = [c for c in keep if c in df.columns]
    return df[keep]

def _merge_once(wide: pd.DataFrame | None, dev_df: pd.DataFrame) -> pd.DataFrame:
    if wide is None:
        return dev_df
    overlap = (set(wide.columns) & set(dev_df.columns)) - {"timestamp"}
    for c in overlap:
        wide[c] = wide[c].combine_first(dev_df[c])
    dev_df = dev_df.drop(columns=list(overlap), errors="ignore")
    return pd.merge(wide, dev_df, on="timestamp", how="outer", copy=False)

def build_wide_batch_range(container: ContainerClient, t_start: pd.Timestamp, t_end: pd.Timestamp) -> pd.DataFrame:
    years = range(t_start.year, t_end.year + 1)
    by_device = {}
    for y in years:
        for path in list_csv_anywhere(f"data_per_sensor_clean/{y}/"):
            device = os.path.splitext(os.path.basename(path))[0]
            try:
                df = read_csv_anywhere(path)
                if df.empty or "timestamp" not in df.columns:
                    continue
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
                df = df[(df["timestamp"] >= t_start) & (df["timestamp"] < t_end)]
                if df.empty: continue
                df = _rename_keep_columns(df, device)
                if df.empty: continue
                by_device.setdefault(device, []).append(df)
            except Exception as e:
                log.warning(f"[wide] {path}: {e}")

    if not by_device:
        return pd.DataFrame(columns=["timestamp"])

    wide = None
    for device, parts in by_device.items():
        df_dev = pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0].copy()
        df_dev = (df_dev.sort_values("timestamp")
                        .drop_duplicates(subset=["timestamp"], keep="last")
                        .reset_index(drop=True))
        log.info("[wide/device] %s parts=%d rows=%d cols=%d",
                 device, len(parts), len(df_dev), max(df_dev.shape[1]-1, 0))
        wide = _merge_once(wide, df_dev)

    drop_rx = [
        r"^Desk_\d{2}-\d{2}_vdd$",
        r"^LT_05-01_battery$",
        r"^Light_05-(01|02)_(light_status|pir_status|battery)$",
        r"^Room_05-01_region_count$",
        r"^Son_05-(01|02)_battery$",
        r"^TempEx_05-01_battery$",
    ]
    to_drop = [c for c in wide.columns for p in drop_rx if re.match(p, c)]
    wide = wide.drop(columns=to_drop, errors="ignore")
    out = (wide.sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"])
                .reset_index(drop=True))
    yc = out["timestamp"].dt.year.value_counts().sort_index().to_dict() if not out.empty else {}
    log.info(f"[wide] built {out.shape} for {t_start}..{t_end} rows_by_year={yc}")
    return out

# ---------- Incremental raw→per-sensor ----------
def process_raw_data_incremental(min_utc: datetime, max_utc: datetime):
    cc = _cc(_bsc())
    t = min_utc
    while t < max_utc:
        prev_hour = (t - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        day = prev_hour.strftime("%Y-%m-%d")
        h   = prev_hour.hour
        path = get_data_path(day, h)
        log.info(f"[ingest] scanning {path}")
        if not any_json_under(cc, path):
            log.info(f"[ingest] no json under {path}; skip")
            t += timedelta(hours=1); continue
        raw_df = read_all_json_under(cc, path)
        if raw_df.empty:
            t += timedelta(hours=1); continue

        df = _clean_occupancy(raw_df).dropna(subset=["timestamp"])
        if df.empty:
            t += timedelta(hours=1); continue

        current_hour_iso = (prev_hour + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        wrote_rows = 0; wrote_devices = set()

        for device, g in df.groupby("device"):
            if device is None: continue
            device = str(device)
            is_air = device.startswith("Air")
            csv_path = _construct_device_csv(device, day)
            last_ts = _get_last_ts_from_csv(cc, csv_path, device)

            g = (g.assign(timestamp=pd.to_datetime(g["timestamp"], errors="coerce", utc=True))
                   .dropna(subset=["timestamp"])
                   .sort_values("timestamp"))
            g = g[g["timestamp"] >= last_ts] if is_air else g[g["timestamp"] > last_ts]
            if g.empty:
                continue

            cleaned = _clean_sensor_batch(device, g, is_air=is_air, current_hour_iso=current_hour_iso)
            if cleaned.empty or "timestamp" not in cleaned.columns:
                continue

            cleaned["timestamp"] = pd.to_datetime(cleaned["timestamp"], errors="coerce", utc=True)
            cleaned = cleaned.dropna(subset=["timestamp"])
            if cleaned.empty:
                continue

            _append_per_sensor_df(cc, csv_path, cleaned)
            _update_last_ts_sidecar(cc, device, cleaned["timestamp"].max())
            wrote_rows += len(cleaned); wrote_devices.add(device)

        t += timedelta(hours=1)
        log.info(f"[ingest] {path} -> appended {wrote_rows} rows across {len(wrote_devices)} devices")

# ---------- Timer entrypoint ----------
def _parse_env_ts(varname: str):
    s = os.getenv(varname)
    if not s: return None
    s = s.strip().strip('"\'').strip()
    if s.lower() in ("", "none", "null", "false", "off", "0"): return None
    try:
        ts = pd.Timestamp(s)
    except Exception:
        return None
    return ts.tz_convert("UTC") if getattr(ts, "tz", None) else pd.Timestamp(ts, tz="UTC")

def main(mytimer: func.TimerRequest) -> None:
    try:
        log.info("=== Timer tick ===")
        bsc = _bsc(); cc = _cc(bsc)

        if _is_on("PRIME_SIDECARS_ON_START"):
            # optional one-time priming; cheap to keep enabled
            from_time = datetime.now(timezone.utc)  # no-op without implementation hook
            log.info("[bootstrap] sidecars priming flag is ON")

        # Choose window
        now = datetime.now(timezone.utc)
        bf_from = _parse_env_ts("BACKFILL_FROM_UTC")
        bf_to   = _parse_env_ts("BACKFILL_TO_UTC")
        default_max = pd.Timestamp(now).ceil("h")

        if bf_from:
            t_min = bf_from
            t_max = min(bf_to or default_max, default_max)
            if t_max <= t_min:
                t_max = min(t_min + pd.Timedelta(hours=1), default_max)
        else:
            prev = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            t_min = pd.Timestamp(prev)
            t_max = t_min + pd.Timedelta(hours=1)
        log.info(f"[window] {t_min} → {t_max}")

        # (1) Ingestion
        ingest_lb = _parse_env_ts("INGEST_LOWER_BOUND_UTC")
        ingest_min = max(t_min, ingest_lb) if ingest_lb else t_min
        process_raw_data_incremental(ingest_min.to_pydatetime(), t_max.to_pydatetime())

        # (2) Wide with small overlap
        overlap = pd.Timedelta(hours=2)
        w_start = t_min - overlap
        w_end   = t_max
        df_full = build_wide_batch_range(cc, w_start, w_end)
        if df_full.empty:
            log.info("[wide] no rows; exit")
            return

        # (3) FULL pipeline (writes step1/clean/datasets/common_t0/online)
        res = run_pipeline_full(df_full, log)
        log.info(f"[full] processed={res.get('processed',0)} last_ts={res.get('last_ts')}")
    except Exception:
        log.exception("Timer run failed")
        raise

if __name__ == "__main__":
    # Local debug runner (optional)
    import types, json
    settings_path = Path(__file__).resolve().parents[1] / "local.settings.json"
    if settings_path.exists():
        with open(settings_path) as f:
            for k, v in json.load(f).get("Values", {}).items():
                os.environ[k] = str(v)
    dummy = types.SimpleNamespace(past_due=False)
    print("Running manually…"); main(dummy); print("✔ Done")