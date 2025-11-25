# anomaly_pipeline.py — FULL cross-year incremental pipeline
# Writes under:
#   data_for_ml/full/step1_df_full.csv
#   data_for_ml/full/clean_full.csv
#   data_for_ml/full/datasets/{minutes|hours|days|weeks}/clean_full_*.csv
#   data_for_ml/full/list_of_common_t0.csv
# State:
#   data_for_ml/state/full_last_ts.json
#
# Design:
#  - One pass per trigger: process only new timestamps (strictly > state)
#  - Avoid re-reading blobs: we re-use the in-memory "clean old" for building datasets
#  - De-dup on append: ['t0','t1_actual'] for datasets, and 'timestamp' for step1/clean
#  - Safe when files are missing: create headers once
#  - Minimal logging (use caller's logger)

import os
import io
import json
import logging
from typing import Optional, List, Dict
import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient
from joblib import load as joblib_load
import logging


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
# Silence Azure SDK HTTP chatter
for n in [
    "azure", 
    "azure.storage", 
    "azure.core",
    "azure.core.pipeline.policies.http_logging_policy",
]:
    logging.getLogger(n).setLevel(logging.WARNING)
    logging.getLogger(n).propagate = False

log = logging.getLogger("func")

def _flag_on(name: str, default: str = "0") -> bool:
    """
    Helper to read boolean-ish env vars.
    Returns True if the env var is in {1, true, yes, y} (case-insensitive).
    """
    val = os.getenv(name, default)
    return (val or default).strip().lower() in ("1", "true", "yes", "y")


# ---------- Config ----------
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "blob-events-container-paris-chateaudun")
ML_FULL_PREFIX = os.getenv("FULL_PREFIX", "data_for_ml/full").rstrip("/")
FULL_STATE_BLOB = os.getenv("FULL_STATE_BLOB", "data_for_ml/state/full_last_ts.json")
FULL_COMMON_T0_BLOB = os.getenv("FULL_COMMON_T0_BLOB", f"{ML_FULL_PREFIX}/list_of_common_t0.csv")
MODEL_BLOB = os.getenv("MODEL_BLOB", "models/impute_models.joblib")
DONOR_MAP_BLOB = os.getenv("DONOR_MAP_BLOB", "models/donor_map.json")
TZ_LOCAL = os.getenv("LOCAL_TZ", "Europe/Paris")

DATASETS_DEBUG = str(os.getenv("FULL_DATASETS_DEBUG", "0")).lower() in ("1","true","yes","y")


# Valid ranges
RANGES = {
    "desk_occupancy": (0, 2),
    "desk_temperature": (-10, 50),
    "desk_humidity": (0, 100),
    "air_co2": (300, 5000),
    "air_cov": (0, 5000),
    "air_h": (0, 100),
    "air_t": (-10, 50),
    "room_people": (0, np.inf),
    "noise": (0, 130),
    "tempex": (-30, 100),
}

# Dataset windows / tolerances
TS_COL = "timestamp"
COUNTER_COL = "LT_05-01_presentvalue"
MINUTE_HORIZONS = [10, 20, 30, 40, 50, 60]
HOUR_HORIZONS   = list(range(1, 25))
DAY_HORIZONS    = list(range(1, 8))
WEEK_HORIZONS   = list(range(1, 13))
TOL_MIN_MINUTES = 0.0
TOL_MIN_HOURS   = 15.0
TOL_MIN_DAYS    = 60.0
TOL_MIN_WEEKS   = 360.0

FORBIDDEN_SNAPSHOT = {
    "LT_presentvalue", "LT_05-01_presentvalue", "cons_rate", "cons",
    "duration_h", "t1_requested", "t1_actual", "match_type", "seg_id"
}
DESK_KEEP_SNAPSHOT = {
    "Desk_all_occupancy_sum",
    "Desk_all_temperature_mean",
    "Desk_all_temperature_std",
    "Desk_all_humidity_mean",
    "Desk_all_humidity_std",
}
PARAMS = [
    "Air_05-01_CO2","Air_05-01_COV","Air_05-01_H","Air_05-01_T",
    "Room_05-01_people_count_all","Room_05-01_people_count_max",
    "Son_05-01_lmax","Son_05-01_leq","Son_05-01_spl",
    "Son_05-02_lmax","Son_05-02_leq","Son_05-02_spl",
    "TempEx_05-01_temperature","TempEx_05-01_humidity",
    "Desk_all_occupancy_sum","Desk_all_temperature_mean","Desk_all_temperature_std",
    "Desk_all_humidity_mean","Desk_all_humidity_std",
]
TF_COLS = [
    "tf_hour","tf_minute","tf_weekday","tf_month","tf_dayofyear","tf_weekofyear",
    "tf_is_weekend","tf_is_workhour",
    "tf_hour_sin","tf_hour_cos","tf_dow_sin","tf_dow_cos","tf_mon_sin","tf_mon_cos",
]

import os, io, json
from pathlib import Path
import pandas as pd

# Use the same connection utils if ever needed
try:
    from azure.storage.blob import BlobServiceClient
except Exception:
    BlobServiceClient = None  # local mode won't need it

# ---------------- mode & paths ----------------
def _mode() -> str:
    return (os.getenv("OUTPUT_MODE", "azure") or "azure").lower()

def _root() -> Path:
    return Path(os.getenv("LOCAL_OUTPUT_ROOT", "./_local_io")).resolve()

def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

# ---------------- low-level blob helpers ----------------
def _cc(bsc):
    # in azure mode we expect a valid BlobServiceClient
    return bsc.get_container_client(os.getenv("AZURE_STORAGE_CONTAINER_NAME"))

# ---------------- text & bytes ----------------
def blob_exists_anywhere(bsc, name: str) -> bool:
    if _mode() == "local":
        return (_root() / name).exists()
    try:
        _cc(bsc).get_blob_client(name).get_blob_properties()
        return True
    except Exception:
        return False

def download_blob_text_anywhere(bsc, name: str) -> str:
    if _mode() == "local":
        p = _root() / name
        return p.read_text(encoding="utf-8") if p.exists() else ""
    return _cc(bsc).get_blob_client(name).download_blob().readall().decode("utf-8")

def download_blob_bytes_anywhere(bsc, name: str) -> bytes:
    if _mode() == "local":
        p = _root() / name
        return p.read_bytes() if p.exists() else b""
    return _cc(bsc).get_blob_client(name).download_blob().readall()

def upload_blob_bytes_anywhere(bsc, name: str, data: bytes, overwrite=True):
    if _mode() == "local":
        p = _root() / name
        _ensure_parent(p)
        p.write_bytes(data)
        return
    _cc(bsc).get_blob_client(name).upload_blob(data, overwrite=overwrite)

# ---------------- CSV helpers ----------------
def read_csv_anywhere(bsc, name: str, parse_ts=True) -> pd.DataFrame:
    try:
        raw = download_blob_bytes_anywhere(bsc, name)
        if not raw:
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(raw.decode("utf-8")))
        if parse_ts and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            df = (df.dropna(subset=["timestamp"])
                    .sort_values("timestamp")
                    .reset_index(drop=True))
        return df
    except Exception:
        return pd.DataFrame()

def ensure_csv_with_header_anywhere(bsc, name: str, header_like: pd.DataFrame):
    if blob_exists_anywhere(bsc, name):
        return
    buf = io.StringIO()
    pd.DataFrame(columns=list(header_like.columns)).to_csv(buf, index=False)
    upload_blob_bytes_anywhere(bsc, name, buf.getvalue().encode("utf-8"))

def append_csv_dedup_anywhere(bsc, name: str, df: pd.DataFrame, subset_keys: list[str], dt_keys=frozenset({"timestamp","t0","t1_requested","t1_actual"})):
    base = read_csv_anywhere(bsc, name, parse_ts=False)
    out = pd.concat([base, df], ignore_index=True) if not base.empty else df.copy()

    # Normalize datetime keys to tz-aware UTC
    for k in subset_keys:
        if k in out.columns and k in dt_keys:
            out[k] = pd.to_datetime(out[k], errors="coerce", utc=True)
    out = out.dropna(subset=[k for k in subset_keys if k in dt_keys])

    if subset_keys:
        out = out.sort_values(subset_keys).drop_duplicates(subset=subset_keys, keep="first")

    upload_blob_bytes_anywhere(bsc, name, out.to_csv(index=False).encode("utf-8"))

# ---------------- listing ----------------
def list_csv_anywhere(bsc, prefix: str) -> list[str]:
    if _mode() == "local":
        base = _root() / (prefix if prefix.endswith("/") else (prefix + "/"))
        if not base.exists():
            return []
        return [str(p.relative_to(_root()).as_posix()) for p in base.rglob("*.csv")]
    paths = []
    for b in _cc(bsc).list_blobs(name_starts_with=prefix if prefix.endswith("/") else prefix + "/"):
        if b.name.endswith(".csv"):
            paths.append(b.name)
    return sorted(paths)


# ---------- Azure helpers ----------
def get_bsc() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])

def cc(bsc: BlobServiceClient):
    return bsc.get_container_client(CONTAINER)

def blob_exists(bsc: BlobServiceClient, name: str) -> bool:
    return blob_exists_anywhere(bsc, name)

def download_blob_text(bsc: BlobServiceClient, name: str) -> str:
    return download_blob_text_anywhere(bsc, name)

def download_blob_bytes(bsc: BlobServiceClient, name: str) -> bytes:
    return download_blob_bytes_anywhere(bsc, name)

def upload_blob_bytes(bsc: BlobServiceClient, name: str, data: bytes, overwrite=True):
    return upload_blob_bytes_anywhere(bsc, name, data, overwrite=overwrite)


def read_csv_blob(bsc: BlobServiceClient, name: str, parse_ts=True) -> pd.DataFrame:
    return read_csv_anywhere(bsc, name, parse_ts=parse_ts)


def ensure_csv_with_header(bsc: BlobServiceClient, name: str, header_like: pd.DataFrame):
    return ensure_csv_with_header_anywhere(bsc, name, header_like)

# anomaly_pipeline.py
DT_KEYS = {"timestamp", "t0", "t1_requested", "t1_actual"}

def append_csv_dedup(bsc: BlobServiceClient, name: str, df: pd.DataFrame, subset_keys: list[str]):
    append_csv_dedup_anywhere(bsc, name, df, subset_keys=subset_keys)


# ---------- State ----------
def read_last_ts_full(bsc: BlobServiceClient) -> Optional[pd.Timestamp]:
    if not blob_exists(bsc, FULL_STATE_BLOB):
        return None
    try:
        js = json.loads(download_blob_text(bsc, FULL_STATE_BLOB))
        return pd.to_datetime(js.get("last_processed_ts"), utc=True)
    except Exception:
        return None

def write_last_ts_full(bsc: BlobServiceClient, ts: pd.Timestamp):
    payload = json.dumps({"last_processed_ts": ts.isoformat()})
    upload_blob_bytes(bsc, FULL_STATE_BLOB, payload.encode("utf-8"))

# ---------- Models ----------
def load_impute_assets(bsc: BlobServiceClient):
    """
    STRICT AZURE MODE:
    Always load from Azure Blob Storage (ignores OUTPUT_MODE/local paths).

    Expects:
      - models/impute_models.joblib
      - models/donor_map.json

    Returns:
      (models_dict, donor_map_dict)

    Raises:
      FileNotFoundError / ValueError with actionable messages if blobs are missing/empty/invalid.
    """
    container = bsc.get_container_client(CONTAINER)

    def _require_blob_bytes(name: str) -> bytes:
        try:
            bc = container.get_blob_client(name)
            # existence + size check
            props = bc.get_blob_properties()
            size = getattr(props, "size", 0) or 0
            if size <= 0:
                raise ValueError(f"Blob {name!r} exists but is empty (size=0).")
            return bc.download_blob().readall()
        except Exception as e:
            # refine common cases
            msg = str(e)
            if "The specified blob does not exist" in msg or "404" in msg:
                raise FileNotFoundError(
                    f"Missing required blob {name!r} in container {CONTAINER!r}. "
                    f"Upload the asset or fix MODEL_BLOB/DONOR_MAP_BLOB env vars."
                )
            raise

    def _require_blob_text(name: str) -> str:
        raw = _require_blob_bytes(name)
        try:
            txt = raw.decode("utf-8")
        except Exception:
            raise ValueError(f"Blob {name!r} is not valid UTF-8 text.")
        if not txt.strip():
            raise ValueError(f"Blob {name!r} is empty after decoding.")
        return txt

    # --- Load model joblib (binary) ---
    models_bin = _require_blob_bytes(MODEL_BLOB)
    try:
        models = joblib_load(io.BytesIO(models_bin))
    except Exception as e:
        raise ValueError(f"Failed to load joblib from {MODEL_BLOB!r}: {e}")

    # normalize: allow single pipeline or dict-like payloads
    if not isinstance(models, dict):
        models = {"__default__": models}

    # --- Load donor map (JSON) ---
    donor_txt = _require_blob_text(DONOR_MAP_BLOB)
    try:
        donor_map = json.loads(donor_txt)
    except Exception as e:
        raise ValueError(f"Failed to parse JSON in {DONOR_MAP_BLOB!r}: {e}")

    if not isinstance(donor_map, dict):
        raise ValueError(f"{DONOR_MAP_BLOB!r} must be a JSON object (mapping); got {type(donor_map).__name__}.")

    log.info(f"[impute] loaded models: {len(models)} target pack(s); donor_map keys: {len(donor_map)}.")
    return models, donor_map


# ---------- Helpers ----------
def group_columns(cols: List[str]) -> Dict[str, List[str]]:
    import re
    return {
        "desk_occupancy":  [c for c in cols if re.match(r"Desk_\d{2}-\d{2}_occupancy", c)],
        "desk_temperature":[c for c in cols if re.match(r"Desk_\d{2}-\d{2}_temperature", c)],
        "desk_humidity":   [c for c in cols if re.match(r"Desk_\d{2}-\d{2}_humidity", c)],
        "air_co2":         [c for c in cols if re.match(r"Air_\d{2}-\d{2}_CO2", c)],
        "air_cov":         [c for c in cols if re.match(r"Air_\d{2}-\d{2}_COV", c)],
        "air_h":           [c for c in cols if re.match(r"Air_\d{2}-\d{2}_H", c)],
        "air_t":           [c for c in cols if re.match(r"Air_\d{2}-\d{2}_T", c)],
        "room_people":     [c for c in cols if re.match(r"Room_\d{2}-\d{2}_people_count_.*", c)],
        "noise":           [c for c in cols if re.match(r"Son_\d{2}-\d{2}_(lmax|leq|spl)", c, re.IGNORECASE)],
        "tempex":          [c for c in cols if re.match(r"TempEx_\d{2}-\d{2}_(temperature|humidity)", c)],
    }

def step1_concatenate_and_energy(df_new: pd.DataFrame, clean_tail: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df_new.empty: return df_new
    df_new = df_new.copy()
    df_new["timestamp"] = pd.to_datetime(df_new["timestamp"], errors="coerce", utc=True)
    df_new = df_new.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # Bridge last clean row to compute correct deltas on the boundary
    bridge = None
    if clean_tail is not None and not clean_tail.empty and "LT_05-01_presentvalue" in clean_tail.columns:
        bridge = clean_tail[["timestamp","LT_05-01_presentvalue"]].dropna().tail(1).copy()
        bridge["timestamp"] = pd.to_datetime(bridge["timestamp"], errors="coerce", utc=True)
    df_comb = pd.concat([bridge, df_new], ignore_index=True) if bridge is not None else df_new
    df_comb["timestamp"] = pd.to_datetime(df_comb["timestamp"], errors="coerce", utc=True)
    df_comb = df_comb.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    col = "LT_05-01_presentvalue"
    if col not in df_comb.columns:
        return df_comb

    df = df_comb.copy()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col]).reset_index(drop=True)

    ts0 = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    ts1 = pd.to_datetime(df["timestamp"].shift(-1), errors="coerce", utc=True)
    df["cons_raw"]   = df[col].shift(-1) - df[col]
    df["duration_h"] = (ts1 - ts0).dt.total_seconds() / 3600

    q_hi = df["cons_raw"].quantile(0.999) if df["cons_raw"].notna().any() else np.nan
    ok = (df["cons_raw"].ge(0)) & (df["duration_h"].gt(0)) & (df["cons_raw"].le(q_hi) if np.isfinite(q_hi) else True)
    df = df[ok].reset_index(drop=True)

    df["seg_id"] = (df[col].diff() < 0).cumsum()
    df["cons"] = df["cons_raw"]
    df["cons_rate"] = df["cons"] / df["duration_h"]
    return df.drop(columns=["cons_raw"])

def normalize_occupancy(df: pd.DataFrame, occ_cols: List[str]):
    if occ_cols:
        df[occ_cols] = df[occ_cols].replace(1, 2)

def clip_ranges_to_nan(df: pd.DataFrame, groups: Dict[str, List[str]]):
    for g, cols in groups.items():
        if g not in RANGES or not cols: continue
        lo, hi = RANGES[g]
        df[cols] = df[cols].mask((df[cols] < lo) | (df[cols] > hi))

def time_interp_short(df: pd.DataFrame, cols: List[str], timestamp_col="timestamp", limit=2):
    if not cols or df.empty: return
    dfi = df.set_index(pd.to_datetime(df[timestamp_col], utc=True))
    for c in cols:
        if c in dfi.columns:
            s = pd.to_numeric(dfi[c], errors="coerce")
            if s.isna().any():
                dfi[c] = s.interpolate(method="time", limit=limit, limit_direction="both")
    out = dfi.reset_index(drop=True)
    df[cols] = out[cols]

def add_room_aggregates_and_time(df: pd.DataFrame, groups: Dict[str, List[str]], tz=TZ_LOCAL):
    """
    Build all aggregates + time features in a single block and append once.
    Avoids DataFrame fragmentation warnings and speeds things up.
    """
    if df.empty:
        return

    # --- Aggregates ---
    new_cols = {}

    # Desk occupancy sum
    occ = groups.get("desk_occupancy", [])
    if occ:
        new_cols["Desk_all_occupancy_sum"] = df[occ].sum(axis=1, min_count=1)

    # Desk temperature stats (require >=5 signals)
    dtemps = groups.get("desk_temperature", [])
    if dtemps:
        cnt_t = df[dtemps].notna().sum(axis=1)
        mean_t = df[dtemps].mean(axis=1)
        std_t  = df[dtemps].std(axis=1, ddof=0)
        new_cols["Desk_all_temperature_mean"] = mean_t.where(cnt_t >= 5)
        new_cols["Desk_all_temperature_std"]  = std_t.where(cnt_t >= 5)

    # Desk humidity stats (require >=5 signals)
    dhums = groups.get("desk_humidity", [])
    if dhums:
        cnt_h = df[dhums].notna().sum(axis=1)
        mean_h = df[dhums].mean(axis=1)
        std_h  = df[dhums].std(axis=1, ddof=0)
        new_cols["Desk_all_humidity_mean"] = mean_h.where(cnt_h >= 5)
        new_cols["Desk_all_humidity_std"]  = std_h.where(cnt_h >= 5)

    # --- Time features (computed once, appended once) ---
    t = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_convert(tz)
    tf_hour    = t.dt.hour.astype("Int16")
    tf_weekday = t.dt.weekday.astype("Int16")
    tf_month   = t.dt.month.astype("Int16")

    new_cols.update({
        "tf_hour": tf_hour,
        "tf_weekday": tf_weekday,
        "tf_month": tf_month,
        "tf_is_weekend": tf_weekday.isin([5, 6]).astype("Int8"),
        "tf_is_workhour": ((tf_hour >= 8) & (tf_hour <= 19)).astype("Int8"),
    })

    # cyclic encodings
    two_pi = 2 * np.pi
    new_cols["tf_hour_sin"] = np.sin(two_pi * (tf_hour.astype("float64") / 24.0))
    new_cols["tf_hour_cos"] = np.cos(two_pi * (tf_hour.astype("float64") / 24.0))
    new_cols["tf_dow_sin"]  = np.sin(two_pi * (tf_weekday.astype("float64") / 7.0))
    new_cols["tf_dow_cos"]  = np.cos(two_pi * (tf_weekday.astype("float64") / 7.0))
    new_cols["tf_mon_sin"]  = np.sin(two_pi * ((tf_month.astype("float64") - 1) / 12.0))
    new_cols["tf_mon_cos"]  = np.cos(two_pi * ((tf_month.astype("float64") - 1) / 12.0))

    # Append in one shot to avoid fragmentation
    new_df = pd.DataFrame(new_cols, index=df.index)
    # concatenate as a single block; the final copy() defragments the blocks
    df[:] = df  # keep original object reference if callers rely on mutation
    df_new = pd.concat([df, new_df], axis=1)
    df.drop(df.columns.difference(df.columns[:0]), axis=1, inplace=True)  # clear df
    df[df_new.columns] = df_new
    # optional hard defragment (keeps the same object identity for callers)
    _ = df.copy()  # triggers block consolidation internally


def ml_impute_inplace(df: pd.DataFrame, models, donor_map: Dict[str, List[str]], groups: Dict[str, List[str]], min_present_frac=0.5):
    if df.empty or not models: return
    for target, pack in models.items():
        feats = donor_map.get(target, pack.get("features") if isinstance(pack, dict) else None)
        if not feats: continue
        use = [c for c in feats if c in df.columns]
        if not use or target not in df.columns: continue

        miss = df[target].isna()
        if not miss.any(): continue

        present_req = max(1, int(np.ceil(len(use)*min_present_frac)))
        ok = (df[use].notna().sum(axis=1) >= present_req) & miss
        if not ok.any(): continue

        pipe = pack["pipe"] if isinstance(pack, dict) and "pipe" in pack else pack
        try:
            pred = pipe.predict(df.loc[ok, use])
        except Exception:
            continue

        g = next((gn for gn, cols in groups.items() if target in cols), None)
        if g in RANGES:
            lo, hi = RANGES[g]
            pred = np.clip(pred, lo, hi)
        df.loc[ok, target] = pred

    # finalize types
    for c in [c for c in df.columns if c.startswith("Desk_") and c.endswith("_occupancy")]:
        s = pd.to_numeric(df[c], errors="coerce")
        df[c] = s.where(s.isin([0,2]), 0).astype("Int8")
    for c in [c for c in df.columns if "people_count" in c]:
        s = pd.to_numeric(df[c], errors="coerce").clip(lower=0).round()
        df[c] = s.astype("Int64")

# ---------- Dataset builders ----------
from pandas.api.types import is_numeric_dtype

def expected_schema() -> List[str]:
    lead = ["t0","timestamp","t1_requested","t1_actual","match_type","duration_h","cons",
            "LT_05-01_presentvalue_t0","LT_05-01_presentvalue_t1"]
    snap = PARAMS[:]
    tfs  = TF_COLS[:]
    prev = []
    for p in PARAMS:
        prev += [f"{p}_prev_mean", f"{p}_prev_min", f"{p}_prev_max"]
    lags = ["cons_prev","cons_rate_prev"]
    cols = lead + snap + tfs + prev + lags
    assert len(cols) == 101
    return cols

def _empty_schema_df() -> pd.DataFrame:
    return pd.DataFrame(columns=expected_schema())

def ensure_full_dataset_skeleton(bsc: BlobServiceClient):
    empty = _empty_schema_df()
    for m in MINUTE_HORIZONS:
        ensure_csv_with_header(bsc, f"{ML_FULL_PREFIX}/datasets/minutes/clean_full_{m}m.csv", empty)
    for h in HOUR_HORIZONS:
        ensure_csv_with_header(bsc, f"{ML_FULL_PREFIX}/datasets/hours/clean_full_{h}h.csv", empty)
    for d in DAY_HORIZONS:
        ensure_csv_with_header(bsc, f"{ML_FULL_PREFIX}/datasets/days/clean_full_{d}d.csv", empty)
    for w in WEEK_HORIZONS:
        ensure_csv_with_header(bsc, f"{ML_FULL_PREFIX}/datasets/weeks/clean_full_{w}w.csv", empty)

def _to_naive_utc(s: pd.Series, assume_tz: Optional[str]) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    if getattr(s.dtype, "tz", None) is not None:
        return s.dt.tz_convert("UTC").dt.tz_localize(None)
    if assume_tz:
        s = pd.to_datetime(s, errors="coerce")
        s = s.dt.tz_localize(assume_tz, nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert("UTC").dt.tz_localize(None)
    return s.astype("datetime64[ns]")

def normalize_timestamp(df: pd.DataFrame, ts_col: str, assume_tz: Optional[str]) -> pd.DataFrame:
    out = df.copy()
    out[ts_col] = _to_naive_utc(out[ts_col], assume_tz)
    return out[out[ts_col].notna()].sort_values(ts_col).reset_index(drop=True)

def find_nearest_index_within(ts_series: pd.Series, t_req: pd.Timestamp, tol_min: Optional[float]) -> Optional[int]:
    idx = pd.Index(ts_series)
    j = int(idx.searchsorted(t_req, side="left"))
    cands = []
    if j < len(idx): cands.append((abs(idx[j] - t_req), j))
    if j - 1 >= 0:   cands.append((abs(idx[j-1] - t_req), j-1))
    if not cands: return None
    cands.sort(key=lambda x: x[0])
    d, k = cands[0]
    if tol_min is None or abs(d) <= pd.Timedelta(minutes=float(tol_min)):
        return int(k)
    return None

def add_time_features_from_t0(df_with_t0: pd.DataFrame, t0_col="t0") -> pd.DataFrame:
    out = df_with_t0.copy()
    t0 = pd.to_datetime(out[t0_col], errors="coerce")
    out["tf_hour"] = t0.dt.hour; out["tf_minute"] = t0.dt.minute; out["tf_weekday"] = t0.dt.weekday
    out["tf_month"] = t0.dt.month; out["tf_dayofyear"] = t0.dt.dayofyear
    try:
        out["tf_weekofyear"] = t0.dt.isocalendar().week.astype(int)
    except Exception:
        out["tf_weekofyear"] = t0.dt.week
    out["tf_is_weekend"] = (out["tf_weekday"] >= 5).astype(int)
    out["tf_is_workhour"]= ((out["tf_weekday"] < 5) & (out["tf_hour"].between(8, 18))).astype(int)
    two_pi = 2*np.pi
    out["tf_hour_sin"] = np.sin(two_pi*out["tf_hour"]/24.0); out["tf_hour_cos"] = np.cos(two_pi*out["tf_hour"]/24.0)
    out["tf_dow_sin"]  = np.sin(two_pi*out["tf_weekday"]/7.0); out["tf_dow_cos"]  = np.cos(two_pi*out["tf_weekday"]/7.0)
    out["tf_mon_sin"]  = np.sin(two_pi*(out["tf_month"]-1)/12.0); out["tf_mon_cos"]  = np.cos(two_pi*(out["tf_month"]-1)/12.0)
    return out

def compute_prev_aggs_for_params(df_idx: pd.DataFrame, params: List[str], window_minutes: int) -> pd.DataFrame:
    numeric = [c for c in params if (c in df_idx.columns and is_numeric_dtype(df_idx[c]))]
    if not numeric: return pd.DataFrame(index=df_idx.index)
    win = f"{int(window_minutes)}min"
    roll = df_idx[numeric].rolling(win, min_periods=1, closed="both")
    return pd.concat(
        [roll.mean().rename(columns=lambda c: f"{c}_prev_mean"),
         roll.min().rename(columns=lambda c: f"{c}_prev_min"),
         roll.max().rename(columns=lambda c: f"{c}_prev_max")],
        axis=1
    )

def compute_prev_cons_anchored(df_idx: pd.DataFrame, counter_col: str, window_minutes: int, tol_min: Optional[float]) -> pd.DataFrame:
    if counter_col not in df_idx.columns or df_idx.empty:
        return pd.DataFrame(index=df_idx.index)

    # Ensure a tz-aware DateTimeIndex
    dfi = df_idx.copy().sort_index()
    idx_utc = pd.to_datetime(dfi.index, utc=True)
    dfi.index = idx_utc

    # Targets (t0 - window)
    t0 = idx_utc.to_series()
    target = (t0 - pd.Timedelta(minutes=int(window_minutes))).rename("t_target")

    # Build left/right for asof using pandas dtypes (no .values)
    right = (
        dfi[[counter_col]]
        .reset_index()
        .rename(columns={dfi.index.name or "index": "timestamp"})
        .assign(timestamp=lambda x: pd.to_datetime(x["timestamp"], utc=True))
        .sort_values("timestamp")
    )
    left = (
        pd.DataFrame({"t_target": target.values})
        .assign(t_target=lambda x: pd.to_datetime(x["t_target"], utc=True))
        .sort_values("t_target")
    )

    m = pd.merge_asof(
        left,
        right,
        left_on="t_target",
        right_on="timestamp",
        direction="nearest",
        tolerance=None if tol_min is None else pd.Timedelta(minutes=float(tol_min)),
    )

    # Align and ensure numeric
    c0 = pd.to_numeric(dfi[counter_col], errors="coerce")
    c_prev = pd.to_numeric(m[counter_col], errors="coerce").set_axis(c0.index)

    # Pure pandas timedelta -> hours (as float Series aligned to index)
    dt_h = (t0.reset_index(drop=True) - pd.to_datetime(m["timestamp"], utc=True)).dt.total_seconds() / 3600.0
    dt_h = pd.to_numeric(dt_h, errors="coerce").set_axis(c0.index)

    cons_prev = (c0 - c_prev).where(dt_h.gt(0))
    rate_prev = (cons_prev / dt_h).where(dt_h.gt(0))

    out = pd.DataFrame({"cons_prev": cons_prev, "cons_rate_prev": rate_prev}, index=dfi.index)
    out.index = pd.to_datetime(out.index, utc=True).tz_localize(None)

    return out

def filter_desk_snapshot_whitelist(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns.tolist()
    desk_prev = [c for c in cols if c.startswith("Desk") and "_prev_" in c]
    desk_snap_all = [c for c in cols if c.startswith("Desk") and "_prev_" not in c]
    desk_snap_keep = [c for c in desk_snap_all if c in DESK_KEEP_SNAPSHOT]
    desk_snap_drop = [c for c in desk_snap_all if c not in DESK_KEEP_SNAPSHOT]
    out = df.drop(columns=desk_snap_drop, errors="ignore")
    order = [c for c in cols if not c.startswith("Desk")] + desk_prev + desk_snap_keep
    return out[[c for c in out.columns if c in order]]

def build_intrahour_dataset(df: pd.DataFrame, window_minutes: int, tol_min: float, assume_tz: Optional[str]=None) -> pd.DataFrame:
    df = normalize_timestamp(df, TS_COL, assume_tz)
    dbg = DATASETS_DEBUG
    if dbg:
        log.info(f"[builder] win={window_minutes}m tol={tol_min} base_rows={len(df)} "
                f"has_counter={COUNTER_COL in df.columns}")

    if COUNTER_COL not in df.columns: return pd.DataFrame()

    snapshot_cols = [c for c in PARAMS if (c in df.columns and c not in FORBIDDEN_SNAPSHOT)]
    
    if dbg:
        log.info(f"[builder] win={window_minutes}m snapshot_cols_present={len(snapshot_cols)}")


    if not snapshot_cols:
        return pd.DataFrame()
    
    df_idx = df.set_index(TS_COL).sort_index()
    ts_series = df[TS_COL].reset_index(drop=True)
    prev_aggs = compute_prev_aggs_for_params(df_idx, snapshot_cols, window_minutes=window_minutes)
    cons_prev = compute_prev_cons_anchored(df_idx, COUNTER_COL, window_minutes=window_minutes, tol_min=tol_min)

    snap = df[[TS_COL] + snapshot_cols].copy().rename(columns={TS_COL: "t0"})
    snap = add_time_features_from_t0(snap, t0_col="t0")
    # --- normalize t0 to naive UTC on BOTH sides ---
    snap["t0"] = pd.to_datetime(snap["t0"], utc=True).dt.tz_localize(None)

    prev_aggs_df = prev_aggs.reset_index().rename(columns={TS_COL: "t0"})
    if "t0" in prev_aggs_df.columns:
        prev_aggs_df["t0"] = pd.to_datetime(prev_aggs_df["t0"], utc=True).dt.tz_localize(None)

    cons_prev_df = cons_prev.reset_index().rename(columns={TS_COL: "t0"})
    if "t0" in cons_prev_df.columns:
        cons_prev_df["t0"] = pd.to_datetime(cons_prev_df["t0"], utc=True).dt.tz_localize(None)

    # Merges (now both sides are datetime64[ns], naive UTC)
    snap = snap.merge(prev_aggs_df, on="t0", how="left")
    snap = snap.merge(cons_prev_df, on="t0", how="left")

    
    snap = filter_desk_snapshot_whitelist(snap)

    rows = []
    for i in range(len(df)):
        t0 = df.iloc[i][TS_COL]; c0 = df.iloc[i][COUNTER_COL]
        if pd.isna(c0): continue
        t1_req = t0 + pd.Timedelta(minutes=int(window_minutes))
        j = find_nearest_index_within(ts_series, t1_req, tol_min=tol_min)
        if j is None: continue
        t1_act = ts_series.iloc[j]
        if t1_act == t0: continue
        gap = abs((t1_act - t1_req).total_seconds())/60.0
        if tol_min is not None and gap > float(tol_min): continue
        duration_h = (t1_act - t0).total_seconds()/3600.0
        if duration_h <= 0: continue
        c1 = df.iloc[j][COUNTER_COL]
        cons = np.nan if pd.isna(c1) else float(c1) - float(c0)
        snap_row = snap.iloc[i].to_dict(); snap_row.pop("timestamp", None); snap_row.pop("t0", None)
        rows.append({
            "t0": t0, "timestamp": t0,
            "t1_requested": t1_req, "t1_actual": t1_act,
            "match_type": "exact" if t1_act == t1_req else "approx",
            "duration_h": duration_h, "cons": cons,
            "LT_05-01_presentvalue_t0": c0, "LT_05-01_presentvalue_t1": c1,
            **snap_row
        })
    out = pd.DataFrame(rows)
    if out.empty: return out

    out["gap_min"] = (pd.to_datetime(out["t1_actual"]) - pd.to_datetime(out["t1_requested"])).abs().dt.total_seconds()/60.0
    out = out.sort_values(["t0","t1_actual","gap_min","t1_requested"]).drop_duplicates(subset=["t0","t1_actual"], keep="first").reset_index(drop=True)
    out = out.sort_values(["t0","t1_requested"]).drop(columns=["gap_min"], errors="ignore")
    required = ["t0","t1_actual","duration_h","cons",
                "LT_05-01_presentvalue_t0","LT_05-01_presentvalue_t1"]
    out = (out[out["t1_actual"] != out["t0"]]
        .dropna(subset=required)
        .reset_index(drop=True))

    schema = expected_schema()
    for c in schema:
        if c not in out.columns: out[c] = np.nan
    return out[schema]

# ---------- Dataset writing ----------
def _dedup_t0_t1(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    return df.sort_values(["t0","t1_actual"]).drop_duplicates(subset=["t0","t1_actual"], keep="first").reset_index(drop=True)

def list_full_dataset_blobs(bsc: BlobServiceClient) -> list[str]:
    out = []
    for p in [f"{ML_FULL_PREFIX}/datasets/minutes/",
              f"{ML_FULL_PREFIX}/datasets/hours/",
              f"{ML_FULL_PREFIX}/datasets/days/",
              f"{ML_FULL_PREFIX}/datasets/weeks/"]:
        out.extend(list_csv_anywhere(bsc, p))
    return sorted([p for p in out if p.endswith(".csv")])


def read_csv_any(bsc: BlobServiceClient, name: str) -> pd.DataFrame:
    return read_csv_anywhere(bsc, name, parse_ts=False)


def ensure_common_t0_header(bsc: BlobServiceClient):
    if not blob_exists(bsc, FULL_COMMON_T0_BLOB):
        buf = io.StringIO(); pd.DataFrame(columns=["t0"]).to_csv(buf, index=False)
        upload_blob_bytes(bsc, FULL_COMMON_T0_BLOB, buf.getvalue().encode("utf-8"))

def read_existing_common_t0_max(bsc: BlobServiceClient) -> Optional[pd.Timestamp]:
    if not blob_exists(bsc, FULL_COMMON_T0_BLOB): return None
    df = read_csv_any(bsc, FULL_COMMON_T0_BLOB)
    if df.empty or "t0" not in df.columns: return None
    t0s = pd.to_datetime(df["t0"], errors="coerce", utc=True)
    return t0s.max() if t0s.notna().any() else None

def append_common_t0(bsc: BlobServiceClient, t0_series: pd.Series):
    if t0_series is None or t0_series.empty: return
    df_new = pd.DataFrame({"t0": pd.to_datetime(t0_series, utc=True)}).dropna().drop_duplicates().sort_values("t0")
    ensure_common_t0_header(bsc)
    append_csv_dedup(bsc, FULL_COMMON_T0_BLOB, df_new, subset_keys=["t0"])

def build_and_update_common_t0_full(bsc: BlobServiceClient):
    """
    Recompute strict common t0 across ALL 49 datasets on every call.
    If any horizon csv is missing/empty/without 't0', do nothing.
    Else, compute the set intersection of t0 across all horizons and overwrite list_of_common_t0.csv.
    """
    ensure_full_dataset_skeleton(bsc)
    # expected 49 dataset file paths
    minute_paths = [f"{ML_FULL_PREFIX}/datasets/minutes/clean_full_{m}m.csv" for m in MINUTE_HORIZONS]
    hour_paths   = [f"{ML_FULL_PREFIX}/datasets/hours/clean_full_{h}h.csv"   for h in HOUR_HORIZONS]
    day_paths    = [f"{ML_FULL_PREFIX}/datasets/days/clean_full_{d}d.csv"    for d in DAY_HORIZONS]
    week_paths   = [f"{ML_FULL_PREFIX}/datasets/weeks/clean_full_{w}w.csv"   for w in WEEK_HORIZONS]
    expected = minute_paths + hour_paths + day_paths + week_paths  # 49 files

    t0_sets = []
    missing = []
    for name in expected:
        if not blob_exists(bsc, name):
            missing.append((name, "missing")); continue
        df = read_csv_any(bsc, name)
        if df.empty or "t0" not in df.columns:
            missing.append((name, "empty/no-t0")); continue
        s = pd.to_datetime(df["t0"], errors="coerce", utc=True).dropna().drop_duplicates()
        if s.empty:
            missing.append((name, "no-rows")); continue
        t0_sets.append(s.astype("int64"))

    if len(t0_sets) != len(expected):
        log.info("[common_t0] strict: not updating; horizons not ready: " +
                 ", ".join([f"{p}({why})" for p, why in missing]))
        return

    common = set(t0_sets[0])
    for s in t0_sets[1:]:
        common &= set(s)
    if not common:
        log.info("[common_t0] strict: no common t0 across all horizons; not writing.")
        return

    # Overwrite file with the exact strict set
    out = pd.DataFrame({"t0": pd.to_datetime(list(common), utc=True).sort_values()})
    buf = io.StringIO(); out.to_csv(buf, index=False)
    upload_blob_bytes(bsc, FULL_COMMON_T0_BLOB, buf.getvalue().encode("utf-8"), overwrite=True)
    log.info(f"[common_t0] strict: wrote {len(out)} rows (overwrite).")


NUM_HORIZONS = 49
T0_COUNTER_PATH = "data_for_ml/state/t0_counter.csv"

def _read_t0_counter(bsc):
    try:
        df = read_csv_anywhere(bsc, T0_COUNTER_PATH, parse_ts=False)
    except Exception:
        df = pd.DataFrame(columns=["t0","cnt"])
    if "t0" in df.columns:
        df["t0"] = pd.to_datetime(df["t0"], errors="coerce", utc=True)
    if "cnt" in df.columns:
        df["cnt"] = pd.to_numeric(df["cnt"], errors="coerce").fillna(0).astype("int64")
    return df

def _write_t0_counter(bsc, df):
    buf = io.StringIO()
    out = df.copy()
    out["t0"] = pd.to_datetime(out["t0"], errors="coerce", utc=True)
    out = out.dropna(subset=["t0"])
    out.to_csv(buf, index=False)
    upload_blob_bytes(bsc, T0_COUNTER_PATH, buf.getvalue().encode("utf-8"), overwrite=True)

def _append_common_t0_incremental(bsc, new_t0_series: pd.Series):
    """
    Increment cnt for provided t0s; when cnt hits NUM_HORIZONS, append to list_of_common_t0.csv.
    """
    if new_t0_series is None or new_t0_series.empty:
        return

    # normalize incoming t0s
    s = pd.to_datetime(new_t0_series, errors="coerce", utc=True).dropna().drop_duplicates()
    if s.empty:
        return

    # load counter
    cnt_df = _read_t0_counter(bsc)

    # align to dict for fast bump
    if cnt_df.empty:
        cnt_df = pd.DataFrame({"t0": s, "cnt": 1})
    else:
        cnt_df = cnt_df.set_index("t0")
        for t in s:
            if t in cnt_df.index:
                cnt_df.loc[t, "cnt"] = int(cnt_df.loc[t, "cnt"]) + 1
            else:
                cnt_df.loc[t, "cnt"] = 1
        cnt_df = cnt_df.reset_index()

    # find newly-complete t0s
    newly_full = cnt_df[(cnt_df["cnt"] >= NUM_HORIZONS)]["t0"].drop_duplicates()
    if not newly_full.empty:
        # only add t0s that are not yet in list_of_common_t0.csv (dedup append takes care of it too)
        ensure_common_t0_header(bsc)
        to_add = pd.DataFrame({"t0": newly_full})
        append_csv_dedup(bsc, FULL_COMMON_T0_BLOB, to_add, subset_keys=["t0"])

    # persist counter
    _write_t0_counter(bsc, cnt_df)


def build_and_write_datasets_full(bsc: BlobServiceClient, full_clean_df: pd.DataFrame, t0_min_inclusive: Optional[pd.Timestamp]):
    ensure_full_dataset_skeleton(bsc)
    if full_clean_df is None or full_clean_df.empty: return

    if DATASETS_DEBUG:
        log.info(f"[datasets] starting build, rows_in={0 if full_clean_df is None else len(full_clean_df)}")
    
    def _append_filtered(name: str, df: pd.DataFrame, label: str):
        ensure_csv_with_header(bsc, name, _empty_schema_df())
        if df.empty:
            return

        to_write = _dedup_t0_t1(df.copy())

        existing = read_csv_any(bsc, name)
        last_t1 = None if existing is None or existing.empty or "t1_actual" not in existing.columns \
                else pd.to_datetime(existing["t1_actual"], errors="coerce", utc=True).max()
        if last_t1 is not None:
            to_write["t1_actual"] = pd.to_datetime(to_write["t1_actual"], errors="coerce", utc=True)
            to_write = to_write[to_write["t1_actual"] > last_t1]
        if to_write.empty:
            return

        # 1) append dataset rows
        append_csv_dedup(bsc, name, to_write, subset_keys=["t0","t1_actual"])

        # 2) incrementally update common t0 counter using *new* t0s only
        _append_common_t0_incremental(bsc, to_write["t0"])


    base = full_clean_df.copy()
    base[TS_COL] = pd.to_datetime(base[TS_COL], errors="coerce", utc=True)
    base = base.dropna(subset=[TS_COL]).sort_values(TS_COL).reset_index(drop=True)

    if DATASETS_DEBUG:
        log.info(f"[datasets] base rows={len(base)} ts_min={base[TS_COL].min()} ts_max={base[TS_COL].max()}")
        has_counter = COUNTER_COL in base.columns
        snap_present = [c for c in PARAMS if c in base.columns]
        log.info(f"[datasets] has_counter={has_counter} counter_col={COUNTER_COL}")
        log.info(f"[datasets] snapshot_cols_present={len(snap_present)}/{len(PARAMS)} e.g. {snap_present[:8]}")


    # keep a small buffer above last_ts (helps with approximate matches)
    upper = base[TS_COL].max()
    lower = (t0_min_inclusive or base[TS_COL].min())

    # max horizon lookback (weeks) and small extra safety margin
    max_lookback = pd.Timedelta(weeks=max(WEEK_HORIZONS))  # 12 weeks
    safety = pd.Timedelta(hours=6)
    cut_from = (lower - max_lookback - safety) if lower is not None else base[TS_COL].min()
    base = base[base[TS_COL] >= cut_from].copy()

    # minutes/hours/days/weeks
    for m in MINUTE_HORIZONS:
        _append_filtered(f"{ML_FULL_PREFIX}/datasets/minutes/clean_full_{m}m.csv",
                         build_intrahour_dataset(base, window_minutes=m, tol_min=TOL_MIN_MINUTES), label=f"{m}m")
    for h in HOUR_HORIZONS:
        _append_filtered(f"{ML_FULL_PREFIX}/datasets/hours/clean_full_{h}h.csv",
                         build_intrahour_dataset(base, window_minutes=h*60, tol_min=TOL_MIN_HOURS), label=f"{h}h")
    for d in DAY_HORIZONS:
        _append_filtered(f"{ML_FULL_PREFIX}/datasets/days/clean_full_{d}d.csv",
                         build_intrahour_dataset(base, window_minutes=d*24*60, tol_min=TOL_MIN_DAYS), label=f"{d}d")
    for w in WEEK_HORIZONS:
        _append_filtered(f"{ML_FULL_PREFIX}/datasets/weeks/clean_full_{w}w.csv",
                         build_intrahour_dataset(base, window_minutes=w*7*24*60, tol_min=TOL_MIN_WEEKS), label=f"{w}w")


def validate_common_t0(bsc: BlobServiceClient, max_check=50):
    """
    Verify that recent t0 values listed in data_for_ml/full/list_of_common_t0.csv
    exist in every horizon dataset. Logs any mismatches.
    """
    path = FULL_COMMON_T0_BLOB  # data_for_ml/full/list_of_common_t0.csv
    if not blob_exists(bsc, path):
        log.info(f"[common_t0] validate: file missing at {path}."); 
        return

    dfc = read_csv_any(bsc, path)
    if dfc.empty or "t0" not in dfc.columns:
        log.info(f"[common_t0] validate: file empty or lacks 't0' at {path}."); 
        return

    sample = (
        pd.to_datetime(dfc["t0"], errors="coerce", utc=True)
        .dropna().sort_values().tail(max_check)
    )
    if sample.empty:
        log.info("[common_t0] validate: no valid t0 rows to sample."); 
        return

    paths = (
        [f"{ML_FULL_PREFIX}/datasets/minutes/clean_full_{m}m.csv" for m in MINUTE_HORIZONS] +
        [f"{ML_FULL_PREFIX}/datasets/hours/clean_full_{h}h.csv"   for h in HOUR_HORIZONS] +
        [f"{ML_FULL_PREFIX}/datasets/days/clean_full_{d}d.csv"    for d in DAY_HORIZONS] +
        [f"{ML_FULL_PREFIX}/datasets/weeks/clean_full_{w}w.csv"   for w in WEEK_HORIZONS]
    )

    bad = []
    sample_i64 = set(sample.astype("int64"))
    for p in paths:
        df = read_csv_any(bsc, p)
        have = set(pd.to_datetime(df.get("t0", []), errors="coerce", utc=True).dropna().astype("int64"))
        miss = [t for t in sample_i64 if t not in have]
        if miss:
            bad.append((p, len(miss)))

    if bad:
        log.warning("[common_t0] validate: mismatches: " + ", ".join([f"{p}({n})" for p, n in bad]))
    else:
        log.info(f"[common_t0] validate: OK for last {len(sample)} t0 at {path}.")

def _air_columns_present(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.startswith("Air_") and c.split("_")[-1] in ("CO2","COV","H","T")]

def last_real_air_ts(df: pd.DataFrame) -> pd.Timestamp | None:
    """
    Return the max timestamp where any Air channel has a non-NaN value.
    Expect df['timestamp'] already parsed to tz-aware UTC earlier in run.
    """
    cols = _air_columns_present(df)
    if not cols or df.empty or "timestamp" not in df.columns:
        return None
    m = df[cols].notna().any(axis=1)
    if not m.any():
        return None
    ts = pd.to_datetime(df.loc[m, "timestamp"], errors="coerce", utc=True)
    ts = ts.dropna()
    return ts.max() if not ts.empty else None

def mark_and_fill_air_synthetic(
    df: pd.DataFrame,
    air_last_real: pd.Timestamp | None,
    max_pad_min: int = 60,
    policy: str = None
) -> tuple[pd.DataFrame, pd.Timestamp | None, pd.Timestamp | None, int]:
    """
    Enforce that step1/clean_full never advance past the last real Air timestamp,
    *except* when Air delivered nothing for the current hour (decode gap).
    In that decode-gap case, allow bounded forward fill (≤ max_pad_min) and cap rows
    at the padded time. All padded rows are marked Air_is_synthetic=1.

    Returns: (df_out, air_last_real, effective_last, n_synthetic_rows)
    """
    out = df.copy()
    cols = _air_columns_present(out)

    # Normalize time
    if out.empty or "timestamp" not in out.columns:
        if "Air_is_synthetic" not in out.columns:
            out["Air_is_synthetic"] = 0
        return out, air_last_real, None, 0

    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp")

    # No Air columns -> nothing to pad/cap
    if not cols:
        out["Air_is_synthetic"] = 0
        return out, air_last_real, out["timestamp"].max() if not out.empty else None, 0

    # If there was never any real Air yet, do not fabricate anything
    if air_last_real is None or pd.isna(air_last_real):
        out["Air_is_synthetic"] = 0
        # Cap to current max (but we won’t fill Air)
        return out, None, out["timestamp"].max() if not out.empty else None, 0

    ts_max = out["timestamp"].max()
    if pd.isna(ts_max):
        out["Air_is_synthetic"] = 0
        return out, air_last_real, None, 0

    # Do we have ANY real Air values after air_last_real in this batch?
    # If yes -> Air is present later, so we must NOT pad; we cap strictly at air_last_real.
    suffix = out[out["timestamp"] > air_last_real]
    suffix_has_real_air = suffix[cols].notna().any(axis=1).any() if not suffix.empty else False

    # Optional env policy (default: pad only on decode gaps)
    eff_policy = (os.getenv("AIR_FILL_POLICY", "") or policy or "pad_on_decode_gap").lower()
    pad_on_gap = (eff_policy in ("pad_on_decode_gap", "gap_only", "gap"))

    # Decide effective cap:
    # - If we see any real Air beyond air_last_real: strict cap at air_last_real.
    # - Else (decode gap) and gap ≤ max_pad_min: allow bounded ffill and cap to ts_max.
    # - Else: strict cap at air_last_real.
    gap = ts_max - air_last_real
    allow_pad = (pad_on_gap and not suffix_has_real_air and gap <= pd.Timedelta(minutes=int(max_pad_min)))

    if not allow_pad:
        # STRICT CAP — do not produce rows beyond the last real Air sample
        out = out[out["timestamp"] <= air_last_real].copy()
        out["Air_is_synthetic"] = 0
        return out, air_last_real, air_last_real, 0

    # PAD CASE — bounded forward-fill just for readability/continuity; mark & cap
    # Limit forward-fill depth in rows (10-min cadence ⇒ max_pad_min/10)
    ffill_limit = max(0, int(round(max_pad_min / 10.0)))
    out_idx = out.set_index("timestamp")
    # forward-fill only Air channels
    out_idx[cols] = out_idx[cols].ffill(limit=ffill_limit)
    out = out_idx.reset_index()

    m_syn = (out["timestamp"] > air_last_real) & (out["timestamp"] <= ts_max)
    out["Air_is_synthetic"] = m_syn.astype("int8")

    # Cap to ts_max (bounded by allow_pad check above)
    out = out[out["timestamp"] <= ts_max].copy()
    n_synth = int(m_syn.sum())

    return out, air_last_real, ts_max, n_synth

def log_air_synthetic_use(bsc, start_ts: pd.Timestamp, end_ts: pd.Timestamp, n_rows: int, reason: str):
    """
    Append a one-line record to data_for_ml/state/air_synthetic_log.csv
    """
    path = "data_for_ml/state/air_synthetic_log.csv"
    import io, pandas as pd
    try:
        # read (parse_ts=False)
        base = read_csv_any(bsc, path)
    except Exception:
        base = pd.DataFrame(columns=["start_ts","end_ts","n_rows","reason"])
    row = pd.DataFrame([{
        "start_ts": pd.to_datetime(start_ts, utc=True),
        "end_ts": pd.to_datetime(end_ts, utc=True),
        "n_rows": int(n_rows),
        "reason": reason
    }])
    out = pd.concat([base, row], ignore_index=True)
    buf = io.StringIO(); out.to_csv(buf, index=False)
    upload_blob_bytes(bsc, path, buf.getvalue().encode("utf-8"), overwrite=True)


# ---------- Public entrypoint ----------
def run_pipeline_full(df_full_wide: pd.DataFrame, log: logging.Logger):
    """
    Input: a wide batch (any cross-year window) with 'timestamp' and sensor columns.
    Steps (incremental):
      1) Load last FULL state (t_last). If exists, filter step1 to strictly newer than t_last.
      2) Compute step1 deltas (bridge with last clean row so energy never restarts).
      3) Clean (clip/ranges/interp/aggregates/impute).
      4) Append to step1_df_full.csv and clean_full.csv (dedup on timestamp).
      5) Build/append datasets, filtering to t1_actual > last_ts, and de-dup ['t0','t1_actual'].
      6) Update FULL state to max timestamp we just committed.
      7) Update list_of_common_t0.csv for any brand-new t0’s across ALL datasets.
    """

    # Configurable: by default SKIP_FULL_DATASETS=1 → we DO NOT build 49 datasets + common_t0
    #    Set SKIP_FULL_DATASETS=0 (or "false") to re-enable full dataset/common_t0 building.
    DO_DATASETS = not _flag_on("SKIP_FULL_DATASETS", default="0")

    bsc = get_bsc()
    logger = log or logging.getLogger("pipeline_full")

    step1_blob = f"{ML_FULL_PREFIX}/step1_df_full.csv"
    clean_blob = f"{ML_FULL_PREFIX}/clean_full.csv"

    df = df_full_wide.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if df.empty:
        return {"processed": 0, "skipped": False}

    # ensure headers once
    ensure_csv_with_header(bsc, step1_blob, df[["timestamp"]])
    ensure_csv_with_header(bsc, clean_blob, df[["timestamp"]])

    # one-time model load
    try:
        models, donor_map = load_impute_assets(bsc)  # strict blob
    except Exception as e:
        logger.error(f"[impute] asset load failed (blob-only): {e}")
        raise

    # --- One-off rebuild controls (env) ---
    def _parse_env_ts(s):
        if not s: return None
        try:
            return pd.to_datetime(s.strip().strip('"\''), utc=True)
        except Exception:
            return None

    reset_state = str(os.getenv("FULL_RESET_STATE", "")).lower() in ("1","true","yes","y")
    rebuild_from = _parse_env_ts(os.getenv("FULL_REBUILD_FROM_UTC", ""))  # e.g. "2024-01-01T00:00:00Z"

    # last committed FULL ts
    last_ts_before = None if (reset_state or rebuild_from is not None) else read_last_ts_full(bsc)

    # read existing clean once (for bridge) — we’ll reuse it to build datasets after we append
    clean_old = read_csv_blob(bsc, clean_blob, parse_ts=True)
    tail = clean_old.tail(2) if not clean_old.empty else None

    # compute step1
    step1 = step1_concatenate_and_energy(df, tail)

    # --- Air effective-cap + synthetic advance (bounded) ---
    MAX_PAD_MIN = int(os.getenv("AIR_MAX_PAD_MIN", "60"))
    air_last = last_real_air_ts(step1)

    # Build a temp frame to decide effective cap (use the same timestamps as step1)
    # We haven’t run interpolation/impute yet; that’s OK.
    step1_cap, air_last, air_effective_last, n_syn = mark_and_fill_air_synthetic(step1, air_last, max_pad_min=MAX_PAD_MIN)

    if air_effective_last is not None and air_last is not None and air_effective_last > air_last and n_syn > 0:
        log.info(f"[air] synthetic advance: {air_last} → {air_effective_last} rows={n_syn}")
        try:
            log_air_synthetic_use(bsc, air_last, air_effective_last, n_syn, reason="decode_gap_forward_fill")
        except Exception:
            pass
    else:
        # no gap or no advance
        pass

    # continue the pipeline on the capped/marked frame
    step1 = step1_cap

    if last_ts_before is not None and not step1.empty:
        step1 = step1[pd.to_datetime(step1["timestamp"], errors="coerce", utc=True) > last_ts_before]
        step1 = step1.sort_values("timestamp").reset_index(drop=True)

    if step1.empty:
        # nothing new — ensure datasets & common_t0 headers and try appending > last_ts_before (no-op)
        try:
            if not clean_old.empty:
                if DO_DATASETS:
                    build_and_write_datasets_full(bsc, clean_old, t0_min_inclusive=last_ts_before)
                    if os.getenv("COMMON_T0_STRICT_REBUILD", "0").lower() in ("1","true","y"):
                        build_and_update_common_t0_full(bsc)
                        validate_common_t0(bsc)
                else:
                    logger.info("[full] SKIP_FULL_DATASETS=1 → skipping datasets/common_t0 (no-new-data branch).")
            
                try:
                    build_online_rollup_csv(bsc, clean_old)
                except Exception as e:
                    logger.warning(f"[online] rollup (no-new-data) failed: {e}")
        except Exception:
            logger.exception("[full] error in no-new-data branch")
        return {"processed": 0, "skipped": False}

    # minimal cleaning/impute
    groups = group_columns(step1.columns.tolist())
    normalize_occupancy(step1, groups.get("desk_occupancy", []))
    clip_ranges_to_nan(step1, groups)
    cols_to_interp = sum([groups.get(g, []) for g in ["desk_temperature","desk_humidity","air_t","air_h","tempex","air_co2","air_cov"] if g in groups], [])
    time_interp_short(step1, cols_to_interp, limit=2)
    add_room_aggregates_and_time(step1, groups, tz=TZ_LOCAL)
    ml_impute_inplace(step1, models, donor_map, groups, min_present_frac=0.5)

    # append step1 & clean (dedup on timestamp)
    step1 = step1.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    append_csv_dedup(bsc, step1_blob, step1, subset_keys=["timestamp"])


    import re
    per_desk_rx = re.compile(r"^Desk_\d{2}-\d{2}_(temperature|humidity|occupancy)$")
    cleaned = step1.copy()
    keep_cols = [c for c in cleaned.columns if not per_desk_rx.match(c) and c != "seg_id"]
    dropped_cols = sorted(set(cleaned.columns) - set(keep_cols))
    if dropped_cols:
        log.info("[prune] dropping %d per-desk columns from clean_full (e.g., %s)",
                len(dropped_cols), dropped_cols[:8])
    cleaned = cleaned.loc[:, keep_cols]
    append_csv_dedup(bsc, clean_blob, cleaned, subset_keys=["timestamp"])

    # build datasets using (clean_old + cleaned) in-memory, avoiding re-read
    full_clean_now = pd.concat([clean_old, cleaned], ignore_index=True)
    full_clean_now = full_clean_now.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)

    try:
        if DO_DATASETS:
            build_and_write_datasets_full(bsc, full_clean_now, t0_min_inclusive=last_ts_before)  # ignored by the writer now
            if os.getenv("COMMON_T0_STRICT_REBUILD","0").lower() in ("1","true","y"):
                build_and_update_common_t0_full(bsc)
                validate_common_t0(bsc)
        else:
            logger.info("[full] SKIP_FULL_DATASETS=1 → skipping build_and_write_datasets_full and build_and_update_common_t0_full.")

    except Exception:
        logger.exception("[full] error while building datasets/common_t0 (skipped or failed)")

    new_last_ts = pd.to_datetime(cleaned["timestamp"], errors="coerce", utc=True).max()
    log.info(f"[full] appended: step1={len(step1)} cleaned={len(cleaned)}  new_last_ts={new_last_ts}")

    write_last_ts_full(bsc, new_last_ts)
    try:
        build_online_rollup_csv(bsc, full_clean_now)
    except Exception as e:
        logger.warning(f"[online] rollup failed: {e}")
        
    return {
        "processed": int(len(cleaned)),
        "last_ts": new_last_ts.isoformat() if pd.notna(new_last_ts) else None,
        "step1_blob": step1_blob,
        "clean_blob": clean_blob,
        "state_blob": FULL_STATE_BLOB,
    }

# --- Add (or keep) these near your ONLINE_* config ---
ONLINE_PREFIX       = os.getenv("ONLINE_PREFIX", "data_for_ml/online").rstrip("/")
ONLINE_ROLLUP_CSV   = os.getenv("ONLINE_ROLLUP_CSV", f"{ONLINE_PREFIX}/latest_rollup.csv")
ONLINE_ROLLUP_LBL   = os.getenv("ONLINE_ROLLUP_LABELS", f"{ONLINE_PREFIX}/latest_rollup_horizons.txt")
ONLINE_LAST_TS_TXT  = os.getenv("ONLINE_LAST_TS_TXT", f"{ONLINE_PREFIX}/last_timestamp.txt")

# EXACT column order (no leading 'feature' column)
CURR_ORDER = [
    "Air_05-01_CO2","Air_05-01_COV","Air_05-01_H","Air_05-01_T",
    "Room_05-01_people_count_all","Room_05-01_people_count_max",
    "Son_05-01_lmax","Son_05-01_leq","Son_05-01_spl",
    "Son_05-02_lmax","Son_05-02_leq","Son_05-02_spl",
    "TempEx_05-01_temperature","TempEx_05-01_humidity",
    "Desk_all_occupancy_sum","Desk_all_temperature_mean","Desk_all_temperature_std",
    "Desk_all_humidity_mean","Desk_all_humidity_std",
]
PREV_BASES = [
    "Air_05-01_CO2","Air_05-01_COV","Air_05-01_H","Air_05-01_T",
    "Room_05-01_people_count_all","Room_05-01_people_count_max",
    "Son_05-01_lmax","Son_05-01_leq","Son_05-01_spl",
    "Son_05-02_lmax","Son_05-02_leq","Son_05-02_spl",
    "TempEx_05-01_temperature","TempEx_05-01_humidity",
    "Desk_all_occupancy_sum","Desk_all_temperature_mean","Desk_all_temperature_std",
    "Desk_all_humidity_mean","Desk_all_humidity_std",
]
TIMEFEATS_ORDER = [
    "tf_hour","tf_minute","tf_weekday","tf_month","tf_dayofyear","tf_weekofyear",
    "tf_is_weekend","tf_is_workhour","tf_hour_sin","tf_hour_cos",
    "tf_dow_sin","tf_dow_cos","tf_mon_sin","tf_mon_cos",
]

def _py_scalar(v):
    import numpy as np, pandas as pd
    if v is None: return None
    if pd.isna(v): return None
    if isinstance(v, np.generic): return v.item()
    return v

def _time_feats_from_single_t0_row(t0: pd.Timestamp) -> dict:
    s = pd.Series([t0], name="t0")
    tmp = add_time_features_from_t0(pd.DataFrame({"t0": s}), t0_col="t0").iloc[0]
    return {k: _py_scalar(tmp[k]) for k in TIMEFEATS_ORDER if k in tmp}

def build_online_rollup_csv(bsc: BlobServiceClient, full_clean_df: pd.DataFrame):
    """
    Writes:
      - data_for_ml/online/latest_rollup.csv  (49 rows, columns start at duration_h)
      - data_for_ml/online/latest_rollup_horizons.txt  (49 lines, labels in row order)
      - data_for_ml/online/last_timestamp.txt  (ISO t0)
    """
    if full_clean_df is None or full_clean_df.empty:
        return

    df = full_clean_df.copy()
    df[TS_COL] = pd.to_datetime(df[TS_COL], errors="coerce", utc=True)
    df = df.dropna(subset=[TS_COL]).sort_values(TS_COL).reset_index(drop=True)
    if df.empty: return

    t0 = df[TS_COL].max()
    df_idx = df.set_index(TS_COL).sort_index()

    # ensure aggregates exist for current snapshot if missing
    groups = group_columns(df.columns.tolist())
    need_agg = any(c not in df.columns for c in [
        "Desk_all_occupancy_sum","Desk_all_temperature_mean","Desk_all_temperature_std",
        "Desk_all_humidity_mean","Desk_all_humidity_std"
    ])
    if need_agg:
        tmp = df.copy()
        add_room_aggregates_and_time(tmp, groups, tz=TZ_LOCAL)
        for c in ["Desk_all_occupancy_sum","Desk_all_temperature_mean","Desk_all_temperature_std",
                  "Desk_all_humidity_mean","Desk_all_humidity_std"]:
            if c in tmp.columns and c not in df.columns:
                df[c] = tmp[c]
        df_idx = df.set_index(TS_COL).sort_index()

    # current snapshot at t0
    curr = {}
    snap_cols_present = [c for c in CURR_ORDER if c in df_idx.columns]
    if snap_cols_present:
        try:
            row = df_idx.loc[t0, snap_cols_present]
        except KeyError:
            row = df_idx.iloc[-1][snap_cols_present]
        curr = {c: _py_scalar(row.get(c)) for c in snap_cols_present}

    # time features at t0
    tf = _time_feats_from_single_t0_row(t0)

    # horizon ordering (exact row order)
    minute_labels = [f"{m}m" for m in MINUTE_HORIZONS]
    hour_labels   = [f"{h}h" for h in HOUR_HORIZONS]
    day_labels    = [f"{d}d" for d in DAY_HORIZONS]
    week_labels   = [f"{w}w" for w in WEEK_HORIZONS]
    labels = minute_labels + hour_labels + day_labels + week_labels
    win_minutes = (
        [m for m in MINUTE_HORIZONS] +
        [h*60 for h in HOUR_HORIZONS] +
        [d*24*60 for d in DAY_HORIZONS] +
        [w*7*24*60 for w in WEEK_HORIZONS]
    )

    rows = []
    for minutes in win_minutes:
        prev_aggs = compute_prev_aggs_for_params(df_idx, PREV_BASES, window_minutes=minutes)
        cons_prev = compute_prev_cons_anchored(df_idx, COUNTER_COL, window_minutes=minutes, tol_min=None)

        rec = {"t0": t0.isoformat(),"duration_h": float(minutes)/60.0}

        # current snapshot
        for c in CURR_ORDER:
            rec[c] = _py_scalar(curr.get(c))

        # time features
        for c in TIMEFEATS_ORDER:
            rec[c] = _py_scalar(tf.get(c))

        # prev aggregates (mean/min/max)
        if not prev_aggs.empty:
            last_prev = prev_aggs.iloc[-1]
            for base in PREV_BASES:
                rec[f"{base}_prev_mean"] = _py_scalar(last_prev.get(f"{base}_prev_mean"))
                rec[f"{base}_prev_min"]  = _py_scalar(last_prev.get(f"{base}_prev_min"))
                rec[f"{base}_prev_max"]  = _py_scalar(last_prev.get(f"{base}_prev_max"))
        else:
            for base in PREV_BASES:
                rec[f"{base}_prev_mean"] = None
                rec[f"{base}_prev_min"]  = None
                rec[f"{base}_prev_max"]  = None

        # deltas
        if not cons_prev.empty:
            last_cons = cons_prev.iloc[-1]
            rec["cons_prev"] = _py_scalar(last_cons.get("cons_prev"))
            rec["cons_rate_prev"] = _py_scalar(last_cons.get("cons_rate_prev"))
        else:
            rec["cons_prev"] = None
            rec["cons_rate_prev"] = None

        rows.append(rec)

    out = pd.DataFrame(rows)

    # enforce final column order EXACTLY as required
    final_order = (
        ["t0","duration_h"] +
        CURR_ORDER +
        TIMEFEATS_ORDER +
        [f"{b}_{suf}" for b in PREV_BASES for suf in ("prev_mean","prev_min","prev_max")] +
        ["cons_prev","cons_rate_prev"]
    )
    out = out.reindex(columns=[c for c in final_order if c in out.columns])

    # write CSV + labels + last ts
    import io
    buf = io.StringIO()
    out.to_csv(buf, index=False)
    upload_blob_bytes(bsc, ONLINE_ROLLUP_CSV, buf.getvalue().encode("utf-8"), overwrite=True)

    # horizons sidecar (row i → labels[i])
    upload_blob_bytes(bsc, ONLINE_ROLLUP_LBL, ("\n".join(labels) + "\n").encode("utf-8"), overwrite=True)

    # freshness file
    upload_blob_bytes(bsc, ONLINE_LAST_TS_TXT, (t0.isoformat() + "\n").encode("utf-8"), overwrite=True)
    
    log.info(f"[online] wrote rollup rows={len(out)} t0={t0.isoformat()} "
         f"csv={ONLINE_ROLLUP_CSV} labels={ONLINE_ROLLUP_LBL} tsfile={ONLINE_LAST_TS_TXT}")
