import sys
import os
from shared import constants
from shared.br_uncompress import compute_data_absolute_timestamp, uncompress
from datetime import datetime
import logging
import pandas as pd



def clean_air_df(df: pd.DataFrame, current_hour_timestamp: datetime):

    desired_columns = ["values", "raw", "device", "deveui", "timestamp", "metadata"]
    if not df.columns.isin(desired_columns).sum() or df.empty:
        logging.debug("No air sensor data available")
        return pd.DataFrame()

    df, dropped_rows = drop_air_rows(df.reset_index())
    logging.debug("dropped %d rows from the air df", len(dropped_rows))

    try:
        # Desencode values
        df[["values", "batch_relative_timestamp"]] = df["raw"].apply(
    lambda raw: pd.Series(_safe_decode_air(raw))
)
        # Drop rows that still failed to decode
        df = df.dropna(subset=["values", "batch_relative_timestamp"]).reset_index(drop=True)


        # Flatten values
        flattened_values = pd.json_normalize(df["values"].values)

        # Add to df
        new_df = (
            pd.DataFrame(flattened_values.values.tolist(), index=flattened_values.index)
            .stack()
            .apply(pd.Series)
            .reset_index(level=1, drop=True)
        )
        new_df.columns = [
            "data_relative_timestamp",
            "value",
            "data.label",
            "label_name",
        ]
        r_df_reset = df.reset_index(drop=True)
        new_df["device"] = r_df_reset["device"]
        new_df["timestamp"] = r_df_reset["timestamp"]
        new_df["batch_relative_timestamp"] = r_df_reset["batch_relative_timestamp"]
        # Compute time_stamp
        new_df["timestamp"] = new_df["timestamp"].apply(
            lambda x: reformat_time_stamp(x)
        )
        new_df["timestamp"] = new_df.apply(
            lambda x: compute_data_absolute_timestamp(
                x["timestamp"],
                x["batch_relative_timestamp"],
                x["data_relative_timestamp"],
            ),
            axis=1,
        )

        # Cast timestamp into datetime
        new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], utc=True)
        new_df["timestamp"] = new_df["timestamp"].dt.floor("10min")

        # Preprocessing
        new_df = pivot_table_air(new_df)
        new_df = add_timestamp(new_df, current_hour_timestamp)

        columns_to_divide = ["Air_05-01_T", "Air_05-01_H"]
        for col in columns_to_divide:
            if col in new_df.columns:
                new_df[col] = new_df[col].apply(lambda x: x / 100)

        # ffill and bfill the new df
        new_df.ffill(inplace=True)
        new_df.bfill(inplace=True)

        new_df.reset_index(inplace=True, drop=True)
    except Exception as e:
        logging.error(
            f"An error occurred while cleaning the air data, skipping them. The columns: {df.columns}, the error: {e}"
        )
        new_df = pd.DataFrame()

    return new_df

def _safe_decode_air(raw):
    """
    Return (dataset, batch_relative_timestamp) or (None, None) on failure.
    Keeps the assignment shape stable and avoids 'Columns must be same length as key'.
    """
    try:
        r = get_air_value(raw)
        return r.get("dataset", None), r.get("batch_relative_timestamp", None)
    except Exception:
        return None, None


def clean_air_df_v2(df: pd.DataFrame, current_hour_timestamp: datetime):
    desired_columns = ["values", "raw", "device", "timestamp"]
    ignored_columns_for_replicating = [
        "timestamp",
        "index",
        "values",
        "batch_relative_timestamp",
    ]  # necessaire pour répliquer les datas dans les lignes

    if not df.columns.isin(desired_columns).sum() or df.empty:
        logging.debug("No air sensor data available")
        return pd.DataFrame()
    df, dropped_rows = drop_air_rows(df.reset_index())
    logging.debug(f"dropped {len(dropped_rows)} rows from the air df")
    try:
        # Desencode values
        df[["values", "batch_relative_timestamp"]] = df["raw"].apply(
    lambda raw: pd.Series(_safe_decode_air(raw))
)
        # Drop rows that still failed to decode
        df = df.dropna(subset=["values", "batch_relative_timestamp"]).reset_index(drop=True)

        # Flatten values
        flattened_values = pd.json_normalize(df["values"].values)
        # Add to df
        new_df = (
            pd.DataFrame(flattened_values.values.tolist(), index=flattened_values.index)
            .stack()
            .apply(pd.Series)
            .reset_index(level=1, drop=True)
        )
        new_df.columns = [
            "data_relative_timestamp",
            "value",
            "data.label",
            "label_name",
        ]
        r_df_reset = df.reset_index(drop=True)
        new_df["device"] = r_df_reset["device"]
        new_df["timestamp"] = r_df_reset["timestamp"]
        new_df["batch_relative_timestamp"] = r_df_reset["batch_relative_timestamp"]

        new_df["timestamp"] = new_df["timestamp"].apply(
            lambda x: reformat_time_stamp(x)
        )
        new_df["timestamp"] = new_df.apply(
            lambda x: compute_data_absolute_timestamp(
                x["timestamp"],
                x["batch_relative_timestamp"],
                x["data_relative_timestamp"],
            ),
            axis=1,
        )
        # Cast timestamp into datetime
        new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], utc=True)
        new_df["timestamp"] = new_df["timestamp"].dt.floor("10min")

        # Preprocessing
        new_df = pivot_table_air(new_df)
        new_df = add_timestamp(new_df, current_hour_timestamp)
        columns_to_divide = ["Air_05-01_T", "Air_05-01_H"]
        for col in columns_to_divide:
            if col in new_df.columns:
                new_df[col] = new_df[col].apply(lambda x: x / 100)

        # ffill and bfill the new df
        new_df.ffill(inplace=True)
        new_df.bfill(inplace=True)

        new_df.reset_index(inplace=True, drop=True)

        # Ajouter les autres colonnes notament metadata
        for col in df.columns:
            if col not in ignored_columns_for_replicating:
                new_df[col] = str(df[col][0])
    except Exception as e:
        logging.error(
            f"An error occurred while cleaning the air data, skipping them. The columns: {df.columns}, the error: {e}"
        )
        new_df = pd.DataFrame()
    return new_df


def drop_air_rows(air_df: pd.DataFrame):
    rows_to_drop = []
    for i, row in air_df.iterrows():
        if pd.notnull(row["values"]) and row["values"] not in ({}, "{}", "[]"):
            continue
        try:
            _ = get_air_value(row["raw"])
        except Exception as e:
            logging.error(
                f"An error occurred trying to get air value from raw because it is a standard tram raw : {row['raw']} : {e}"
            )
            rows_to_drop.append(i)

    # Drop rows with null values in the 'values' column
    return (air_df.drop(rows_to_drop), rows_to_drop)


def get_air_value(raw_value):
    result = uncompress(
        3,
        [
            {"taglbl": 1, "resol": 10.0, "sampletype": 7, "lblname": "T"},
            {"taglbl": 2, "resol": 100.0, "sampletype": 6, "lblname": "H"},
            {"taglbl": 3, "resol": 10.0, "sampletype": 6, "lblname": "CO2"},
            {"taglbl": 4, "resol": 10.0, "sampletype": 6, "lblname": "COV"},
        ],
        raw_value,
    )
    return result


def pivot_table_air(df: pd.DataFrame):
    result = df.pivot_table(
        index="timestamp",
        columns=["device", "label_name"],
        values="value",
        aggfunc="first",
    )
    # Aplatir les niveaux d'en-tête de colonne
    result.columns = ["{}_{}".format(device, label) for device, label in result.columns]
    # Réinitialiser l'index
    result.reset_index(inplace=True)
    # Afficher le résultat
    return result


def reformat_time_stamp(original_date_time):
    """Reformat the timestamp to ISO format."""
    if isinstance(original_date_time, pd.Timestamp):
        original_date_time = original_date_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    elif not isinstance(original_date_time, str):
        original_date_time = str(original_date_time)

    dt = datetime.strptime(original_date_time, "%Y-%m-%d %H:%M:%S.%f")
    iso_format = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return iso_format


def add_timestamp(df: pd.DataFrame, current_hour_timestamp: datetime):
    """Resample the DataFrame at 10-minute intervals and fill missing timestamps."""

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    df.sort_values(by="timestamp", inplace=True)
    # Set timestamp as the index
    df.set_index("timestamp", inplace=True)
    # Drop duplicate timestamps, if any
    df = df[~df.index.duplicated()]

    # Resample the DataFrame at 10-minute intervals and fill missing timestamps
    df_resampled = df.resample(rule="10min", origin=current_hour_timestamp).asfreq()
    # Reset the index to convert timestamp back to a column
    df_resampled.reset_index(inplace=True)
    # Fill missing values with the last known value
    df_resampled.ffill(inplace=True)

    return df_resampled
