import os
import io
import base64
import json
import re
import threading
from datetime import datetime, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, send_file, request


app = Flask(__name__)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# Public share link on Yandex Disk where the Excel tender files live.  When
# updating this value please include the full URL to the folder as provided by
# the customer.
YDX_PUBLIC_LINK = os.environ.get(
    "YDX_PUBLIC_LINK", "https://disk.yandex.ru/d/sbXFGWQRkrFssA"
)

# Mapping file (Privyazka) location on GitHub.  The code will download this
# Excel file on startup to map cities or settlements to branch names (filials).
# Should the file ever move within the repository you can update this path
# accordingly.  The raw.githubusercontent.com domain is used here because it
# serves files without HTML wrappers and does not require an access token.
PRIVYAZKA_URL = os.environ.get(
    "PRIVYAZKA_URL",
    "https://raw.githubusercontent.com/Anatoliy031/KK-Adigeya/main/Privyazka.xlsx",
)

# Filial names expected on the final page.  The last entry 'Прочие' is used
# for records that cannot be associated with any branch.
FILIAL_NAMES = [
    "Адыгейские ЭС",
    "Армавирские ЭС",
    "Краснодарские ЭС",
    "Лабинские ЭС",
    "Ленинградские ЭС",
    "Славянские ЭС",
    "Сочинские ЭС",
    "Тимашевские ЭС",
    "Тихорецкие ЭС",
    "Усть-Лабинские ЭС",
    "Юго-Западные ЭС",
    "Прочие",  # 12th conditional “branch” for unmatched records
]

# Data container for processed tenders.  This is a dictionary keyed by
# filial name containing a list of dictionaries (rows) with tender
# information.  It is populated by the ``update_data`` function.
data_by_filial: dict[str, list[dict]] = {name: [] for name in FILIAL_NAMES}

# Timestamp of the last update; used to display when the dataset was refreshed.
last_update: datetime | None = None


def download_yandex_listing(public_link: str) -> dict:
    """Retrieve a listing of files in a public Yandex.Disk folder.

    The Yandex Disk REST API supports listing the contents of a publicly shared
    folder using the ``public/resources`` endpoint.  The returned JSON
    structure contains an ``embedded`` section with an ``items`` array of
    resources.  See https://yandex.ru/dev/disk/poligon/ for more details.

    Args:
        public_link: The full URL of the public share (e.g. ``https://disk.yandex.ru/d/...``).

    Returns:
        A dictionary representing the JSON response.
    """
    params = {
        "public_key": public_link,
        "limit": 1000,
        "path": "/",
    }
    url = "https://cloud-api.yandex.net/v1/disk/public/resources"
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def download_yandex_file(public_link: str, path: str) -> bytes:
    """Download a single file from a Yandex.Disk public folder.

    The API call returns a short-lived URL inside the ``href`` field that can
    then be used to fetch the actual bytes.  This additional indirection
    prevents unauthenticated clients from enumerating all files directly.

    Args:
        public_link: The public link to the shared folder.
        path: The relative path inside the folder returned by the listing.

    Returns:
        The raw bytes of the requested file.
    """
    url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
    params = {
        "public_key": public_link,
        "path": path,
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    href = response.json().get("href")
    if not href:
        raise RuntimeError(f"No download href returned for path {path}")
    download_resp = requests.get(href, timeout=120)
    download_resp.raise_for_status()
    return download_resp.content


def load_mapping() -> pd.DataFrame:
    """Download and parse the Privyazka mapping file from GitHub.

    The mapping file associates cities or settlements with filial names.  The
    expected format is an Excel workbook with at least two columns: column A
    containing a locality (city or settlement) and column B containing the
    corresponding filial.  Rows with missing data are ignored.

    Returns:
        A DataFrame with two columns: ``locality`` and ``filial``.  Both are
        lowercased to simplify matching.
    """
    try:
        resp = requests.get(PRIVYAZKA_URL, timeout=60)
        resp.raise_for_status()
        excel_bytes = resp.content
    except Exception as ex:
        # If the mapping file cannot be retrieved, log the error and return an
        # empty DataFrame.  The app will still run but unmatched tenders will
        # be placed into the "Прочие" group.
        print(f"Warning: unable to download mapping file: {ex}")
        return pd.DataFrame(columns=["locality", "filial"])
    try:
        df_map = pd.read_excel(io.BytesIO(excel_bytes), dtype=str)
    except Exception as ex:
        print(f"Warning: cannot parse mapping file: {ex}")
        return pd.DataFrame(columns=["locality", "filial"])
    # Drop completely empty rows and rename columns to consistent names
    df_map = df_map.dropna(how="all")
    # Normalize column names by stripping whitespace and lowering
    df_map.columns = [str(col).strip().lower() for col in df_map.columns]
    # Identify columns that likely contain locality and filial names.  Since the
    # provided file uses Russian names (e.g. ``Filial``), we search for
    # substrings.
    locality_col = None
    filial_col = None
    for col in df_map.columns:
        if locality_col is None and ("locality" in col or "насел" in col or "город" in col):
            locality_col = col
        if filial_col is None and ("filial" in col or "филиал" in col):
            filial_col = col
    if locality_col is None or filial_col is None:
        # Fallback: assume first two columns
        if df_map.shape[1] >= 2:
            locality_col = df_map.columns[0]
            filial_col = df_map.columns[1]
        else:
            return pd.DataFrame(columns=["locality", "filial"])
    df_map = df_map[[locality_col, filial_col]].dropna(how="any")
    df_map = df_map.rename(columns={locality_col: "locality", filial_col: "filial"})
    # Lowercase for comparison
    df_map["locality"] = df_map["locality"].str.strip().str.lower()
    df_map["filial"] = df_map["filial"].str.strip()
    return df_map


def determine_filial(locality: str, mapping_df: pd.DataFrame) -> str:
    """Determine the filial for a given locality based on the mapping table.

    Args:
        locality: The locality string extracted from the tender row.
        mapping_df: DataFrame containing ``locality`` and ``filial`` columns.

    Returns:
        The name of the filial if a match is found; otherwise returns the
        placeholder "Прочие".
    """
    if not locality:
        return FILIAL_NAMES[-1]
    loc_lower = locality.lower()
    # Search for an exact match first
    match = mapping_df[mapping_df["locality"] == loc_lower]
    if not match.empty:
        return match.iloc[0]["filial"]
    # Fallback: look for substring matches
    for _, row in mapping_df.iterrows():
        if row["locality"] in loc_lower:
            return row["filial"]
    return FILIAL_NAMES[-1]


def parse_tender_files(files_json: dict, mapping_df: pd.DataFrame) -> dict[str, list[dict]]:
    """Download, parse and classify tender data from Excel files.

    Args:
        files_json: The JSON dictionary returned by ``download_yandex_listing``.
        mapping_df: DataFrame with the locality→filial mapping.

    Returns:
        A dictionary keyed by filial names containing a list of row dictionaries.
    """
    result: dict[str, list[dict]] = {name: [] for name in FILIAL_NAMES}
    items = files_json.get("_embedded", {}).get("items", [])
    # Only process files with .xls or .xlsx extension
    excel_items = [item for item in items if isinstance(item, dict) and item.get("name", "").lower().endswith((".xlsx", ".xls"))]
    for item in excel_items:
        path = item.get("path")
        name = item.get("name")
        if not path:
            continue
        try:
            file_bytes = download_yandex_file(YDX_PUBLIC_LINK, path)
            df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
        except Exception as ex:
            # Skip files that cannot be downloaded or parsed
            print(f"Warning: failed to process {name}: {ex}")
            continue
        # Normalize column names by stripping whitespace
        df.columns = [str(c).strip() for c in df.columns]
        # Identify the column that contains the tender name (Наименование закупки)
        tender_col = None
        for col in df.columns:
            if "наименование" in col.lower():
                tender_col = col
                break
        # Identify a potential locality column (e.g. "Населенный пункт" or similar)
        locality_col = None
        for col in df.columns:
            if "насел" in col.lower() or "город" in col.lower() or "мест" in col.lower():
                locality_col = col
                break
        if tender_col is None:
            continue
        # Fill NaNs with empty strings for ease of filtering
        df[tender_col] = df[tender_col].fillna("")
        # Group 1: tenders related to outdoor lighting containing the substring 'наруж'
        mask_group1 = df[tender_col].str.contains("наруж", case=False, na=False)
        # Group 2: construction/installation works: contains 'КЛ-' or 'ВЛ-' or 'ТП', but not 'отпу' or 'БКТП'
        mask_include = df[tender_col].str.contains(r"(КЛ-|ВЛ-|\bТП\b)", case=False, na=False, regex=True)
        mask_exclude = df[tender_col].str.contains("отпу|БКТП", case=False, na=False)
        mask_group2 = mask_include & ~mask_exclude
        df_filtered = df[mask_group1 | mask_group2]
        if df_filtered.empty:
            continue
        # Iterate through filtered rows
        for _, row in df_filtered.iterrows():
            row_dict = row.to_dict()
            # Determine locality string if available
            locality_value = None
            if locality_col and pd.notna(row.get(locality_col)):
                locality_value = str(row.get(locality_col))
            filial = determine_filial(locality_value, mapping_df)
            # Unknown filials fallback to placeholder
            if filial not in result:
                filial = FILIAL_NAMES[-1]
            result[filial].append(row_dict)
    return result


def update_data():
    """Fetch the latest tender files and refresh the in-memory data store.

    This function downloads the list of Excel files from the Yandex Disk public
    folder, parses each file according to the defined rules, and updates the
    global ``data_by_filial`` dictionary.  It also updates the ``last_update``
    timestamp.  If any network or parsing errors occur, the previous data
    remains unchanged.
    """
    global data_by_filial, last_update
    print("Starting data refresh...")
    try:
        listing = download_yandex_listing(YDX_PUBLIC_LINK)
        mapping_df = load_mapping()
        new_data = parse_tender_files(listing, mapping_df)
        # Replace the global data atomically
        data_by_filial = new_data
        last_update = datetime.utcnow()
        print("Data refresh completed successfully.")
    except Exception as ex:
        # Log the error; retain old data on failure
        print(f"Error during data refresh: {ex}")


def schedule_daily_update(interval_hours: float = 24.0):
    """Schedule the ``update_data`` function to run periodically.

    Args:
        interval_hours: Number of hours between updates.  Defaults to 24 hours.
    """
    def run_periodically():
        while True:
            update_data()
            # Sleep for the specified interval
            seconds = interval_hours * 3600
            for _ in range(int(seconds)):
                # Use a short sleep loop to allow graceful shutdowns (not needed here)
                time.sleep(1)

    import time  # imported locally to avoid global import if scheduling is not used
    t = threading.Thread(target=run_periodically, daemon=True)
    t.start()


@app.route("/")
def index():
    """Render the main page with the available filial names and last update time."""
    return render_template(
        "index.html",
        filials=FILIAL_NAMES,
        last_update=last_update.strftime("%Y-%m-%d %H:%M UTC") if last_update else "никогда",
    )


@app.route("/data")
def data_endpoint():
    """Return JSON representation of the tender registry.

    The optional ``filial`` query parameter filters the result down to a single
    branch.  Without a query parameter, all data is returned.
    """
    filial = request.args.get("filial")
    if filial:
        # Normalize case and fallback to placeholder if unknown
        filial_name = filial.strip()
        if filial_name not in data_by_filial:
            filial_name = FILIAL_NAMES[-1]
        return jsonify({filial_name: data_by_filial.get(filial_name, [])})
    # Return the whole data
    return jsonify(data_by_filial)


@app.route("/export")
def export_csv():
    """Export selected tender records to CSV.

    Accepts a ``filial`` query parameter and an optional ``indices`` list of row
    indices (0-based within that filial) specifying which rows to include.  If
    no indices are provided, all rows for the filial are exported.  If no
    filial is specified, the function returns an empty file.
    """
    filial = request.args.get("filial")
    indices_param = request.args.get("indices")
    if not filial or filial not in data_by_filial:
        return send_file(io.BytesIO(b""), as_attachment=True, download_name="export.csv")
    rows = data_by_filial[filial]
    if indices_param:
        try:
            indices = [int(i) for i in indices_param.split(",") if i.isdigit()]
            selected_rows = [rows[i] for i in indices if 0 <= i < len(rows)]
        except Exception:
            selected_rows = rows
    else:
        selected_rows = rows
    # Convert to DataFrame and write to CSV in memory
    df = pd.DataFrame(selected_rows)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    # Return as CSV download
    return send_file(
        io.BytesIO(buffer.getvalue().encode("utf-8")),
        as_attachment=True,
        download_name=f"{filial}.csv",
        mimetype="text/csv",
    )


if __name__ == "__main__":
    # Perform an initial data update on startup
    update_data()
    # Optionally schedule periodic updates; comment out the following line if
    # another scheduler or cron job will trigger ``update_data`` externally.
    schedule_daily_update(24)
    # Start the Flask application
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))