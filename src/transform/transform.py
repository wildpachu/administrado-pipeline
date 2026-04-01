import os
import glob
import re
import logging
import pandas as pd
from pathlib import Path

# ==========================================
# GLOBAL REGEX PATTERNS
# ==========================================
DIM_PATTERN = re.compile(
    r'(\d+[\.,]\d+|\d+)\s*(?:Ancho|Cm|cm)?\s*[xX]\s*(\d+[\.,]\d+|\d{2,})\s*(?:Alto|Cm|cm)?'
)
CURTAIN_PATTERN = re.compile(r'^[^a-zA-ZñÑ]*(cortina|cortinas)\b', flags=re.IGNORECASE)


# ==========================================
# CLEANING HELPERS
# ==========================================
def clean_amount(value: object) -> int:
    """Parses a sales quantity string from Administrado into an integer.

    Handles formats like "$150.000", "+150000", "150000.0", and NaN.
    Returns 0 for any unparseable value.
    """
    if pd.isna(value): return 0
    s = str(value).replace('+', '').replace('$', '').strip()
    if s.endswith('.0'): s = s[:-2]
    s = s.replace('.', '')
    try: return int(s)
    except (ValueError, AttributeError): return 0

def clean_price(value: object) -> float:
    """Parses a price string from Administrado into a float.

    Handles formats like "$12500,50", "$12500.50", and NaN.
    Returns 0.0 for any unparseable value.
    """
    if pd.isna(value): return 0.0
    s = str(value).replace('$', '').strip()
    s = s.replace(',', '.')
    try: return float(s)
    except (ValueError, AttributeError): return 0.0

def extract_type(title: object) -> str | None:
    """Classifies a curtain listing title into one of three fabric types.

    Returns 'Blackout', 'Sunscreen', or 'Doble'. Returns None if the title
    does not match any known type — callers should drop those rows.
    """
    t = str(title).lower()
    if 'doble' in t: return 'Doble'
    if 'blackout' in t or 'black' in t or 'opaca' in t: return 'Blackout'
    if 'screen' in t or 'sunscreen' in t or 'trasl' in t or 'transl' in t: return 'Sunscreen'
    return None  # unidentifiable → row will be dropped

def normalize(n: str) -> str:
    """Converts a dimension string to whole centimeters.

    Values below 10 are assumed to be in meters and multiplied by 100.
    Accepts both '.' and ',' as decimal separators.
    """
    n = n.replace(',', '.')
    val = float(n)
    if val < 10: return str(int(val * 100))  # meters to cm
    return str(int(val))

def extract_dim(title: object) -> str | None:
    """Extracts the WIDTHxHEIGHTcm dimension string from a curtain listing title.

    Matches patterns like "160x200", "1.60 x 2.00", "160 Cm X 200 Cm".
    Returns None if no valid dimension is found or if the height has fewer
    than 2 digits (likely a data error from Administrado).
    """
    m = DIM_PATTERN.search(str(title))
    if m:
        width_str = m.group(1)
        height_str = m.group(2)
        # discard short numbers returned with errors by administrado (e.g. 160x1)
        if len(height_str) < 2 and '.' not in height_str and ',' not in height_str:
            return None
        width = normalize(width_str)
        height = normalize(height_str)
        return f"{width}x{height}cm"
    return None


# ==========================================
# SKU ASSIGNMENT
# ==========================================
def assign_sku(row, sku_dict):
    """
    Classification logic:
      - No Tipo              → caller already dropped this row
      - Tipo + Dimension     → look up in sku_dict
          found              → real SKU (e.g. CORT0001)
          not found          → "NO TENEMOS"
      - Tipo, no Dimension   → "NO ENCONTRADO"
    """
    if row['Dimension'] is None or pd.isna(row['Dimension']):
        return 'NO ENCONTRADO'
    lookup = row['Tipo'] + row['Dimension']
    return sku_dict.get(lookup, 'NO TENEMOS')


# ==========================================
# MAIN PIPELINE FUNCTION
# ==========================================
def process_market_data(folder_path: str, sku_dict: dict, target_dates: list, output_path: str = None) -> pd.DataFrame:
    """
    Reads competitor Excel files matching any date in target_dates (YYYYMMDD list),
    classifies each row, groups by Tienda + SKU + Tipo + Dimension + Tipo de Publicación,
    and returns a clean DataFrame ready to load.

    Classification:
      - No identifiable Tipo          → dropped
      - Tipo + Dimension in sku_dict  → real SKU
      - Tipo + Dimension not in dict  → "NO TENEMOS"
      - Tipo but no Dimension         → "NO ENCONTRADO"

    If output_path is provided, saves an intermediate CSV before returning.
    """
    all_files = glob.glob(os.path.join(folder_path, '*.xlsx'))
    files = [f for f in all_files if any(d in os.path.basename(f) for d in target_dates)]
    logging.info(f"Market transform: {len(files)}/{len(all_files)} files match dates {target_dates}")

    if not files:
        logging.warning("No .xlsx files found. Returning empty DataFrame.")
        return pd.DataFrame()

    raw_dfs = []

    for file in files:
        filename = os.path.basename(file)
        match = re.search(r'reporte_(.*?)_(\d{8})', filename)
        if match:
            store_name = match.group(1).replace('_', ' ')
            f = match.group(2)
            date_str = f"{f[6:8]}/{f[4:6]}/{f[0:4]}"
        else:
            logging.warning(f"Filename did not match expected pattern, skipping: {filename}")
            continue

        sheets = pd.read_excel(file, sheet_name=None)
        df = pd.DataFrame()

        for _, sheet_df in sheets.items():
            if 'Título' in sheet_df.columns:
                df = sheet_df
                break

        if df.empty:
            logging.warning(f"No sheet with 'Título' column found in: {filename}")
            continue

        # Keep only curtain listings
        df = df[df['Título'].str.contains(CURTAIN_PATTERN, na=False, regex=True)].copy()

        if df.empty:
            logging.info(f"No curtain listings found in: {filename}")
            continue

        # Clean numeric columns
        df['Facturación'] = df['Facturación'].apply(clean_amount)

        # Rename quantity column — validate it exists first
        if 'Cantidad Vendida' in df.columns:
            df = df.rename(columns={'Cantidad Vendida': 'Cantidad'})
        elif 'Cantidad' not in df.columns:
            logging.warning(f"No quantity column found in: {filename}. Defaulting to 0.")
            df['Cantidad'] = 0

        # Extract Tipo and Dimension
        df['Tipo'] = df['Título'].apply(extract_type)
        df['Dimension'] = df['Título'].apply(extract_dim)

        # Drop rows where Tipo is unidentifiable
        before = len(df)
        df = df[df['Tipo'].notna()].copy()
        dropped = before - len(df)
        if dropped:
            logging.info(f"{filename}: dropped {dropped} rows with unidentifiable fabric type.")

        if df.empty:
            continue

        # Assign SKU
        df['SKU'] = df.apply(assign_sku, axis=1, sku_dict=sku_dict)

        # Metadata
        df['Fecha'] = date_str
        df['Tienda'] = store_name
        df['Titulo de Publicacion'] = df['Título']

        logging.info(f"{filename}: {len(df)} rows after classification.")
        raw_dfs.append(df)

    if not raw_dfs:
        logging.warning("No valid records found across all files.")
        return pd.DataFrame()

    df_all = pd.concat(raw_dfs, ignore_index=True)

    # Fill Dimension with empty string for grouping (NO ENCONTRADO rows have None)
    df_all['Dimension'] = df_all['Dimension'].fillna('')

    # Determine grouping columns — Tipo de Publicación may not always exist
    pub_col = 'Tipo de Publicación' if 'Tipo de Publicación' in df_all.columns else None
    group_cols = ['Tienda', 'Fecha', 'SKU', 'Tipo', 'Dimension']
    if pub_col:
        group_cols.append(pub_col)

    agg_cols = {col: 'sum' for col in ['Cantidad', 'Facturación'] if col in df_all.columns}

    df_grouped = df_all.groupby(group_cols, as_index=False).agg(agg_cols)

    # Sort
    df_grouped['_date_sort'] = pd.to_datetime(df_grouped['Fecha'], dayfirst=True)
    df_grouped = df_grouped.sort_values(by=['_date_sort', 'Tienda', 'SKU'])
    df_grouped = df_grouped.drop(columns=['_date_sort'])

    # Final column order
    column_order = ['Fecha', 'Tienda', 'SKU', 'Tipo', 'Dimension', 'Cantidad', 'Facturación']
    if pub_col:
        column_order.append(pub_col)
    existing_columns = [col for col in column_order if col in df_grouped.columns]
    df_grouped = df_grouped[existing_columns]

    logging.info(f"Market transform complete: {len(df_grouped)} grouped rows.")

    # Save intermediate output if path provided
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df_grouped.to_csv(output_path, index=False)
        logging.info(f"Intermediate result saved to: {output_path}")

    return df_grouped


# ==========================================
# DIRECT EXECUTION ENTRY POINT
# ==========================================
if __name__ == '__main__':
    from config import PATH_RAW_MARKET, PATH_PROCESSED_MARKET, CREDENTIALS_FILE, GOOGLE_SHEET_URL
    from src.utils.utils import setup_logger
    from src.load.load_sheets import read_sheet, upload_dataframe

    setup_logger()

    from main import get_target_dates
    target_dates  = get_target_dates()
    date_suffix   = target_dates[0] if len(target_dates) == 1 else f"{target_dates[0]}_{target_dates[-1]}"

    logging.info("Loading SKU dictionary from Google Sheets...")
    df_skus = read_sheet(CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Maestro SKU')
    df_skus['reference'] = df_skus['con cm'].astype(str).str.replace(' ', '')
    sku_dict = dict(zip(df_skus['reference'], df_skus['SKU']))
    logging.info(f"SKU dictionary loaded: {len(sku_dict)} entries.")

    df_result = process_market_data(
        folder_path=PATH_RAW_MARKET,
        sku_dict=sku_dict,
        target_dates=target_dates,
        output_path=f"{PATH_PROCESSED_MARKET}/market_clean_{date_suffix}.csv",
    )

    if not df_result.empty:
        logging.info("Uploading to Google Sheets...")
        upload_dataframe(df_result, CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Limpios')
        logging.info("Done.")
