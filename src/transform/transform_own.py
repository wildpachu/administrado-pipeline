import re
import logging
import pandas as pd
from pathlib import Path
from config import OWN_STORE_NAME

DATE_PATTERN = re.compile(r'_(\d{8})\d*\.xlsx$', re.IGNORECASE)

# Expected column names in the own-store Excel export
COL_DATE       = 'Fecha'
COL_SKU        = 'SKU de la variación'
COL_STATUS     = 'Estado de la orden'
COL_MLA        = 'Número de publicación'
COL_QUANTITY   = 'Cantidad'
COL_REVENUE    = 'Precio total'


# ==========================================
# FILE DISCOVERY
# ==========================================
def find_files(folder: str) -> tuple[list[str], str]:
    """Finds the most recently downloaded own-store Excel files in a folder.

    Scans for files matching the date pattern _YYYYMMDD.xlsx, picks the latest
    date, and returns up to 2 file paths (Administrado splits large exports).

    Returns:
        A tuple of (list of file paths, date string as YYYYMMDD).

    Raises:
        FileNotFoundError: If the folder doesn't exist or contains no valid files.
    """
    directory = Path(folder)

    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"Folder '{folder}' does not exist or is not a directory.")

    valid_files = []
    for file in directory.iterdir():
        if file.is_file():
            match = DATE_PATTERN.search(file.name)
            if match:
                valid_files.append((match.group(1), file))

    if not valid_files:
        raise FileNotFoundError(f"No valid .xlsx files found in {folder}")

    latest_date = max(valid_files, key=lambda x: x[0])[0]
    latest_files = [str(path) for date, path in valid_files if date == latest_date]

    return latest_files[:2], latest_date


# ==========================================
# LOOKUP BUILDERS
# ==========================================
def build_mla_dict(df_mla: pd.DataFrame) -> dict:
    """
    Builds a dict {mla_number: publication_type} from the Maestro MLA sheet.
    Uses the first column as key and second column as value.
    """
    col_mla  = df_mla.columns[0]
    col_type = df_mla.columns[1]
    return dict(zip(df_mla[col_mla].astype(str).str.strip(), df_mla[col_type].astype(str).str.strip()))


def build_sku_attributes(df_skus: pd.DataFrame) -> dict:
    """
    Builds a dict {sku: (tipo, dimension)} from the Maestro SKU sheet.
    'con cm' column format: "Blackout 220x220cm" → Tipo='Blackout', Dimension='220x220cm'
    """
    result = {}
    for _, row in df_skus.iterrows():
        sku = str(row['SKU']).strip()
        con_cm = str(row['con cm']).strip()
        parts = con_cm.split(' ', 1)
        tipo      = parts[0] if len(parts) > 0 else ''
        dimension = parts[1] if len(parts) > 1 else ''
        result[sku] = (tipo, dimension)
    return result


# ==========================================
# MAIN PIPELINE FUNCTION
# ==========================================
def process_own_data(
    folder_path: str,
    mla_dict: dict,
    sku_attributes: dict,
    target_dates: list,
    output_path: str = None,
) -> pd.DataFrame:
    """
    Reads own-store Excel files, filters records matching target_dates (YYYYMMDD list),
    enriches with Tipo de Publicación (from Maestro MLA) and Tipo + Dimension
    (from Maestro SKU), groups by SKU + Tipo de Publicación, and returns
    a DataFrame with the same schema as process_market_data.

    Output columns:
        Fecha | Tienda | SKU | Tipo | Dimension | Cantidad | Facturación | Tipo de Publicación
    """
    files, download_date = find_files(folder_path)
    logging.info(f"Own transform: found {len(files)} file(s). Filtering for dates: {target_dates}")

    # --- Read & merge ---
    dfs = [pd.read_excel(f, header=1) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Initial merge: {len(df)} total records.")

    # --- Filter: target dates ---
    if COL_DATE in df.columns:
        df['_date_temp'] = pd.to_datetime(df[COL_DATE], dayfirst=True, errors='coerce').dt.strftime('%Y%m%d')
        df = df[df['_date_temp'].isin(target_dates)].drop(columns=['_date_temp'])

    # --- Filter: CORT* SKUs only ---
    if COL_SKU in df.columns:
        df = df[df[COL_SKU].astype(str).str.upper().str.startswith('CORT')]
    else:
        logging.error(f"Column '{COL_SKU}' not found. Aborting own transform.")
        return pd.DataFrame()

    # --- Filter: paid orders only ---
    if COL_STATUS in df.columns:
        df = df[df[COL_STATUS].astype(str).str.lower().str.contains('pagad', na=False)]

    logging.info(f"Records after filtering: {len(df)}")

    if df.empty:
        logging.warning("No records remaining after filters.")
        return pd.DataFrame()

    # --- Validate required columns ---
    for col in [COL_MLA, COL_QUANTITY, COL_REVENUE]:
        if col not in df.columns:
            logging.error(f"Required column '{col}' not found in own-store Excel. Aborting.")
            return pd.DataFrame()

    # --- Enrich: Tipo de Publicación from Maestro MLA ---
    df['Tipo de Publicación'] = (
        df[COL_MLA].astype(str).str.strip().map(mla_dict).fillna('Desconocido')
    )

    # --- Enrich: Tipo + Dimension from Maestro SKU ---
    df['Tipo']      = df[COL_SKU].map(lambda s: sku_attributes.get(s, ('', ''))[0])
    df['Dimension'] = df[COL_SKU].map(lambda s: sku_attributes.get(s, ('', ''))[1])

    # --- Metadata ---
    df['Tienda'] = OWN_STORE_NAME
    df['Fecha']  = pd.to_datetime(df[COL_DATE], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
    df['SKU']    = df[COL_SKU]

    # --- Rename revenue column ---
    df = df.rename(columns={COL_REVENUE: 'Facturación'})

    # --- Rename quantity column if needed ---
    if COL_QUANTITY in df.columns and 'Cantidad' not in df.columns:
        df = df.rename(columns={COL_QUANTITY: 'Cantidad'})

    # --- Group ---
    group_cols = ['Fecha', 'Tienda', 'SKU', 'Tipo', 'Dimension', 'Tipo de Publicación']
    agg_cols   = {col: 'sum' for col in ['Cantidad', 'Facturación'] if col in df.columns}

    df_grouped = df.groupby(group_cols, as_index=False).agg(agg_cols)

    # --- Final column order (same schema as market) ---
    column_order = ['Fecha', 'Tienda', 'SKU', 'Tipo', 'Dimension', 'Cantidad', 'Facturación', 'Tipo de Publicación']
    existing     = [col for col in column_order if col in df_grouped.columns]
    df_grouped   = df_grouped[existing]

    logging.info(f"Own transform complete: {len(df_grouped)} grouped rows.")

    # --- Save intermediate ---
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df_grouped.to_csv(output_path, index=False)
        logging.info(f"Intermediate result saved to: {output_path}")

    return df_grouped


# ==========================================
# DIRECT EXECUTION ENTRY POINT
# ==========================================
if __name__ == '__main__':
    from config import PATH_RAW_OWN, PATH_PROCESSED_OWN, CREDENTIALS_FILE, GOOGLE_SHEET_URL
    from src.utils.utils import setup_logger
    from src.load.load_sheets import read_sheet, append_dataframe

    setup_logger()

    logging.info("Loading Maestro MLA from Google Sheets...")
    df_mla   = read_sheet(CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Maestro MLA')
    mla_dict = build_mla_dict(df_mla)
    logging.info(f"Maestro MLA loaded: {len(mla_dict)} entries.")

    logging.info("Loading Maestro SKU from Google Sheets...")
    df_skus        = read_sheet(CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Maestro SKU')
    sku_attributes = build_sku_attributes(df_skus)
    logging.info(f"Maestro SKU loaded: {len(sku_attributes)} entries.")

    from main import get_target_dates
    target_dates = get_target_dates()
    date_suffix  = target_dates[0] if len(target_dates) == 1 else f"{target_dates[0]}_{target_dates[-1]}"

    df_result = process_own_data(
        folder_path=PATH_RAW_OWN,
        mla_dict=mla_dict,
        sku_attributes=sku_attributes,
        target_dates=target_dates,
        output_path=f"{PATH_PROCESSED_OWN}/own_clean_{date_suffix}.csv",
    )

    if not df_result.empty:
        logging.info("Appending to Google Sheets...")
        append_dataframe(df_result, CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Limpios')
        logging.info("Done.")
