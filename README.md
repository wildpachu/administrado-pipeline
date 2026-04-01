# Sheet Pipeline

An automated ETL pipeline that monitors competitor pricing on Mercado Libre and consolidates it with own-store sales data into a shared Google Sheet — replacing a fully manual process that previously took hours each week.

> **Note:** This pipeline runs against [Administrado](https://www.administrado.net), a private analytics platform for Mercado Libre sellers. Running the extraction steps requires valid Administrado credentials and access to specific store/competitor IDs. The transform and load logic can be exercised with the sample data in [`data/sample/`](data/sample/).

---

## What it does

Every weekday (and catching the full weekend on Mondays), the pipeline:

1. **Extracts** — Downloads competitor price reports and own-store sales exports from Administrado via Playwright browser automation
2. **Transforms** — Classifies each curtain listing by fabric type (Blackout / Sunscreen / Doble) and dimensions using regex, maps to internal SKUs, and groups by store + SKU + publication type
3. **Loads** — Writes the cleaned, aggregated data to a Google Sheet used for daily pricing decisions

Sample output: [`data/sample/market_sample.csv`](data/sample/market_sample.csv) · [`data/sample/own_sample.csv`](data/sample/own_sample.csv)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Entry points                       │
│           main.py (CLI)   ·   ui/mockup.py (GUI)        │
└───────────────────────┬─────────────────────────────────┘
                        │  calls
                        ▼
              src/pipeline.py  ─── orchestrates ───────────────────┐
                                                                    │
        ┌───────────────┬─────────────────────┬────────────────┐   │
        ▼               ▼                     ▼                ▼   │
  src/extract/    src/extract/        src/transform/    src/load/        │
  extract_own.py  extract_market.py   transform.py      load_sheets.py   │
                        │             transform_own.py                   │
                        └─── share ──►src/extract/                  │
                                      auth.py (login)               │
                                                                    │
                    ┌───────────────────────────────────────────────┘
                    ▼
            Google Sheets  (reference data in, results out)
```

**Data flow:**

```
Administrado (browser scraping)
    │
    ├─► data/raw/market/  ──► process_market_data()  ──► data/processed/market/
    │                              │ regex classify                │
    └─► data/raw/own/    ──► process_own_data()      ──► data/processed/own/
                                   │ enrich from Sheets            │
                                                                   ▼
                                                         Google Sheets (Limpios tab)
```

---

## Tech stack

| Layer | Tools |
|---|---|
| Extraction | [Playwright](https://playwright.dev/python/) (Chromium headless) |
| Transformation | [pandas](https://pandas.pydata.org/), [openpyxl](https://openpyxl.readthedocs.io/) |
| Loading | [gspread](https://gspread.readthedocs.io/), [gspread-dataframe](https://github.com/robin900/gspread-dataframe) |
| Auth | [google-auth](https://google-auth.readthedocs.io/) (service account) |
| UI | [PyQt6](https://www.riverbankcomputing.com/software/pyqt/) (glassmorphism) |
| Config | [python-dotenv](https://github.com/theskumar/python-dotenv) |
| Testing | [pytest](https://pytest.org/) + unittest |

---

## Project structure

```
administrado-pipeline/
├── main.py                      # CLI entry point
├── config.py                    # Paths, IDs, timeouts
│
├── src/
│   ├── pipeline.py              # Core ETL orchestration (shared by CLI + UI)
│   ├── extract/
│   │   ├── auth.py              # Administrado login (shared)
│   │   ├── extract_market.py    # Download competitor reports
│   │   └── extract_own.py       # Download own store sales
│   ├── transform/
│   │   ├── transform.py         # Classify & group competitor data
│   │   └── transform_own.py     # Enrich & group own sales
│   ├── load/
│   │   └── load_sheets.py       # Google Sheets read/write
│   └── utils/
│       └── utils.py             # Directory setup, rotating logger
│
├── ui/
│   └── mockup.py                # PyQt6 glassmorphism desktop UI
│
├── tests/
│   ├── test_transform.py        # Unit tests: cleaning & classification helpers
│   ├── test_transform_own.py    # Unit tests: lookup dict builders
│   └── test_pipeline_functions.py  # Integration tests: full transform functions
│
└── data/
    └── sample/                  # Anonymized example outputs
        ├── market_sample.csv
        └── own_sample.csv
```

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/wildpachu/administrado-pipeline.git
cd administrado-pipeline
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure credentials

```bash
# Windows
copy .env.example .env

# macOS / Linux
cp .env.example .env
```

Edit `.env` with your Administrado credentials:
```
APP_USERNAME=your_administrado_email
PASSWORD=your_administrado_password
```

Place your Google service account JSON at the project root as `credentials.json`. The service account needs edit access to the target Google Sheet.

### 3. Run

**CLI:**
```bash
python main.py
```

**Desktop UI:**
```bash
python ui/mockup.py
```

### 4. Run tests

```bash
pytest
```

The test suite (52 tests) does not require Administrado credentials or network access — it uses synthetic Excel fixtures.

---

## Key design decisions

**Why Playwright instead of an API?** Administrado does not expose a public API. Browser automation is the only way to export data programmatically.

**SKU classification logic** — The transform layer uses two-stage classification:
1. Regex extracts fabric type (`Blackout | Sunscreen | Doble`) and dimensions (`WxHcm`) from free-text listing titles
2. The `tipo + dimension` pair is looked up in a reference dictionary built from a "Maestro SKU" Google Sheet — so SKU mapping is data-driven and can be updated without touching code

**Monday date logic** — The pipeline runs daily. On Mondays it processes Friday + Saturday + Sunday together to cover the full weekend gap.

**Single source of truth for pipeline logic** — `src/pipeline.py` is the only place where the ETL steps are defined. Both the CLI (`main.py`) and the desktop UI (`ui/mockup.py`) call `run_pipeline()` — there is no duplicated orchestration logic.

---

## What the output looks like

### Competitor data (`Limpios` sheet — market rows)

| Fecha | Tienda | SKU | Tipo | Dimension | Cantidad | Facturación | Tipo de Publicación |
|---|---|---|---|---|---|---|---|
| 30/03/2025 | Competidor Norte | CORT0001 | Blackout | 160x200cm | 12 | 2,340,000 | Clásica |
| 30/03/2025 | Competidor Norte | NO TENEMOS | Blackout | 220x250cm | 3 | 720,000 | Clásica |

- `NO TENEMOS` → we don't carry that size
- `NO ENCONTRADO` → dimension couldn't be extracted from the title

### Own store data (appended below market rows)

| Fecha | Tienda | SKU | Tipo | Dimension | Cantidad | Facturación | Tipo de Publicación |
|---|---|---|---|---|---|---|---|
| 30/03/2025 | IMAGEN | CORT0001 | Blackout | 160x200cm | 5 | 975,000 | Premium |

Full sample data: [`data/sample/`](data/sample/)
