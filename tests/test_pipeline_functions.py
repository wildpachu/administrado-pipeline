"""
tests/test_pipeline_functions.py — Integration-level tests for the main transform functions.

Uses temporary directories with synthetic Excel fixtures to validate the full
classification and grouping logic in process_market_data and process_own_data
without requiring real Administrado credentials or network access.
"""
import os
import shutil
import tempfile
import unittest

import openpyxl
import pandas as pd

from src.transform.transform import process_market_data
from src.transform.transform_own import process_own_data, build_mla_dict, build_sku_attributes


# ── Fixture helpers ────────────────────────────────────────────────────────────

def _make_market_excel(path: str, rows: list[dict]) -> None:
    """Creates a competitor report Excel file in the format produced by Administrado."""
    wb = openpyxl.Workbook()
    ws = wb.active
    headers = ['Título', 'Facturación', 'Cantidad Vendida', 'Tipo de Publicación']
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h, '') for h in headers])
    wb.save(path)


def _make_own_excel(path: str, data_rows: list[list]) -> None:
    """Creates an own-store Excel in the administrado export format.

    Row 0: filler header (skipped by header=1 in process_own_data).
    Row 1: column names.
    Row 2+: data.
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(['Reporte de ventas'])  # filler row — skipped by header=1
    ws.append([
        'Fecha', 'SKU de la variación', 'Estado de la orden',
        'Número de publicación', 'Cantidad', 'Precio total',
    ])
    for row in data_rows:
        ws.append(row)
    wb.save(path)


# ── Tests: process_market_data ─────────────────────────────────────────────────

class TestProcessMarketData(unittest.TestCase):

    SKU_DICT = {
        'Blackout160x200cm':  'CORT0001',
        'Sunscreen120x160cm': 'CORT0002',
    }

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        _make_market_excel(
            os.path.join(self.tmp, 'reporte_TIENDA_A_20240101.xlsx'),
            [
                {'Título': 'Cortina Blackout 160x200cm',   'Facturación': '150000', 'Cantidad Vendida': 3, 'Tipo de Publicación': 'Clásica'},
                {'Título': 'Cortina Sunscreen 120x160cm',  'Facturación': '80000',  'Cantidad Vendida': 2, 'Tipo de Publicación': 'Premium'},
                {'Título': 'Silla ergonómica',             'Facturación': '20000',  'Cantidad Vendida': 1, 'Tipo de Publicación': 'Clásica'},
                # No-dimension row → should get 'NO ENCONTRADO'
                {'Título': 'Cortina Blackout sin medidas', 'Facturación': '10000',  'Cantidad Vendida': 1, 'Tipo de Publicación': 'Clásica'},
                # Dimension not in dict → should get 'NO TENEMOS'
                {'Título': 'Cortina Blackout 999x999cm',   'Facturación': '10000',  'Cantidad Vendida': 1, 'Tipo de Publicación': 'Clásica'},
            ],
        )

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_filters_non_curtain_rows(self):
        df = process_market_data(self.tmp, self.SKU_DICT, ['20240101'])
        self.assertFalse(df.empty)
        titles = df.get('Titulo de Publicacion', pd.Series()).tolist()
        self.assertNotIn('Silla ergonómica', titles)

    def test_assigns_known_skus(self):
        df = process_market_data(self.tmp, self.SKU_DICT, ['20240101'])
        self.assertIn('CORT0001', df['SKU'].values)
        self.assertIn('CORT0002', df['SKU'].values)

    def test_date_filter_excludes_other_dates(self):
        df = process_market_data(self.tmp, self.SKU_DICT, ['20240202'])
        self.assertTrue(df.empty)

    def test_returns_empty_for_empty_folder(self):
        empty = tempfile.mkdtemp()
        try:
            df = process_market_data(empty, self.SKU_DICT, ['20240101'])
            self.assertTrue(df.empty)
        finally:
            shutil.rmtree(empty)

    def test_unknown_dimension_marked_no_encontrado(self):
        df = process_market_data(self.tmp, self.SKU_DICT, ['20240101'])
        self.assertIn('NO ENCONTRADO', df['SKU'].values)

    def test_unknown_size_marked_no_tenemos(self):
        df = process_market_data(self.tmp, self.SKU_DICT, ['20240101'])
        self.assertIn('NO TENEMOS', df['SKU'].values)

    def test_output_schema(self):
        df = process_market_data(self.tmp, self.SKU_DICT, ['20240101'])
        for col in ['Fecha', 'Tienda', 'SKU', 'Tipo', 'Dimension', 'Cantidad', 'Facturación']:
            self.assertIn(col, df.columns)


# ── Tests: process_own_data ────────────────────────────────────────────────────

class TestProcessOwnData(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        _make_own_excel(
            os.path.join(self.tmp, 'ventas_20240101.xlsx'),
            [
                ['01/01/2024', 'CORT0001', 'Pagado',    'MLA111', 2, 100000],
                ['01/01/2024', 'NOTCORT',  'Pagado',    'MLA222', 1,  50000],
                ['01/01/2024', 'CORT0002', 'Cancelado', 'MLA333', 3,  80000],
            ],
        )
        df_mla = pd.DataFrame({'MLA': ['MLA111', 'MLA222'], 'Tipo': ['Clásica', 'Premium']})
        df_sku = pd.DataFrame({'SKU': ['CORT0001'], 'con cm': ['Blackout 160x200cm']})
        self.mla_dict       = build_mla_dict(df_mla)
        self.sku_attributes = build_sku_attributes(df_sku)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_filters_non_cort_skus(self):
        df = process_own_data(self.tmp, self.mla_dict, self.sku_attributes, ['20240101'])
        self.assertNotIn('NOTCORT', df['SKU'].tolist())

    def test_filters_unpaid_orders(self):
        # CORT0002 was 'Cancelado' — must be excluded
        df = process_own_data(self.tmp, self.mla_dict, self.sku_attributes, ['20240101'])
        self.assertNotIn('CORT0002', df['SKU'].tolist())

    def test_enriches_publication_type(self):
        df = process_own_data(self.tmp, self.mla_dict, self.sku_attributes, ['20240101'])
        self.assertIn('Tipo de Publicación', df.columns)
        self.assertIn('Clásica', df['Tipo de Publicación'].values)

    def test_enriches_tipo_and_dimension(self):
        df = process_own_data(self.tmp, self.mla_dict, self.sku_attributes, ['20240101'])
        self.assertIn('Blackout', df['Tipo'].values)
        self.assertIn('160x200cm', df['Dimension'].values)

    def test_output_schema(self):
        df = process_own_data(self.tmp, self.mla_dict, self.sku_attributes, ['20240101'])
        for col in ['Fecha', 'Tienda', 'SKU', 'Tipo', 'Dimension', 'Cantidad', 'Facturación', 'Tipo de Publicación']:
            self.assertIn(col, df.columns)


if __name__ == '__main__':
    unittest.main()
