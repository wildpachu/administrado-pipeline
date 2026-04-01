import unittest
import pandas as pd

from src.transform.transform_own import build_mla_dict, build_sku_attributes


class TestBuildMlaDict(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'MLA':  ['MLA123456', 'MLA789012', 'MLA000001'],
            'Tipo': ['Clásica',   'Premium',   'Clásica'],
        })

    def test_known_mla(self):
        d = build_mla_dict(self.df)
        self.assertEqual(d['MLA123456'], 'Clásica')
        self.assertEqual(d['MLA789012'], 'Premium')

    def test_unknown_mla_not_in_dict(self):
        d = build_mla_dict(self.df)
        self.assertNotIn('MLA999999', d)

    def test_uses_first_two_columns(self):
        # Column names shouldn't matter — uses positional first two cols
        df = pd.DataFrame({
            'col_a': ['MLA111'],
            'col_b': ['Premium'],
            'col_c': ['ignored'],
        })
        d = build_mla_dict(df)
        self.assertEqual(d['MLA111'], 'Premium')


class TestBuildSkuAttributes(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'SKU':    ['CORT0001', 'CORT0002', 'CORT0003'],
            'con cm': ['Blackout 220x220cm', 'Sunscreen 160x200cm', 'Doble 140x180cm'],
        })

    def test_tipo_extracted(self):
        attrs = build_sku_attributes(self.df)
        self.assertEqual(attrs['CORT0001'][0], 'Blackout')
        self.assertEqual(attrs['CORT0002'][0], 'Sunscreen')
        self.assertEqual(attrs['CORT0003'][0], 'Doble')

    def test_dimension_extracted(self):
        attrs = build_sku_attributes(self.df)
        self.assertEqual(attrs['CORT0001'][1], '220x220cm')
        self.assertEqual(attrs['CORT0002'][1], '160x200cm')
        self.assertEqual(attrs['CORT0003'][1], '140x180cm')

    def test_unknown_sku_not_in_dict(self):
        attrs = build_sku_attributes(self.df)
        self.assertNotIn('CORT9999', attrs)

    def test_missing_dimension_graceful(self):
        # If 'con cm' has no space (only tipo, no dimension)
        df = pd.DataFrame({'SKU': ['CORT0004'], 'con cm': ['Blackout']})
        attrs = build_sku_attributes(df)
        self.assertEqual(attrs['CORT0004'][0], 'Blackout')
        self.assertEqual(attrs['CORT0004'][1], '')


if __name__ == '__main__':
    unittest.main()
