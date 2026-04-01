import unittest
import pandas as pd

from src.transform.transform import (
    clean_amount,
    clean_price,
    extract_type,
    normalize,
    extract_dim,
    assign_sku,
)


class TestCleanAmount(unittest.TestCase):

    def test_plain_integer(self):
        self.assertEqual(clean_amount("150000"), 150000)

    def test_with_dollar_sign(self):
        self.assertEqual(clean_amount("$150000"), 150000)

    def test_with_plus_sign(self):
        self.assertEqual(clean_amount("+150000"), 150000)

    def test_with_dot_thousands(self):
        self.assertEqual(clean_amount("150.000"), 150000)

    def test_float_string(self):
        self.assertEqual(clean_amount("150000.0"), 150000)

    def test_nan(self):
        self.assertEqual(clean_amount(pd.NA), 0)
        self.assertEqual(clean_amount(float('nan')), 0)

    def test_invalid_string(self):
        self.assertEqual(clean_amount("N/A"), 0)


class TestCleanPrice(unittest.TestCase):

    def test_plain_float(self):
        self.assertAlmostEqual(clean_price("12500.50"), 12500.50)

    def test_with_dollar_sign(self):
        self.assertAlmostEqual(clean_price("$12500.50"), 12500.50)

    def test_comma_as_decimal(self):
        self.assertAlmostEqual(clean_price("12500,50"), 12500.50)

    def test_nan(self):
        self.assertEqual(clean_price(pd.NA), 0.0)

    def test_invalid_string(self):
        self.assertEqual(clean_price("N/A"), 0.0)


class TestExtractType(unittest.TestCase):

    def test_blackout(self):
        self.assertEqual(extract_type("Cortina Blackout 160x200"), "Blackout")

    def test_blackout_opaca(self):
        self.assertEqual(extract_type("Cortina opaca 140x180"), "Blackout")

    def test_sunscreen(self):
        self.assertEqual(extract_type("Cortina Sunscreen 5% 120x160"), "Sunscreen")

    def test_screen_translucida(self):
        self.assertEqual(extract_type("Cortina traslucida 100x150"), "Sunscreen")

    def test_doble(self):
        self.assertEqual(extract_type("Cortina Doble roller 140x200"), "Doble")

    def test_unidentifiable_returns_none(self):
        # No tipo → must return None so the row gets dropped
        self.assertIsNone(extract_type("Cortina enrollable 120x160"))

    def test_case_insensitive(self):
        self.assertEqual(extract_type("cortina BLACKOUT 160x200"), "Blackout")


class TestNormalize(unittest.TestCase):

    def test_centimeters(self):
        self.assertEqual(normalize("160"), "160")

    def test_meters_to_cm(self):
        self.assertEqual(normalize("1.60"), "160")

    def test_comma_decimal(self):
        self.assertEqual(normalize("1,60"), "160")

    def test_whole_meters(self):
        self.assertEqual(normalize("2"), "200")


class TestExtractDim(unittest.TestCase):

    def test_standard_cm(self):
        self.assertEqual(extract_dim("Cortina Blackout 160x200cm"), "160x200cm")

    def test_lowercase_x(self):
        self.assertEqual(extract_dim("Cortina 140x180"), "140x180cm")

    def test_with_spaces(self):
        self.assertEqual(extract_dim("Cortina 160 x 200"), "160x200cm")

    def test_meters(self):
        self.assertEqual(extract_dim("Cortina 1.60 x 2.00"), "160x200cm")

    def test_no_dimension_returns_none(self):
        self.assertIsNone(extract_dim("Cortina enrollable sin medidas"))

    def test_discards_short_height(self):
        self.assertIsNone(extract_dim("Cortina 160x1"))


class TestAssignSku(unittest.TestCase):

    SKU_DICT = {
        "Blackout160x200cm": "CORT0001",
        "Sunscreen120x160cm": "CORT0002",
    }

    def _row(self, tipo, dimension):
        return pd.Series({'Tipo': tipo, 'Dimension': dimension})

    def test_known_sku(self):
        row = self._row("Blackout", "160x200cm")
        self.assertEqual(assign_sku(row, self.SKU_DICT), "CORT0001")

    def test_dimension_not_in_dict(self):
        # Has dimension but we don't carry that size
        row = self._row("Blackout", "125x123cm")
        self.assertEqual(assign_sku(row, self.SKU_DICT), "NO TENEMOS")

    def test_no_dimension(self):
        # Has tipo but no extractable dimension
        row = self._row("Blackout", None)
        self.assertEqual(assign_sku(row, self.SKU_DICT), "NO ENCONTRADO")

    def test_another_known_sku(self):
        row = self._row("Sunscreen", "120x160cm")
        self.assertEqual(assign_sku(row, self.SKU_DICT), "CORT0002")


if __name__ == '__main__':
    unittest.main()
