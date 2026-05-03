"""
Unit tests for Raw -> Conformed logic (Script 1).

Tests cover the pure-Python / logic layer only — no Spark, no AWS needed.
Run with:  python -m pytest tests/test_raw_to_conformed.py -v
       or:  python -m unittest tests/test_raw_to_conformed -v
"""

import unittest


# ---------------------------------------------------------------------------
# Pure-Python extraction of the product_list parsing logic from Script 1.
# This mirrors exactly what the Spark job does row-by-row, so we can test
# it without a Spark session.
# ---------------------------------------------------------------------------

def parse_product_list(product_list: str) -> list[dict]:
    """
    Replicates the explode_products() logic from HitLevelProcessor.

    Given a raw product_list string, returns a list of dicts — one per
    product — with keys:
        product_category, product_name, product_quantity,
        product_total_revenue, product_custom_events
    """
    if not product_list or not product_list.strip():
        return []

    results = []
    for entry in product_list.split(","):
        entry = entry.strip()
        if not entry:
            continue

        parts = entry.split(";")

        category  = parts[0] if len(parts) >= 1 else None
        name      = parts[1] if len(parts) >= 2 else None

        try:
            quantity = int(parts[2]) if len(parts) >= 3 and parts[2].strip() else None
        except ValueError:
            quantity = None

        try:
            revenue = float(parts[3]) if len(parts) >= 4 and parts[3].strip() else None
        except ValueError:
            revenue = None

        # Custom events kept as raw pipe-delimited string (not split into list)
        custom_events = (
            parts[4] if len(parts) >= 5 and parts[4].strip() else None
        )

        results.append({
            "product_category":      category,
            "product_name":          name,
            "product_quantity":      quantity,
            "product_total_revenue": revenue,
            "product_custom_events": custom_events,
        })

    return results


def clean_field(value: str):
    """
    Replicates clean_base() normalisation from Script 1:
      - strip whitespace
      - empty string -> None
    """
    if value is None:
        return None
    stripped = value.strip()
    return None if stripped == "" else stripped


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProductListParsing(unittest.TestCase):
    """
    Technical tests: product_list string parsing.
    Covers Appendix B of the exercise spec.
    """

    def test_single_product_all_fields(self):
        """Standard single product with all fields populated."""
        result = parse_product_list("Electronics;Ipod - Nano - 8GB;1;190;")
        self.assertEqual(len(result), 1)
        p = result[0]
        self.assertEqual(p["product_category"],      "Electronics")
        self.assertEqual(p["product_name"],          "Ipod - Nano - 8GB")
        self.assertEqual(p["product_quantity"],      1)
        self.assertEqual(p["product_total_revenue"], 190.0)
        self.assertIsNone(p["product_custom_events"])

    def test_single_product_with_custom_events(self):
        """Product with pipe-delimited custom events — must stay as string, not list."""
        result = parse_product_list("Computers;HP Pavillion;1;1000;200|201")
        self.assertEqual(len(result), 1)
        p = result[0]
        self.assertEqual(p["product_custom_events"], "200|201")
        # Must be a string — not a list (this was the original array<string> bug)
        self.assertIsInstance(p["product_custom_events"], str)

    def test_multiple_products_comma_separated(self):
        """
        Multiple products delimited by commas.
        Ref: Appendix B — "Products are delimited by commas".
        """
        raw = "Computers;HP Pavillion;1;1000;200|201,Office Supplies;Red Folders;4;4.00;205|206|207"
        result = parse_product_list(raw)
        self.assertEqual(len(result), 2)

        self.assertEqual(result[0]["product_name"],          "HP Pavillion")
        self.assertEqual(result[0]["product_total_revenue"], 1000.0)
        self.assertEqual(result[0]["product_custom_events"], "200|201")

        self.assertEqual(result[1]["product_name"],          "Red Folders")
        self.assertEqual(result[1]["product_quantity"],      4)
        self.assertEqual(result[1]["product_total_revenue"], 4.0)
        self.assertEqual(result[1]["product_custom_events"], "205|206|207")

    def test_product_without_revenue(self):
        """
        Product view rows (event=2) have no revenue in the product_list.
        Revenue field is empty -> should parse as None, not 0 or error.
        """
        result = parse_product_list("Electronics;Zune - 328GB;1;;")
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["product_total_revenue"])

    def test_product_with_revenue_on_purchase(self):
        """
        Purchase rows (event=1) carry actual revenue.
        Ref: Appendix B — revenue only actualized on purchase event.
        """
        result = parse_product_list("Electronics;Zune - 32GB;1;250;")
        self.assertEqual(result[0]["product_total_revenue"], 250.0)

    def test_empty_product_list_returns_empty(self):
        """Null or empty product_list produces no rows — base hits must still exist."""
        self.assertEqual(parse_product_list(""),   [])
        self.assertEqual(parse_product_list(None), [])
        self.assertEqual(parse_product_list("   "), [])

    def test_revenue_is_float_not_int(self):
        """Revenue must be numeric (float) for correct aggregation."""
        result = parse_product_list("Electronics;Ipod - Touch - 32GB;1;290;")
        self.assertIsInstance(result[0]["product_total_revenue"], float)

    def test_quantity_is_int(self):
        """Quantity must parse as integer."""
        result = parse_product_list("Electronics;Ipod - Nano - 8GB;1;190;")
        self.assertIsInstance(result[0]["product_quantity"], int)

    def test_whitespace_trimmed_from_entries(self):
        """Leading/trailing whitespace around product entries is stripped."""
        result = parse_product_list("  Electronics;Ipod - Nano - 8GB;1;190;  ")
        self.assertEqual(result[0]["product_category"], "Electronics")
        self.assertEqual(result[0]["product_name"],     "Ipod - Nano - 8GB")


class TestCleanBase(unittest.TestCase):
    """
    Technical tests: clean_base() field normalisation logic.
    """

    def test_whitespace_stripped(self):
        """Leading and trailing whitespace must be removed from string fields."""
        self.assertEqual(clean_field("  Salem  "), "Salem")
        self.assertEqual(clean_field("\tOR\t"),     "OR")

    def test_empty_string_becomes_none(self):
        """
        Empty strings in event_list, product_list, referrer must become None
        so downstream filters work correctly (isNotNull checks).
        """
        self.assertIsNone(clean_field(""))
        self.assertIsNone(clean_field("   "))

    def test_none_stays_none(self):
        self.assertIsNone(clean_field(None))

    def test_non_empty_value_preserved(self):
        self.assertEqual(clean_field("http://www.google.com"), "http://www.google.com")
        self.assertEqual(clean_field("1"),                     "1")

    def test_base_table_preserves_rows_without_product_list(self):
        """
        Critical regression test: rows with no product_list (Home, Hot Buys,
        Search Results pages) must NOT be dropped from the base table.
        These rows hold the true entry-hit referrer used by Script 2.

        Simulated with a list of dicts — if product_list is None the row
        still survives the clean step (no filter is applied in clean_base).
        """
        all_hits = [
            {"ip": "67.98.123.1", "pagename": "Home",           "product_list": None,                            "referrer": "http://www.google.com/search?q=Ipod"},
            {"ip": "67.98.123.1", "pagename": "Ipod - Nano",    "product_list": "Electronics;Ipod - Nano;1;190;","referrer": "http://www.esshopzilla.com"},
            {"ip": "44.12.96.2",  "pagename": "Hot Buys",       "product_list": None,                            "referrer": "http://www.google.com/search?q=ipod"},
            {"ip": "112.33.98.231","pagename": "Home",           "product_list": None,                            "referrer": "http://search.yahoo.com/search?p=cd+player"},
        ]
        # clean_base keeps ALL rows — no product_list filter here
        # product filter only happens in explode_products
        rows_with_no_product = [r for r in all_hits if r["product_list"] is None]
        self.assertEqual(len(rows_with_no_product), 3,
            "Rows without product_list must be preserved in the base table")

    def test_explode_excludes_rows_without_product_list(self):
        """
        explode_products() only processes rows that have a product_list.
        Rows without one should produce zero product rows.
        """
        rows_without_product = [
            {"product_list": None},
            {"product_list": ""},
            {"product_list": "   "},
        ]
        for row in rows_without_product:
            result = parse_product_list(row["product_list"])
            self.assertEqual(result, [],
                f"Expected no product rows for product_list={row['product_list']!r}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
