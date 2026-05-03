"""
Unit tests for Conformed -> Curated logic (Script 2).

Tests cover the pure-Python / logic layer only — no Spark, no AWS needed.
Run with:  python -m pytest tests/test_conformed_to_curated.py -v
       or:  python -m unittest tests/test_conformed_to_curated -v
"""

import re
import unittest
from urllib.parse import parse_qs, urlparse


# ---------------------------------------------------------------------------
# Pure-Python extraction of Script 2 logic.
# Mirrors exactly what the UDFs and processor methods do, so we can test
# them without a Spark session.
# ---------------------------------------------------------------------------

SEARCH_ENGINE_PARAMS = {
    "google.com":     ["q"],
    "bing.com":       ["q"],
    "yahoo.com":      ["p", "q"],
    "msn.com":        ["q"],
    "ask.com":        ["q"],
    "aol.com":        ["q", "query"],
    "baidu.com":      ["wd", "word"],
    "duckduckgo.com": ["q"],
}


def _registered_domain(hostname: str):
    if not hostname:
        return None
    host = hostname.lower().lstrip("www.")
    for domain in SEARCH_ENGINE_PARAMS:
        if host == domain or host.endswith("." + domain):
            return domain
    return None


def extract_search_engine(referrer: str):
    if not referrer:
        return None
    try:
        return _registered_domain(urlparse(referrer).hostname or "")
    except Exception:
        return None


def extract_keyword(referrer: str):
    if not referrer:
        return None
    try:
        parsed = urlparse(referrer)
        domain = _registered_domain(parsed.hostname or "")
        if not domain:
            return None
        qs = parse_qs(parsed.query)
        for param in SEARCH_ENGINE_PARAMS[domain]:
            if param in qs and qs[param]:
                return qs[param][0].strip().lower()
    except Exception:
        pass
    return None


def is_purchase_event(event_list: str) -> bool:
    """
    Returns True if event_list contains the standalone purchase event "1".
    Guards against matching events 10, 11, 12, 13, 14.
    Mirrors the rlike pattern used in get_purchase_revenue().
    """
    if not event_list:
        return False
    return bool(re.search(r'(^|,)\s*1\s*(,|$)', event_list))


def get_first_hit_per_ip(hits: list[dict]) -> dict[str, dict]:
    """
    Simulates get_session_search_origin():
    Returns {ip: first_hit_row} where first_hit_row is the row with
    the lowest hit_time_gmt for that IP.
    """
    first_hits = {}
    for hit in sorted(hits, key=lambda r: r["hit_time_gmt"]):
        ip = hit["ip"]
        if ip not in first_hits:
            first_hits[ip] = hit
    return first_hits


def attribute_revenue(
    hits: list[dict],
    product_rows: list[dict],
) -> list[dict]:
    """
    Simulates the full Script 2 pipeline end-to-end in pure Python:

      1. Find the first hit per IP → extract search engine + keyword
      2. Filter product rows to purchase events only (event_list has "1")
      3. Left-join: every search-driven IP appears; no-purchase IPs get 0
      4. Aggregate revenue by (search_engine_domain, search_keyword)
      5. Sort descending by Revenue

    Returns list of dicts:
        [{"Search Engine Domain": ..., "Search Keyword": ..., "Revenue": ...}]
    """
    # Step 1: session origin per IP
    first_hits = get_first_hit_per_ip(hits)
    session_origin = {}  # ip -> {search_engine_domain, search_keyword}
    for ip, hit in first_hits.items():
        engine  = extract_search_engine(hit.get("referrer", ""))
        keyword = extract_keyword(hit.get("referrer", ""))
        if engine and keyword:
            session_origin[ip] = {"search_engine_domain": engine,
                                  "search_keyword": keyword}

    # Step 2: purchase revenue per IP (left join means we still include $0 IPs)
    revenue_by_ip = {}  # ip -> total_revenue
    for ip in session_origin:
        revenue_by_ip[ip] = 0.0  # default: no purchase = 0

    for row in product_rows:
        ip = row["ip"]
        if ip not in session_origin:
            continue
        if not is_purchase_event(row.get("event_list", "")):
            continue
        rev = row.get("product_total_revenue") or 0.0
        revenue_by_ip[ip] = revenue_by_ip.get(ip, 0.0) + rev

    # Step 3+4: aggregate by (engine, keyword)
    aggregated = {}
    for ip, origin in session_origin.items():
        key = (origin["search_engine_domain"], origin["search_keyword"])
        aggregated[key] = aggregated.get(key, 0.0) + revenue_by_ip.get(ip, 0.0)

    # Step 5: sort descending
    return sorted(
        [
            {"Search Engine Domain": k[0], "Search Keyword": k[1], "Revenue": v}
            for k, v in aggregated.items()
        ],
        key=lambda r: r["Revenue"],
        reverse=True,
    )


# ---------------------------------------------------------------------------
# Sample data mirroring data.sql
# ---------------------------------------------------------------------------

# All hits — represents the base/all_hits conformed table
SAMPLE_ALL_HITS = [
    # 67.98.123.1 — entered via Google searching "Ipod"
    {"ip": "67.98.123.1", "hit_time_gmt": 1254033280, "pagename": "Home",
     "referrer": "http://www.google.com/search?hl=en&q=Ipod", "event_list": None},
    {"ip": "67.98.123.1", "hit_time_gmt": 1254033676, "pagename": "Search Results",
     "referrer": "http://www.esshopzilla.com", "event_list": None},
    {"ip": "67.98.123.1", "hit_time_gmt": 1254033973, "pagename": "Ipod - Nano - 8 GB",
     "referrer": "http://www.esshopzilla.com/search/?k=Ipod", "event_list": "2"},
    {"ip": "67.98.123.1", "hit_time_gmt": 1254034567, "pagename": "Ipod - Touch - 32 GB",
     "referrer": "http://www.esshopzilla.com/search/?k=Ipod", "event_list": "2"},
    {"ip": "67.98.123.1", "hit_time_gmt": 1254034864, "pagename": "Shopping Cart",
     "referrer": "http://www.esshopzilla.com/product/?pid=as23233", "event_list": "12"},
    {"ip": "67.98.123.1", "hit_time_gmt": 1254035062, "pagename": "Order Checkout Details",
     "referrer": "http://www.esshopzilla.com/cart/", "event_list": "11"},
    {"ip": "67.98.123.1", "hit_time_gmt": 1254035161, "pagename": "Order Confirmation",
     "referrer": "https://www.esshopzilla.com/checkout/", "event_list": None},
    {"ip": "67.98.123.1", "hit_time_gmt": 1254035260, "pagename": "Order Complete",
     "referrer": "https://www.esshopzilla.com/checkout/?a=confirm", "event_list": "1"},

    # 23.8.61.21 — entered via Bing searching "Zune"
    {"ip": "23.8.61.21",  "hit_time_gmt": 1254033379, "pagename": "Zune - 32 GB",
     "referrer": "http://www.bing.com/search?q=Zune", "event_list": "2"},
    {"ip": "23.8.61.21",  "hit_time_gmt": 1254033775, "pagename": "Shopping Cart",
     "referrer": "http://www.esshopzilla.com/product/?pid=asfe13", "event_list": "12"},
    {"ip": "23.8.61.21",  "hit_time_gmt": 1254034072, "pagename": "Order Checkout Details",
     "referrer": "http://www.esshopzilla.com/cart/", "event_list": "11"},
    {"ip": "23.8.61.21",  "hit_time_gmt": 1254034369, "pagename": "Order Confirmation",
     "referrer": "https://www.esshopzilla.com/checkout/", "event_list": None},
    {"ip": "23.8.61.21",  "hit_time_gmt": 1254034666, "pagename": "Order Complete",
     "referrer": "https://www.esshopzilla.com/checkout/?a=confirm", "event_list": "1"},

    # 44.12.96.2 — entered via Google searching "ipod" (lowercase)
    {"ip": "44.12.96.2",  "hit_time_gmt": 1254033577, "pagename": "Hot Buys",
     "referrer": "http://www.google.com/search?q=ipod", "event_list": None},
    {"ip": "44.12.96.2",  "hit_time_gmt": 1254033874, "pagename": "Ipod - Nano - 8 GB",
     "referrer": "http://www.esshopzilla.com/hotbuys/", "event_list": "2"},
    {"ip": "44.12.96.2",  "hit_time_gmt": 1254034171, "pagename": "Shopping Cart",
     "referrer": "http://www.esshopzilla.com/product/?pid=as23233", "event_list": "12"},
    {"ip": "44.12.96.2",  "hit_time_gmt": 1254034468, "pagename": "Order Checkout Details",
     "referrer": "http://www.esshopzilla.com/cart/", "event_list": "11"},
    {"ip": "44.12.96.2",  "hit_time_gmt": 1254034765, "pagename": "Order Confirmation",
     "referrer": "https://www.esshopzilla.com/checkout/", "event_list": None},
    {"ip": "44.12.96.2",  "hit_time_gmt": 1254034963, "pagename": "Order Complete",
     "referrer": "https://www.esshopzilla.com/checkout/?a=confirm", "event_list": "1"},

    # 112.33.98.231 — entered via Yahoo searching "cd player", NO purchase
    {"ip": "112.33.98.231","hit_time_gmt": 1254033478, "pagename": "Home",
     "referrer": "http://search.yahoo.com/search?p=cd+player", "event_list": None},
]

# Product-level rows — represents the product_level conformed table
SAMPLE_PRODUCT_ROWS = [
    # event=2 (product view) — revenue NOT actualized, product_total_revenue is None
    {"ip": "23.8.61.21",  "event_list": "2",  "product_name": "Zune - 328GB",      "product_total_revenue": None},
    {"ip": "44.12.96.2",  "event_list": "2",  "product_name": "Ipod - Nano - 8GB", "product_total_revenue": None},
    {"ip": "67.98.123.1", "event_list": "2",  "product_name": "Ipod - Nano - 8GB", "product_total_revenue": None},
    {"ip": "67.98.123.1", "event_list": "2",  "product_name": "Ipod - Touch - 32GB","product_total_revenue": None},
    # event=1 (purchase) — revenue IS actualized
    {"ip": "23.8.61.21",  "event_list": "1",  "product_name": "Zune - 32GB",       "product_total_revenue": 250.0},
    {"ip": "44.12.96.2",  "event_list": "1",  "product_name": "Ipod - Nano - 8GB", "product_total_revenue": 190.0},
    {"ip": "67.98.123.1", "event_list": "1",  "product_name": "Ipod - Touch - 32GB","product_total_revenue": 290.0},
]


# ---------------------------------------------------------------------------
# Tests — Search Engine / Keyword Extraction
# ---------------------------------------------------------------------------

class TestExtractSearchEngine(unittest.TestCase):
    """Technical tests: referrer URL -> search engine domain."""

    def test_google_with_www(self):
        url = "http://www.google.com/search?q=Ipod"
        self.assertEqual(extract_search_engine(url), "google.com")

    def test_bing(self):
        url = "http://www.bing.com/search?q=Zune&form=QBLH"
        self.assertEqual(extract_search_engine(url), "bing.com")

    def test_yahoo(self):
        url = "http://search.yahoo.com/search?p=cd+player"
        self.assertEqual(extract_search_engine(url), "yahoo.com")

    def test_msn(self):
        url = "http://www.msn.com/search?q=laptop"
        self.assertEqual(extract_search_engine(url), "msn.com")

    def test_internal_referrer_returns_none(self):
        """esshopzilla.com is not a search engine — must return None."""
        self.assertIsNone(extract_search_engine("http://www.esshopzilla.com"))
        self.assertIsNone(extract_search_engine("http://www.esshopzilla.com/search/?k=Ipod"))
        self.assertIsNone(extract_search_engine("http://www.esshopzilla.com/hotbuys/"))

    def test_none_referrer_returns_none(self):
        self.assertIsNone(extract_search_engine(None))
        self.assertIsNone(extract_search_engine(""))

    def test_subdomain_of_known_engine(self):
        """search.yahoo.com should resolve to yahoo.com."""
        self.assertEqual(
            extract_search_engine("http://search.yahoo.com/search?p=tablet"),
            "yahoo.com"
        )


class TestExtractKeyword(unittest.TestCase):
    """Technical tests: referrer URL -> search keyword (always lowercase)."""

    def test_google_keyword_mixed_case_lowercased(self):
        """'Ipod' in URL must come out as 'ipod' — case-insensitive requirement."""
        url = "http://www.google.com/search?q=Ipod"
        self.assertEqual(extract_keyword(url), "ipod")

    def test_google_keyword_already_lowercase(self):
        url = "http://www.google.com/search?q=ipod"
        self.assertEqual(extract_keyword(url), "ipod")

    def test_bing_keyword(self):
        url = "http://www.bing.com/search?q=Zune&go=&form=QBLH&qs=n"
        self.assertEqual(extract_keyword(url), "zune")

    def test_yahoo_uses_p_param(self):
        url = "http://search.yahoo.com/search?p=cd+player&toggle=1"
        self.assertEqual(extract_keyword(url), "cd player")

    def test_yahoo_falls_back_to_q_param(self):
        url = "http://search.yahoo.com/search?q=headphones"
        self.assertEqual(extract_keyword(url), "headphones")

    def test_internal_referrer_returns_none(self):
        self.assertIsNone(extract_keyword("http://www.esshopzilla.com/search/?k=Ipod"))

    def test_none_referrer_returns_none(self):
        self.assertIsNone(extract_keyword(None))
        self.assertIsNone(extract_keyword(""))

    def test_keyword_whitespace_stripped(self):
        url = "http://www.google.com/search?q=+ipod+"
        self.assertEqual(extract_keyword(url), "ipod")

    def test_case_normalization_groups_same_keyword(self):
        """
        Functional requirement: 'Ipod' and 'ipod' are the same keyword.
        Both referrers must produce the identical lowercase keyword.
        """
        kw1 = extract_keyword("http://www.google.com/search?q=Ipod")
        kw2 = extract_keyword("http://www.google.com/search?q=ipod")
        self.assertEqual(kw1, kw2)
        self.assertEqual(kw1, "ipod")


# ---------------------------------------------------------------------------
# Tests — Purchase Event Detection
# ---------------------------------------------------------------------------

class TestPurchaseEventDetection(unittest.TestCase):
    """
    Technical tests: is_purchase_event().
    Ref: Appendix A — event 1 = Purchase.
    Must not match events 10, 11, 12, 13, 14.
    """

    def test_standalone_purchase(self):
        self.assertTrue(is_purchase_event("1"))

    def test_purchase_with_other_events(self):
        self.assertTrue(is_purchase_event("1,2"))
        self.assertTrue(is_purchase_event("2,1"))
        self.assertTrue(is_purchase_event("2,1,12"))

    def test_event_10_not_purchase(self):
        """Event 10 = Shopping Cart Open — must NOT match."""
        self.assertFalse(is_purchase_event("10"))

    def test_event_11_not_purchase(self):
        """Event 11 = Shopping Cart Checkout — must NOT match."""
        self.assertFalse(is_purchase_event("11"))

    def test_event_12_not_purchase(self):
        """Event 12 = Shopping Cart Add — must NOT match."""
        self.assertFalse(is_purchase_event("12"))

    def test_event_13_not_purchase(self):
        """Event 13 = Shopping Cart Remove — must NOT match."""
        self.assertFalse(is_purchase_event("13"))

    def test_event_14_not_purchase(self):
        """Event 14 = Shopping Cart View — must NOT match."""
        self.assertFalse(is_purchase_event("14"))

    def test_event_2_not_purchase(self):
        """Event 2 = Product View — must NOT match."""
        self.assertFalse(is_purchase_event("2"))

    def test_none_event_list_not_purchase(self):
        """112.33.98.231 has no event_list at all — must NOT match."""
        self.assertFalse(is_purchase_event(None))
        self.assertFalse(is_purchase_event(""))


# ---------------------------------------------------------------------------
# Tests — Session Attribution (first hit per IP)
# ---------------------------------------------------------------------------

class TestSessionOrigin(unittest.TestCase):
    """
    Technical tests: get_first_hit_per_ip().
    The true first hit per IP is the one with the lowest hit_time_gmt,
    regardless of whether that hit has a product_list.
    """

    def test_first_hit_is_lowest_timestamp(self):
        """First hit must be the earliest by hit_time_gmt."""
        first_hits = get_first_hit_per_ip(SAMPLE_ALL_HITS)
        # 67.98.123.1 first hit is Home page (hit_time_gmt=1254033280)
        self.assertEqual(first_hits["67.98.123.1"]["pagename"], "Home")
        self.assertEqual(first_hits["67.98.123.1"]["hit_time_gmt"], 1254033280)

    def test_first_hit_without_product_list_is_captured(self):
        """
        Critical regression: for 67.98.123.1 and 44.12.96.2 the first hit
        has no product_list. It must still be the entry hit used for
        session attribution — not silently dropped.
        """
        first_hits = get_first_hit_per_ip(SAMPLE_ALL_HITS)

        # 67.98.123.1 entry = Home page with google referrer, no product_list
        self.assertEqual(first_hits["67.98.123.1"]["pagename"], "Home")
        engine = extract_search_engine(first_hits["67.98.123.1"]["referrer"])
        self.assertEqual(engine, "google.com")

        # 44.12.96.2 entry = Hot Buys page with google referrer, no product_list
        self.assertEqual(first_hits["44.12.96.2"]["pagename"], "Hot Buys")
        engine = extract_search_engine(first_hits["44.12.96.2"]["referrer"])
        self.assertEqual(engine, "google.com")

    def test_ip_with_no_search_referrer_excluded(self):
        """
        IPs whose first hit came from an internal page (not a search engine)
        should not appear in the session origin map.
        Here we simulate an IP that first arrived from esshopzilla directly.
        """
        hits = [
            {"ip": "1.2.3.4", "hit_time_gmt": 100,
             "referrer": "http://www.esshopzilla.com", "pagename": "Home"},
            {"ip": "1.2.3.4", "hit_time_gmt": 200,
             "referrer": "http://www.google.com/search?q=test", "pagename": "Search"},
        ]
        first_hits = get_first_hit_per_ip(hits)
        engine = extract_search_engine(first_hits["1.2.3.4"]["referrer"])
        # First hit was NOT a search engine — should be excluded
        self.assertIsNone(engine)


# ---------------------------------------------------------------------------
# Tests — Full Pipeline / Functional (end-to-end with sample data)
# ---------------------------------------------------------------------------

class TestFullPipelineFunctional(unittest.TestCase):
    """
    Functional tests: full attribute_revenue() pipeline using data.sql sample.

    These directly validate the business requirements from the exercise spec:
        "How much revenue is the client getting from external Search Engines,
         and which keywords are performing the best based on revenue?"
    """

    def setUp(self):
        self.result = attribute_revenue(SAMPLE_ALL_HITS, SAMPLE_PRODUCT_ROWS)

    def test_output_has_correct_columns(self):
        """Output must have exactly: Search Engine Domain, Search Keyword, Revenue."""
        for row in self.result:
            self.assertIn("Search Engine Domain", row)
            self.assertIn("Search Keyword",       row)
            self.assertIn("Revenue",              row)

    def test_output_sorted_descending_by_revenue(self):
        """
        Deliverable requirement: sorted by revenue descending so the client
        can easily review which keyword is performing best.
        """
        revenues = [r["Revenue"] for r in self.result]
        self.assertEqual(revenues, sorted(revenues, reverse=True))

    def test_google_ipod_aggregates_both_sessions(self):
        """
        Functional: 67.98.123.1 (Ipod/290) and 44.12.96.2 (ipod/190) both
        came from Google searching for ipod (different cases).
        After case-normalisation they must be grouped into one row: 480.0.
        """
        google_ipod = next(
            (r for r in self.result
             if r["Search Engine Domain"] == "google.com"
             and r["Search Keyword"] == "ipod"),
            None
        )
        self.assertIsNotNone(google_ipod, "Expected google.com/ipod row in output")
        self.assertEqual(google_ipod["Revenue"], 480.0)

    def test_bing_zune_revenue(self):
        """23.8.61.21 arrived via Bing/Zune and purchased a Zune for 250."""
        bing_zune = next(
            (r for r in self.result
             if r["Search Engine Domain"] == "bing.com"
             and r["Search Keyword"] == "zune"),
            None
        )
        self.assertIsNotNone(bing_zune, "Expected bing.com/zune row in output")
        self.assertEqual(bing_zune["Revenue"], 250.0)

    def test_yahoo_cd_player_shows_zero_revenue(self):
        """
        Functional: 112.33.98.231 arrived via Yahoo/cd player but never
        purchased. Must appear in output with Revenue = 0, not be dropped.
        """
        yahoo_cd = next(
            (r for r in self.result
             if r["Search Engine Domain"] == "yahoo.com"
             and r["Search Keyword"] == "cd player"),
            None
        )
        self.assertIsNotNone(yahoo_cd,
            "112.33.98.231 (yahoo/cd player, no purchase) must appear with Revenue=0")
        self.assertEqual(yahoo_cd["Revenue"], 0.0)

    def test_all_three_search_engines_present(self):
        """All three search-driven IPs must be represented in the output."""
        domains = {r["Search Engine Domain"] for r in self.result}
        self.assertIn("google.com", domains)
        self.assertIn("bing.com",   domains)
        self.assertIn("yahoo.com",  domains)

    def test_product_view_revenue_not_counted(self):
        """
        Functional: event=2 (product view) rows have no revenue.
        Per Appendix B, revenue is only actualized on event=1 (purchase).
        Total google/ipod revenue must be 290+190=480, not inflated by views.
        """
        google_ipod = next(
            r for r in self.result
            if r["Search Engine Domain"] == "google.com"
        )
        self.assertEqual(google_ipod["Revenue"], 480.0)

    def test_internal_referrer_sessions_excluded(self):
        """
        No esshopzilla.com rows should appear as a Search Engine Domain —
        internal navigation must never be treated as a search engine.
        """
        domains = {r["Search Engine Domain"] for r in self.result}
        self.assertNotIn("esshopzilla.com", domains)

    def test_revenue_is_numeric(self):
        """Revenue values must be numeric (float) for correct sorting/display."""
        for row in self.result:
            self.assertIsInstance(row["Revenue"], float,
                f"Revenue must be float, got {type(row['Revenue'])} for {row}")

    def test_keywords_are_lowercase(self):
        """All keywords in output must be lowercase."""
        for row in self.result:
            kw = row["Search Keyword"]
            self.assertEqual(kw, kw.lower(),
                f"Keyword '{kw}' is not lowercase")

    def test_output_row_count(self):
        """
        With sample data: 3 distinct (engine, keyword) combinations:
          google.com/ipod, bing.com/zune, yahoo.com/cd player
        """
        self.assertEqual(len(self.result), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
