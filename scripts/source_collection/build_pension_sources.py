#!/usr/bin/env python3
"""
Build a starter pension-source inventory for:
1) State employee systems (50 states + DC + selected territories)
2) 50 largest non-state public pension systems

AUM baseline: Public Plans Data (RetSysData latest FY market assets).
Document links: Public Plans Data financial-report endpoint (plan/year reports).
Territories: manual official links where available.
"""

from __future__ import annotations

import json
import re
import time
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import pandas as pd
import requests

PPD_XLSX_URL = "https://publicplansdata.org/wp-content/uploads/2024/09/RetSysData.xlsx"
PPD_AJAX_URL = "https://publicplansdata.org/wp-admin/admin-ajax.php"
PPD_ROOT = "https://publicplansdata.org"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}

BAD_SEARCH_DOMAINS = {
    "publicplansdata.org",
    "nasra.org",
    "wikipedia.org",
    "facebook.com",
    "x.com",
    "twitter.com",
    "linkedin.com",
}

STATE_PLAN_IDS = {
    "AL": 1,
    "AK": 3,
    "AZ": 6,
    "AR": 7,
    "CA": 9,
    "CO": 13,
    "CT": 14,
    "DE": 18,
    "FL": 23,
    "GA": 24,
    "HI": 26,
    "ID": 28,
    "IL": 30,
    "IN": 130,
    "IA": 35,
    "KS": 36,
    "KY": 37,
    "LA": 40,
    "ME": 42,
    "MD": 43,
    "MA": 44,
    "MI": 48,
    "MN": 51,
    "MS": 53,
    "MO": 57,
    "MT": 59,
    "NE": 61,
    "NV": 62,
    "NH": 63,
    "NJ": 64,
    "NM": 65,
    "NY": 71,
    "NC": 68,
    "ND": 69,
    "OH": 72,
    "OK": 77,
    "OR": 79,
    "PA": 81,
    "RI": 83,
    "SC": 86,
    "SD": 87,
    "TN": 94,
    "TX": 91,
    "UT": 96,
    "VT": 97,
    "VA": 99,
    "WA": 100,
    "WV": 101,
    "WI": 102,
    "WY": 103,
    "DC": 17,
}

# Report-plan IDs from PPD "state-plans" endpoint for the chosen state-employee plan in each state/DC.
STATE_REPORT_PLAN_IDS = {
    "AL": 1,
    "AK": 3,
    "AZ": 6,
    "AR": 7,
    "CA": 9,
    "CO": 15,
    "CT": 16,
    "DE": 21,
    "FL": 26,
    "GA": 27,
    "HI": 29,
    "ID": 31,
    "IL": 33,
    "IN": 36,
    "IA": 38,
    "KS": 39,
    "KY": 41,
    "LA": 44,
    "ME": 47,
    "MD": 48,
    "MA": 50,
    "MI": 54,
    "MN": 57,
    "MS": 59,
    "MO": 63,
    "MT": 65,
    "NE": 67,
    "NV": 69,
    "NH": 70,
    "NJ": 71,
    "NM": 74,
    "NY": 83,
    "NC": 80,
    "ND": 81,
    "OH": 85,
    "OK": 89,
    "OR": 91,
    "PA": 93,
    "RI": 95,
    "SC": 100,
    "SD": 101,
    "TN": 110,
    "TX": 105,
    "UT": 112,
    "VT": 113,
    "VA": 115,
    "WA": 119,
    "WV": 123,
    "WI": 125,
    "WY": 126,
    "DC": 19,
}

STATE_NAMES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}

NON_STATE_PLAN_IDS = [
    95,
    76,
    39,
    58,
    124,
    29,
    90,
    92,
    85,
    114,
    116,
    123,
    113,
    73,
    115,
    84,
    121,
    46,
    117,
    11,
    16,
    118,
    55,
    111,
    122,
    126,
    183,
    185,
    125,
    112,
    27,
    143,
    141,
    119,
    179,
    133,
    135,
    120,
    176,
    82,
    128,
    12,
    105,
    106,
    132,
    136,
    186,
    138,
    150,
    131,
    157,
]

# Map RetSysData system_id -> PPD report-plan ID for the same non-state system.
NON_STATE_REPORT_PLAN_IDS = {
    95: 111,
    76: 76,
    39: 43,
    58: 77,
    124: 150,
    29: 32,
    90: 104,
    92: 107,
    85: 98,
    114: 140,
    116: 142,
    123: 149,
    113: 139,
    73: 86,
    115: 141,
    84: 97,
    121: 147,
    46: 52,
    117: 143,
    11: 11,
    16: 18,
    118: 144,
    55: 61,
    111: 137,
    122: 148,
    126: 152,
    183: 208,
    185: 211,
    125: 151,
    112: 138,
    27: 30,
    143: 166,
    141: 164,
    119: 145,
    179: 204,
    133: 156,
    135: 158,
    120: 146,
    176: 201,
    82: 94,
    128: 136,
    12: 12,
    105: 128,
    106: 129,
    132: 155,
    136: 159,
    186: 212,
    138: 161,
    150: 173,
    131: 154,
    157: 181,
}

TERRITORY_ROWS = [
    {
        "segment": "territory",
        "jurisdiction_code": "PR",
        "jurisdiction_name": "Puerto Rico",
        "pension_name": "Puerto Rico Government Employees Retirement System",
        "official_home_url": "https://hacienda.pr.gov/",
        "annual_report_url": "https://hacienda.pr.gov/sites/default/files/employee_retirement_system.pdf",
        "latest_annual_pdf_url": "https://hacienda.pr.gov/sites/default/files/employee_retirement_system.pdf",
        "aum_usd_billions": None,
        "aum_fiscal_year": None,
        "aum_source": "Needs manual extraction from territory filing",
        "investment_docs_url": "",
        "actuarial_or_alm_url": "",
        "consultant_reports_url": "",
        "board_materials_url": "",
        "notes": "Official filing URL identified; AUM pending manual extraction.",
    },
    {
        "segment": "territory",
        "jurisdiction_code": "GU",
        "jurisdiction_name": "Guam",
        "pension_name": "Guam Retirement Fund",
        "official_home_url": "https://ggrf.com/",
        "annual_report_url": "https://ggrf.com/about-us/financial-reports",
        "latest_annual_pdf_url": "",
        "aum_usd_billions": None,
        "aum_fiscal_year": None,
        "aum_source": "Needs manual extraction from territory filing",
        "investment_docs_url": "",
        "actuarial_or_alm_url": "",
        "consultant_reports_url": "",
        "board_materials_url": "",
        "notes": "Official financial-reports page identified; latest PDF to be selected manually.",
    },
    {
        "segment": "territory",
        "jurisdiction_code": "VI",
        "jurisdiction_name": "U.S. Virgin Islands",
        "pension_name": "Virgin Islands Government Employees' Retirement System",
        "official_home_url": "https://www.usvigers.com/",
        "annual_report_url": "https://www.usvigers.com/reports/annual-reports/",
        "latest_annual_pdf_url": "",
        "aum_usd_billions": None,
        "aum_fiscal_year": None,
        "aum_source": "Needs manual extraction from territory filing",
        "investment_docs_url": "",
        "actuarial_or_alm_url": "",
        "consultant_reports_url": "",
        "board_materials_url": "",
        "notes": "Official annual-report page identified; AUM pending extraction.",
    },
    {
        "segment": "territory",
        "jurisdiction_code": "AS",
        "jurisdiction_name": "American Samoa",
        "pension_name": "American Samoa Government Employees Retirement Fund",
        "official_home_url": "https://www.americansamoa.gov/",
        "annual_report_url": "https://www.americansamoa.gov/",
        "latest_annual_pdf_url": "",
        "aum_usd_billions": None,
        "aum_fiscal_year": None,
        "aum_source": "Public annual-report URL not clearly published",
        "investment_docs_url": "",
        "actuarial_or_alm_url": "",
        "consultant_reports_url": "",
        "board_materials_url": "",
        "notes": "Public pension report endpoint not clearly discoverable from official site.",
    },
    {
        "segment": "territory",
        "jurisdiction_code": "MP",
        "jurisdiction_name": "Northern Mariana Islands",
        "pension_name": "Northern Mariana Islands Settlement Fund",
        "official_home_url": "https://www.nmisf.com/",
        "annual_report_url": "https://www.nmisf.com/",
        "latest_annual_pdf_url": "https://www.nmisf.com/wp-content/uploads/2024/11/NMISF-Financial-Statements-FY2023-Final.pdf",
        "aum_usd_billions": None,
        "aum_fiscal_year": None,
        "aum_source": "Needs manual extraction from territory filing",
        "investment_docs_url": "",
        "actuarial_or_alm_url": "",
        "consultant_reports_url": "",
        "board_materials_url": "",
        "notes": "Official FY2023 financial statement PDF identified.",
    },
]

MANUAL_ANNUAL_URL_OVERRIDES = {
    "District of Columbia Retirement Board": "https://dcrb.dc.gov/service/annual-comprehensive-financial-reports",
    "Connecticut State Employees Retirement System": "https://osc.ct.gov/retirement/sers/",
    "Massachusetts State Employees' Retirement System": "https://archives.lib.state.ma.us/collections/778f1046-40c8-4a73-9678-f8cdc25f0c5b",
    "Vermont State Employees Retirement System": "https://legislature.vermont.gov/Documents/2024/WorkGroups/Joint%20Pension%20Oversight/Documents%20and%20Testimony/W~Chris%20Rupe~VSERS%20Actuarial%20Valuation%20Report~11-6-2024.pdf",
    "Wisconsin Retirement System": "https://etf.wi.gov/about-etf/reports-and-studies/financial-reports-and-statements",
    "North Carolina Retirement Systems": "https://www.myncretirement.gov/governance/valuations-and-annual-comprehensive-financial-reports",
    "Texas County & District Retirement System": "https://www.tcdrs.org/about/financial-reports/",
    "Los Angeles Fire and Police": "https://content.lafpp.lacity.gov/wp-content/uploads/2025/03/LAFPP_AnnualReport_2024_web-FINAL-WEB-3-4-2025.pdf",
    "Orange County ERS": "https://www.ocers.org/financial-reports",
    "Los Angeles City Employees Retirement System": "https://www.lacers.org/reports",
    "Cook County Employees": "https://www.cookcountypension.com/about/annual-financial-reports/",
    "Contra Costa County Employees' Retirement Association": "https://www.cccera.gov/financial-reports",
    "San Diego City ERS": "https://www.sdcers.org/financial-reports",
    "Missouri Local Government Employees Retirement System": "https://www.molagers.org/financial-reports/",
    "Milwaukee City ERS": "https://city.milwaukee.gov/ERS/Reports",
    "Phoenix Employees' Retirement System": "https://www.phoenix.gov/administration/departments/retirement/pension-plan-reports",
    "Pennsylvania Municipal Retirement System": "https://pmrs.pa.gov/financial-reports/",
    "Jacksonville Police and Fire": "https://www.jacksonville.gov/departments/finance/retirement-system",
    "Philadelphia Municipal Retirement System": "https://www.phila.gov/departments/board-of-pensions-and-retirement/reports/",
    "Nashville-Davidson Metropolitan Employees Benefit Trust Fund": "https://www.nashville.gov/sites/default/files/2025-03/Metro-Disclosure-2024.pdf?ct=1743432775",
    "Connecticut Municipal": "https://osc.ct.gov/public/retirement/cmers-annual-reports/",
    "Jacksonville General Employee Pension Plan": "https://www.jacksonville.gov/departments/finance/retirement-system",
}


def request_get(url: str, params: dict[str, Any] | None = None) -> requests.Response:
    resp = requests.get(url, params=params, headers=HEADERS, timeout=40)
    resp.raise_for_status()
    return resp


def base_site(url: str) -> str:
    if not url:
        return ""
    p = urlparse(url)
    if not p.scheme or not p.netloc:
        return ""
    return f"{p.scheme}://{p.netloc}/"


def clean_ddg_url(raw_url: str) -> str:
    if raw_url.startswith("//duckduckgo.com/l/?") or "duckduckgo.com/l/?" in raw_url:
        if raw_url.startswith("//"):
            raw_url = "https:" + raw_url
        qs = parse_qs(urlparse(raw_url).query)
        if "uddg" in qs and qs["uddg"]:
            return unquote(qs["uddg"][0])
    return raw_url


def ddg_lookup_annual(plan_name: str, jurisdiction_hint: str) -> str:
    time.sleep(0.35)
    query = f"{plan_name} {jurisdiction_hint} annual report"
    url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        text = request_get(url).text
    except Exception:
        return ""

    links = re.findall(r'class="result__a" href="([^"]+)"', text)
    scored: list[tuple[int, str]] = []
    for raw in links:
        u = clean_ddg_url(raw).split("#")[0]
        if not u.startswith("http"):
            continue
        host = urlparse(u).netloc.lower()
        host = host[4:] if host.startswith("www.") else host
        if any(host == b or host.endswith("." + b) for b in BAD_SEARCH_DOMAINS):
            continue
        low = u.lower()
        score = 0
        if ".gov" in host:
            score += 3
        if low.endswith(".pdf"):
            score += 2
        if any(k in low for k in ["annual", "acfr", "cafr", "financial", "report"]):
            score += 2
        if "login" in low:
            score -= 2
        scored.append((score, u))
    if not scored:
        return ""
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def url_looks_like_pdf(url: str) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    return path.endswith(".pdf")


def find_pdf_from_annual_page(url: str, plan_name: str) -> str:
    if not url:
        return ""
    if url_looks_like_pdf(url):
        return url
    try:
        resp = request_get(url)
    except Exception:
        return ""
    ctype = resp.headers.get("content-type", "").lower()
    if "pdf" in ctype:
        return url
    if "html" not in ctype and not resp.text:
        return ""

    html = resp.text
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I)
    direct_pdf_candidates: list[str] = []
    linked_candidates: list[tuple[float, str]] = []
    plan_terms = set(re.sub(r"[^a-z0-9 ]+", " ", plan_name.lower()).split())
    year_tokens = {"2026", "2025", "2024", "2023", "2022", "2021"}
    for href in hrefs:
        href = unescape(href.strip())
        full = urljoin(resp.url, href)
        if not full.startswith("http"):
            continue
        if url_looks_like_pdf(full):
            direct_pdf_candidates.append(full)
            continue

        low = full.lower()
        toks = set(re.sub(r"[^a-z0-9 ]+", " ", low).split())
        score: float = 0
        if any(y in low for y in year_tokens):
            score += 4
        if any(k in low for k in ["annual", "acfr", "cafr", "financial", "report", "pafr"]):
            score += 5
        if any(k in low for k in ["wrs", "retirement", "pension"]):
            score += 1.5
        if any(k in low for k in ["valuation", "actuarial", "gasb"]):
            score += 0.5
        if any(k in low for k in ["minutes", "agenda", "form", "newsletter"]):
            score -= 3
        score += len(plan_terms & toks) * 0.4
        if score > 0:
            linked_candidates.append((score, full))

    if direct_pdf_candidates:
        best_score = None
        best_url = ""
        plan_terms = set(re.sub(r"[^a-z0-9 ]+", " ", plan_name.lower()).split())
        for c in direct_pdf_candidates:
            low = c.lower()
            toks = set(re.sub(r"[^a-z0-9 ]+", " ", low).split())
            score: float = 0
            if any(y in low for y in {"2026", "2025", "2024", "2023", "2022"}):
                score += 4
            if any(k in low for k in ["annual", "acfr", "cafr", "financial", "report", "pafr"]):
                score += 5
            if any(k in low for k in ["wrs", "retirement", "pension"]):
                score += 1.5
            if any(k in low for k in ["minutes", "agenda", "form", "newsletter"]):
                score -= 3
            score += len(plan_terms & toks) * 0.4
            if best_score is None or score > best_score:
                best_score = score
                best_url = c
        if best_url:
            return best_url

    # Some official sites use non-.pdf links (for example "Open PDF" endpoints).
    linked_candidates.sort(key=lambda x: x[0], reverse=True)
    for _, cand in linked_candidates[:20]:
        try:
            c_resp = request_get(cand)
        except Exception:
            continue
        ctype = c_resp.headers.get("content-type", "").lower()
        if "pdf" in ctype:
            return c_resp.url
        if url_looks_like_pdf(c_resp.url):
            return c_resp.url
    return ""


def money_k_to_billion(value: float | int | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value) / 1_000_000, 3)


def load_latest_ret_sys_data() -> pd.DataFrame:
    usecols = [
        "system_id",
        "RetirementSystemName",
        "RetirementSystemStateAbbrev",
        "fy",
        "MktAssets_net",
    ]
    df = pd.read_excel(PPD_XLSX_URL, usecols=usecols)
    latest = df.sort_values(["system_id", "fy"]).groupby("system_id").tail(1).copy()
    latest["aum_usd_billions"] = latest["MktAssets_net"].apply(money_k_to_billion)
    latest["aum_fiscal_year"] = latest["fy"].astype("Int64")
    return latest


def parse_option_values(html: str) -> list[str]:
    return re.findall(r"<option[^>]*>([^<]+)</option>", html)


def parse_report_links(html: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for href, label in re.findall(r"<a[^>]*href=['\"]([^'\"]+)['\"][^>]*>([^<]+)</a>", html):
        url = urljoin(PPD_ROOT, href.strip())
        out.append((label.strip(), url))
    return out


def fetch_latest_plan_reports(plan_id: int) -> dict[str, Any]:
    years_html = request_get(
        PPD_AJAX_URL,
        params={"action": "ppd_fetch_report_data", "type": "plan-report-years", "planid": plan_id},
    ).text
    years = []
    for y in parse_option_values(years_html):
        y = y.strip()
        if y.isdigit():
            years.append(int(y))
    if not years:
        return {"report_fy": None, "links": []}

    report_fy = max(years)
    links_html = request_get(
        PPD_AJAX_URL,
        params={
            "action": "ppd_fetch_report_data",
            "type": "plan-reports",
            "planid": plan_id,
            "fy": report_fy,
        },
    ).text
    links = parse_report_links(links_html)
    return {"report_fy": report_fy, "links": links}


def choose_link(
    links: list[tuple[str, str]], include_terms: list[str], priority_labels: list[str]
) -> str:
    if not links:
        return ""
    for p in priority_labels:
        for lbl, url in links:
            if lbl.lower() == p.lower():
                return url
    for lbl, url in links:
        low = lbl.lower()
        if any(t in low for t in include_terms):
            return url
    return ""


def build_row(
    *,
    segment: str,
    jurisdiction_code: str,
    jurisdiction_name: str,
    system_id: int,
    report_plan_id: int | None,
    latest_df: pd.DataFrame,
    report_map: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    latest_match = latest_df[latest_df["system_id"] == system_id]
    if latest_match.empty:
        pension_name = f"Plan {system_id}"
        aum_b = None
        aum_fy = None
    else:
        r = latest_match.iloc[0]
        pension_name = str(r["RetirementSystemName"])
        aum_b = r["aum_usd_billions"]
        aum_fy = int(r["aum_fiscal_year"]) if pd.notna(r["aum_fiscal_year"]) else None

    reports = report_map.get(report_plan_id or -1, {"report_fy": None, "links": []})
    links: list[tuple[str, str]] = reports["links"]
    labels = [lbl for lbl, _ in links]

    annual_url = choose_link(
        links,
        include_terms=["financial report", "annual", "acfr", "cafr"],
        priority_labels=["Financial Report"],
    )
    investment_url = choose_link(
        links,
        include_terms=["investment", "invpol", "policy"],
        priority_labels=["Investment Policies"],
    )
    actuarial_url = choose_link(
        links,
        include_terms=["av", "actuarial", "gasb67", "gasb68"],
        priority_labels=["AV", "GASB67-68 AV"],
    )
    consultant_url = choose_link(
        links,
        include_terms=["consultant", "advisor", "consulting"],
        priority_labels=[],
    )

    notes = ""
    if labels:
        notes = "PPD report types for latest report FY: " + ", ".join(labels)

    official_home = base_site(annual_url)

    return {
        "segment": segment,
        "jurisdiction_code": jurisdiction_code,
        "jurisdiction_name": jurisdiction_name,
        "pension_name": pension_name,
        "official_home_url": official_home,
        "annual_report_url": annual_url,
        "latest_annual_pdf_url": annual_url,
        "aum_usd_billions": aum_b,
        "aum_fiscal_year": aum_fy,
        "aum_source": "Public Plans Data RetSysData.xlsx latest FY market assets",
        "investment_docs_url": investment_url,
        "actuarial_or_alm_url": actuarial_url,
        "consultant_reports_url": consultant_url,
        "board_materials_url": "",
        "notes": notes,
    }


def download_pdf(url: str, path: Path) -> bool:
    if not url:
        return False
    try:
        resp = request_get(url)
        ctype = resp.headers.get("content-type", "").lower()
        if "pdf" not in ctype and not url.lower().endswith(".pdf"):
            return False
        path.write_bytes(resp.content)
        return True
    except Exception:
        return False


def write_markdown_summary(
    df_all: pd.DataFrame, out_path: Path, downloads_ok: int, downloads_fail: int
) -> None:
    lines = [
        "# Pension Sources (Starter Baseline)",
        "",
        "Generated: 2026-03-01",
        "",
        "## Coverage",
        "- 50 states + DC (state employee system focus)",
        "- 5 territories (Puerto Rico, Guam, U.S. Virgin Islands, American Samoa, Northern Mariana Islands)",
        "- 50 largest non-state public pension systems (plus supplemental rows when needed to keep >=50 with annual-report PDFs)",
        "",
        "## Notes",
        "- AUM uses Public Plans Data market assets from each plan's latest available fiscal year.",
        "- Annual report and related document links are sourced from Public Plans Data report endpoints (or manual territory official links).",
        "- Hawaii and New Jersey state-system AUM values are currently blank and require manual extraction from official filings.",
        "- For schema discovery, columns include additional document classes: investment, actuarial/ALM, consultant, and board materials.",
        "",
        "## Record Counts",
        f"- Total: {len(df_all)}",
        f"- State/DC/Territory: {len(df_all[df_all['segment'].isin(['state_employee_or_dc', 'territory'])])}",
        f"- Non-state largest: {len(df_all[df_all['segment'] == 'largest_non_state_public_plan'])}",
        "",
        "## Local Document Download",
        f"- Downloaded annual PDFs (all segments): {downloads_ok}",
        f"- Failed/skipped downloads: {downloads_fail}",
        "- Local folder: `/Users/teacher/Library/CloudStorage/Dropbox/Learning/Code/Pension-Data-local-docs/annual_reports`",
        "",
        "## Output Files",
        "- `state_and_territory_pensions.csv`",
        "- `non_state_largest_public_pensions.csv`",
        "- `pension_sources_inventory.csv`",
        "- `annual_report_download_log.json`",
        "- `state_download_log.json`",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    out_dir = Path("doc/Sources")
    out_dir.mkdir(parents=True, exist_ok=True)

    local_docs_root = Path(
        "/Users/teacher/Library/CloudStorage/Dropbox/Learning/Code/Pension-Data-local-docs/annual_reports"
    )
    local_docs_root.mkdir(parents=True, exist_ok=True)

    latest_df = load_latest_ret_sys_data()

    report_map: dict[int, dict[str, Any]] = {}
    all_report_plan_ids = sorted(
        set(list(STATE_REPORT_PLAN_IDS.values()) + list(NON_STATE_REPORT_PLAN_IDS.values()))
    )
    for report_plan_id in all_report_plan_ids:
        report_map[report_plan_id] = fetch_latest_plan_reports(report_plan_id)

    rows: list[dict[str, Any]] = []

    for code, system_id in STATE_PLAN_IDS.items():
        rows.append(
            build_row(
                segment="state_employee_or_dc",
                jurisdiction_code=code,
                jurisdiction_name=STATE_NAMES[code],
                system_id=system_id,
                report_plan_id=STATE_REPORT_PLAN_IDS.get(code),
                latest_df=latest_df,
                report_map=report_map,
            )
        )

    rows.extend(TERRITORY_ROWS)

    for system_id in NON_STATE_PLAN_IDS:
        match = latest_df[latest_df["system_id"] == system_id]
        state_code = ""
        if not match.empty:
            state_code = str(match.iloc[0]["RetirementSystemStateAbbrev"])
        rows.append(
            build_row(
                segment="largest_non_state_public_plan",
                jurisdiction_code=state_code,
                jurisdiction_name=state_code,
                system_id=system_id,
                report_plan_id=NON_STATE_REPORT_PLAN_IDS.get(system_id),
                latest_df=latest_df,
                report_map=report_map,
            )
        )

    df = pd.DataFrame(rows)
    cols = [
        "segment",
        "jurisdiction_code",
        "jurisdiction_name",
        "pension_name",
        "official_home_url",
        "annual_report_url",
        "latest_annual_pdf_url",
        "aum_usd_billions",
        "aum_fiscal_year",
        "aum_source",
        "investment_docs_url",
        "actuarial_or_alm_url",
        "consultant_reports_url",
        "board_materials_url",
        "notes",
    ]
    df = df[cols]

    # Fill unresolved annual-report URLs with targeted web lookup.
    missing_idx = df[df["annual_report_url"].fillna("").eq("")].index.tolist()
    for idx in missing_idx:
        plan_name = str(df.at[idx, "pension_name"])
        jurisdiction = str(df.at[idx, "jurisdiction_name"])
        found = ddg_lookup_annual(plan_name, jurisdiction)
        if not found:
            continue
        df.at[idx, "annual_report_url"] = found
        if not str(df.at[idx, "latest_annual_pdf_url"] or "") and found.lower().endswith(".pdf"):
            df.at[idx, "latest_annual_pdf_url"] = found
        if not str(df.at[idx, "official_home_url"] or ""):
            df.at[idx, "official_home_url"] = base_site(found)
        note = str(df.at[idx, "notes"] or "")
        suffix = "Annual report URL filled by targeted web lookup."
        df.at[idx, "notes"] = (note + " " + suffix).strip()

    # Deterministic overrides for plans where stable official report pages are known.
    for idx, row in df.iterrows():
        pname = str(row["pension_name"])
        if pname not in MANUAL_ANNUAL_URL_OVERRIDES:
            continue
        annual_url = MANUAL_ANNUAL_URL_OVERRIDES[pname]
        df.at[idx, "annual_report_url"] = annual_url
        df.at[idx, "official_home_url"] = base_site(annual_url)
        if url_looks_like_pdf(annual_url):
            df.at[idx, "latest_annual_pdf_url"] = annual_url
        note = str(df.at[idx, "notes"] or "")
        suffix = "Manual override for official annual-report URL."
        df.at[idx, "notes"] = (note + " " + suffix).strip()

    # Resolve latest annual PDF URL from annual-report landing pages.
    for idx, row in df.iterrows():
        current_pdf = str(row["latest_annual_pdf_url"] or "")
        annual_url = str(row["annual_report_url"] or "")
        if current_pdf and url_looks_like_pdf(current_pdf):
            continue
        source_url = annual_url if annual_url else current_pdf
        found_pdf = find_pdf_from_annual_page(source_url, str(row["pension_name"]))
        if not found_pdf:
            continue
        df.at[idx, "latest_annual_pdf_url"] = found_pdf
        if not str(df.at[idx, "official_home_url"] or ""):
            df.at[idx, "official_home_url"] = base_site(found_pdf)
        note = str(df.at[idx, "notes"] or "")
        suffix = "Latest annual PDF URL resolved from annual-report page."
        df.at[idx, "notes"] = (note + " " + suffix).strip()

    state_df = df[df["segment"].isin(["state_employee_or_dc", "territory"])].copy()
    non_state_df = df[df["segment"] == "largest_non_state_public_plan"].copy()

    state_df.to_csv(out_dir / "state_and_territory_pensions.csv", index=False)
    non_state_df.to_csv(out_dir / "non_state_largest_public_pensions.csv", index=False)
    df.to_csv(out_dir / "pension_sources_inventory.csv", index=False)

    download_log: list[dict[str, Any]] = []
    state_download_log: list[dict[str, Any]] = []
    ok_count = 0
    fail_count = 0
    for _, r in df.iterrows():
        pdf_url = str(r["latest_annual_pdf_url"] or "")
        seg = str(r["segment"])
        seg_dir = local_docs_root / seg
        seg_dir.mkdir(parents=True, exist_ok=True)
        slug = re.sub(r"[^a-z0-9]+", "_", str(r["pension_name"]).lower()).strip("_")
        out_file = seg_dir / f"{r['jurisdiction_code']}_{slug}.pdf"
        if not pdf_url:
            fail_count += 1
            item = {
                "segment": seg,
                "jurisdiction_code": r["jurisdiction_code"],
                "pension_name": r["pension_name"],
                "status": "skipped_no_pdf_url",
                "url": "",
                "file_path": "",
            }
            download_log.append(item)
            if seg in {"state_employee_or_dc", "territory"}:
                state_download_log.append(item)
            continue
        ok = download_pdf(pdf_url, out_file)
        if ok:
            ok_count += 1
        else:
            fail_count += 1
        item = {
            "segment": seg,
            "jurisdiction_code": r["jurisdiction_code"],
            "pension_name": r["pension_name"],
            "status": "downloaded" if ok else "failed_download",
            "url": pdf_url,
            "file_path": str(out_file) if ok else "",
        }
        download_log.append(item)
        if seg in {"state_employee_or_dc", "territory"}:
            state_download_log.append(item)

    (out_dir / "annual_report_download_log.json").write_text(
        json.dumps(download_log, indent=2), encoding="utf-8"
    )
    (out_dir / "state_download_log.json").write_text(
        json.dumps(state_download_log, indent=2), encoding="utf-8"
    )
    write_markdown_summary(df, out_dir / "README.md", ok_count, fail_count)

    print(f"Wrote {out_dir / 'state_and_territory_pensions.csv'} ({len(state_df)} rows)")
    print(f"Wrote {out_dir / 'non_state_largest_public_pensions.csv'} ({len(non_state_df)} rows)")
    print(f"Wrote {out_dir / 'pension_sources_inventory.csv'} ({len(df)} rows)")
    print(f"Wrote {out_dir / 'annual_report_download_log.json'}")
    print(f"Wrote {out_dir / 'state_download_log.json'}")
    print(f"Wrote {out_dir / 'README.md'}")
    print(f"Downloaded PDFs: {ok_count}; failed/skipped: {fail_count}")
    print(f"Local docs folder: {local_docs_root}")


if __name__ == "__main__":
    main()
