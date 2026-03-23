import os
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("rix-credit-api")

APP_NAME = "Rix Credit API"
APP_VERSION = "3.3.1"

# -----------------------------------------------------------------------------
# Environment / Config
# -----------------------------------------------------------------------------

CH_API_KEY = os.getenv("CH_API_KEY", "").strip()
CH_API_BASE = "https://api.company-information.service.gov.uk"

EXPERIAN_MODE = os.getenv("EXPERIAN_MODE", "mock").strip().lower()  # mock | live
EXPERIAN_BASE_URL = os.getenv("EXPERIAN_BASE_URL", "https://sandbox-uk-api.experian.com").strip()
EXPERIAN_CLIENT_ID = os.getenv("EXPERIAN_CLIENT_ID", "").strip()
EXPERIAN_CLIENT_SECRET = os.getenv("EXPERIAN_CLIENT_SECRET", "").strip()
EXPERIAN_USERNAME = os.getenv("EXPERIAN_USERNAME", "").strip()
EXPERIAN_PASSWORD = os.getenv("EXPERIAN_PASSWORD", "").strip()

EXPERIAN_TOKEN_PATH = os.getenv("EXPERIAN_TOKEN_PATH", "/oauth2/v1/token").strip()
EXPERIAN_SEARCH_PATH = os.getenv("EXPERIAN_SEARCH_PATH", "/businessinformation/businesses/v1/search").strip()
EXPERIAN_REPORT_PATH_TEMPLATE = os.getenv(
    "EXPERIAN_REPORT_PATH_TEMPLATE",
    "/businessinformation/businesses/v1/{business_id}/report"
).strip()

HTTP_TIMEOUT = (3.05, 12)
EXPERIAN_TIMEOUT = (3.05, 10)

ALLOW_PARTIAL_RESULTS = os.getenv("ALLOW_PARTIAL_RESULTS", "true").strip().lower() == "true"

app = FastAPI(title=APP_NAME, version=APP_VERSION)


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return int(value)
        return int(float(value))
    except Exception:
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "yes", "y", "1"}:
            return True
        if v in {"false", "no", "n", "0"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def elapsed_ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


def get_first(*values):
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def money(amount: Optional[float], currency: str = "GBP") -> Dict[str, Any]:
    return {
        "amount": amount if amount is None else float(amount),
        "currency": currency
    }


def get_in(obj: Any, *path: str) -> Any:
    cur = obj
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


# -----------------------------------------------------------------------------
# Companies House
# -----------------------------------------------------------------------------

def ch_session() -> requests.Session:
    s = requests.Session()
    s.auth = (CH_API_KEY, "")
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": f"rix-credit-api/{APP_VERSION}",
    })
    return s


def ch_get(url: str, params=None) -> Dict[str, Any]:
    if not CH_API_KEY:
        raise HTTPException(500, "CH_API_KEY is missing")

    with ch_session() as s:
        r = s.get(url, params=params, timeout=HTTP_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"CH API error: {r.status_code} {r.text[:300]}")

    return r.json()


def get_companies_house_bundle(company_number: str) -> Dict[str, Any]:
    warnings: List[str] = []

    try:
        profile = ch_get(f"{CH_API_BASE}/company/{company_number}")
    except Exception as e:
        logger.exception("Failed to fetch company profile")
        return {
            "available": False,
            "source": "companies_house",
            "company_profile": {},
            "officers": [],
            "pscs": [],
            "charges": [],
            "filing_history": [],
            "warnings": [f"company_profile unavailable: {str(e)}"],
        }

    def safe_fetch(url: str, key: str) -> Dict[str, Any]:
        try:
            return ch_get(url)
        except Exception as exc:
            warnings.append(f"{key} unavailable: {str(exc)}")
            return {}

    officers_raw = safe_fetch(f"{CH_API_BASE}/company/{company_number}/officers", "officers")
    pscs_raw = safe_fetch(f"{CH_API_BASE}/company/{company_number}/persons-with-significant-control", "pscs")
    charges_raw = safe_fetch(f"{CH_API_BASE}/company/{company_number}/charges", "charges")
    filings_raw = safe_fetch(f"{CH_API_BASE}/company/{company_number}/filing-history", "filing_history")

    return {
        "available": True,
        "source": "companies_house",
        "company_profile": profile,
        "officers": officers_raw.get("items", []) if isinstance(officers_raw, dict) else [],
        "pscs": pscs_raw.get("items", []) if isinstance(pscs_raw, dict) else [],
        "charges": charges_raw.get("items", []) if isinstance(charges_raw, dict) else [],
        "filing_history": filings_raw.get("items", []) if isinstance(filings_raw, dict) else [],
        "warnings": warnings,
    }


# -----------------------------------------------------------------------------
# Experian
# -----------------------------------------------------------------------------

def experian_session(token: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": f"rix-credit-api/{APP_VERSION}",
    })
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    return s


def experian_get_token() -> str:
    if not EXPERIAN_CLIENT_ID or not EXPERIAN_CLIENT_SECRET:
        raise HTTPException(500, "Missing Experian client credentials")

    if not EXPERIAN_USERNAME or not EXPERIAN_PASSWORD:
        raise HTTPException(500, "Missing Experian username/password")

    url = f"{EXPERIAN_BASE_URL.rstrip('/')}{EXPERIAN_TOKEN_PATH}"

    payload = {
        "username": EXPERIAN_USERNAME,
        "password": EXPERIAN_PASSWORD,
        "client_id": EXPERIAN_CLIENT_ID,
        "client_secret": EXPERIAN_CLIENT_SECRET,
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Grant_type": "password",
        "User-Agent": f"rix-credit-api/{APP_VERSION}",
    }

    r = requests.post(url, json=payload, headers=headers, timeout=EXPERIAN_TIMEOUT)

    if not r.ok:
        raise HTTPException(
            502,
            f"Experian token error: {r.status_code} {r.text[:500]}"
        )

    data = r.json()
    token = data.get("access_token")

    if not token:
        raise HTTPException(
            502,
            f"Experian token response missing access_token: {data}"
        )

    return token


def experian_search_company_live(
    token: str,
    company_number: str,
    company_name: Optional[str] = None
) -> Dict[str, Any]:

    url = f"{EXPERIAN_BASE_URL.rstrip('/')}/v2/businesssearch"

    params = {
        "registrationNumber": company_number,
        "country": "GBR"
    }

    with experian_session(token) as s:
        r = s.get(url, params=params, timeout=EXPERIAN_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"Experian search error: {r.status_code} {r.text[:500]}")

    return r.json()


def experian_extract_business_id(search_payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(search_payload, dict):
        return None

    candidate_lists = [
        search_payload.get("results"),
        search_payload.get("businesses"),
        search_payload.get("items"),
        search_payload.get("data"),
    ]

    for candidate_list in candidate_lists:
        if isinstance(candidate_list, list) and candidate_list:
            first = candidate_list[0]
            if isinstance(first, dict):
                for key in ["businessId", "business_id", "id", "companyId", "company_id", "reference"]:
                    if first.get(key):
                        return str(first.get(key))

    for key in ["businessId", "business_id", "id", "companyId", "company_id", "reference"]:
        if search_payload.get(key):
            return str(search_payload.get(key))

    return None


def experian_get_company_report_live(token: str, business_id: str) -> Dict[str, Any]:
    path = EXPERIAN_REPORT_PATH_TEMPLATE.format(business_id=business_id)
    url = f"{EXPERIAN_BASE_URL.rstrip('/')}{path}"

    with experian_session(token) as s:
        r = s.get(url, timeout=EXPERIAN_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"Experian report error: {r.status_code} {r.text[:500]}")

    return r.json()


def experian_mock_report(company_number: str, company_name: Optional[str] = None) -> Dict[str, Any]:
    seed = sum(ord(c) for c in company_number) % 25
    delphi_score = 75 + min(seed, 20)
    delphi_band = (
        "Very Low Risk" if delphi_score >= 90 else
        "Low Risk" if delphi_score >= 80 else
        "Low to Moderate Risk" if delphi_score >= 70 else
        "Moderate Risk"
    )

    credit_limit_value = 110000.0 if delphi_score >= 90 else 60000.0 if delphi_score >= 80 else 25000.0
    credit_rating_value = 35000.0 if delphi_score >= 90 else 20000.0 if delphi_score >= 80 else 12000.0
    company_dbt = None
    company_payment_data_available = False
    industry_dbt_current = 45
    ccj_count = 0
    outstanding_charges = 2 if "SC" in company_number.upper() else 0
    satisfied_charges = 1 if outstanding_charges > 0 else 0

    history_4y = [
        {
            "date": "2025-05-31",
            "turnover": None,
            "tangible_assets": 2188002.0,
            "total_fixed_assets": 2198002.0,
            "debtors": 630358.0,
            "cash_at_bank": 65452.0,
            "total_current_assets": 709775.0,
            "total_current_liabilities": 1129388.0,
            "working_capital": -419613.0,
            "capital_employed": 1778389.0,
            "total_long_term_liabilities": 805548.0,
            "provisions": 0.0,
            "total_net_assets": 583876.0,
            "shareholders_funds": 583876.0,
            "net_worth": 583876.0,
            "employees": None,
        },
        {
            "date": "2024-05-31",
            "turnover": None,
            "tangible_assets": 1865427.0,
            "total_fixed_assets": 1875427.0,
            "debtors": 556303.0,
            "cash_at_bank": 43383.0,
            "total_current_assets": 616532.0,
            "total_current_liabilities": 1048573.0,
            "working_capital": -432041.0,
            "capital_employed": 1443386.0,
            "total_long_term_liabilities": 590138.0,
            "provisions": 0.0,
            "total_net_assets": 512345.0,
            "shareholders_funds": 512345.0,
            "net_worth": 512345.0,
            "employees": None,
        },
        {
            "date": "2023-05-31",
            "turnover": None,
            "tangible_assets": 2138697.0,
            "total_fixed_assets": 2148697.0,
            "debtors": 679164.0,
            "cash_at_bank": 46494.0,
            "total_current_assets": 742547.0,
            "total_current_liabilities": 1375729.0,
            "working_capital": -633182.0,
            "capital_employed": 1515515.0,
            "total_long_term_liabilities": 815972.0,
            "provisions": 0.0,
            "total_net_assets": 382877.0,
            "shareholders_funds": 382877.0,
            "net_worth": 382877.0,
            "employees": None,
        },
        {
            "date": "2022-05-31",
            "turnover": None,
            "tangible_assets": 1421829.0,
            "total_fixed_assets": 1431829.0,
            "debtors": 643522.0,
            "cash_at_bank": 47249.0,
            "total_current_assets": 701132.0,
            "total_current_liabilities": 1285088.0,
            "working_capital": -583956.0,
            "capital_employed": 847873.0,
            "total_long_term_liabilities": 348254.0,
            "provisions": 0.0,
            "total_net_assets": 244437.0,
            "shareholders_funds": 244437.0,
            "net_worth": 244437.0,
            "employees": None,
        },
    ]

    ratios_latest = {
        "current_ratio": 0.63,
        "acid_test": 0.62,
        "debtor_days": 174.01,
        "stock_turn_days": 20.08,
        "gearing_pct": 137.97
    }

    matched_name = company_name or f"Mock match for {company_number}"

    return {
        "available": True,
        "source": "experian_mock",
        "reference": f"EXP-{company_number}",
        "report_date": now_utc_iso()[:10],
        "matched_company_name": matched_name,
        "score": delphi_score,
        "score_description": delphi_band,
        "risk_band": delphi_band,
        "credit_limit": money(credit_limit_value),
        "credit_rating": money(credit_rating_value),
        "ccj_count_last_2y": ccj_count,
        "payment_behaviour": {
            "average_dbt": company_dbt,
            "company_payment_data_available": company_payment_data_available,
            "industry_dbt_current": industry_dbt_current,
            "ccj_count_last_2y": ccj_count,
            "ccj_flag": ccj_count > 0,
            "insolvency_flag": False,
        },
        "opinion": {
            "summary": "A very low risk company; credit may be considered comfortably within the recommended rating and with caution around structural encumbrances."
        },
        "credit_values": {
            "credit_limit": money(credit_limit_value),
            "credit_rating": money(credit_rating_value),
        },
        "commercial_delphi": {
            "score": delphi_score,
            "band": delphi_band,
            "failure_odds": "176:1" if delphi_score >= 90 else "48:1" if delphi_score >= 80 else "24:1",
            "calculated_at": now_utc_iso(),
            "history_12m": [
                {"period": "2025-03", "score": delphi_score - 2, "credit_limit": credit_limit_value - 5000, "credit_rating": credit_rating_value - 3000},
                {"period": "2025-06", "score": delphi_score - 1, "credit_limit": credit_limit_value, "credit_rating": credit_rating_value},
                {"period": "2025-09", "score": delphi_score, "credit_limit": credit_limit_value, "credit_rating": credit_rating_value},
                {"period": "2025-12", "score": delphi_score, "credit_limit": credit_limit_value, "credit_rating": credit_rating_value},
            ],
            "sector_comparisons": {
                "same_industry_group": {
                    "average_score": 39,
                    "failure_odds": "16:1",
                    "percentile_or_better_than": 98 if delphi_score >= 90 else 85
                },
                "same_asset_size_group": {
                    "average_score": 68,
                    "failure_odds": "32:1",
                    "percentile_or_better_than": 85
                },
                "same_age_group": {
                    "average_score": 63,
                    "failure_odds": "29:1",
                    "percentile_or_better_than": 90
                },
                "comparison_sector_details": {
                    "industry_group": "Land Transport; Transport Via Pipelines",
                    "asset_size_group": "£1,000,000 to £5,000,000",
                    "age_group": "Incorporated between March 1995 and March 2006"
                }
            }
        },
        "payment_profile": {
            "company_payment_data_available": company_payment_data_available,
            "company_dbt": company_dbt,
            "company_dbt_text": "There is no current payment performance data for this company" if not company_payment_data_available else None,
            "industry_dbt": {
                "current": industry_dbt_current,
                "last_3m": 44,
                "last_6m": 43,
                "last_12m": 44,
            },
            "unpaid_accounts": {
                "one_month": None,
                "two_months": None,
                "three_plus_months": None,
            },
            "trend": "unknown"
        },
        "legal": {
            "ccj_count_last_2y": ccj_count,
            "ccj_flag": ccj_count > 0,
            "most_recent_legal_notices_text": "No Legal Notices Recorded",
            "legal_notices_count": 0
        },
        "alerts": {
            "count": 1,
            "items": [
                {
                    "type": "director_alert",
                    "text": "Review other directorships of the current board."
                }
            ]
        },
        "financials": {
            "currency": "GBP",
            "latest_accounts_date": history_4y[0]["date"],
            "latest_confirmation_date": None,
            "accounts_reference_date": None,
            "summary_latest": {
                "turnover": 2907777.0,
                "pre_tax_profit": None,
                "pre_tax_profit_margin_pct": None,
                "total_assets": history_4y[0]["total_fixed_assets"] + history_4y[0]["total_current_assets"],
                "working_capital": history_4y[0]["working_capital"],
                "shareholders_funds": history_4y[0]["shareholders_funds"],
            },
            "history_4y": history_4y,
            "ratios": {
                "latest": ratios_latest,
                "history_4y": [
                    {"date": "2025-05-31", **ratios_latest},
                    {"date": "2024-05-31", "current_ratio": 0.59, "acid_test": 0.58, "debtor_days": 165.4, "stock_turn_days": 18.2, "gearing_pct": 115.2},
                    {"date": "2023-05-31", "current_ratio": 0.54, "acid_test": 0.53, "debtor_days": 177.8, "stock_turn_days": 22.1, "gearing_pct": 213.1},
                    {"date": "2022-05-31", "current_ratio": 0.55, "acid_test": 0.54, "debtor_days": 169.2, "stock_turn_days": 19.7, "gearing_pct": 142.5},
                ]
            },
            "cash_flow_available": False,
            "profit_loss_available": False,
        },
        "directors_summary": {
            "current_directors_count": 2,
            "current_directors_may_also_be_shareholders": 2
        },
        "corporate_structure": {
            "is_group_member": False,
            "summary": "This company is not part of a group"
        },
        "charges_summary": {
            "outstanding_count": outstanding_charges,
            "satisfied_count": satisfied_charges
        },
        "warnings": [],
        "raw": {
            "mode": "mock",
            "company_number": company_number
        },
    }


def map_experian_live_payload(
    raw_report: Dict[str, Any],
    business_id: Optional[str] = None,
    matched_company_name: Optional[str] = None
) -> Dict[str, Any]:
    delphi_src = get_first(
        raw_report.get("commercial_delphi"),
        raw_report.get("commercialDelphi"),
        raw_report.get("delphi"),
        {}
    ) or {}

    payment_src = get_first(
        raw_report.get("payment_profile"),
        raw_report.get("paymentProfile"),
        raw_report.get("payment_behaviour"),
        raw_report.get("paymentBehaviour"),
        {}
    ) or {}

    legal_src = get_first(
        raw_report.get("legal"),
        raw_report.get("legalNotices"),
        raw_report.get("courtInformation"),
        {}
    ) or {}

    alerts_src = get_first(
        raw_report.get("alerts"),
        raw_report.get("alertsSummary"),
        {}
    ) or {}

    financials_src = get_first(
        raw_report.get("financials"),
        raw_report.get("financialInformation"),
        raw_report.get("accounts"),
        {}
    ) or {}

    directors_src = get_first(
        raw_report.get("directors_summary"),
        raw_report.get("directorsSummary"),
        {}
    ) or {}

    corporate_src = get_first(
        raw_report.get("corporate_structure"),
        raw_report.get("corporateStructure"),
        raw_report.get("group_links"),
        raw_report.get("corporateLinkage"),
        {}
    ) or {}

    charges_src = get_first(
        raw_report.get("charges_summary"),
        raw_report.get("chargesSummary"),
        {}
    ) or {}

    opinion_src = get_first(
        raw_report.get("opinion"),
        raw_report.get("creditOpinion"),
        {}
    ) or {}

    credit_limit_value = safe_float(
        get_first(
            get_in(raw_report, "credit_limit", "amount"),
            get_in(raw_report, "creditLimit", "amount"),
            raw_report.get("credit_limit"),
            raw_report.get("creditLimit"),
            get_in(raw_report, "credit_values", "credit_limit", "amount"),
            get_in(raw_report, "creditValues", "creditLimit", "amount"),
        )
    )

    credit_rating_value = safe_float(
        get_first(
            get_in(raw_report, "credit_rating", "amount"),
            get_in(raw_report, "creditRating", "amount"),
            raw_report.get("credit_rating"),
            raw_report.get("creditRating"),
            get_in(raw_report, "credit_values", "credit_rating", "amount"),
            get_in(raw_report, "creditValues", "creditRating", "amount"),
        )
    )

    delphi_score = safe_int(
        get_first(
            raw_report.get("score"),
            raw_report.get("commercial_delphi_score"),
            raw_report.get("commercialDelphiScore"),
            delphi_src.get("score"),
            delphi_src.get("commercialDelphiScore"),
        )
    )

    delphi_band = get_first(
        raw_report.get("risk_band"),
        raw_report.get("commercial_delphi_band"),
        raw_report.get("commercialDelphiBand"),
        delphi_src.get("band"),
        delphi_src.get("riskBand"),
        delphi_src.get("commercialDelphiBand"),
    )

    company_dbt = safe_int(
        get_first(
            payment_src.get("company_dbt"),
            payment_src.get("companyDBT"),
            payment_src.get("average_dbt"),
            payment_src.get("averageDBT"),
            raw_report.get("company_dbt"),
            raw_report.get("companyDBT"),
        )
    )

    company_payment_data_available = safe_bool(
        get_first(
            payment_src.get("company_payment_data_available"),
            payment_src.get("companyPaymentDataAvailable"),
            raw_report.get("company_payment_data_available"),
            raw_report.get("companyPaymentDataAvailable"),
        )
    )

    industry_dbt_current = safe_int(
        get_first(
            get_in(payment_src, "industry_dbt", "current"),
            get_in(payment_src, "industryDBT", "current"),
            payment_src.get("industry_dbt_current"),
            payment_src.get("industryDBTCurrent"),
            raw_report.get("industry_dbt_current"),
            raw_report.get("industryDBTCurrent"),
        )
    )

    ccj_count_last_2y = safe_int(
        get_first(
            legal_src.get("ccj_count_last_2y"),
            legal_src.get("ccjCountLast2Y"),
            legal_src.get("ccj_count"),
            raw_report.get("ccj_count_last_2y"),
            raw_report.get("ccjCountLast2Y"),
        )
    )

    insolvency_flag = safe_bool(
        get_first(
            legal_src.get("insolvency_flag"),
            legal_src.get("insolvencyFlag"),
            raw_report.get("insolvency_flag"),
            raw_report.get("insolvencyFlag"),
        )
    )
    if insolvency_flag is None:
        insolvency_count = safe_int(get_first(
            legal_src.get("insolvency_count"),
            legal_src.get("insolvencyCount"),
            raw_report.get("insolvency_count"),
            raw_report.get("insolvencyCount"),
        ))
        insolvency_flag = False if insolvency_count in (None, 0) else True

    summary_latest = get_first(
        financials_src.get("summary_latest"),
        financials_src.get("summaryLatest"),
        raw_report.get("financial_summary_latest"),
        raw_report.get("financialSummaryLatest"),
        {}
    ) or {}

    history_4y = get_first(
        financials_src.get("history_4y"),
        financials_src.get("history4y"),
        raw_report.get("financial_history_4y"),
        raw_report.get("financialHistory4Y"),
        []
    ) or []

    ratios = get_first(
        financials_src.get("ratios"),
        raw_report.get("financial_ratios"),
        raw_report.get("financialRatios"),
        {}
    ) or {}

    alerts_items = get_first(
        alerts_src.get("items"),
        alerts_src.get("alerts"),
        raw_report.get("alerts"),
        []
    ) or []

    alerts_count = safe_int(
        get_first(
            alerts_src.get("count"),
            alerts_src.get("alerts_count"),
            raw_report.get("alerts_count"),
            raw_report.get("alertsCount"),
            len(alerts_items) if isinstance(alerts_items, list) else None,
        )
    )

    corporate_is_group_member = safe_bool(
        get_first(
            corporate_src.get("is_group_member"),
            corporate_src.get("isGroupMember"),
            raw_report.get("is_group_member"),
            raw_report.get("isGroupMember"),
        )
    )

    effective_matched_name = get_first(
        matched_company_name,
        raw_report.get("matched_company_name"),
        raw_report.get("matchedCompanyName"),
        get_in(raw_report, "business", "name"),
        get_in(raw_report, "company", "name"),
    )

    return {
        "available": True,
        "source": "experian_live",
        "reference": get_first(raw_report.get("reference"), raw_report.get("reportId"), business_id),
        "report_date": get_first(raw_report.get("report_date"), raw_report.get("reportDate"), now_utc_iso()[:10]),
        "matched_company_name": effective_matched_name,
        "score": delphi_score,
        "score_description": delphi_band,
        "risk_band": delphi_band,
        "credit_limit": money(credit_limit_value),
        "credit_rating": money(credit_rating_value),
        "ccj_count_last_2y": ccj_count_last_2y,
        "payment_behaviour": {
            "average_dbt": company_dbt,
            "company_payment_data_available": company_payment_data_available,
            "industry_dbt_current": industry_dbt_current,
            "ccj_count_last_2y": ccj_count_last_2y,
            "ccj_flag": (ccj_count_last_2y or 0) > 0,
            "insolvency_flag": bool(insolvency_flag),
        },
        "opinion": {
            "summary": get_first(opinion_src.get("summary"), raw_report.get("credit_opinion"), raw_report.get("creditOpinion"))
        },
        "credit_values": {
            "credit_limit": money(credit_limit_value),
            "credit_rating": money(credit_rating_value),
        },
        "commercial_delphi": {
            "score": delphi_score,
            "band": delphi_band,
            "failure_odds": get_first(
                delphi_src.get("failure_odds"),
                delphi_src.get("failureOdds"),
                raw_report.get("failure_odds"),
                raw_report.get("failureOdds")
            ),
            "calculated_at": get_first(
                delphi_src.get("calculated_at"),
                delphi_src.get("calculatedAt"),
                raw_report.get("delphi_calculated_at"),
                raw_report.get("delphiCalculatedAt")
            ),
            "history_12m": get_first(
                delphi_src.get("history_12m"),
                delphi_src.get("history12m"),
                raw_report.get("commercial_delphi_history_12m"),
                raw_report.get("commercialDelphiHistory12M"),
                []
            ) or [],
            "sector_comparisons": get_first(
                delphi_src.get("sector_comparisons"),
                delphi_src.get("sectorComparisons"),
                raw_report.get("sector_comparisons"),
                raw_report.get("sectorComparisons"),
                {}
            ) or {}
        },
        "payment_profile": {
            "company_payment_data_available": company_payment_data_available,
            "company_dbt": company_dbt,
            "company_dbt_text": get_first(
                payment_src.get("company_dbt_text"),
                payment_src.get("companyDBTText"),
                raw_report.get("company_dbt_text"),
                raw_report.get("companyDBTText")
            ),
            "industry_dbt": {
                "current": industry_dbt_current,
                "last_3m": safe_int(get_first(
                    get_in(payment_src, "industry_dbt", "last_3m"),
                    get_in(payment_src, "industryDBT", "last3m"),
                    payment_src.get("industry_dbt_last_3m"),
                    payment_src.get("industryDBTLast3M"),
                    raw_report.get("industry_dbt_last_3m"),
                    raw_report.get("industryDBTLast3M")
                )),
                "last_6m": safe_int(get_first(
                    get_in(payment_src, "industry_dbt", "last_6m"),
                    get_in(payment_src, "industryDBT", "last6m"),
                    payment_src.get("industry_dbt_last_6m"),
                    payment_src.get("industryDBTLast6M"),
                    raw_report.get("industry_dbt_last_6m"),
                    raw_report.get("industryDBTLast6M")
                )),
                "last_12m": safe_int(get_first(
                    get_in(payment_src, "industry_dbt", "last_12m"),
                    get_in(payment_src, "industryDBT", "last12m"),
                    payment_src.get("industry_dbt_last_12m"),
                    payment_src.get("industryDBTLast12M"),
                    raw_report.get("industry_dbt_last_12m"),
                    raw_report.get("industryDBTLast12M")
                )),
            },
            "unpaid_accounts": {
                "one_month": safe_int(get_first(
                    get_in(payment_src, "unpaid_accounts", "one_month"),
                    get_in(payment_src, "unpaidAccounts", "oneMonth"),
                    payment_src.get("unpaid_accounts_1m"),
                    raw_report.get("unpaid_accounts_1m")
                )),
                "two_months": safe_int(get_first(
                    get_in(payment_src, "unpaid_accounts", "two_months"),
                    get_in(payment_src, "unpaidAccounts", "twoMonths"),
                    payment_src.get("unpaid_accounts_2m"),
                    raw_report.get("unpaid_accounts_2m")
                )),
                "three_plus_months": safe_int(get_first(
                    get_in(payment_src, "unpaid_accounts", "three_plus_months"),
                    get_in(payment_src, "unpaidAccounts", "threePlusMonths"),
                    payment_src.get("unpaid_accounts_3m_plus"),
                    raw_report.get("unpaid_accounts_3m_plus")
                )),
            },
            "trend": get_first(payment_src.get("trend"), payment_src.get("payment_trend"), raw_report.get("payment_trend")),
        },
        "legal": {
            "ccj_count_last_2y": ccj_count_last_2y,
            "ccj_flag": (ccj_count_last_2y or 0) > 0,
            "most_recent_legal_notices_text": get_first(
                legal_src.get("most_recent_legal_notices_text"),
                legal_src.get("legalNoticesText"),
                raw_report.get("legal_notices_text"),
                raw_report.get("legalNoticesText")
            ),
            "legal_notices_count": safe_int(get_first(
                legal_src.get("legal_notices_count"),
                legal_src.get("legalNoticesCount"),
                raw_report.get("legal_notices_count"),
                raw_report.get("legalNoticesCount")
            ))
        },
        "alerts": {
            "count": alerts_count,
            "items": alerts_items if isinstance(alerts_items, list) else []
        },
        "financials": {
            "currency": get_first(financials_src.get("currency"), raw_report.get("financial_currency"), "GBP"),
            "latest_accounts_date": get_first(
                financials_src.get("latest_accounts_date"),
                financials_src.get("latestAccountsDate"),
                raw_report.get("latest_accounts_date"),
                raw_report.get("latestAccountsDate")
            ),
            "latest_confirmation_date": get_first(
                financials_src.get("latest_confirmation_date"),
                financials_src.get("latestConfirmationDate"),
                raw_report.get("latest_confirmation_date"),
                raw_report.get("latestConfirmationDate")
            ),
            "accounts_reference_date": get_first(
                financials_src.get("accounts_reference_date"),
                financials_src.get("accountsReferenceDate"),
                raw_report.get("accounts_reference_date"),
                raw_report.get("accountsReferenceDate")
            ),
            "summary_latest": summary_latest if isinstance(summary_latest, dict) else {},
            "history_4y": history_4y if isinstance(history_4y, list) else [],
            "ratios": ratios if isinstance(ratios, dict) else {},
            "cash_flow_available": safe_bool(get_first(
                financials_src.get("cash_flow_available"),
                financials_src.get("cashFlowAvailable"),
                raw_report.get("cash_flow_available"),
                raw_report.get("cashFlowAvailable")
            )),
            "profit_loss_available": safe_bool(get_first(
                financials_src.get("profit_loss_available"),
                financials_src.get("profitLossAvailable"),
                raw_report.get("profit_loss_available"),
                raw_report.get("profitLossAvailable")
            )),
        },
        "directors_summary": {
            "current_directors_count": safe_int(get_first(
                directors_src.get("current_directors_count"),
                directors_src.get("currentDirectorsCount"),
                raw_report.get("current_directors_count"),
                raw_report.get("currentDirectorsCount")
            )),
            "current_directors_may_also_be_shareholders": safe_int(get_first(
                directors_src.get("current_directors_may_also_be_shareholders"),
                directors_src.get("currentDirectorsMayAlsoBeShareholders"),
                raw_report.get("current_directors_may_also_be_shareholders"),
                raw_report.get("currentDirectorsMayAlsoBeShareholders")
            )),
        },
        "corporate_structure": {
            "is_group_member": corporate_is_group_member,
            "summary": get_first(
                corporate_src.get("summary"),
                corporate_src.get("structureSummary"),
                corporate_src.get("parent_name"),
                corporate_src.get("parentName"),
                raw_report.get("corporate_structure_summary")
            )
        },
        "charges_summary": {
            "outstanding_count": safe_int(get_first(
                charges_src.get("outstanding_count"),
                charges_src.get("outstandingCount"),
                raw_report.get("outstanding_charges_count"),
                raw_report.get("outstandingChargesCount")
            )),
            "satisfied_count": safe_int(get_first(
                charges_src.get("satisfied_count"),
                charges_src.get("satisfiedCount"),
                raw_report.get("satisfied_charges_count"),
                raw_report.get("satisfiedChargesCount")
            ))
        },
        "warnings": [],
        "raw": raw_report,
    }


def empty_experian_response(source: str, company_name: Optional[str], warning: str, raw: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "available": False,
        "source": source,
        "reference": None,
        "report_date": now_utc_iso()[:10],
        "matched_company_name": company_name,
        "score": None,
        "score_description": None,
        "risk_band": None,
        "credit_limit": money(None),
        "credit_rating": money(None),
        "ccj_count_last_2y": None,
        "payment_behaviour": {
            "average_dbt": None,
            "company_payment_data_available": None,
            "industry_dbt_current": None,
            "ccj_count_last_2y": None,
            "ccj_flag": False,
            "insolvency_flag": False,
        },
        "opinion": {"summary": None},
        "credit_values": {
            "credit_limit": money(None),
            "credit_rating": money(None),
        },
        "commercial_delphi": {
            "score": None,
            "band": None,
            "failure_odds": None,
            "calculated_at": None,
            "history_12m": [],
            "sector_comparisons": {},
        },
        "payment_profile": {
            "company_payment_data_available": None,
            "company_dbt": None,
            "company_dbt_text": None,
            "industry_dbt": {
                "current": None,
                "last_3m": None,
                "last_6m": None,
                "last_12m": None,
            },
            "unpaid_accounts": {
                "one_month": None,
                "two_months": None,
                "three_plus_months": None,
            },
            "trend": None,
        },
        "legal": {
            "ccj_count_last_2y": None,
            "ccj_flag": False,
            "most_recent_legal_notices_text": None,
            "legal_notices_count": None,
        },
        "alerts": {
            "count": None,
            "items": [],
        },
        "financials": {
            "currency": "GBP",
            "latest_accounts_date": None,
            "latest_confirmation_date": None,
            "accounts_reference_date": None,
            "summary_latest": {},
            "history_4y": [],
            "ratios": {},
            "cash_flow_available": None,
            "profit_loss_available": None,
        },
        "directors_summary": {
            "current_directors_count": None,
            "current_directors_may_also_be_shareholders": None,
        },
        "corporate_structure": {
            "is_group_member": None,
            "summary": None,
        },
        "charges_summary": {
            "outstanding_count": None,
            "satisfied_count": None,
        },
        "warnings": [warning],
        "raw": raw,
    }


def get_experian_report(company_number: str, company_name: Optional[str] = None) -> Dict[str, Any]:
    if EXPERIAN_MODE == "mock":
        return experian_mock_report(company_number, company_name)

    try:
        token = experian_get_token()
        search_result = experian_search_company_live(token, company_number, company_name)
        business_id = experian_extract_business_id(search_result)

        if not business_id:
            return empty_experian_response(
                source="experian_live",
                company_name=company_name,
                warning="Experian search returned no usable business ID",
                raw={"search_result": search_result}
            )

        raw_report = experian_get_company_report_live(token, business_id)
        mapped = map_experian_live_payload(raw_report, business_id=business_id, matched_company_name=company_name)

        if not mapped.get("matched_company_name"):
            mapped["matched_company_name"] = company_name

        mapped["raw"] = {
            "search_result": search_result,
            "report_result": raw_report
        }
        return mapped

    except Exception as e:
        logger.exception("Experian fetch failed")
        return empty_experian_response(
            source="experian_live",
            company_name=company_name,
            warning=f"Experian fetch failed: {str(e)}",
            raw=None
        )


# -----------------------------------------------------------------------------
# Internal model / calibration
# -----------------------------------------------------------------------------

def build_internal_model(companies_house: Dict[str, Any], experian: Dict[str, Any]) -> Dict[str, Any]:
    score = 50
    drivers: List[str] = []
    caps_applied: List[str] = []
    warnings: List[str] = []

    payment = experian.get("payment_behaviour") or {}
    charges = companies_house.get("charges") or []
    profile = companies_house.get("company_profile") or {}

    financials = experian.get("financials") or {}
    history_4y = financials.get("history_4y") or []
    latest_hist = history_4y[0] if isinstance(history_4y, list) and history_4y else {}
    summary_latest = financials.get("summary_latest") or {}

    net_worth = safe_float(get_first(
        latest_hist.get("net_worth"),
        latest_hist.get("shareholders_funds"),
        summary_latest.get("shareholders_funds"),
        summary_latest.get("net_worth"),
    ))
    current_assets = safe_float(get_first(
        latest_hist.get("total_current_assets"),
        summary_latest.get("current_assets"),
    ))
    current_liabilities = safe_float(get_first(
        latest_hist.get("total_current_liabilities"),
        summary_latest.get("current_liabilities"),
    ))
    avg_dbt = safe_int(payment.get("average_dbt"))
    delphi_score = safe_int(experian.get("score"))
    credit_limit_value = safe_float(get_in(experian, "credit_limit", "amount"))
    credit_rating_value = safe_float(get_in(experian, "credit_rating", "amount"))
    ccj_count = safe_int(payment.get("ccj_count_last_2y")) or 0
    outstanding_charges = safe_int(get_in(experian, "charges_summary", "outstanding_count"))

    if delphi_score is not None:
        if delphi_score >= 90:
            score += 18
            drivers.append("Excellent Delphi score")
        elif delphi_score >= 80:
            score += 12
            drivers.append("Strong Delphi score")
        elif delphi_score >= 70:
            score += 8
            drivers.append("Good Delphi score")
        elif delphi_score < 50:
            score -= 12
            drivers.append("Weak Delphi score")

    if credit_rating_value is not None:
        if credit_rating_value >= 50000:
            score += 10
            drivers.append("Strong Experian credit rating")
        elif credit_rating_value >= 20000:
            score += 6
            drivers.append("Supportive Experian credit rating")
        elif credit_rating_value <= 5000:
            score -= 8
            drivers.append("Constrained Experian credit rating")

    if credit_limit_value is not None:
        if credit_limit_value >= 100000:
            score += 8
            drivers.append("High bureau credit limit")
        elif credit_limit_value >= 25000:
            score += 4
            drivers.append("Supportive bureau credit limit")

    if net_worth is not None:
        if net_worth > 500000:
            score += 12
            drivers.append("Strong net worth")
        elif net_worth > 100000:
            score += 8
            drivers.append("Positive net worth")
        elif net_worth < 0:
            score -= 20
            drivers.append("Negative net worth")

    if current_assets is not None and current_liabilities not in (None, 0):
        current_ratio = current_assets / current_liabilities
        if current_ratio >= 1.5:
            score += 10
            drivers.append("Healthy liquidity")
        elif current_ratio >= 1.0:
            score += 5
            drivers.append("Adequate liquidity")
        else:
            score -= 8
            drivers.append("Weak liquidity")

    if avg_dbt is not None:
        if avg_dbt <= 10:
            score += 6
            drivers.append("Good payment behaviour")
        elif avg_dbt <= 30:
            score += 2
            drivers.append("Acceptable payment behaviour")
        else:
            score -= 12
            drivers.append("Poor payment behaviour")
    else:
        company_payment_data_available = safe_bool(payment.get("company_payment_data_available"))
        if company_payment_data_available is False:
            warnings.append("No company payment performance data available")

    if ccj_count > 0:
        score -= min(25, ccj_count * 10)
        drivers.append("CCJ history present")
        caps_applied.append("ccj_cap")

    if payment.get("insolvency_flag"):
        score -= 25
        drivers.append("Insolvency indicator present")
        caps_applied.append("insolvency_cap")

    ch_outstanding = len([c for c in charges if str(c.get("status", "")).lower() == "outstanding"])
    effective_outstanding_charges = outstanding_charges if outstanding_charges is not None else ch_outstanding

    if effective_outstanding_charges > 0:
        score -= min(10, effective_outstanding_charges * 3)
        drivers.append("Outstanding charge history present")
        caps_applied.append("charges_cap")

    status = str(profile.get("company_status", "")).lower().strip()
    if status and status != "active":
        score -= 25
        drivers.append(f"Company status is {status}")
        caps_applied.append("status_cap")

    if not experian.get("available"):
        score -= 5
        warnings.append("Experian unavailable - internal model confidence reduced")

    if not companies_house.get("available"):
        warnings.append("Companies House unavailable - structural context reduced")

    score = max(0, min(100, score))

    if score >= 85:
        grade = "A"
        risk_label = "Low"
        limit = credit_limit_value or credit_rating_value or 50000.0
    elif score >= 70:
        grade = "B"
        risk_label = "Low to Moderate"
        limit = credit_rating_value or 25000.0
    elif score >= 55:
        grade = "C"
        risk_label = "Moderate"
        limit = min(credit_rating_value or 12000.0, credit_limit_value or 12000.0)
    elif score >= 40:
        grade = "D"
        risk_label = "Moderate to High"
        limit = min(credit_rating_value or 5000.0, 5000.0)
    else:
        grade = "E"
        risk_label = "High"
        limit = 0.0

    return {
        "available": True,
        "score": score,
        "grade": grade,
        "risk_label": risk_label,
        "suggested_limit": money(limit),
        "drivers": drivers,
        "caps_applied": caps_applied,
        "warnings": warnings,
    }


def calibrate(experian: Dict[str, Any], internal_model: Dict[str, Any]) -> Dict[str, Any]:
    exp_score = safe_int(experian.get("score"))
    int_score = safe_int(internal_model.get("score"))
    exp_limit = safe_float(get_in(experian, "credit_limit", "amount"))
    int_limit = safe_float(get_in(internal_model, "suggested_limit", "amount"))
    exp_credit_rating = safe_float(get_in(experian, "credit_rating", "amount"))

    if exp_score is None:
        return {
            "status": "internal_only",
            "difference_summary": "Experian unavailable - decision based primarily on internal model",
            "score_alignment": {
                "internal_score": int_score,
                "experian_score": None,
                "score_delta": None,
            },
            "limit_alignment": {
                "internal_limit": int_limit,
                "experian_limit": None,
                "limit_delta": None,
            },
            "decision_bias": "internal_model",
            "reasoning": [
                "Experian data unavailable",
                "Internal model retained as primary fallback",
            ],
            "observations": {
                "credit_rating": None,
                "delphi_band": None,
            }
        }

    score_delta = exp_score - (int_score or 0)
    limit_delta = (exp_limit or 0) - (int_limit or 0)

    if abs(score_delta) <= 5:
        status = "aligned"
        summary = "Experian and internal model are broadly aligned"
        bias = "blended"
    elif score_delta > 5:
        status = "divergence"
        summary = "Experian is more positive than internal model"
        bias = "conservative_middle"
    else:
        status = "divergence"
        summary = "Internal model is more positive than Experian"
        bias = "experian_weighted"

    reasoning: List[str] = []
    if score_delta > 5:
        reasoning.append("Bureau view is stronger than internal assessment")
        reasoning.append("Structural caution should still be considered")
    elif score_delta < -5:
        reasoning.append("Bureau view is weaker than internal assessment")
        reasoning.append("External adverse/payment evidence should carry greater weight")
    else:
        reasoning.append("Independent models point in a similar direction")

    if exp_credit_rating is not None:
        reasoning.append(f"Experian credit rating is £{int(exp_credit_rating):,}")

    return {
        "status": status,
        "difference_summary": summary,
        "score_alignment": {
            "internal_score": int_score,
            "experian_score": exp_score,
            "score_delta": score_delta,
        },
        "limit_alignment": {
            "internal_limit": int_limit,
            "experian_limit": exp_limit,
            "limit_delta": limit_delta,
        },
        "decision_bias": bias,
        "reasoning": reasoning,
        "observations": {
            "credit_rating": exp_credit_rating,
            "delphi_band": get_in(experian, "commercial_delphi", "band"),
        }
    }


# -----------------------------------------------------------------------------
# Policy layer
# -----------------------------------------------------------------------------

def build_policy_overrides(
    companies_house: Dict[str, Any],
    experian: Dict[str, Any],
    internal_model: Dict[str, Any],
    calibration: Dict[str, Any]
) -> Dict[str, Any]:
    profile = companies_house.get("company_profile") or {}
    payment = experian.get("payment_behaviour") or {}
    legal = experian.get("legal") or {}
    ratios = get_in(experian, "financials", "ratios", "latest") or {}

    company_status = str(profile.get("company_status", "")).lower().strip()
    charge_count = safe_int(get_in(experian, "charges_summary", "outstanding_count"))
    if charge_count is None:
        charge_count = len([c for c in companies_house.get("charges", []) if str(c.get("status", "")).lower() == "outstanding"])

    ccj_count = safe_int(legal.get("ccj_count_last_2y")) or 0
    no_payment_data = safe_bool(payment.get("company_payment_data_available")) is False
    insolvency_flag = safe_bool(payment.get("insolvency_flag")) is True
    current_ratio = safe_float(ratios.get("current_ratio"))
    gearing_pct = safe_float(ratios.get("gearing_pct"))
    delphi_score = safe_int(experian.get("score")) or 0
    credit_rating = safe_float(get_in(experian, "credit_rating", "amount")) or 0.0
    credit_limit = safe_float(get_in(experian, "credit_limit", "amount")) or 0.0

    hard_stop = None
    if company_status == "dissolved":
        hard_stop = "dissolved_company"
    elif insolvency_flag:
        hard_stop = "insolvency_indicator"
    elif ccj_count >= 2:
        hard_stop = "multiple_ccjs"

    caution_flags: List[str] = []
    if charge_count > 0:
        caution_flags.append("outstanding_charges")
    if no_payment_data:
        caution_flags.append("no_company_payment_data")
    if current_ratio is not None and current_ratio < 1.0:
        caution_flags.append("weak_liquidity")
    if gearing_pct is not None and gearing_pct >= 150:
        caution_flags.append("high_gearing")
    if ccj_count == 1:
        caution_flags.append("single_ccj")

    if hard_stop:
        max_limit = 0.0
        max_uplift_pct_of_credit_rating = 0.0
        max_stance = "Decline"
    else:
        if caution_flags:
            if "outstanding_charges" in caution_flags or "no_company_payment_data" in caution_flags:
                max_uplift_pct_of_credit_rating = 1.00
            else:
                max_uplift_pct_of_credit_rating = 1.25
        else:
            if delphi_score >= 90:
                max_uplift_pct_of_credit_rating = 1.50
            elif delphi_score >= 80:
                max_uplift_pct_of_credit_rating = 1.35
            else:
                max_uplift_pct_of_credit_rating = 1.25

        if credit_rating > 0:
            max_limit = min(credit_limit or credit_rating, credit_rating * max_uplift_pct_of_credit_rating)
        else:
            max_limit = credit_limit

        if caution_flags:
            max_stance = "Approve with normal controls"
        else:
            max_stance = "Approve"

    return {
        "hard_stop": hard_stop,
        "caution_flags": caution_flags,
        "max_uplift_pct_of_credit_rating": max_uplift_pct_of_credit_rating,
        "max_limit_after_policy": round(float(max_limit), 2),
        "max_stance_after_policy": max_stance,
    }


# -----------------------------------------------------------------------------
# Final decision
# -----------------------------------------------------------------------------

def build_final_decision(
    companies_house: Dict[str, Any],
    experian: Dict[str, Any],
    internal_model: Dict[str, Any],
    calibration: Dict[str, Any]
) -> Dict[str, Any]:
    warnings: List[str] = []
    rationale: List[str] = []

    payment = experian.get("payment_behaviour") or {}
    legal = experian.get("legal") or {}
    exp_score = safe_int(experian.get("score"))
    int_score = safe_int(internal_model.get("score")) or 0
    exp_credit_limit = safe_float(get_in(experian, "credit_limit", "amount")) or 0.0
    exp_credit_rating = safe_float(get_in(experian, "credit_rating", "amount")) or 0.0
    outstanding_charges = safe_int(get_in(experian, "charges_summary", "outstanding_count")) or 0
    delphi_band = get_in(experian, "commercial_delphi", "band")
    ccj_count = safe_int(legal.get("ccj_count_last_2y")) or 0
    no_payment_data = safe_bool(payment.get("company_payment_data_available")) is False

    policy = build_policy_overrides(companies_house, experian, internal_model, calibration)

    if policy["hard_stop"] == "dissolved_company":
        return {
            "risk_rating": "High",
            "credit_stance": "Decline",
            "suggested_limit": money(0.0),
            "confidence": "high",
            "rationale": [
                "Company is dissolved and no longer trading",
                "Credit exposure cannot be supported"
            ],
            "warnings": ["Company dissolved"],
            "policy_overrides": policy,
        }

    if policy["hard_stop"] == "insolvency_indicator":
        return {
            "risk_rating": "High",
            "credit_stance": "Decline",
            "suggested_limit": money(0.0),
            "confidence": "high",
            "rationale": [
                "Insolvency indicator present in bureau/legal data",
                "Credit exposure cannot be supported"
            ],
            "warnings": ["Insolvency indicator present"],
            "policy_overrides": policy,
        }

    if policy["hard_stop"] == "multiple_ccjs":
        return {
            "risk_rating": "High",
            "credit_stance": "Decline",
            "suggested_limit": money(0.0),
            "confidence": "high",
            "rationale": [
                "Multiple CCJs recorded in recent legal history",
                "External legal risk is too high for normal credit terms"
            ],
            "warnings": ["Multiple recent CCJs"],
            "policy_overrides": policy,
        }

    if exp_score is None:
        final_score = int_score
        rationale.append("Experian unavailable; fallback to internal model")
    else:
        bias = calibration.get("decision_bias")
        if bias == "blended":
            final_score = round((exp_score + int_score) / 2)
            rationale.append("Experian and internal model are aligned")
        elif bias == "conservative_middle":
            final_score = round((exp_score * 0.45) + (int_score * 0.55))
            rationale.append("Experian is stronger, but conservative weighting retained")
        elif bias == "experian_weighted":
            final_score = round((exp_score * 0.7) + (int_score * 0.3))
            rationale.append("Experian is weaker, so bureau view is weighted more heavily")
        else:
            final_score = int_score
            rationale.append("Fallback internal model weighting used")

    if outstanding_charges > 0:
        warnings.append("Outstanding charge history present")
        rationale.append("Outstanding charge history moderates lending appetite")

    if ccj_count > 0:
        warnings.append("Recent CCJ history present")
        rationale.append("Recent legal history reduces confidence")

    if safe_bool(get_in(experian, "corporate_structure", "is_group_member")):
        warnings.append("Group linkage identified but support not assumed")
        rationale.append("Group structure may help resilience, but support is not relied upon")

    alerts_count = safe_int(get_in(experian, "alerts", "count"))
    if alerts_count and alerts_count > 0:
        warnings.append(f"{alerts_count} bureau alert(s) present")
        rationale.append("Bureau alerts should be considered alongside the financial stance")

    if no_payment_data:
        warnings.append("No company payment data available")
        rationale.append("No company payment data is available, so limit confidence is reduced")

    if delphi_band:
        rationale.append(f"Delphi band: {delphi_band}")

    bias = calibration.get("decision_bias")

    if final_score >= 85:
        risk_rating = "Low"
        credit_stance = "Approve"
        if bias == "conservative_middle":
            if exp_credit_rating > 0:
                suggested_limit = exp_credit_rating
            else:
                suggested_limit = exp_credit_limit * 0.4 if exp_credit_limit > 0 else 50000.0
            confidence = "medium"
        else:
            suggested_limit = max(exp_credit_limit, exp_credit_rating, 0.0)
            confidence = "high" if experian.get("available") else "medium"

    elif final_score >= 70:
        risk_rating = "Low to Moderate"
        credit_stance = "Approve with normal controls"
        if exp_credit_limit > 0 and exp_credit_rating > 0:
            suggested_limit = min(
                exp_credit_limit,
                max(exp_credit_rating, round((exp_credit_limit + exp_credit_rating) / 2))
            )
        else:
            suggested_limit = exp_credit_rating or exp_credit_limit or 25000.0
        confidence = "medium"

    elif final_score >= 55:
        risk_rating = "Moderate"
        credit_stance = "Approve with caution"
        if exp_credit_limit > 0 and exp_credit_rating > 0:
            suggested_limit = min(
                exp_credit_limit,
                max(exp_credit_rating, round((exp_credit_limit + exp_credit_rating) / 2))
            )
        else:
            suggested_limit = exp_credit_rating or exp_credit_limit or 10000.0
        confidence = "medium"

    elif final_score >= 40:
        risk_rating = "Moderate to High"
        credit_stance = "Restricted terms / reduced limit"
        suggested_limit = min(5000.0, exp_credit_rating or 5000.0)
        confidence = "low"

    else:
        risk_rating = "High"
        credit_stance = "Decline or cash-with-order"
        suggested_limit = 0.0
        confidence = "low"

    max_stance = policy["max_stance_after_policy"]
    stance_rank = {
        "Approve": 4,
        "Approve with normal controls": 3,
        "Approve with caution": 2,
        "Restricted terms / reduced limit": 1,
        "Decline": 0,
        "Decline or cash-with-order": 0,
    }
    if stance_rank.get(credit_stance, 0) > stance_rank.get(max_stance, 0):
        rationale.append(f"Policy cap applied: stance limited to '{max_stance}'")
        credit_stance = max_stance

    if suggested_limit > policy["max_limit_after_policy"]:
        rationale.append(
            f"Policy cap applied: limit reduced from £{int(round(suggested_limit)):,} to £{int(round(policy['max_limit_after_policy'])):,}"
        )
        suggested_limit = policy["max_limit_after_policy"]

    if no_payment_data and confidence == "high":
        confidence = "medium"

    return {
        "risk_rating": risk_rating,
        "credit_stance": credit_stance,
        "suggested_limit": money(float(suggested_limit)),
        "confidence": confidence,
        "rationale": rationale,
        "warnings": warnings,
        "policy_overrides": policy,
    }


def build_credit_decision(company_number: str, company_name: Optional[str] = None) -> Dict[str, Any]:
    total_start = time.perf_counter()

    ch_start = time.perf_counter()
    try:
        companies_house = get_companies_house_bundle(company_number)
    except Exception as e:
        logger.exception("Companies House bundle failed")
        companies_house = {
            "available": False,
            "source": "companies_house",
            "company_profile": {},
            "officers": [],
            "pscs": [],
            "charges": [],
            "filing_history": [],
            "warnings": [f"Companies House failed: {str(e)}"],
        }
    ch_ms = elapsed_ms(ch_start)

    if not company_name:
        company_name = (companies_house.get("company_profile") or {}).get("company_name")

    exp_start = time.perf_counter()
    experian = get_experian_report(company_number, company_name)
    exp_ms = elapsed_ms(exp_start)

    if not ALLOW_PARTIAL_RESULTS and not experian.get("available"):
        raise HTTPException(503, "Experian unavailable and partial responses are disabled")

    internal_model = build_internal_model(companies_house, experian)
    calibration = calibrate(experian, internal_model)
    final_decision = build_final_decision(companies_house, experian, internal_model, calibration)

    return {
        "request": {
            "company_number": company_number,
            "company_name": company_name,
            "mode": EXPERIAN_MODE,
            "timestamp_utc": now_utc_iso(),
        },
        "companies_house": companies_house,
        "experian": experian,
        "internal_model": internal_model,
        "calibration": calibration,
        "final_decision": final_decision,
        "meta": {
            "service_status": {
                "companies_house": "ok" if companies_house.get("available") else "degraded",
                "experian": "ok" if experian.get("available") else "degraded",
                "internal_model": "ok",
            },
            "timings_ms": {
                "companies_house": ch_ms,
                "experian": exp_ms,
                "internal_model": 1,
                "total": elapsed_ms(total_start),
            },
        },
    }


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@app.get("/")
def root():
    return {
        "status": "ok",
        "service": APP_NAME,
        "version": APP_VERSION,
        "mode": EXPERIAN_MODE,
        "endpoints": [
            "/healthz",
            "/debug-env",
            "/rix-credit/company/{company_number}",
            "/experian/company/{company_number}",
            "/rix-credit/company/{company_number}/credit-decision",
            "/rix-credit/company/{company_number}/credit-assessment"
        ],
        "notes": [
            "OCR / PDF accounts extraction removed from live path",
            "Experian is primary bureau layer",
            "Companies House retained for structural/context data",
            "Experian payload upgraded with credit rating, Delphi, legal, alerts, and 4Y financials",
            "Policy overrides applied to final stance and limit",
            "credit-assessment kept as alias to calibrated decision for compatibility"
        ]
    }


@app.get("/healthz")
def healthz():
    return {
        "status": "ok",
        "service": APP_NAME,
        "version": APP_VERSION,
        "mode": EXPERIAN_MODE,
        "timestamp_utc": now_utc_iso(),
    }


@app.get("/debug-env")
def debug_env():
    return {
        "experian_mode": EXPERIAN_MODE,
        "ch_api_key_set": bool(CH_API_KEY),
        "experian_client_id_set": bool(EXPERIAN_CLIENT_ID),
        "experian_client_secret_set": bool(EXPERIAN_CLIENT_SECRET),
        "experian_username_set": bool(EXPERIAN_USERNAME),
        "experian_password_set": bool(EXPERIAN_PASSWORD),
        "experian_base_url": EXPERIAN_BASE_URL,
    }


@app.get("/rix-credit/company/{company_number}")
def company_bundle(company_number: str):
    return get_companies_house_bundle(company_number)


@app.get("/experian/company/{company_number}")
def experian_company_report(
    company_number: str,
    company_name: Optional[str] = Query(default=None)
):
    start = time.perf_counter()
    report = get_experian_report(company_number, company_name)

    status_code = 200 if report.get("available") else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "request": {
                "company_number": company_number,
                "company_name": company_name,
                "mode": EXPERIAN_MODE,
                "timestamp_utc": now_utc_iso(),
            },
            "experian": report,
            "meta": {
                "timings_ms": {
                    "total": elapsed_ms(start)
                }
            }
        }
    )


@app.get("/rix-credit/company/{company_number}/credit-decision")
def credit_decision(
    company_number: str,
    company_name: Optional[str] = Query(default=None)
):
    return build_credit_decision(company_number, company_name)


@app.get("/rix-credit/company/{company_number}/credit-assessment")
def credit_assessment(
    company_number: str,
    company_name: Optional[str] = Query(default=None)
):
    return build_credit_decision(company_number, company_name)
