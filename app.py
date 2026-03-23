import os
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("rix-credit-api")

APP_NAME = "Rix Credit API"
APP_VERSION = "3.0.0"

# -----------------------------------------------------------------------------
# Environment / Config
# -----------------------------------------------------------------------------

CH_API_KEY = os.getenv("CH_API_KEY", "").strip()
CH_API_BASE = "https://api.company-information.service.gov.uk"

EXPERIAN_MODE = os.getenv("EXPERIAN_MODE", "mock").strip().lower()  # mock | live
EXPERIAN_BASE_URL = os.getenv("EXPERIAN_BASE_URL", "https://sandbox-uk-api.experian.com").strip()
EXPERIAN_CLIENT_ID = os.getenv("EXPERIAN_CLIENT_ID", "").strip()
EXPERIAN_CLIENT_SECRET = os.getenv("EXPERIAN_CLIENT_SECRET", "").strip()

# These are placeholders until your exact Experian product docs confirm paths.
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
        return int(value)
    except Exception:
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def elapsed_ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


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
    filings_raw = safe_fetch(
        f"{CH_API_BASE}/company/{company_number}/filing-history",
        "filing_history"
    )

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
        raise HTTPException(500, "Experian credentials missing")

    url = f"{EXPERIAN_BASE_URL.rstrip('/')}{EXPERIAN_TOKEN_PATH}"

    payload = {
        "grant_type": "client_credentials",
        "client_id": EXPERIAN_CLIENT_ID,
        "client_secret": EXPERIAN_CLIENT_SECRET,
    }

    with experian_session() as s:
        r = s.post(url, data=payload, timeout=EXPERIAN_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"Experian token error: {r.status_code} {r.text[:500]}")

    data = r.json()
    token = data.get("access_token")
    if not token:
        raise HTTPException(502, "Experian token response did not include access_token")

    return token


def experian_search_company_live(
    token: str,
    company_number: str,
    company_name: Optional[str] = None
) -> Dict[str, Any]:
    url = f"{EXPERIAN_BASE_URL.rstrip('/')}{EXPERIAN_SEARCH_PATH}"

    # This payload is intentionally flexible until your exact docs confirm the schema.
    payload = {
        "registrationNumber": company_number,
        "country": "GB",
    }
    if company_name:
        payload["name"] = company_name

    with experian_session(token) as s:
        r = s.post(url, json=payload, timeout=EXPERIAN_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"Experian search error: {r.status_code} {r.text[:500]}")

    return r.json()


def experian_extract_business_id(search_payload: Dict[str, Any]) -> Optional[str]:
    """
    Flexible extractor because exact sandbox response structure may vary by product/version.
    """
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
                for key in [
                    "businessId",
                    "business_id",
                    "id",
                    "companyId",
                    "company_id",
                    "reference",
                ]:
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
    seed = sum(ord(c) for c in company_number) % 40
    score = 45 + seed
    risk_band = "A" if score >= 80 else "B" if score >= 65 else "C" if score >= 50 else "D"
    limit = 50000 if score >= 80 else 25000 if score >= 65 else 12000 if score >= 50 else 3000
    avg_dbt = max(0, 30 - (score - 45))

    return {
        "available": True,
        "source": "experian_mock",
        "reference": f"EXP-{company_number}",
        "matched_company_name": company_name,
        "score": score,
        "score_description": (
            "Low risk" if score >= 80 else
            "Low to moderate risk" if score >= 65 else
            "Moderate risk" if score >= 50 else
            "Elevated risk"
        ),
        "risk_band": risk_band,
        "credit_limit": {
            "amount": float(limit),
            "currency": "GBP",
        },
        "payment_behaviour": {
            "days_beyond_terms": avg_dbt,
            "average_dbt": avg_dbt,
            "severe_dbt_flag": avg_dbt > 30,
            "payment_trend": "stable" if score >= 60 else "mixed",
            "ccj_flag": score < 48,
            "insolvency_flag": False,
        },
        "financials": {
            "turnover": 1800000.0 if score >= 60 else 850000.0,
            "total_assets": 620000.0 if score >= 60 else 250000.0,
            "current_assets": 310000.0 if score >= 60 else 90000.0,
            "current_liabilities": 190000.0 if score >= 60 else 120000.0,
            "net_worth": 210000.0 if score >= 60 else 35000.0,
            "cash": 92000.0 if score >= 60 else 18000.0,
        },
        "group_links": {
            "is_group_member": score >= 70,
            "parent_name": "Example Holdings Ltd" if score >= 70 else None,
            "parent_company_number": "07654321" if score >= 70 else None,
            "ultimate_parent_name": None,
            "linked_entities": [],
        },
        "warnings": [],
        "raw": {
            "mode": "mock",
            "company_number": company_number,
        },
    }


def map_experian_live_payload(
    raw_report: Dict[str, Any],
    business_id: Optional[str] = None,
    matched_company_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    This is intentionally tolerant and will need tightening once you confirm
    the exact Commercial Credit sandbox response fields.
    """
    financials = raw_report.get("financials") or raw_report.get("financialInformation") or {}
    group_links = raw_report.get("group_links") or raw_report.get("corporateLinkage") or {}
    payment = raw_report.get("payment_behaviour") or raw_report.get("paymentBehaviour") or {}

    credit_limit_value = None
    raw_credit_limit = raw_report.get("credit_limit") or raw_report.get("creditLimit")
    if isinstance(raw_credit_limit, dict):
        credit_limit_value = safe_float(raw_credit_limit.get("amount"))
    else:
        credit_limit_value = safe_float(raw_credit_limit)

    return {
        "available": True,
        "source": "experian_live",
        "reference": raw_report.get("reference") or raw_report.get("reportId") or business_id,
        "matched_company_name": matched_company_name,
        "score": safe_int(raw_report.get("score") or raw_report.get("commercialScore")),
        "score_description": raw_report.get("score_description") or raw_report.get("scoreText"),
        "risk_band": raw_report.get("risk_band") or raw_report.get("riskBand"),
        "credit_limit": {
            "amount": credit_limit_value,
            "currency": "GBP",
        },
        "payment_behaviour": {
            "days_beyond_terms": safe_int(
                payment.get("days_beyond_terms") or payment.get("daysBeyondTerms")
            ),
            "average_dbt": safe_int(
                payment.get("average_dbt") or payment.get("averageDBT")
            ),
            "severe_dbt_flag": payment.get("severe_dbt_flag"),
            "payment_trend": payment.get("payment_trend") or payment.get("trend"),
            "ccj_flag": payment.get("ccj_flag"),
            "insolvency_flag": payment.get("insolvency_flag"),
        },
        "financials": {
            "turnover": safe_float(financials.get("turnover")),
            "total_assets": safe_float(financials.get("total_assets") or financials.get("totalAssets")),
            "current_assets": safe_float(financials.get("current_assets") or financials.get("currentAssets")),
            "current_liabilities": safe_float(financials.get("current_liabilities") or financials.get("currentLiabilities")),
            "net_worth": safe_float(financials.get("net_worth") or financials.get("netWorth")),
            "cash": safe_float(financials.get("cash")),
        },
        "group_links": {
            "is_group_member": group_links.get("is_group_member") or group_links.get("isGroupMember"),
            "parent_name": group_links.get("parent_name") or group_links.get("parentName"),
            "parent_company_number": group_links.get("parent_company_number") or group_links.get("parentCompanyNumber"),
            "ultimate_parent_name": group_links.get("ultimate_parent_name") or group_links.get("ultimateParentName"),
            "linked_entities": group_links.get("linked_entities") or group_links.get("linkedEntities") or [],
        },
        "warnings": [],
        "raw": raw_report,
    }


def get_experian_report(company_number: str, company_name: Optional[str] = None) -> Dict[str, Any]:
    if EXPERIAN_MODE == "mock":
        return experian_mock_report(company_number, company_name)

    try:
        token = experian_get_token()
        search_result = experian_search_company_live(token, company_number, company_name)
        business_id = experian_extract_business_id(search_result)

        if not business_id:
            return {
                "available": False,
                "source": "experian_live",
                "reference": None,
                "matched_company_name": company_name,
                "score": None,
                "score_description": None,
                "risk_band": None,
                "credit_limit": {"amount": None, "currency": "GBP"},
                "payment_behaviour": {
                    "days_beyond_terms": None,
                    "average_dbt": None,
                    "severe_dbt_flag": None,
                    "payment_trend": None,
                    "ccj_flag": None,
                    "insolvency_flag": None,
                },
                "financials": {
                    "turnover": None,
                    "total_assets": None,
                    "current_assets": None,
                    "current_liabilities": None,
                    "net_worth": None,
                    "cash": None,
                },
                "group_links": {
                    "is_group_member": None,
                    "parent_name": None,
                    "parent_company_number": None,
                    "ultimate_parent_name": None,
                    "linked_entities": [],
                },
                "warnings": ["Experian search returned no usable business ID"],
                "raw": {
                    "search_result": search_result
                },
            }

        raw_report = experian_get_company_report_live(token, business_id)
        return map_experian_live_payload(raw_report, business_id=business_id, matched_company_name=company_name)

    except Exception as e:
        logger.exception("Experian fetch failed")
        return {
            "available": False,
            "source": "experian_live",
            "reference": None,
            "matched_company_name": company_name,
            "score": None,
            "score_description": None,
            "risk_band": None,
            "credit_limit": {"amount": None, "currency": "GBP"},
            "payment_behaviour": {
                "days_beyond_terms": None,
                "average_dbt": None,
                "severe_dbt_flag": None,
                "payment_trend": None,
                "ccj_flag": None,
                "insolvency_flag": None,
            },
            "financials": {
                "turnover": None,
                "total_assets": None,
                "current_assets": None,
                "current_liabilities": None,
                "net_worth": None,
                "cash": None,
            },
            "group_links": {
                "is_group_member": None,
                "parent_name": None,
                "parent_company_number": None,
                "ultimate_parent_name": None,
                "linked_entities": [],
            },
            "warnings": [f"Experian fetch failed: {str(e)}"],
            "raw": None,
        }


# -----------------------------------------------------------------------------
# Internal model / calibration
# -----------------------------------------------------------------------------

def build_internal_model(companies_house: Dict[str, Any], experian: Dict[str, Any]) -> Dict[str, Any]:
    score = 50
    drivers: List[str] = []
    caps_applied: List[str] = []
    warnings: List[str] = []

    financials = experian.get("financials") or {}
    payment = experian.get("payment_behaviour") or {}
    charges = companies_house.get("charges") or []

    net_worth = safe_float(financials.get("net_worth"))
    current_assets = safe_float(financials.get("current_assets"))
    current_liabilities = safe_float(financials.get("current_liabilities"))
    avg_dbt = safe_int(payment.get("average_dbt"))

    if net_worth is not None:
        if net_worth > 200000:
            score += 15
            drivers.append("Strong net worth")
        elif net_worth > 50000:
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
            score -= 12
            drivers.append("Weak liquidity")

    if avg_dbt is not None:
        if avg_dbt <= 10:
            score += 8
            drivers.append("Good payment behaviour")
        elif avg_dbt <= 30:
            score += 2
            drivers.append("Acceptable payment behaviour")
        else:
            score -= 12
            drivers.append("Poor payment behaviour")

    if payment.get("ccj_flag"):
        score -= 20
        drivers.append("CCJ/adverse indicator present")
        caps_applied.append("adverse_events_cap")

    if payment.get("insolvency_flag"):
        score -= 25
        drivers.append("Insolvency indicator present")
        caps_applied.append("insolvency_cap")

    if len(charges) > 0:
        score -= min(10, len(charges) * 2)
        drivers.append("Charge history present")
        caps_applied.append("charges_cap")

    profile = companies_house.get("company_profile") or {}
    status = str(profile.get("company_status", "")).lower().strip()
    if status and status != "active":
        score -= 20
        drivers.append(f"Company status is {status}")
        caps_applied.append("status_cap")

    if not experian.get("available"):
        score -= 5
        warnings.append("Experian unavailable - internal model confidence reduced")

    if not companies_house.get("available"):
        warnings.append("Companies House unavailable - structural context reduced")

    score = max(0, min(100, score))

    if score >= 80:
        grade = "A"
        risk_label = "Low"
        limit = 50000
    elif score >= 65:
        grade = "B"
        risk_label = "Low to Moderate"
        limit = 25000
    elif score >= 50:
        grade = "C"
        risk_label = "Moderate"
        limit = 12000
    elif score >= 35:
        grade = "D"
        risk_label = "Moderate to High"
        limit = 5000
    else:
        grade = "E"
        risk_label = "High"
        limit = 0

    return {
        "available": True,
        "score": score,
        "grade": grade,
        "risk_label": risk_label,
        "suggested_limit": {
            "amount": float(limit),
            "currency": "GBP",
        },
        "drivers": drivers,
        "caps_applied": caps_applied,
        "warnings": warnings,
    }


def calibrate(experian: Dict[str, Any], internal_model: Dict[str, Any]) -> Dict[str, Any]:
    exp_score = safe_int(experian.get("score"))
    int_score = safe_int(internal_model.get("score"))

    exp_limit = safe_float((experian.get("credit_limit") or {}).get("amount"))
    int_limit = safe_float((internal_model.get("suggested_limit") or {}).get("amount"))

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
    }


def build_final_decision(
    companies_house: Dict[str, Any],
    experian: Dict[str, Any],
    internal_model: Dict[str, Any],
    calibration: Dict[str, Any]
) -> Dict[str, Any]:
    warnings: List[str] = []
    rationale: List[str] = []

    exp_score = safe_int(experian.get("score"))
    int_score = safe_int(internal_model.get("score")) or 0

    if exp_score is None:
        final_score = int_score
        rationale.append("Experian unavailable; fallback to internal model")
    else:
        bias = calibration.get("decision_bias")
        if bias == "blended":
            final_score = round((exp_score + int_score) / 2)
            rationale.append("Experian and internal model are aligned")
        elif bias == "conservative_middle":
            final_score = round((exp_score * 0.4) + (int_score * 0.6))
            rationale.append("Experian is stronger, but conservative weighting retained")
        elif bias == "experian_weighted":
            final_score = round((exp_score * 0.7) + (int_score * 0.3))
            rationale.append("Experian is weaker, so bureau view is weighted more heavily")
        else:
            final_score = int_score
            rationale.append("Fallback internal model weighting used")

    charges = companies_house.get("charges") or []
    if len(charges) > 0:
        warnings.append("Charge history present")
        rationale.append("Charge history moderates lending appetite")

    payment = experian.get("payment_behaviour") or {}
    if payment.get("ccj_flag"):
        warnings.append("Adverse payment/legal indicator present")
        rationale.append("Adverse payment marker reduces confidence")

    group_links = experian.get("group_links") or {}
    if group_links.get("is_group_member"):
        warnings.append("Group linkage identified but support not assumed")
        rationale.append("Group structure may help resilience, but support is not relied upon")

    if final_score >= 80:
        risk_rating = "Low"
        credit_stance = "Approve"
        suggested_limit = max(
            safe_float((internal_model.get("suggested_limit") or {}).get("amount")) or 0,
            safe_float((experian.get("credit_limit") or {}).get("amount")) or 0,
        )
        confidence = "high" if experian.get("available") else "medium"
    elif final_score >= 65:
        risk_rating = "Low to Moderate"
        credit_stance = "Approve with normal controls"
        suggested_limit = min(
            max(safe_float((internal_model.get("suggested_limit") or {}).get("amount")) or 0, 15000),
            safe_float((experian.get("credit_limit") or {}).get("amount")) or 25000,
        ) if experian.get("available") else (safe_float((internal_model.get("suggested_limit") or {}).get("amount")) or 15000)
        confidence = "medium"
    elif final_score >= 50:
        risk_rating = "Moderate"
        credit_stance = "Approve with caution"
        exp_limit = safe_float((experian.get("credit_limit") or {}).get("amount")) or 0
        int_limit = safe_float((internal_model.get("suggested_limit") or {}).get("amount")) or 0
        if exp_limit > 0 and int_limit > 0:
            suggested_limit = min(exp_limit, round((exp_limit + int_limit) / 2))
        else:
            suggested_limit = int_limit or exp_limit or 10000
        confidence = "medium"
    elif final_score >= 35:
        risk_rating = "Moderate to High"
        credit_stance = "Restricted terms / reduced limit"
        suggested_limit = min(
            safe_float((internal_model.get("suggested_limit") or {}).get("amount")) or 5000,
            safe_float((experian.get("credit_limit") or {}).get("amount")) or 5000,
        ) if experian.get("available") else (safe_float((internal_model.get("suggested_limit") or {}).get("amount")) or 5000)
        confidence = "low"
    else:
        risk_rating = "High"
        credit_stance = "Decline or cash-with-order"
        suggested_limit = 0
        confidence = "low"

    return {
        "risk_rating": risk_rating,
        "credit_stance": credit_stance,
        "suggested_limit": {
            "amount": float(suggested_limit),
            "currency": "GBP",
        },
        "confidence": confidence,
        "rationale": rationale,
        "warnings": warnings,
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
    """
    Compatibility alias. Previously this endpoint used OCR + extracted accounts.
    It now returns the Experian-first calibrated decision.
    """
    return build_credit_decision(company_number, company_name)
