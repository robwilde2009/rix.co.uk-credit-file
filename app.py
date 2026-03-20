import os
import io
import logging
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from extractor import extract_financials_from_pdf_bytes
from scorer import score_financials

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("rix-credit-api")

APP_NAME = "Rix Credit API"
APP_VERSION = "2.2.0"

CH_API_KEY = os.getenv("CH_API_KEY", "").strip()
CH_API_BASE = "https://api.company-information.service.gov.uk"
CH_DOC_BASE = "https://document-api.company-information.service.gov.uk"

HTTP_TIMEOUT = (3.05, 20)
PDF_TIMEOUT = (3.05, 25)

app = FastAPI(title=APP_NAME, version=APP_VERSION)


@app.get("/")
def root():
    return {
        "status": "ok",
        "service": APP_NAME,
        "version": APP_VERSION,
        "endpoints": [
            "/healthz",
            "/rix-credit/company/{company_number}",
            "/rix-credit/company/{company_number}/latest-accounts-metadata",
            "/rix-credit/company/{company_number}/latest-accounts.pdf",
            "/rix-credit/company/{company_number}/latest-accounts-financials",
            "/rix-credit/company/{company_number}/credit-assessment",
        ],
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


def ch_session():
    s = requests.Session()
    s.auth = (CH_API_KEY, "")
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "rix-credit-api/2.2.0",
    })
    return s


def ch_get(url: str, params=None):
    if not CH_API_KEY:
        raise HTTPException(500, "CH_API_KEY is missing")

    with ch_session() as s:
        r = s.get(url, params=params, timeout=HTTP_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"CH API error: {r.status_code} {r.text[:300]}")

    return r.json()


def doc_get(url: str):
    if not CH_API_KEY:
        raise HTTPException(500, "CH_API_KEY is missing")

    with ch_session() as s:
        r = s.get(url, timeout=HTTP_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"Document API error: {r.status_code} {r.text[:300]}")

    return r.json()


def doc_pdf(url: str) -> bytes:
    if not CH_API_KEY:
        raise HTTPException(500, "CH_API_KEY is missing")

    with ch_session() as s:
        r = s.get(url, timeout=PDF_TIMEOUT, headers={"Accept": "application/pdf"})

    if not r.ok:
        raise HTTPException(502, f"PDF fetch failed: {r.status_code}")

    return r.content


def normalize_doc_url(link: Optional[str]) -> Optional[str]:
    if not link:
        return None

    link = link.strip()

    if link.startswith("http://") or link.startswith("https://"):
        return link

    if link.startswith("/"):
        return f"{CH_DOC_BASE}{link}"

    return f"{CH_DOC_BASE}/{link.lstrip('/')}"


def looks_like_accounts_filing(item):
    text = " ".join([
        str(item.get("category", "")),
        str(item.get("description", "")),
        str(item.get("type", "")),
        str(item.get("description_values", "")),
    ]).lower()

    markers = [
        "accounts",
        "annual accounts",
        "interim",
        "micro-entity",
        "filleted accounts",
        "dormant",
        "unaudited",
    ]
    return any(m in text for m in markers)


def get_latest_accounts(company_number: str):
    filings = ch_get(
        f"{CH_API_BASE}/company/{company_number}/filing-history",
        params={"items_per_page": 100},
    )

    for item in filings.get("items", []):
        if not looks_like_accounts_filing(item):
            continue

        link = item.get("links", {}).get("document_metadata")
        meta_url = normalize_doc_url(link)
        if not meta_url:
            continue

        metadata = doc_get(meta_url)
        pdf_url = meta_url.rstrip("/") + "/content"

        return {
            "filing": item,
            "metadata": metadata,
            "pdf_url": pdf_url,
        }

    return None


@app.get("/rix-credit/company/{company_number}")
def company_bundle(company_number: str):
    return {
        "company_profile": ch_get(f"{CH_API_BASE}/company/{company_number}"),
        "officers": ch_get(f"{CH_API_BASE}/company/{company_number}/officers"),
        "pscs": ch_get(f"{CH_API_BASE}/company/{company_number}/persons-with-significant-control"),
        "charges": ch_get(f"{CH_API_BASE}/company/{company_number}/charges"),
    }


@app.get("/rix-credit/company/{company_number}/latest-accounts-metadata")
def latest_accounts_metadata(company_number: str):
    data = get_latest_accounts(company_number)
    if not data:
        raise HTTPException(404, "No accounts found")
    return data


@app.get("/rix-credit/company/{company_number}/latest-accounts.pdf")
def latest_accounts_pdf(company_number: str):
    data = get_latest_accounts(company_number)
    if not data:
        raise HTTPException(404, "No accounts PDF")

    pdf = doc_pdf(data["pdf_url"])

    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": "inline; filename=accounts.pdf"},
    )


@app.get("/rix-credit/company/{company_number}/latest-accounts-financials")
def latest_accounts_financials(company_number: str):
    try:
        data = get_latest_accounts(company_number)
        if not data:
            return {"status": "no_accounts"}

        pdf = doc_pdf(data["pdf_url"])
        extracted = extract_financials_from_pdf_bytes(pdf, company_number)

        return {
            "status": "success",
            "company_number": company_number,
            "financials": extracted,
        }

    except Exception as e:
        logger.exception("Financial extraction failed")
        return JSONResponse(
            status_code=200,
            content={
                "status": "failed",
                "company_number": company_number,
                "error": str(e),
            },
        )


@app.get("/rix-credit/company/{company_number}/credit-assessment")
def credit_assessment(company_number: str):
    try:
        profile = ch_get(f"{CH_API_BASE}/company/{company_number}")
        data = get_latest_accounts(company_number)

        if not data:
            return {
                "status": "no_accounts",
                "company_number": company_number,
                "company_name": profile.get("company_name"),
            }

        pdf = doc_pdf(data["pdf_url"])
        extracted = extract_financials_from_pdf_bytes(pdf, company_number)
        scoring = score_financials(extracted)

        return {
            "status": "success",
            "company_number": company_number,
            "company_name": profile.get("company_name"),
            "company_status": profile.get("company_status"),
            "accounts_type": ((profile.get("accounts") or {}).get("last_accounts") or {}).get("type"),
            "made_up_to": ((profile.get("accounts") or {}).get("last_accounts") or {}).get("made_up_to"),
            "financials": extracted,
            "credit_assessment": scoring,
        }

    except Exception as e:
        logger.exception("Credit assessment failed")
        return JSONResponse(
            status_code=200,
            content={
                "status": "failed",
                "company_number": company_number,
                "error": str(e),
            },
        )
