import os
import io
import logging
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from extractor import extract_financials_from_pdf_bytes

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("rix-credit-api")

APP_NAME = "Rix Credit API"
APP_VERSION = "2.1.0"

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
        ],
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


def ch_session():
    s = requests.Session()
    s.auth = (CH_API_KEY, "")
    return s


def ch_get(url: str, params=None):
    with ch_session() as s:
        r = s.get(url, params=params, timeout=HTTP_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"CH API error: {r.status_code} {r.text[:200]}")

    return r.json()


def doc_get(url: str):
    with ch_session() as s:
        r = s.get(url, timeout=HTTP_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, f"Document API error: {r.status_code}")

    return r.json()


def doc_pdf(url: str) -> bytes:
    with ch_session() as s:
        r = s.get(url, timeout=PDF_TIMEOUT)

    if not r.ok:
        raise HTTPException(502, "PDF fetch failed")

    return r.content


def normalize_doc_url(link: Optional[str]) -> Optional[str]:
    if not link:
        return None

    if link.startswith("http"):
        return link

    return f"{CH_DOC_BASE}/{link.lstrip('/')}"


def looks_like_accounts_filing(item):
    text = str(item).lower()
    return "accounts" in text


def get_latest_accounts(company_number: str):
    filings = ch_get(
        f"{CH_API_BASE}/company/{company_number}/filing-history",
        params={"items_per_page": 50},
    )

    for item in filings.get("items", []):
        if not looks_like_accounts_filing(item):
            continue

        link = item.get("links", {}).get("document_metadata")
        meta_url = normalize_doc_url(link)
        if not meta_url:
            continue

        metadata = doc_get(meta_url)
        pdf_url = meta_url + "/content"

        return {
            "filing": item,
            "metadata": metadata,
            "pdf_url": pdf_url,
        }

    return None


@app.get("/rix-credit/company/{company_number}/latest-accounts-financials")
def latest_accounts_financials(company_number: str):
    try:
        data = get_latest_accounts(company_number)
        if not data:
            return {"status": "no_accounts"}

        pdf = doc_pdf(data["pdf_url"])

        try:
            return {
    "status": "debug",
    "company_number": company_number,
    "pdf_size": len(pdf),
}

            return {
                "status": "success",
                "company_number": company_number,
                "financials": extracted,
            }

        except Exception as e:
            logger.exception("Extraction failed")

            return {
                "status": "failed",
                "company_number": company_number,
                "error": str(e),
            }

    except Exception as e:
        logger.exception("Top level failure")

        return {
            "status": "error",
            "error": str(e),
        }
