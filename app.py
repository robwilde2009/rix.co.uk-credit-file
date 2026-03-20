import os
import io
import json
import time
import logging
import multiprocessing as mp
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from extractor import extract_financials_from_pdf_bytes

logging.basicConfig(level="INFO")
logger = logging.getLogger("rix-credit-api")

APP_NAME = "Rix Credit API"
APP_VERSION = "2.0.0"

CH_API_KEY = os.getenv("CH_API_KEY", "")
CH_API_BASE = "https://api.company-information.service.gov.uk"
CH_DOC_BASE = "https://document-api.company-information.service.gov.uk"

HTTP_TIMEOUT = (3.05, 20)
PDF_TIMEOUT = (3.05, 25)
EXTRACTION_TIMEOUT = int(os.getenv("EXTRACTION_TIMEOUT_SECONDS", "30"))

app = FastAPI(title=APP_NAME, version=APP_VERSION)


# ---------------------------
# MODELS
# ---------------------------

class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


# ---------------------------
# ROOT (FIX FOR 404)
# ---------------------------

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


# ---------------------------
# HEALTH
# ---------------------------

@app.get("/healthz", response_model=HealthResponse)
def healthz():
    return HealthResponse(status="ok", service=APP_NAME, version=APP_VERSION)


# ---------------------------
# HELPERS
# ---------------------------

def ch_session():
    s = requests.Session()
    s.auth = (CH_API_KEY, "")
    return s


def ch_get(url):
    try:
        r = ch_session().get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(502, f"CH API error: {str(e)}")


def doc_get(url):
    try:
        r = ch_session().get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(502, f"Document API error: {str(e)}")


def doc_pdf(url):
    try:
        r = ch_session().get(url, timeout=PDF_TIMEOUT, headers={"Accept": "application/pdf"})
        r.raise_for_status()
        return r.content
    except Exception as e:
        raise HTTPException(502, f"PDF fetch error: {str(e)}")


# ---------------------------
# CORE LOGIC
# ---------------------------

def get_latest_accounts(company_number: str):
    filings = ch_get(f"{CH_API_BASE}/company/{company_number}/filing-history")

    for item in filings.get("items", []):
        if "accounts" in (item.get("description") or "").lower():
            link = item.get("links", {}).get("document_metadata")
            if link:
                meta_url = f"{CH_DOC_BASE}{link}"
                metadata = doc_get(meta_url)
                pdf_url = meta_url + "/content"

                return {
                    "filing": item,
                    "metadata": metadata,
                    "pdf_url": pdf_url,
                }

    return None


def run_extraction(pdf_bytes, company_number):
    def worker(q):
        try:
            result = extract_financials_from_pdf_bytes(pdf_bytes, company_number)
            q.put(result)
        except Exception as e:
            q.put({"error": str(e)})

    q = mp.Queue()
    p = mp.Process(target=worker, args=(q,))
    p.start()
    p.join(EXTRACTION_TIMEOUT)

    if p.is_alive():
        p.terminate()
        raise HTTPException(504, "Extraction timeout")

    result = q.get()

    if "error" in result:
        raise HTTPException(422, result["error"])

    return result


# ---------------------------
# ENDPOINTS
# ---------------------------

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
    data = get_latest_accounts(company_number)
    if not data:
        return JSONResponse(
            status_code=404,
            content={"status": "no_accounts"},
        )

    pdf = doc_pdf(data["pdf_url"])

    result = run_extraction(pdf, company_number)

    return {
        "company_number": company_number,
        "status": "ok",
        "financials": result,
    }
