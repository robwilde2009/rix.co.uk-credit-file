import os
import io
import logging
import multiprocessing as mp
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from extractor import extract_financials_from_pdf_bytes

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("rix-credit-api")

APP_NAME = "Rix Credit API"
APP_VERSION = "2.0.3"

CH_API_KEY = os.getenv("CH_API_KEY", "").strip()
CH_API_BASE = "https://api.company-information.service.gov.uk"
CH_DOC_BASE = "https://document-api.company-information.service.gov.uk"

HTTP_TIMEOUT = (3.05, 20)
PDF_TIMEOUT = (3.05, 25)
EXTRACTION_TIMEOUT = int(os.getenv("EXTRACTION_TIMEOUT_SECONDS", "30"))

app = FastAPI(title=APP_NAME, version=APP_VERSION)


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


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


@app.get("/healthz", response_model=HealthResponse)
def healthz():
    return HealthResponse(status="ok", service=APP_NAME, version=APP_VERSION)


def ch_session() -> requests.Session:
    s = requests.Session()
    s.auth = (CH_API_KEY, "")
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "rix-credit-api/2.0.3",
    })
    return s


def raise_upstream_error(prefix: str, url: str, response: requests.Response) -> None:
    body = response.text[:2000] if response.text else ""
    raise HTTPException(
        status_code=502,
        detail={
            "message": prefix,
            "upstream_status": response.status_code,
            "url": url,
            "response_text": body,
        },
    )


def ch_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not CH_API_KEY:
        raise HTTPException(status_code=500, detail="CH_API_KEY is missing")

    try:
        with ch_session() as s:
            r = s.get(url, params=params, timeout=HTTP_TIMEOUT)

        if not r.ok:
            raise_upstream_error("Companies House request failed", r.url, r)

        return r.json()

    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "CH API transport error",
                "url": url,
                "error": str(e),
            },
        )


def doc_get(url: str) -> Dict[str, Any]:
    if not CH_API_KEY:
        raise HTTPException(status_code=500, detail="CH_API_KEY is missing")

    try:
        with ch_session() as s:
            r = s.get(url, timeout=HTTP_TIMEOUT)

        if not r.ok:
            raise_upstream_error("Document API request failed", r.url, r)

        return r.json()

    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Document API transport error",
                "url": url,
                "error": str(e),
            },
        )


def doc_pdf(url: str) -> bytes:
    if not CH_API_KEY:
        raise HTTPException(status_code=500, detail="CH_API_KEY is missing")

    try:
        with ch_session() as s:
            r = s.get(url, timeout=PDF_TIMEOUT, headers={"Accept": "application/pdf"})

        if not r.ok:
            raise_upstream_error("PDF fetch failed", r.url, r)

        return r.content

    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "PDF transport error",
                "url": url,
                "error": str(e),
            },
        )


def normalize_doc_url(link: Optional[str]) -> Optional[str]:
    if not link:
        return None

    link = link.strip()

    if link.startswith("http://") or link.startswith("https://"):
        return link

    if "document-api.company-information.service.gov.uk" in link:
        if link.startswith("//"):
            return f"https:{link}"
        if not link.startswith("https://"):
            return f"https://{link.lstrip('/')}"
        return link

    if link.startswith("/"):
        return f"{CH_DOC_BASE}{link}"

    return f"{CH_DOC_BASE}/{link.lstrip('/')}"


def looks_like_accounts_filing(item: Dict[str, Any]) -> bool:
    text = " ".join([
        str(item.get("category", "")),
        str(item.get("description", "")),
        str(item.get("type", "")),
        str(item.get("subcategory", "")),
        str(item.get("description_values", "")),
    ]).lower()

    markers = [
        "accounts",
        "annual accounts",
        "micro-entity",
        "micro entity",
        "filleted accounts",
        "dormant",
        "small company accounts",
        "total exemption",
        "unaudited",
        "interim",
    ]
    return any(m in text for m in markers)


def get_latest_accounts(company_number: str) -> Optional[Dict[str, Any]]:
    _profile = ch_get(f"{CH_API_BASE}/company/{company_number}")

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


def run_extraction(pdf_bytes: bytes, company_number: str) -> Dict[str, Any]:
    def worker(q):
        try:
            result = extract_financials_from_pdf_bytes(pdf_bytes, company_number)
            q.put({"ok": True, "result": result})
        except Exception as e:
            q.put({
                "ok": False,
                "error": str(e),
            })

    q = mp.Queue()
    p = mp.Process(target=worker, args=(q,))
    p.start()
    p.join(EXTRACTION_TIMEOUT)

    if p.is_alive():
        p.terminate()
        p.join(2)
        return {
            "status": "timeout",
            "message": f"Extraction exceeded {EXTRACTION_TIMEOUT}s",
        }

    if q.empty():
        return {
            "status": "error",
            "message": "Extraction returned no result",
        }

    payload = q.get()

    if not payload.get("ok"):
        return {
            "status": "failed",
            "error": payload.get("error", "Unknown extraction error"),
        }

    return {
        "status": "success",
        "data": payload["result"],
    }


@app.get("/rix-credit/company/{company_number}")
def company_bundle(company_number: str):
    profile = ch_get(f"{CH_API_BASE}/company/{company_number}")
    officers = ch_get(f"{CH_API_BASE}/company/{company_number}/officers")
    pscs = ch_get(f"{CH_API_BASE}/company/{company_number}/persons-with-significant-control")
    charges = ch_get(f"{CH_API_BASE}/company/{company_number}/charges")

    return {
        "company_profile": profile,
        "officers": officers,
        "pscs": pscs,
        "charges": charges,
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
        "status": result.get("status", "unknown"),
        "financials": result,
    }
