import os
import re
import io
import json
import time
import base64
import logging
import multiprocessing as mp
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from extractor import extract_financials_from_pdf_bytes

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("rix-credit-api")

APP_NAME = "Rix Credit API"
APP_VERSION = "2.0.0"

CH_API_KEY = os.getenv("CH_API_KEY", "").strip()
CH_API_BASE = os.getenv("CH_API_BASE", "https://api.company-information.service.gov.uk").rstrip("/")
CH_DOC_BASE = os.getenv("CH_DOC_BASE", "https://document-api.company-information.service.gov.uk").rstrip("/")

HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "3.05"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "20"))
PDF_FETCH_TIMEOUT = float(os.getenv("PDF_FETCH_TIMEOUT", "25"))
EXTRACTION_TIMEOUT_SECONDS = int(os.getenv("EXTRACTION_TIMEOUT_SECONDS", "30"))

if not CH_API_KEY:
    logger.warning("CH_API_KEY is not set. Companies House requests will fail until configured.")

app = FastAPI(title=APP_NAME, version=APP_VERSION)


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


class LatestAccountsMetadataResponse(BaseModel):
    company_number: str
    filing: Optional[Dict[str, Any]] = None
    document_metadata: Optional[Dict[str, Any]] = None
    pdf_available: bool = False
    pdf_url: Optional[str] = None


def ch_session() -> requests.Session:
    session = requests.Session()
    session.auth = (CH_API_KEY, "")
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "rix-credit-api/2.0",
    })
    return session


def ch_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    timeout = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)
    try:
        with ch_session() as session:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Resource not found")
            if resp.status_code == 401:
                raise HTTPException(status_code=500, detail="Companies House authentication failed")
            resp.raise_for_status()
            return resp.json()
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Upstream Companies House request timed out")
    except requests.RequestException as exc:
        logger.exception("Companies House request failed: %s", exc)
        raise HTTPException(status_code=502, detail="Companies House request failed")


def doc_get_json(url: str) -> Dict[str, Any]:
    timeout = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)
    try:
        with ch_session() as session:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Document metadata not found")
            if resp.status_code == 401:
                raise HTTPException(status_code=500, detail="Companies House document authentication failed")
            resp.raise_for_status()
            return resp.json()
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Document metadata request timed out")
    except requests.RequestException as exc:
        logger.exception("Document metadata request failed: %s", exc)
        raise HTTPException(status_code=502, detail="Document metadata request failed")


def doc_get_content_bytes(url: str) -> bytes:
    timeout = (HTTP_CONNECT_TIMEOUT, PDF_FETCH_TIMEOUT)
    headers = {"Accept": "application/pdf"}
    try:
        with ch_session() as session:
            resp = session.get(url, timeout=timeout, headers=headers)
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Accounts PDF not found")
            if resp.status_code == 401:
                raise HTTPException(status_code=500, detail="Companies House document authentication failed")
            resp.raise_for_status()
            return resp.content
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Accounts PDF download timed out")
    except requests.RequestException as exc:
        logger.exception("Accounts PDF download failed: %s", exc)
        raise HTTPException(status_code=502, detail="Accounts PDF download failed")


def normalize_document_metadata_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None
    raw_url = raw_url.strip()
    if raw_url.startswith("http://") or raw_url.startswith("https://"):
        return raw_url
    if raw_url.startswith("/"):
        return f"{CH_DOC_BASE}{raw_url}"
    return f"{CH_DOC_BASE}/{raw_url}"


def document_content_url_from_metadata_url(metadata_url: str) -> str:
    return metadata_url.rstrip("/") + "/content"


def get_company_profile(company_number: str) -> Dict[str, Any]:
    return ch_get_json(f"{CH_API_BASE}/company/{company_number}")


def get_officers(company_number: str) -> Dict[str, Any]:
    return ch_get_json(f"{CH_API_BASE}/company/{company_number}/officers")


def get_pscs(company_number: str) -> Dict[str, Any]:
    return ch_get_json(f"{CH_API_BASE}/company/{company_number}/persons-with-significant-control")


def get_charges(company_number: str) -> Dict[str, Any]:
    return ch_get_json(f"{CH_API_BASE}/company/{company_number}/charges")


def get_filing_history(company_number: str, items_per_page: int = 100) -> Dict[str, Any]:
    return ch_get_json(
        f"{CH_API_BASE}/company/{company_number}/filing-history",
        params={"items_per_page": items_per_page},
    )


def extract_document_metadata_url(item: Dict[str, Any]) -> Optional[str]:
    links = item.get("links") or {}
    candidates = [
        links.get("document_metadata"),
        links.get("document"),
    ]
    for candidate in candidates:
        normalized = normalize_document_metadata_url(candidate)
        if normalized:
            return normalized
    transaction = item.get("transaction_id")
    if transaction:
        # Best effort fallback; not always available, but useful as a backup path pattern.
        return None
    return None


def is_accounts_filing(item: Dict[str, Any]) -> bool:
    category = (item.get("category") or "").lower()
    description = (item.get("description") or "").lower()
    description_values = json.dumps(item.get("description_values") or {}).lower()
    text = " ".join([category, description, description_values])

    accounts_markers = [
        "accounts",
        "annual accounts",
        "micro-entity",
        "micro entity",
        "total exemption full accounts",
        "total exemption small company accounts",
        "dormant",
        "unaudited abridged",
        "filleted accounts",
        "small company accounts",
    ]
    return any(marker in text for marker in accounts_markers)


def get_latest_accounts_metadata(company_number: str) -> LatestAccountsMetadataResponse:
    filing_history = get_filing_history(company_number, items_per_page=100)
    items = filing_history.get("items", []) or []

    accounts_items = [item for item in items if is_accounts_filing(item)]
    if not accounts_items:
        return LatestAccountsMetadataResponse(company_number=company_number)

    latest_item = accounts_items[0]
    metadata_url = extract_document_metadata_url(latest_item)

    document_metadata = None
    pdf_url = None
    pdf_available = False

    if metadata_url:
        document_metadata = doc_get_json(metadata_url)
        content_url = document_content_url_from_metadata_url(metadata_url)
        resources = (document_metadata.get("resources") or {})
        pdf_available = "application/pdf" in resources or True
        pdf_url = content_url

    return LatestAccountsMetadataResponse(
        company_number=company_number,
        filing=latest_item,
        document_metadata=document_metadata,
        pdf_available=pdf_available if pdf_url else False,
        pdf_url=pdf_url,
    )


def _extraction_worker(pdf_bytes: bytes, company_number: str, q: mp.Queue) -> None:
    try:
        result = extract_financials_from_pdf_bytes(pdf_bytes, company_number=company_number)
        q.put({"ok": True, "result": result})
    except Exception as exc:
        logger.exception("Extraction worker failed: %s", exc)
        q.put({"ok": False, "error": str(exc)})


def run_extraction_with_timeout(pdf_bytes: bytes, company_number: str, timeout_seconds: int) -> Dict[str, Any]:
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    process = ctx.Process(target=_extraction_worker, args=(pdf_bytes, company_number, q), daemon=True)

    started = time.time()
    process.start()
    process.join(timeout_seconds)

    if process.is_alive():
        logger.warning("Extraction timeout reached for company %s; terminating worker", company_number)
        process.terminate()
        process.join(3)
        raise HTTPException(
            status_code=504,
            detail=f"Financial extraction timed out after {timeout_seconds} seconds",
        )

    elapsed_ms = int((time.time() - started) * 1000)

    if q.empty():
        raise HTTPException(status_code=502, detail="Financial extraction failed with no result")

    payload = q.get()
    if not payload.get("ok"):
        raise HTTPException(status_code=422, detail=f"Financial extraction failed: {payload.get('error', 'unknown error')}")

    result = payload["result"]
    result["timing"] = {"extraction_ms": elapsed_ms}
    return result


@app.get("/healthz", response_model=HealthResponse)
def healthz() -> HealthResponse:
    return HealthResponse(status="ok", service=APP_NAME, version=APP_VERSION)


@app.get("/rix-credit/company/{company_number}")
def get_company_bundle(company_number: str):
    profile = get_company_profile(company_number)
    officers = get_officers(company_number)
    pscs = get_pscs(company_number)
    charges = get_charges(company_number)
    filing_history = get_filing_history(company_number, items_per_page=50)
    latest_accounts = get_latest_accounts_metadata(company_number)

    return {
        "company_profile": profile,
        "officers": officers,
        "pscs": pscs,
        "charges": charges,
        "filing_history": filing_history,
        "recent_accounts": latest_accounts.filing,
        "latest_accounts_financials": {
            "status": "metadata_only",
            "pdf_available": latest_accounts.pdf_available,
            "pdf_url": latest_accounts.pdf_url,
        },
    }


@app.get("/rix-credit/company/{company_number}/latest-accounts-metadata")
def latest_accounts_metadata(company_number: str):
    return get_latest_accounts_metadata(company_number).model_dump()


@app.get("/rix-credit/company/{company_number}/latest-accounts.pdf")
def latest_accounts_pdf(company_number: str):
    metadata = get_latest_accounts_metadata(company_number)
    if not metadata.pdf_url:
        raise HTTPException(status_code=404, detail="No accounts PDF available")

    pdf_bytes = doc_get_content_bytes(metadata.pdf_url)
    filename = f"{company_number}-latest-accounts.pdf"

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.get("/rix-credit/company/{company_number}/latest-accounts-financials")
def latest_accounts_financials(company_number: str):
    metadata = get_latest_accounts_metadata(company_number)
    if not metadata.pdf_url:
        return JSONResponse(
            status_code=404,
            content={
                "company_number": company_number,
                "status": "no_accounts_pdf",
                "message": "No accounts PDF available for latest filing",
                "financials": None,
            },
        )

    pdf_bytes = doc_get_content_bytes(metadata.pdf_url)
    result = run_extraction_with_timeout(
        pdf_bytes=pdf_bytes,
        company_number=company_number,
        timeout_seconds=EXTRACTION_TIMEOUT_SECONDS,
    )

    return {
        "company_number": company_number,
        "status": "ok",
        "filing": metadata.filing,
        "document_metadata": metadata.document_metadata,
        "financials": result,
    }
