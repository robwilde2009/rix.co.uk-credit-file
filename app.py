import os
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

from extractor import extract_financials_from_pdf_bytes
from scorer import score_financials

app = FastAPI()

CH_API_BASE = "https://api.company-information.service.gov.uk"
CH_DOC_BASE = "https://document-api.company-information.service.gov.uk"

CH_API_KEY = os.getenv("CH_API_KEY")


# ---------------------------
# Helpers
# ---------------------------

def ch_get(url: str):
    try:
        r = requests.get(url, auth=(CH_API_KEY, ""), timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CH API error: {str(e)}")


def doc_get_metadata(url: str):
    try:
        r = requests.get(url, auth=(CH_API_KEY, ""), timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Document API error: {str(e)}")


def doc_get_pdf(url: str):
    try:
        r = requests.get(url, auth=(CH_API_KEY, ""), timeout=20)
        r.raise_for_status()
        return r.content
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Document download error: {str(e)}")


def get_latest_accounts(company_number: str):
    filing = ch_get(f"{CH_API_BASE}/company/{company_number}/filing-history")

    items = filing.get("items", [])
    accounts = [i for i in items if i.get("category") == "accounts"]

    if not accounts:
        return None

    latest = accounts[0]

    doc_meta_url = latest.get("links", {}).get("document_metadata")
    if not doc_meta_url:
        return None

    metadata = doc_get_metadata(doc_meta_url)

    pdf_url = metadata.get("links", {}).get("document")

    return {
        "filing": latest,
        "metadata": metadata,
        "pdf_url": pdf_url,
    }


# ---------------------------
# Health
# ---------------------------

@app.get("/healthz")
def health():
    return {
        "status": "ok",
        "service": "Rix Credit API",
        "version": "2.1.0",
        "endpoints": [
            "/healthz",
            "/rix-credit/company/{company_number}",
            "/rix-credit/company/{company_number}/latest-accounts-metadata",
            "/rix-credit/company/{company_number}/latest-accounts.pdf",
            "/rix-credit/company/{company_number}/latest-accounts-financials",
            "/rix-credit/company/{company_number}/credit-assessment"
        ],
    }


# ---------------------------
# Core endpoints
# ---------------------------

@app.get("/rix-credit/company/{company_number}")
def company(company_number: str):
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
        raise HTTPException(status_code=404, detail="No accounts found")

    return data


@app.get("/rix-credit/company/{company_number}/latest-accounts.pdf")
def latest_accounts_pdf(company_number: str):
    data = get_latest_accounts(company_number)
    if not data:
        raise HTTPException(status_code=404, detail="No accounts found")

    pdf = doc_get_pdf(data["pdf_url"])

    return Response(content=pdf, media_type="application/pdf")


@app.get("/rix-credit/company/{company_number}/latest-accounts-financials")
def latest_accounts_financials(company_number: str):
    data = get_latest_accounts(company_number)
    if not data:
        raise HTTPException(status_code=404, detail="No accounts found")

    pdf = doc_get_pdf(data["pdf_url"])

    try:
        financials = extract_financials_from_pdf_bytes(pdf, company_number)
    except Exception as e:
        return {
            "status": "failed",
            "company_number": company_number,
            "error": str(e),
        }

    return {
        "status": "success",
        "company_number": company_number,
        "financials": financials,
    }


# ---------------------------
# CREDIT ASSESSMENT (MAIN)
# ---------------------------

@app.get("/rix-credit/company/{company_number}/credit-assessment")
def credit_assessment(company_number: str):
    try:
        profile = ch_get(f"{CH_API_BASE}/company/{company_number}")
        charges = ch_get(f"{CH_API_BASE}/company/{company_number}/charges")
        data = get_latest_accounts(company_number)

        if not data:
            return {
                "status": "no_accounts",
                "company_number": company_number,
                "company_name": profile.get("company_name"),
            }

        pdf = doc_get_pdf(data["pdf_url"])

        extracted = extract_financials_from_pdf_bytes(pdf, company_number)

        scoring = score_financials(
            extracted,
            profile=profile,
            charges=charges
        )

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
        return {
            "status": "error",
            "company_number": company_number,
            "error": str(e),
        })
