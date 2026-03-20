import io
import re
from typing import Any, Dict, List, Optional

import pdfplumber
from pdf2image import convert_from_bytes
import pytesseract


MIN_TEXT_LENGTH = 100


def extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    text_parts = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            if txt.strip():
                text_parts.append(txt)

    return "\n".join(text_parts)


def extract_text_ocr(pdf_bytes: bytes) -> str:
    # LIMIT pages for safety
    images = convert_from_bytes(pdf_bytes, first_page=1, last_page=3, dpi=150)

    text_parts = []

    for img in images:
        txt = pytesseract.image_to_string(img)
        if txt.strip():
            text_parts.append(txt)

    return "\n".join(text_parts)


def parse_number(text: str) -> Optional[int]:
    text = text.replace(",", "").replace("£", "").strip()

    if not re.match(r"-?\d+", text):
        return None

    try:
        return int(float(text))
    except:
        return None


def extract_line_value(line: str) -> Optional[int]:
    numbers = re.findall(r"\(?-?\d[\d,]*\)?", line)
    if not numbers:
        return None

    # take last number (most recent year usually)
    val = numbers[-1]

    if val.startswith("(") and val.endswith(")"):
        val = "-" + val[1:-1]

    return parse_number(val)


def find_value(text: str, keywords: List[str]) -> Optional[int]:
    lines = text.splitlines()

    for line in lines:
        line_l = line.lower()

        if any(k in line_l for k in keywords):
            val = extract_line_value(line)
            if val is not None:
                return val

    return None


def extract_financials_from_pdf_bytes(pdf_bytes: bytes, company_number: str = None) -> Dict[str, Any]:
    # STEP 1 — Try normal extraction
    text = extract_text_pdfplumber(pdf_bytes)

    method = "pdfplumber"

    # STEP 2 — Fallback to OCR
    if len(text) < MIN_TEXT_LENGTH:
        text = extract_text_ocr(pdf_bytes)
        method = "ocr"

    if len(text) < MIN_TEXT_LENGTH:
        raise ValueError("No readable text found (PDF + OCR failed)")

    # STEP 3 — Extract fields
    fixed_assets = find_value(text, ["fixed assets"])
    current_assets = find_value(text, ["current assets"])
    cash = find_value(text, ["cash"])
    debtors = find_value(text, ["debtors"])
    current_liabilities = find_value(text, ["creditors", "within one year"])
    net_assets = find_value(text, ["net assets", "total equity"])

    # Derived
    working_capital = None
    if current_assets is not None and current_liabilities is not None:
        working_capital = current_assets - current_liabilities

    return {
        "company_number": company_number,
        "method": method,
        "fixed_assets": fixed_assets,
        "current_assets": current_assets,
        "cash": cash,
        "debtors": debtors,
        "current_liabilities": current_liabilities,
        "working_capital": working_capital,
        "net_assets": net_assets,
    }
