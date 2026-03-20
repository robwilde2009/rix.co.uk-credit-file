import io
import re
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
from pdf2image import convert_from_bytes
import pytesseract


MIN_TEXT_LENGTH = 100
MAX_OCR_PAGES = 10
OCR_DPI = 150
DEBUG_SNIPPET_LENGTH = 2500

FIELD_KEYWORDS = {
    "fixed_assets": ["fixed assets", "tangible assets", "intangible assets"],
    "current_assets": ["current assets"],
    "cash": ["cash at bank", "cash in hand", "cash"],
    "debtors": ["debtors", "trade debtors"],
    "current_liabilities": [
        "creditors: amounts falling due within one year",
        "amounts falling due within one year",
        "within one year",
        "current liabilities",
        "creditors"
    ],
    "net_assets": [
        "net assets",
        "net liabilities",
        "total equity",
        "shareholders' funds",
        "shareholders funds",
        "members' funds",
        "members funds",
        "capital and reserves",
    ],
}

PAGE_SIGNALS = [
    "balance sheet",
    "statement of financial position",
    "current assets",
    "creditors",
    "net assets",
    "called up share capital",
    "profit and loss account",
    "capital and reserves",
    "shareholders' funds",
    "members' funds",
]


def extract_text_pdfplumber_pages(pdf_bytes: bytes) -> List[str]:
    pages = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            pages.append(txt.strip())
    return pages


def extract_text_ocr_pages(pdf_bytes: bytes) -> List[str]:
    images = convert_from_bytes(
        pdf_bytes,
        first_page=1,
        last_page=MAX_OCR_PAGES,
        dpi=OCR_DPI,
    )
    pages = []
    for img in images:
        txt = pytesseract.image_to_string(img)
        pages.append(txt.strip())
    return pages


def score_page(text: str) -> int:
    t = text.lower()
    score = 0
    for signal in PAGE_SIGNALS:
        if signal in t:
            score += 3
    if re.search(r"\b20\d{2}\b", t):
        score += 1
    if len(re.findall(r"\(?\d[\d,]*\)?", text)) >= 5:
        score += 2
    return score


def pick_best_pages(pages: List[str], top_n: int = 3) -> List[Tuple[int, str]]:
    scored = []
    for i, page in enumerate(pages):
        scored.append((i, score_page(page), page))
    scored.sort(key=lambda x: x[1], reverse=True)
    best = [(i, page) for i, s, page in scored if s > 0][:top_n]
    return best


def parse_number(text: str) -> Optional[int]:
    text = text.replace(",", "").replace("£", "").strip()
    if text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        if re.fullmatch(r"\d+(?:\.\d+)?", inner):
            return -int(round(float(inner)))
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return None
    try:
        return int(round(float(text)))
    except Exception:
        return None


def extract_line_value(line: str) -> Optional[int]:
    numbers = re.findall(r"\(\d[\d,]*\)|-?\d[\d,]*", line)
    if not numbers:
        return None
    for token in reversed(numbers):
        val = parse_number(token)
        if val is not None:
            return val
    return None


def find_value(text: str, keywords: List[str]) -> Optional[int]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for line in lines:
        line_l = line.lower()
        if any(k in line_l for k in keywords):
            val = extract_line_value(line)
            if val is not None:
                return val
    return None


def extract_financials_from_text(text: str, company_number: Optional[str], method: str, debug_pages: List[Tuple[int, str]]) -> Dict[str, Any]:
    fixed_assets = find_value(text, FIELD_KEYWORDS["fixed_assets"])
    current_assets = find_value(text, FIELD_KEYWORDS["current_assets"])
    cash = find_value(text, FIELD_KEYWORDS["cash"])
    debtors = find_value(text, FIELD_KEYWORDS["debtors"])
    current_liabilities = find_value(text, FIELD_KEYWORDS["current_liabilities"])
    net_assets = find_value(text, FIELD_KEYWORDS["net_assets"])

    working_capital = None
    if current_assets is not None and current_liabilities is not None:
        working_capital = current_assets - current_liabilities

    debug_sample_parts = []
    debug_page_numbers = []
    for idx, page_text in debug_pages:
        debug_page_numbers.append(idx + 1)
        snippet = page_text[:800]
        debug_sample_parts.append(f"[PAGE {idx + 1}]\n{snippet}")

    return {
        "company_number": company_number,
        "method": method,
        "debug_page_numbers": debug_page_numbers,
        "debug_text_sample": "\n\n".join(debug_sample_parts)[:DEBUG_SNIPPET_LENGTH],
        "fixed_assets": fixed_assets,
        "current_assets": current_assets,
        "cash": cash,
        "debtors": debtors,
        "current_liabilities": current_liabilities,
        "working_capital": working_capital,
        "net_assets": net_assets,
    }


def extract_financials_from_pdf_bytes(pdf_bytes: bytes, company_number: str = None) -> Dict[str, Any]:
    pdf_pages = extract_text_pdfplumber_pages(pdf_bytes)
    pdf_full_text = "\n\n".join([p for p in pdf_pages if p])

    if len(pdf_full_text) >= MIN_TEXT_LENGTH:
        best_pages = pick_best_pages(pdf_pages)
        if best_pages:
            text = "\n\n".join(page for _, page in best_pages)
        else:
            text = pdf_full_text
        return extract_financials_from_text(
            text=text,
            company_number=company_number,
            method="pdfplumber",
            debug_pages=best_pages if best_pages else [(0, pdf_full_text[:800])],
        )

    ocr_pages = extract_text_ocr_pages(pdf_bytes)
    ocr_full_text = "\n\n".join([p for p in ocr_pages if p])

    if len(ocr_full_text) < MIN_TEXT_LENGTH:
        raise ValueError("No readable text found (PDF + OCR failed)")

    best_pages = pick_best_pages(ocr_pages)
    if best_pages:
        text = "\n\n".join(page for _, page in best_pages)
    else:
        text = ocr_full_text

    return extract_financials_from_text(
        text=text,
        company_number=company_number,
        method="ocr",
        debug_pages=best_pages if best_pages else [(0, ocr_full_text[:800])],
    )
