import io
import re
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
from pdf2image import convert_from_bytes
import pytesseract


MIN_TEXT_LENGTH = 100
OCR_FIRST_PAGE = 4
OCR_LAST_PAGE = 6
OCR_DPI = 120
DEBUG_SNIPPET_LENGTH = 2200

SECTION_HEADERS = [
    "non-current assets",
    "non current assets",
    "current assets",
    "current liabilities",
    "non-current liabilities",
    "non current liabilities",
    "net assets",
    "capital and reserves",
    "shareholders' funds",
    "members' funds",
    "total equity",
]

PAGE_POSITIVE_SIGNALS = [
    "statement of financial position",
    "balance sheet",
    "current assets",
    "current liabilities",
    "non-current assets",
    "non-current liabilities",
    "net assets",
    "total liabilities",
]

PAGE_NEGATIVE_SIGNALS = [
    "notes to the financial statements",
    "statement of directors",
    "directors’ responsibilities",
    "directors' responsibilities",
    "statement of changes in equity",
    "cash flow statement",
]

LINE_PATTERNS = {
    "non_current_assets_total": [
        "total non-current assets",
        "total non current assets",
    ],
    "current_assets_total": [
        "total current assets",
    ],
    "total_assets": [
        "total assets",
    ],
    "cash": [
        "cash and cash equivalents",
        "cash at bank and in hand",
        "cash at bank",
        "cash in hand",
        "cash",
    ],
    "debtors": [
        "trade and other receivables",
        "trade debtors",
        "debtors",
        "receivables",
    ],
    "current_liabilities_total": [
        "total current liabilities",
    ],
    "current_liabilities_due_within_one_year": [
        "creditors: amounts due within one year",
        "creditors: amounts falling due within one year",
        "amounts due within one year",
        "amounts falling due within one year",
    ],
    "non_current_liabilities_total": [
        "total non-current liabilities",
        "total non current liabilities",
    ],
    "non_current_liabilities_after_one_year": [
        "creditors: amounts due after one year",
        "creditors: amounts falling due after more than one year",
        "amounts due after one year",
        "amounts falling due after more than one year",
    ],
    "total_liabilities": [
        "total liabilities",
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
        "total assets less current liabilities",
    ],
}


def normalize_spaces(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def extract_text_pdfplumber_pages(pdf_bytes: bytes) -> List[str]:
    pages: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            pages.append(normalize_spaces(txt))
    return pages


def extract_text_ocr_pages(pdf_bytes: bytes) -> List[str]:
    try:
        images = convert_from_bytes(
            pdf_bytes,
            first_page=OCR_FIRST_PAGE,
            last_page=OCR_LAST_PAGE,
            dpi=OCR_DPI,
            grayscale=True,
        )
    except Exception:
        return []

    pages: List[str] = []
    for img in images:
        try:
            txt = pytesseract.image_to_string(img, config="--oem 1 --psm 6")
            pages.append(normalize_spaces(txt))
        except Exception:
            pages.append("")
    return pages


def score_page(text: str) -> int:
    t = text.lower()
    score = 0

    for signal in PAGE_POSITIVE_SIGNALS:
        if signal in t:
            score += 4

    for signal in PAGE_NEGATIVE_SIGNALS:
        if signal in t:
            score -= 6

    if re.search(r"\b20\d{2}\b", t):
        score += 1

    numeric_count = len(re.findall(r"\(?\d[\d,]*\)?", text))
    if numeric_count >= 5:
        score += 2
    if numeric_count >= 12:
        score += 2

    return score


def pick_best_pages(pages: List[str], top_n: int = 2) -> List[Tuple[int, str]]:
    scored: List[Tuple[int, int, str]] = []
    for i, page in enumerate(pages):
        scored.append((i, score_page(page), page))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [(i, page) for i, s, page in scored if s > 0][:top_n]


def parse_number(token: str) -> Optional[int]:
    token = token.replace(",", "").replace("£", "").replace("$", "").strip()

    if token.startswith("(") and token.endswith(")"):
        inner = token[1:-1].strip()
        if re.fullmatch(r"\d+(?:\.\d+)?", inner):
            return -int(round(float(inner)))

    if not re.fullmatch(r"-?\d+(?:\.\d+)?", token):
        return None

    try:
        return int(round(float(token)))
    except Exception:
        return None


def extract_candidate_numbers(line: str) -> List[int]:
    raw = re.findall(r"\(\d[\d,]*\)|-?\d[\d,]*", line)
    vals: List[int] = []
    for tok in raw:
        val = parse_number(tok)
        if val is not None:
            vals.append(val)
    return vals


def extract_best_value_from_line(line: str) -> Optional[int]:
    values = extract_candidate_numbers(line)
    if not values:
        return None

    large_values = [v for v in values if abs(v) >= 1000]
    if large_values:
        return large_values[-1]

    medium_values = [v for v in values if abs(v) >= 100]
    if medium_values:
        return medium_values[-1]

    return values[-1]


def split_lines(text: str) -> List[str]:
    return [l.strip() for l in text.splitlines() if l.strip()]


def find_section_range(lines: List[str], header_patterns: List[str], stop_patterns: List[str]) -> Tuple[int, int]:
    start_idx = -1
    end_idx = len(lines)

    for i, line in enumerate(lines):
        ll = line.lower()
        if start_idx == -1 and any(p in ll for p in header_patterns):
            start_idx = i
            continue

        if start_idx != -1 and any(p in ll for p in stop_patterns):
            end_idx = i
            break

    if start_idx == -1:
        return (0, len(lines))

    return (start_idx, end_idx)


def get_current_assets_section(lines: List[str]) -> List[str]:
    start, end = find_section_range(
        lines,
        header_patterns=["current assets"],
        stop_patterns=["current liabilities"],
    )
    section = lines[start:end]
    out = []
    for line in section:
        ll = line.lower()
        if "non-current" in ll or "non current" in ll:
            continue
        out.append(line)
    return out


def get_non_current_assets_section(lines: List[str]) -> List[str]:
    start, end = find_section_range(
        lines,
        header_patterns=["non-current assets", "non current assets"],
        stop_patterns=["current assets"],
    )
    return lines[start:end]


def get_current_liabilities_section(lines: List[str]) -> List[str]:
    start, end = find_section_range(
        lines,
        header_patterns=["current liabilities"],
        stop_patterns=["non-current liabilities", "non current liabilities", "net assets", "capital and reserves"],
    )
    return lines[start:end]


def get_non_current_liabilities_section(lines: List[str]) -> List[str]:
    start, end = find_section_range(
        lines,
        header_patterns=["non-current liabilities", "non current liabilities"],
        stop_patterns=["net assets", "capital and reserves", "shareholders' funds", "members' funds"],
    )
    return lines[start:end]


def get_whole_balance_sheet_scope(lines: List[str]) -> List[str]:
    return lines


def find_best_line(lines: List[str], patterns: List[str], prefer_total: bool = False, exclude_patterns: Optional[List[str]] = None) -> Optional[str]:
    exclude_patterns = exclude_patterns or []
    candidates: List[Tuple[int, str]] = []

    for line in lines:
        line_l = line.lower()

        if exclude_patterns and any(p in line_l for p in exclude_patterns):
            continue

        if not any(p in line_l for p in patterns):
            continue

        values = extract_candidate_numbers(line)

        score = 10
        if "total " in line_l:
            score += 8
        if prefer_total and "total " in line_l:
            score += 8
        if values:
            score += 5
        if any(abs(v) >= 1000 for v in values):
            score += 5
        if not values:
            score -= 10

        candidates.append((score, line))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def find_value_from_patterns(
    lines: List[str],
    patterns: List[str],
    prefer_total: bool = False,
    exclude_patterns: Optional[List[str]] = None,
) -> Tuple[Optional[int], Optional[str]]:
    line = find_best_line(
        lines,
        patterns=patterns,
        prefer_total=prefer_total,
        exclude_patterns=exclude_patterns,
    )
    if not line:
        return None, None
    return extract_best_value_from_line(line), line


def build_result(
    text: str,
    company_number: Optional[str],
    method: str,
    debug_pages: List[Tuple[int, str]],
    page_number_offset: int = 0,
) -> Dict[str, Any]:
    lines = split_lines(text)

    non_current_assets_section = get_non_current_assets_section(lines)
    current_assets_section = get_current_assets_section(lines)
    current_liabilities_section = get_current_liabilities_section(lines)
    non_current_liabilities_section = get_non_current_liabilities_section(lines)
    whole_scope = get_whole_balance_sheet_scope(lines)

    non_current_assets, non_current_assets_line = find_value_from_patterns(
        non_current_assets_section,
        LINE_PATTERNS["non_current_assets_total"],
        prefer_total=True,
    )

    current_assets, current_assets_line = find_value_from_patterns(
        current_assets_section,
        LINE_PATTERNS["current_assets_total"],
        prefer_total=True,
    )

    total_assets, total_assets_line = find_value_from_patterns(
        current_assets_section,
        LINE_PATTERNS["total_assets"],
        prefer_total=True,
        exclude_patterns=["non-current", "non current"],
    )

    debtors, debtors_line = find_value_from_patterns(
        current_assets_section,
        LINE_PATTERNS["debtors"],
    )

    cash, cash_line = find_value_from_patterns(
        current_assets_section + current_liabilities_section + whole_scope,
        LINE_PATTERNS["cash"],
    )

    current_liabilities_total, current_liabilities_total_line = find_value_from_patterns(
        current_liabilities_section,
        LINE_PATTERNS["current_liabilities_total"],
        prefer_total=True,
    )

    current_liabilities_due_within_one_year, current_liabilities_due_within_one_year_line = find_value_from_patterns(
        current_liabilities_section,
        LINE_PATTERNS["current_liabilities_due_within_one_year"],
    )

    non_current_liabilities_total, non_current_liabilities_total_line = find_value_from_patterns(
        non_current_liabilities_section,
        LINE_PATTERNS["non_current_liabilities_total"],
        prefer_total=True,
    )

    non_current_liabilities_after_one_year, non_current_liabilities_after_one_year_line = find_value_from_patterns(
        non_current_liabilities_section,
        LINE_PATTERNS["non_current_liabilities_after_one_year"],
    )

    total_liabilities, total_liabilities_line = find_value_from_patterns(
        whole_scope,
        LINE_PATTERNS["total_liabilities"],
        prefer_total=True,
    )

    net_assets, net_assets_line = find_value_from_patterns(
        whole_scope,
        LINE_PATTERNS["net_assets"],
    )

    # Use total current liabilities if present, else the within-one-year line.
    current_liabilities = (
        current_liabilities_total
        if current_liabilities_total is not None
        else current_liabilities_due_within_one_year
    )

    # Use total non-current liabilities if present, else after-one-year line.
    non_current_liabilities = (
        non_current_liabilities_total
        if non_current_liabilities_total is not None
        else non_current_liabilities_after_one_year
    )

    # current_assets should come from a true current-assets total.
    # If that is missing, use total assets only as an explicit fallback.
    final_current_assets = (
        current_assets
        if current_assets is not None
        else total_assets
    )

    working_capital = None
    if final_current_assets is not None and current_liabilities is not None:
        working_capital = final_current_assets - current_liabilities

    fixed_assets = non_current_assets

    debug_page_numbers: List[int] = []
    debug_parts: List[str] = []

    for idx, page_text in debug_pages:
        page_num = idx + 1 + page_number_offset
        debug_page_numbers.append(page_num)
        debug_parts.append(f"[PAGE {page_num}]\n{page_text[:700]}")

    populated_fields = sum(
        v is not None for v in [
            fixed_assets,
            non_current_assets,
            final_current_assets,
            total_assets,
            cash,
            debtors,
            current_liabilities,
            non_current_liabilities,
            total_liabilities,
            net_assets,
        ]
    )

    if populated_fields >= 8:
        extraction_confidence = "high"
    elif populated_fields >= 5:
        extraction_confidence = "medium"
    else:
        extraction_confidence = "low"

    return {
        "company_number": company_number,
        "method": method,
        "extraction_confidence": extraction_confidence,
        "debug_page_numbers": debug_page_numbers,
        "debug_text_sample": "\n\n".join(debug_parts)[:DEBUG_SNIPPET_LENGTH],
        "fixed_assets": fixed_assets,
        "non_current_assets": non_current_assets,
        "current_assets": final_current_assets,
        "total_assets": total_assets,
        "cash": cash,
        "debtors": debtors,
        "current_liabilities": current_liabilities,
        "current_liabilities_due_within_one_year": current_liabilities_due_within_one_year,
        "current_liabilities_total": current_liabilities_total,
        "non_current_liabilities": non_current_liabilities,
        "non_current_liabilities_after_one_year": non_current_liabilities_after_one_year,
        "non_current_liabilities_total": non_current_liabilities_total,
        "total_liabilities": total_liabilities,
        "working_capital": working_capital,
        "net_assets": net_assets,
        "matched_lines": {
            "non_current_assets": non_current_assets_line,
            "current_assets": current_assets_line,
            "total_assets": total_assets_line,
            "cash": cash_line,
            "debtors": debtors_line,
            "current_liabilities_due_within_one_year": current_liabilities_due_within_one_year_line,
            "current_liabilities_total": current_liabilities_total_line,
            "non_current_liabilities_after_one_year": non_current_liabilities_after_one_year_line,
            "non_current_liabilities_total": non_current_liabilities_total_line,
            "total_liabilities": total_liabilities_line,
            "net_assets": net_assets_line,
        },
    }


def extract_financials_from_pdf_bytes(pdf_bytes: bytes, company_number: str = None) -> Dict[str, Any]:
    pdf_pages = extract_text_pdfplumber_pages(pdf_bytes)
    pdf_full_text = "\n\n".join([p for p in pdf_pages if p])

    if len(pdf_full_text) >= MIN_TEXT_LENGTH:
        best_pages = pick_best_pages(pdf_pages)
        text = "\n\n".join(page for _, page in best_pages) if best_pages else pdf_full_text
        result = build_result(
            text=text,
            company_number=company_number,
            method="pdfplumber",
            debug_pages=best_pages if best_pages else [(0, pdf_full_text[:700])],
            page_number_offset=0,
        )

        if any(
            result[field] is not None
            for field in [
                "non_current_assets",
                "current_assets",
                "total_assets",
                "cash",
                "debtors",
                "current_liabilities",
                "non_current_liabilities",
                "total_liabilities",
                "net_assets",
            ]
        ):
            return result

    ocr_pages = extract_text_ocr_pages(pdf_bytes)
    ocr_full_text = "\n\n".join([p for p in ocr_pages if p])

    if len(ocr_full_text) < MIN_TEXT_LENGTH:
        raise ValueError("No readable text found (PDF + OCR failed)")

    best_pages = pick_best_pages(ocr_pages)
    text = "\n\n".join(page for _, page in best_pages) if best_pages else ocr_full_text

    return build_result(
        text=text,
        company_number=company_number,
        method="ocr",
        debug_pages=best_pages if best_pages else [(0, ocr_full_text[:700])],
        page_number_offset=OCR_FIRST_PAGE - 1,
    )
