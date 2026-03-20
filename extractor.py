import io
import re
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber


MIN_TEXT_LENGTH = 100


FIELD_ALIASES = {
    "fixed_assets": [
        "fixed assets",
        "tangible assets",
        "intangible assets",
        "total fixed assets",
    ],
    "current_assets": [
        "current assets",
        "total current assets",
    ],
    "cash": [
        "cash at bank and in hand",
        "cash at bank",
        "cash in hand",
        "cash",
    ],
    "debtors": [
        "debtors",
        "trade debtors",
    ],
    "current_liabilities": [
        "creditors: amounts falling due within one year",
        "creditors amounts falling due within one year",
        "amounts falling due within one year",
        "within one year",
        "current liabilities",
    ],
    "long_term_liabilities": [
        "creditors: amounts falling due after more than one year",
        "creditors amounts falling due after more than one year",
        "amounts falling due after more than one year",
        "after more than one year",
        "long term liabilities",
        "long-term liabilities",
    ],
    "net_assets": [
        "net assets",
        "net liabilities",
        "capital and reserves",
        "shareholders' funds",
        "shareholders funds",
        "members' funds",
        "members funds",
    ],
    "working_capital": [
        "net current assets",
        "net current liabilities",
        "working capital",
    ],
}


def normalize_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_text_from_pdf(pdf_bytes: bytes) -> Tuple[str, Dict[str, Any]]:
    pages_text: List[str] = []
    page_count = 0

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_count = len(pdf.pages)
        for page in pdf.pages:
            txt = page.extract_text() or ""
            if txt.strip():
                pages_text.append(txt)

    text = normalize_text("\n\n".join(pages_text))
    meta = {
        "method": "pdfplumber",
        "page_count": page_count,
        "text_chars": len(text),
    }
    return text, meta


def split_lines(text: str) -> List[str]:
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line]


def parse_number(token: str) -> Optional[int]:
    token = token.strip()
    if not token:
        return None

    negative = False
    if token.startswith("(") and token.endswith(")"):
        negative = True
        token = token[1:-1]

    token = token.replace(",", "").replace("£", "").replace("$", "").strip()

    if token in {"-", "—", "–"}:
        return 0

    if not re.fullmatch(r"-?\d+(?:\.\d+)?", token):
        return None

    value = float(token)
    if negative:
        value = -value

    return int(round(value))


def extract_numeric_tokens(line: str) -> List[int]:
    raw_tokens = re.findall(r"\(\d[\d,]*\)|-?\d[\d,]*(?:\.\d+)?", line)
    values = [parse_number(tok) for tok in raw_tokens]
    values = [v for v in values if v is not None]

    if len(values) >= 3 and abs(values[0]) <= 99 and abs(values[1]) >= 100:
        values = values[1:]

    if len(values) > 3:
        values = values[-3:]

    return values


def extract_year_headers(text: str) -> List[int]:
    years = re.findall(r"\b(20\d{2}|19\d{2})\b", text)
    years_int: List[int] = []
    for y in years:
        yi = int(y)
        if yi not in years_int:
            years_int.append(yi)
    years_int.sort(reverse=True)
    return years_int[:3]


def find_best_line_for_aliases(lines: List[str], aliases: List[str]) -> Optional[str]:
    candidates: List[Tuple[int, str]] = []

    for line in lines:
        line_l = line.lower()
        for alias in aliases:
            alias_l = alias.lower()
            if alias_l in line_l:
                score = len(alias_l)
                if re.search(r"\d", line):
                    score += 100
                candidates.append((score, line))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def values_to_series(values: List[int]) -> Dict[str, Optional[int]]:
    if len(values) == 0:
        return {"latest": None, "previous": None, "previous_2": None}
    if len(values) == 1:
        return {"latest": values[0], "previous": None, "previous_2": None}
    if len(values) == 2:
        return {"latest": values[0], "previous": values[1], "previous_2": None}
    return {"latest": values[0], "previous": values[1], "previous_2": values[2]}


def line_to_field_payload(line: Optional[str]) -> Dict[str, Any]:
    if not line:
        return {
            "line": None,
            "values": {"latest": None, "previous": None, "previous_2": None},
        }

    values = extract_numeric_tokens(line)
    return {
        "line": line,
        "values": values_to_series(values),
    }


def derive_working_capital(current_assets: Dict[str, Optional[int]], current_liabilities: Dict[str, Optional[int]]) -> Dict[str, Optional[int]]:
    out = {"latest": None, "previous": None, "previous_2": None}
    for key in out.keys():
        ca = current_assets.get(key)
        cl = current_liabilities.get(key)
        if ca is not None and cl is not None:
            out[key] = ca - cl
    return out


def derive_net_assets(
    net_assets_field: Dict[str, Optional[int]],
    working_capital: Dict[str, Optional[int]],
    fixed_assets: Dict[str, Optional[int]],
    long_term_liabilities: Dict[str, Optional[int]],
) -> Dict[str, Optional[int]]:
    if any(v is not None for v in net_assets_field.values()):
        return net_assets_field

    out = {"latest": None, "previous": None, "previous_2": None}
    for key in out.keys():
        fa = fixed_assets.get(key)
        wc = working_capital.get(key)
        ltl = long_term_liabilities.get(key)
        if fa is not None and wc is not None:
            out[key] = fa + wc - (ltl or 0)
    return out


def parse_financial_fields(text: str) -> Dict[str, Any]:
    lines = split_lines(text)
    years = extract_year_headers(text)

    raw = {}
    for field_name, aliases in FIELD_ALIASES.items():
        line = find_best_line_for_aliases(lines, aliases)
        raw[field_name] = line_to_field_payload(line)

    fixed_assets = raw["fixed_assets"]["values"]
    current_assets = raw["current_assets"]["values"]
    cash = raw["cash"]["values"]
    debtors = raw["debtors"]["values"]
    current_liabilities = raw["current_liabilities"]["values"]
    long_term_liabilities = raw["long_term_liabilities"]["values"]
    working_capital = raw["working_capital"]["values"]
    net_assets = raw["net_assets"]["values"]

    if not any(v is not None for v in working_capital.values()):
        working_capital = derive_working_capital(current_assets, current_liabilities)

    net_assets = derive_net_assets(
        net_assets_field=net_assets,
        working_capital=working_capital,
        fixed_assets=fixed_assets,
        long_term_liabilities=long_term_liabilities,
    )

    return {
        "years_detected": years,
        "fields": {
            "fixed_assets": fixed_assets,
            "current_assets": current_assets,
            "cash": cash,
            "debtors": debtors,
            "current_liabilities": current_liabilities,
            "working_capital": working_capital,
            "long_term_liabilities": long_term_liabilities,
            "net_assets": net_assets,
        },
        "source_lines": raw,
    }


def score_extraction(fields: Dict[str, Any]) -> Dict[str, Any]:
    populated = 0
    total = 0
    for field_values in fields["fields"].values():
        total += 1
        if any(v is not None for v in field_values.values()):
            populated += 1

    if populated >= 5:
        confidence = "high"
    elif populated >= 3:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "populated_fields": populated,
        "total_fields": total,
        "confidence": confidence,
    }


def extract_financials_from_pdf_bytes(pdf_bytes: bytes, company_number: Optional[str] = None) -> Dict[str, Any]:
    text, extraction_meta = extract_text_from_pdf(pdf_bytes)

    if len(text) < MIN_TEXT_LENGTH:
        raise ValueError("Insufficient text extracted from PDF")

    parsed = parse_financial_fields(text)
    quality = score_extraction(parsed)

    return {
        "company_number": company_number,
        "extraction": extraction_meta,
        "quality": quality,
        "years_detected": parsed["years_detected"],
        "fixed_assets": parsed["fields"]["fixed_assets"],
        "current_assets": parsed["fields"]["current_assets"],
        "cash": parsed["fields"]["cash"],
        "debtors": parsed["fields"]["debtors"],
        "current_liabilities": parsed["fields"]["current_liabilities"],
        "working_capital": parsed["fields"]["working_capital"],
        "long_term_liabilities": parsed["fields"]["long_term_liabilities"],
        "net_assets": parsed["fields"]["net_assets"],
        "source_lines": parsed["source_lines"],
    }
