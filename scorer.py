from typing import Any, Dict, List, Optional


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def round_if(v: Optional[float], digits: int = 2) -> Optional[float]:
    if v is None:
        return None
    return round(v, digits)


def grade_from_score(score: int) -> str:
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    if score >= 35:
        return "D"
    return "E"


def risk_label_from_score(score: int) -> str:
    if score >= 80:
        return "Low risk"
    if score >= 65:
        return "Moderate risk"
    if score >= 50:
        return "Caution"
    if score >= 35:
        return "High risk"
    return "Very high risk"


def build_flags(financials: Dict[str, Any]) -> List[str]:
    flags: List[str] = []

    current_assets = financials.get("current_assets")
    current_liabilities = financials.get("current_liabilities")
    non_current_liabilities = financials.get("non_current_liabilities")
    total_liabilities = financials.get("total_liabilities")
    net_assets = financials.get("net_assets")
    working_capital = financials.get("working_capital")
    cash = financials.get("cash")
    debtors = financials.get("debtors")

    current_ratio = safe_div(current_assets, current_liabilities)
    liabilities_to_net_assets = safe_div(total_liabilities, net_assets)
    cash_to_current_liabilities = safe_div(cash, current_liabilities)

    if working_capital is not None and working_capital < 0:
        flags.append("Negative working capital")

    if net_assets is not None and net_assets < 0:
        flags.append("Negative net assets")

    if current_ratio is not None and current_ratio < 1.0:
        flags.append("Current ratio below 1.0")

    if current_ratio is not None and current_ratio < 0.8:
        flags.append("Acute short-term liquidity pressure")

    if liabilities_to_net_assets is not None and liabilities_to_net_assets > 2.5:
        flags.append("Liabilities high relative to net assets")

    if cash_to_current_liabilities is not None and cash_to_current_liabilities < 0.15:
        flags.append("Low cash cover against current liabilities")

    if debtors is not None and current_assets not in (None, 0):
        debtor_share = debtors / current_assets
        if debtor_share > 0.6:
            flags.append("Current assets heavily reliant on receivables")

    if non_current_liabilities is not None and total_liabilities not in (None, 0):
        long_term_share = non_current_liabilities / total_liabilities
        if long_term_share > 0.6:
            flags.append("Liability profile weighted to longer-term obligations")

    return flags


def detect_data_quality_warnings(
    financials: Dict[str, Any],
    profile: Optional[Dict[str, Any]] = None,
    charges: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    warnings: List[str] = []
    adjustments: List[str] = []

    method = financials.get("method")
    extraction_confidence = financials.get("extraction_confidence")
    current_assets = financials.get("current_assets")
    total_assets = financials.get("total_assets")
    non_current_assets = financials.get("non_current_assets")
    fixed_assets = financials.get("fixed_assets")
    matched_lines = financials.get("matched_lines") or {}

    accounts_type = None
    if profile:
        accounts_type = (((profile.get("accounts") or {}).get("last_accounts") or {}).get("type"))

    has_outstanding_charges = False
    if charges:
        items = charges.get("items", []) or []
        has_outstanding_charges = any((item.get("status") or "").lower() == "outstanding" for item in items)

    current_assets_inferred_from_total_assets = (
        current_assets is not None
        and total_assets is not None
        and current_assets == total_assets
        and matched_lines.get("current_assets") is None
        and matched_lines.get("total_assets") is not None
    )

    if method == "ocr":
        warnings.append("Financial extraction derived from OCR rather than native PDF text")
        adjustments.append("OCR extraction reduces certainty of line matching")

    if extraction_confidence in {"low", "medium"}:
        warnings.append(f"Extraction confidence assessed as {extraction_confidence}")
        adjustments.append("Moderate or low extraction confidence warrants scoring caution")

    if accounts_type == "interim":
        warnings.append("Latest available accounts are interim rather than full annual accounts")
        adjustments.append("Interim accounts reduce comparability and confidence")

    if current_assets_inferred_from_total_assets:
        warnings.append("Current assets appear inferred from total assets rather than a dedicated current-assets total")
        adjustments.append("Liquidity measures may be overstated")

    if non_current_assets is None and fixed_assets is None:
        warnings.append("Non-current / fixed assets were not cleanly extracted")
        adjustments.append("Balance-sheet completeness is reduced")

    if has_outstanding_charges:
        warnings.append("Outstanding charges are present")
        adjustments.append("Outstanding borrowing security increases structural credit risk")

    return {
        "warnings": warnings,
        "adjustments": adjustments,
        "current_assets_inferred_from_total_assets": current_assets_inferred_from_total_assets,
        "has_outstanding_charges": has_outstanding_charges,
        "accounts_type": accounts_type,
        "method": method,
        "extraction_confidence": extraction_confidence,
    }


def score_financials(
    financials: Dict[str, Any],
    profile: Optional[Dict[str, Any]] = None,
    charges: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    current_assets = financials.get("current_assets")
    current_liabilities = financials.get("current_liabilities")
    non_current_liabilities = financials.get("non_current_liabilities")
    total_liabilities = financials.get("total_liabilities")
    net_assets = financials.get("net_assets")
    working_capital = financials.get("working_capital")
    cash = financials.get("cash")
    debtors = financials.get("debtors")
    non_current_assets = financials.get("non_current_assets")
    fixed_assets = financials.get("fixed_assets")
    total_assets = financials.get("total_assets")

    quality = detect_data_quality_warnings(financials, profile=profile, charges=charges)

    current_assets_for_ratio = current_assets
    # If current assets are inferred from total assets, do not trust them for a strong liquidity ratio.
    if quality["current_assets_inferred_from_total_assets"]:
        current_assets_for_ratio = None

    current_ratio = safe_div(current_assets_for_ratio, current_liabilities)
    cash_ratio = safe_div(cash, current_liabilities)
    liabilities_to_net_assets = safe_div(total_liabilities, net_assets)
    debtors_to_current_assets = safe_div(debtors, current_assets_for_ratio)
    equity_ratio = safe_div(
        net_assets,
        (net_assets + total_liabilities) if net_assets is not None and total_liabilities is not None else None,
    )

    score = 50
    notes: List[str] = []

    # Liquidity
    if current_ratio is not None:
        if current_ratio >= 2.0:
            score += 15
            notes.append("Strong current ratio")
        elif current_ratio >= 1.5:
            score += 10
            notes.append("Good current ratio")
        elif current_ratio >= 1.2:
            score += 5
            notes.append("Adequate current ratio")
        elif current_ratio >= 1.0:
            score += 0
            notes.append("Tight but acceptable current ratio")
        elif current_ratio >= 0.8:
            score -= 10
            notes.append("Weak current ratio")
        else:
            score -= 20
            notes.append("Poor current ratio")
    else:
        notes.append("Current ratio not relied upon due to extraction limitations")

    # Working capital
    if working_capital is not None and not quality["current_assets_inferred_from_total_assets"]:
        if working_capital > 0:
            score += 10
            notes.append("Positive working capital")
        else:
            score -= 15
            notes.append("Negative working capital")
    elif working_capital is not None and quality["current_assets_inferred_from_total_assets"]:
        notes.append("Working capital treated cautiously because current assets are inferred")

    # Net assets
    if net_assets is not None:
        if net_assets > 0:
            score += 10
            notes.append("Positive net assets")
        else:
            score -= 25
            notes.append("Negative net assets")

    # Leverage
    if liabilities_to_net_assets is not None:
        if liabilities_to_net_assets < 1.0:
            score += 10
            notes.append("Low liabilities relative to net assets")
        elif liabilities_to_net_assets < 2.0:
            score += 4
            notes.append("Manageable liabilities relative to net assets")
        elif liabilities_to_net_assets < 3.0:
            score -= 4
            notes.append("Elevated liabilities relative to net assets")
        else:
            score -= 12
            notes.append("High liabilities relative to net assets")

    # Cash support
    if cash_ratio is not None:
        if cash_ratio >= 0.5:
            score += 8
            notes.append("Strong cash cover")
        elif cash_ratio >= 0.25:
            score += 3
            notes.append("Moderate cash cover")
        elif cash_ratio < 0.1:
            score -= 8
            notes.append("Weak cash cover")

    # Asset quality proxy
    if debtors_to_current_assets is not None:
        if debtors_to_current_assets > 0.7:
            score -= 6
            notes.append("Current assets concentrated in receivables")
        elif debtors_to_current_assets < 0.35:
            score += 2
            notes.append("Current assets not overly debtor-heavy")

    populated = sum(
        v is not None for v in [
            current_assets,
            total_assets,
            current_liabilities,
            non_current_liabilities,
            total_liabilities,
            net_assets,
            working_capital,
            cash,
            debtors,
            non_current_assets,
            fixed_assets,
        ]
    )

    if populated < 4:
        score -= 10
        notes.append("Limited financial data available")
    elif populated < 7:
        score -= 5
        notes.append("Only partial balance-sheet coverage available")

    # Data quality penalties
    if quality["method"] == "ocr":
        score -= 4
        notes.append("OCR-derived extraction introduces additional uncertainty")

    if quality["extraction_confidence"] == "medium":
        score -= 6
        notes.append("Moderate extraction confidence")
    elif quality["extraction_confidence"] == "low":
        score -= 12
        notes.append("Low extraction confidence")

    if quality["current_assets_inferred_from_total_assets"]:
        score -= 12
        notes.append("Liquidity metrics softened because current assets are inferred from total assets")

    if non_current_assets is None and fixed_assets is None:
        score -= 6
        notes.append("Non-current asset extraction incomplete")

    if quality["has_outstanding_charges"]:
        score -= 6
        notes.append("Outstanding charges increase structural risk")

    # Score caps
    score_cap = 100
    cap_reasons: List[str] = []

    if quality["accounts_type"] == "interim":
        score_cap = min(score_cap, 82)
        cap_reasons.append("Interim accounts cap the maximum score")

    if quality["method"] == "ocr":
        score_cap = min(score_cap, 80)
        cap_reasons.append("OCR-derived accounts cap the maximum score")

    if quality["current_assets_inferred_from_total_assets"]:
        score_cap = min(score_cap, 72)
        cap_reasons.append("Inferred current assets cap the maximum score")

    if non_current_assets is None and fixed_assets is None:
        score_cap = min(score_cap, 72)
        cap_reasons.append("Missing non-current / fixed asset extraction caps the maximum score")

    if quality["has_outstanding_charges"]:
        score_cap = min(score_cap, 75)
        cap_reasons.append("Outstanding charges cap the maximum score")

    score = max(0, min(100, int(round(score))))
    if score > score_cap:
        score = score_cap

    grade = grade_from_score(score)
    risk_label = risk_label_from_score(score)
    flags = build_flags(financials)

    summary_parts: List[str] = []
    if current_ratio is not None:
        summary_parts.append(f"current ratio {round_if(current_ratio)}")
    else:
        summary_parts.append("current ratio not relied upon")
    if working_capital is not None:
        summary_parts.append(f"working capital {working_capital:,}")
    if net_assets is not None:
        summary_parts.append(f"net assets {net_assets:,}")
    if total_liabilities is not None:
        summary_parts.append(f"total liabilities {total_liabilities:,}")

    summary = "; ".join(summary_parts)

    return {
        "score": score,
        "grade": grade,
        "risk_label": risk_label,
        "ratios": {
            "current_ratio": round_if(current_ratio),
            "cash_ratio": round_if(cash_ratio),
            "liabilities_to_net_assets": round_if(liabilities_to_net_assets),
            "debtors_to_current_assets": round_if(debtors_to_current_assets),
            "equity_ratio": round_if(equity_ratio),
        },
        "flags": flags,
        "warnings": quality["warnings"],
        "data_quality_adjustments": quality["adjustments"],
        "score_notes": notes,
        "score_cap": score_cap,
        "score_cap_reasons": cap_reasons,
        "summary": summary,
    }
