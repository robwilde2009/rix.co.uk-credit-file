from typing import Any, Dict, List, Optional


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    v = safe_div(a, b)
    if v is None:
        return None
    return v * 100


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


def score_financials(financials: Dict[str, Any]) -> Dict[str, Any]:
    current_assets = financials.get("current_assets")
    current_liabilities = financials.get("current_liabilities")
    non_current_liabilities = financials.get("non_current_liabilities")
    total_liabilities = financials.get("total_liabilities")
    net_assets = financials.get("net_assets")
    working_capital = financials.get("working_capital")
    cash = financials.get("cash")
    debtors = financials.get("debtors")
    non_current_assets = financials.get("non_current_assets")

    current_ratio = safe_div(current_assets, current_liabilities)
    cash_ratio = safe_div(cash, current_liabilities)
    liabilities_to_net_assets = safe_div(total_liabilities, net_assets)
    debtors_to_current_assets = safe_div(debtors, current_assets)
    equity_ratio = safe_div(net_assets, (net_assets + total_liabilities) if net_assets is not None and total_liabilities is not None else None)

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

    # Working capital
    if working_capital is not None:
        if working_capital > 0:
            score += 10
            notes.append("Positive working capital")
        else:
            score -= 15
            notes.append("Negative working capital")

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

    # Data completeness
    populated = sum(
        v is not None for v in [
            current_assets,
            current_liabilities,
            non_current_liabilities,
            total_liabilities,
            net_assets,
            working_capital,
            cash,
            debtors,
            non_current_assets,
        ]
    )

    if populated < 4:
        score -= 10
        notes.append("Limited financial data available")

    score = max(0, min(100, int(round(score))))
    grade = grade_from_score(score)
    risk_label = risk_label_from_score(score)
    flags = build_flags(financials)

    summary_parts: List[str] = []
    if current_ratio is not None:
        summary_parts.append(f"current ratio {round_if(current_ratio)}")
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
        "score_notes": notes,
        "summary": summary,
    }
