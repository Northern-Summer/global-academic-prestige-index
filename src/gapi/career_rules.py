"""Conservative, versioned career classification. No fuzzy or LLM decisions."""
from __future__ import annotations

import re
import unicodedata

RULES_VERSION = "career-rules/1"
QUALIFICATION_RULE_VERSION = 'completed-qualification/1'

# Shared exclusion owner for every source. Lexical degree recognition must never
# override an explicit unfinished, student or honorary qualification.
EXCLUSIONS = (r'\b(student|candidate|honorary|honoris causa|pursuing|abd)\b'
              r'|在读|在讀|肄业|肄業|名誉|名譽|未完成|en cours|en curso|cursando|em andamento|studierend'
              r'|not completed|not finished|unfinished|incomplete|withdrawn|non.degree'
              r'|no finalizado|abandonado|interrompido|incompleto|sin terminar|non terminé|non termine')


def excluded_qualification(text):
    return bool(re.search(EXCLUSIONS, normalized(text)))


def valid_orcid(value):
    """ISO 7064 MOD 11-2 check digit; formatting alone cannot bind identity."""
    if not isinstance(value,str) or not re.fullmatch(r'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]',value):
        return False
    digits=value.replace('-','')
    total=0
    for digit in digits[:-1]:total=(total+int(digit))*2
    check=(12-total%11)%11
    return digits[-1]==('X' if check==10 else str(check))


def normalized(text):
    return " ".join(unicodedata.normalize("NFKC", text or "").casefold().split())


def classify(kind, role, *, completed_only=False):
    text = normalized(role)
    # Preserve the frozen acquisition/v1 interpretation. Consumers confirming
    # completed qualifications explicitly apply the stricter derived contract.
    if completed_only and kind == 'education' and excluded_qualification(text):
        return kind, None
    if re.search(r"post[ -]?doc|博士后|博士後", text):
        return "training", "postdoc"
    if kind == "education":
        if re.search(r"\b(candidate|student|honorary|honoris causa|pursuing|abd)\b|在读|在讀|肄业|肄業|名誉|名譽", text):
            return kind, None
        for pattern, stage in [
            (r"\b(ph\.?\s?d\.?|d\.?phil\.?|doctorate|doctor of philosophy)\b|博士", "doctorate"),
            (r"\b(master|m\.?sc\.?|m\.?a\.?|m\.?eng\.?)\b|硕士|碩士", "master"),
            (r"\b(bachelor|b\.?sc\.?|b\.?a\.?|b\.?eng\.?)\b|学士|學士", "bachelor"),
        ]:
            if re.search(pattern, text):
                return kind, stage
        return kind, None
    if re.search(r"\b(professor|lecturer|researcher|research scientist|research fellow)\b|教授|研究员|研究員", text):
        return "employment", "academic"
    return kind, None


def year(value):
    if value and re.fullmatch(r"\d{4}", str(value)) and 1000 <= int(value) <= 2100:
        return int(value)
    return None


def institution_id(external_id, authority):
    authority = normalized(authority)
    value = (external_id or "").rstrip("/").rsplit("/", 1)[-1]
    if authority == "ror" and re.fullmatch(r"0[a-hj-km-np-z0-9]{6}[0-9]{2}", value):
        return "ror:" + value
    if authority == "grid" and re.fullmatch(r"grid\.\d+\.[a-z0-9]+", value):
        return "grid:" + value
    if authority == "ringgold" and value.isdigit():
        return "ringgold:" + value
    return None
