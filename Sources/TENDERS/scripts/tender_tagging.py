"""Shared tagging logic for Bihar flood tenders.

Both `flood_tenders_1.py` (which tags the raw monthly tenders) and
`transformer_4.py` (which turns geotagged tenders into monthly variable CSVs)
use these helpers, so the response-type taxonomy is defined in exactly one place.

Response-type categories follow the IDS-DRR data dictionary
(government response group):
    Relief and Mitigation, Restoration Measures, Preparedness Measures, Others
"""
import re

# Schemes looked for in the tender reference number / title / description.
SCHEME_KEYWORDS = {'ridf', 'sdrf', 'sopd', 'cidf', 'ltif', 'sdmf', 'ndrf'}

# Response-type keywords. Matched as whole words: short abbreviations like "IM"
# (Immediate Measures) and "GR" (Gratuitous Relief) are meaningless as substrings
# and would otherwise pull in every "improvement", "limestone" or "programme".
RELIEF_AND_MITIGATION_KEYWORDS = ['sdrf', 'im', 'i/m', 'gr', 'g/r', 'relief', 'immediate', 'emergency']
RESTORATION_KEYWORDS = ['improvement', 'repair', 'restoration', 'raising', 'strengthening', 'renovation']
PREPAREDNESS_KEYWORDS = ['shelter', 'responder kit', 'aapda mitra', 'protection', 'stockpile']


def _word_matchers(keywords):
    return [re.compile(r'\b{}\b'.format(re.escape(kw))) for kw in keywords]


_RELIEF_AND_MITIGATION_RE = _word_matchers(RELIEF_AND_MITIGATION_KEYWORDS)
_RESTORATION_RE = _word_matchers(RESTORATION_KEYWORDS)
_PREPAREDNESS_RE = _word_matchers(PREPAREDNESS_KEYWORDS)

RELIEF_AND_MITIGATION = 'Relief and Mitigation'
RESTORATION_MEASURES = 'Restoration Measures'
PREPAREDNESS_MEASURES = 'Preparedness Measures'
OTHERS = 'Others'

RESPONSE_TYPES = [RELIEF_AND_MITIGATION, RESTORATION_MEASURES, PREPAREDNESS_MEASURES, OTHERS]

# Data dictionary variable name for each response type.
RESPONSE_TYPE_VARIABLES = {
    RELIEF_AND_MITIGATION: 'relief_and_mitigation_sanction_value',
    RESTORATION_MEASURES: 'restoration_measures_tenders_awarded_value',
    PREPAREDNESS_MEASURES: 'preparedness_measures_tenders_awarded_value',
    OTHERS: 'others_tenders_awarded_value',
}


def tender_slug(row):
    """Reference no. + title + description of a tender, stripped of punctuation."""
    slug = "{} {} {}".format(row.get('Tender Ref No :', ''),
                             row.get('Tender Title :', ''),
                             row.get('Work Description', ''))
    return re.sub(r'[^a-zA-Z0-9 \n\.]', ' ', slug).lower()


def tag_scheme(row):
    """Return the scheme (SDRF, RIDF, ...) a tender was granted under, or ''."""
    slug_tokens = set(re.split(r'[-.,()_\s/]\s*', tender_slug(row)))
    matches = list(slug_tokens & SCHEME_KEYWORDS)
    return matches[0].upper() if matches else ''


def classify_response(row):
    """Bucket a tender into one of RESPONSE_TYPES."""
    slug = tender_slug(row)
    if any(matcher.search(slug) for matcher in _RELIEF_AND_MITIGATION_RE):
        return RELIEF_AND_MITIGATION
    elif any(matcher.search(slug) for matcher in _RESTORATION_RE):
        return RESTORATION_MEASURES
    elif any(matcher.search(slug) for matcher in _PREPAREDNESS_RE):
        return PREPAREDNESS_MEASURES
    return OTHERS
