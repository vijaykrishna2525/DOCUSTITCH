# docustitch/parsers/xml_parser.py
from __future__ import annotations
from typing import Dict, List, Optional
import re
from bs4 import BeautifulSoup

SPACE_RX = re.compile(r"\s+")
# captures 115.152, 115.152a, 115.152-1, 115.152(b) (we'll normalize later)
LABEL_RX = re.compile(r"(\d{1,3}\.\d{1,3}[A-Za-z0-9\-]*(?:\([a-z0-9]+\))?)")

def _clean(s: str) -> str:
    return SPACE_RX.sub(" ", (s or "")).strip()

def _normalize_minimal(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\r", "")
    s = re.sub(r"(\w)-\n(\w)", r"\1\2", s)  # de-hyphenate across line breaks
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n", s)
    return s.strip()

def _text_of(node) -> str:
    return _normalize_minimal(node.get_text(" ", strip=True)) if node else ""

def _count_tokens(text: str) -> int:
    return max(1, len((text or "").split()))

def _normalize_single_id(raw_id: str) -> str:
    """
    Normalize a single section id: drop anything after a hyphen/en-dash.
    E.g., '115.152-115.153' -> '115.152'
    """
    if not raw_id:
        return raw_id
    return re.split(r"\s*[-–]\s*", raw_id)[0]

def _expand_reserved_range(sectno_text: str, heading_text: str) -> List[str]:
    """
    If 'sectno_text' starts with '§§' and matches a range like '115.152–115.153'
    AND heading is '[Reserved...]', return list of individual ids ['115.152','115.153'].
    Otherwise return [].
    """
    if not sectno_text.strip().startswith("§§"):
        return []
    if not heading_text.strip().lower().startswith("[reserved"):
        return []
    m = re.search(r"(\d{1,3})\.(\d{1,3})\s*[–-]\s*(\d{1,3})\.(\d{1,3})", sectno_text)
    if not m:
        return []
    a1, a2, b1, b2 = m.groups()
    if a1 != b1:
        return []
    start, end = int(a2), int(b2)
    if end < start or end - start > 500:
        return []
    return [f"{a1}.{k}" for k in range(start, end + 1)]

def parse_xml_text(xml_text: str, doc_id: str) -> List[Dict]:
    """
    BeautifulSoup(XML):
      - Build subpart map (including ranges in '§§ ...–...' cases).
      - Extract SECTION/DIV* TYPE=section.
      - Expand '[Reserved]' ranges into individual sections.
      - Normalize single SECTNO ids by trimming any hyphen/dash tail.
    """
    soup = BeautifulSoup(xml_text, "xml")

    # --- Build subpart map: section-id (like '37.3') -> subpart title
    subpart_map: Dict[str, str] = {}
    subpart_nodes = soup.find_all(
        ["SUBPART", "DIV3", "DIV4", "DIV5", "DIV6", "DIV7", "DIV8"],
        attrs={"TYPE": re.compile(r"(?i)subpart")}
    )
    for sp in subpart_nodes:
        subj = sp.find(["SUBJECT", "HEAD"])
        sp_title = _text_of(subj) if subj else "Subpart"
        for sec in sp.find_all(
            ["SECTION", "DIV3", "DIV4", "DIV5", "DIV6", "DIV7", "DIV8"],
            attrs={"TYPE": re.compile(r"(?i)section")}
        ):
            sectno = sec.find(re.compile("(?i)SECTNO"))
            sectno_text = _text_of(sectno) if sectno else ""
            # handle range "[Reserved]" in subpart map so both ids map to this subpart
            expanded = _expand_reserved_range(sectno_text, _text_of(sp.find(["SUBJECT","HEAD"])) or "")
            if expanded:
                for sid in expanded:
                    subpart_map[sid] = sp_title
                continue
            if sectno:
                m = LABEL_RX.search(_text_of(sectno))
                if m:
                    subpart_map[_normalize_single_id(m.group(1))] = sp_title

    # --- Find all SECTION nodes (robust to varying DIV levels)
    section_nodes = soup.find_all(
        ["SECTION", "DIV3", "DIV4", "DIV5", "DIV6", "DIV7", "DIV8"],
        attrs={"TYPE": re.compile(r"(?i)section")}
    )
    if not section_nodes:
        section_nodes = soup.find_all("SECTION")

    out: List[Dict] = []
    seen = set()

    for sec in section_nodes:
        sectno = sec.find(re.compile("(?i)SECTNO"))
        sectno_text = _text_of(sectno) if sectno else ""

        # Heading: SUBJECT or HEAD
        subject = sec.find(re.compile("(?i)SUBJECT")) or sec.find("HEAD")
        heading = _text_of(subject)

        # Handle "§§ ...–..." [Reserved] by expanding to individual records
        expanded_ids = _expand_reserved_range(sectno_text, heading)
        if expanded_ids:
            for sid in expanded_ids:
                sec_id = f"§{sid}"
                if sec_id in seen:
                    continue
                rec = {
                    "doc_id": doc_id,
                    "sec_id": sec_id,
                    "label": sec_id,
                    "heading": "[Reserved]",
                    "text": "",
                    "hierarchy_path": [subpart_map.get(sid, "")] if subpart_map.get(sid) else [],
                    "tokens": 1,
                }
                seen.add(sec_id)
                out.append(rec)
            # skip the rest of this node (already emitted)
            continue

        # Otherwise parse single id from SECTNO/heading and normalize
        sect_id: Optional[str] = None
        if sectno:
            m = LABEL_RX.search(sectno_text)
            if m:
                sect_id = _normalize_single_id(m.group(1))

        if not sect_id:
            # Try to detect from heading if SECTNO absent
            m2 = LABEL_RX.search(heading)
            if m2:
                sect_id = _normalize_single_id(m2.group(1))

        # Body text: all <P> elements if present, otherwise section text
        ps = sec.find_all("P")
        para_texts = [_text_of(p) for p in ps] if ps else [_text_of(sec)]
        full_text = _normalize_minimal(" ".join(pt for pt in para_texts if pt))

        # Subpart path
        subpart = subpart_map.get(sect_id or "", "")
        hierarchy_path = [subpart] if subpart else []

        # Canonical sec_id
        sec_id = "§" + (sect_id or "UNKNOWN")
        rec = {
            "doc_id": doc_id,
            "sec_id": sec_id.replace(" ", ""),
            "label": ("§ " + (sect_id or "UNKNOWN")),
            "heading": heading,
            "text": full_text,
            "hierarchy_path": hierarchy_path,
            "tokens": _count_tokens(full_text),
        }
        if rec["sec_id"] in seen:
            continue
        seen.add(rec["sec_id"])
        out.append(rec)

    print(f"[xml_parser(bs4)] sections={len(out)}")
    return out
