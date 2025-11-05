# docustitch/parsers/pdf_fallback.py
from __future__ import annotations
import re, json
from typing import List, Dict

# --------- Patterns ---------
SEC_HDR = re.compile(
    r"^\s*§\s*(\d{1,3}\.\d{1,3}[A-Za-z0-9\-]*(?:\([a-z0-9]+\))*)\s*(.*)$",
    re.IGNORECASE
)
NOISE_START = re.compile(r"^(Authority:|Source:|Editorial Note:|HISTORY:)\b", re.IGNORECASE)
PAGE_JUNK = re.compile(r"^(VerDate|[0-9]{4}\s*CFR\s*.*|[0-9]+\s*\|\s*Page|\[\s*\d+\s*\])$", re.IGNORECASE)

# --------- Small helpers ---------
def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _clean_line(l: str) -> str:
    l = (l or "").rstrip("\u200b")
    l = re.sub(r"\s+", " ", l).strip()
    return l

def _dehyphenate(lines: List[str]) -> List[str]:
    out=[]
    for i, l in enumerate(lines):
        if out and out[-1].endswith("-") and l and l[:1].isalnum():
            out[-1] = out[-1][:-1] + l  # join hyphen break across lines
        else:
            out.append(l)
    return out

def _merge_wrapped_headers(lines: List[str]) -> List[str]:
    """
    If a header line starts with '§ 37.3' but has no trailing text, and the next
    line looks like a continuation (not another header or junk), join them.
    """
    out=[]; i=0
    while i < len(lines):
        line = lines[i].strip()
        m = SEC_HDR.match(line)
        if m:
            label, rest = m.group(1).strip(), (m.group(2) or "").strip()
            if not rest and i+1 < len(lines):
                nxt = lines[i+1].strip()
                if not SEC_HDR.match(nxt) and not PAGE_JUNK.match(nxt) and not NOISE_START.match(nxt):
                    out.append(f"{label} {nxt}")
                    i += 2
                    continue
        out.append(lines[i])
        i += 1
    return out

# --------- Main: lines -> sections ---------
def lines_to_sections(lines: List[str], doc_id: str) -> List[Dict]:
    # 1) basic cleanup
    lines = [_clean_line(x) for x in lines if _clean_line(x)]
    # 2) drop page headers/footers/junk
    lines = [l for l in lines if not PAGE_JUNK.match(l)]
    # 3) fix hyphenation & wrapped headers
    lines = _dehyphenate(lines)
    lines = _merge_wrapped_headers(lines)

    sections=[]; cur_label=None; cur_heading=""; buf=[]
    def flush():
        nonlocal cur_label, cur_heading, buf
        if cur_label:
            body = normalize(" ".join(buf))
            sections.append({
                "doc_id": doc_id,
                "sec_id": "§" + cur_label.split("§", 1)[-1].replace(" ", ""),
                "label": cur_label,
                "heading": cur_heading,
                "text": body,
                "hierarchy_path": [],
                "tokens": max(1, len(body.split()))
            })
        cur_label, cur_heading, buf = None, "", []

    for line in lines:
        if NOISE_START.match(line):
            # skip lines that begin Authority:/Source:/etc.
            continue
        m = SEC_HDR.match(line)
        if m:
            flush()
            cur_label  = m.group(1).strip()
            cur_heading = normalize(m.group(2) or "")
        else:
            buf.append(line)
    flush()
    return sections

def dedupe_by_sec_id(pdf_secs):
    """Keep one record per sec_id; choose the one with the longest body."""
    best = {}
    for s in pdf_secs:
        sid = s["sec_id"].replace(" ", "")
        if sid not in best or len(s["text"]) > len(best[sid]["text"]):
            best[sid] = s
    return list(best.values())


# --------- XML truth set utilities ---------
def load_xml_map(path: str) -> Dict[str, Dict]:
    mapping={}
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            o = json.loads(ln)
            # normalize key to avoid space issues
            mapping[o["sec_id"].replace(" ","")] = o
    return mapping

def score_alignment(pdf_secs, xml_map):
    xml_ids = set(xml_map.keys())
    pdf_ids = set(s["sec_id"] for s in pdf_secs)
    overlap = len(xml_ids & pdf_ids)
    only_in_xml = len(xml_ids - pdf_ids)
    only_in_pdf = len(pdf_ids - xml_ids)
    precision = (overlap/len(pdf_ids)) if pdf_ids else 0.0
    recall    = (overlap/len(xml_ids)) if xml_ids else 0.0
    f1 = (2*precision*recall/(precision+recall)) if (precision+recall) else 0.0

    ratios=[]
    pdf_by_id = {s["sec_id"]: s for s in pdf_secs}
    for sid in (xml_ids & pdf_ids):
        a = max(1, len(xml_map[sid]["text"]))
        b = max(1, len(pdf_by_id[sid]["text"]))
        ratios.append(b/a)
    ratios.sort()
    med = ratios[len(ratios)//2] if ratios else 0.0
    q1  = ratios[len(ratios)//4] if ratios else 0.0
    q3  = ratios[(len(ratios)*3)//4] if ratios else 0.0

    return dict(
        overlap=overlap, only_in_xml=only_in_xml, only_in_pdf=only_in_pdf,
        precision=precision, recall=recall, f1=f1,
        len_ratio_median=med, len_ratio_iqr=(q3-q1)
    )
