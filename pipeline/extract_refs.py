# pipeline/extract_refs.py
import argparse, json, os, re
from typing import List, Dict

# --- Optional: use legal-citation-parser if available for robustness ---
try:
    from legal_citation_parser import parse_citation
except Exception:
    parse_citation = None

# --- Base patterns (rich) ---
PAT_SEC      = re.compile(r"§{1,2}\s*\d{1,3}\.\d{1,3}[A-Za-z0-9\-]*(?:\([a-z0-9]+\))?", re.I)
PAT_RANGE    = re.compile(r"§{2}\s*(\d{1,3}\.\d{1,3})\s*(?:–|-|to|through)\s*(\d{1,3}\.\d{1,3})", re.I)
PAT_INWORD   = re.compile(r"\bsections?\s+(\d{1,3}\.\d{1,3}[A-Za-z0-9\-]*)", re.I)
PAT_CFR_SEC  = re.compile(r"\b(\d+)\s*CFR\s*§{1,2}\s*(\d{1,3}\.\d{1,3}[A-Za-z0-9\-]*)", re.I)
PAT_CFR_PART = re.compile(r"\b(\d+)\s*CFR\s+part\s+(\d{1,3})\b", re.I)

# --- NEW: span-capture patterns (for UI/audit) ---
SPAN_PATTERNS = [
    ("range",   PAT_RANGE),
    ("local",   PAT_SEC),
    ("inword",  PAT_INWORD),
    ("cfr_sec", PAT_CFR_SEC),
    ("cfr_part",PAT_CFR_PART),
]

def read_jsonl(path: str) -> List[Dict]:
    out=[]
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            out.append(json.loads(ln))
    return out

def expand_range(a: str, b: str) -> List[str]:
    try:
        a1,a2 = a.split("."); b1,b2 = b.split(".")
        if a1 == b1:
            a2=int(a2); b2=int(b2)
            if 0 <= b2-a2 <= 500:
                return [f"{a1}.{k}" for k in range(a2, b2+1)]
    except Exception:
        pass
    return [a,b]

def normalize_local(sec_text: str) -> List[str]:
    """Regex-only normalizer to §X.Y ids (plus cross-doc tokens)."""
    hits=set()

    # §§ range
    for m in PAT_RANGE.finditer(sec_text):
        for t in expand_range(m.group(1), m.group(2)):
            hits.add(f"§{t}")

    # § single
    for m in PAT_SEC.finditer(sec_text):
        t = m.group(0)
        t = "§" + re.sub(r"^[§\s]+", "", t).replace(" ", "")
        hits.add(t)

    # "sections 115.10"
    for m in PAT_INWORD.finditer(sec_text):
        hits.add(f"§{m.group(1)}")

    # Cross-doc: "20 CFR § 408.210"
    for m in PAT_CFR_SEC.finditer(sec_text):
        title, num = m.group(1), m.group(2)
        hits.add(f"{title}CFR §{num}")

    # Cross-doc: "6 CFR part 115"
    for m in PAT_CFR_PART.finditer(sec_text):
        hits.add(f"{m.group(1)}CFR part {m.group(2)}")

    return sorted(hits)

def normalize_with_lcp(sec_text: str) -> List[str]:
    """Try legal-citation-parser for extra coverage; fall back to regex."""
    if not parse_citation:
        return normalize_local(sec_text)
    out=set(normalize_local(sec_text))
    try:
        for token in re.split(r"(?<=[\.\)])\s+", sec_text):
            try:
                parsed = parse_citation(token)
            except Exception:
                continue
            if not parsed:
                continue
            title = (parsed.get("title") or "").strip()
            section = (parsed.get("section") or "").strip()
            part = (parsed.get("part") or "").strip()
            if section:
                out.add(f"§{section}")
                if title:
                    out.add(f"{title}CFR §{section}")
            elif part:
                if title:
                    out.add(f"{title}CFR part {part}")
    except Exception:
        pass
    return sorted(out)

# span capture helper (records where matches occur)
def find_spans(text: str) -> List[Dict]:
    spans=[]; seen=set()
    t = text or ""
    for kind, rx in SPAN_PATTERNS:
        for m in rx.finditer(t):
            start, end = m.start(), m.end()
            raw = m.group(0)
            if (start,end,raw) in seen:  # de-dup
                continue
            seen.add((start,end,raw))
            if kind == "range":
                raw_id = f"§§{m.group(1)}–{m.group(2)}"
            elif kind == "cfr_sec":
                raw_id = f"{m.group(1)}CFR §{m.group(2)}"
            elif kind == "cfr_part":
                raw_id = f"{m.group(1)}CFR part {m.group(2)}"
            elif kind == "inword":
                raw_id = f"§{m.group(1)}"
            else:
                raw_id = "§" + re.sub(r"^[§\s]+", "", raw).replace(" ", "")
            span_kind = "crossdoc" if kind in ("cfr_sec","cfr_part") else ("range" if kind=="range" else "local")
            spans.append({"ref_text": raw, "raw_id": raw_id, "start": int(start), "end": int(end), "kind": span_kind})
    spans.sort(key=lambda s: (s["start"], s["end"]))
    return spans

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml-sections", required=True,
                    help="path to sections.jsonl (or sections_pdf.jsonl)")
    ap.add_argument("--out", required=True, help="output refs jsonl")
    ap.add_argument("--mode", choices=["simple","rich"], default="rich")
    a = ap.parse_args()

    os.makedirs(os.path.dirname(a.out), exist_ok=True)

    rows = read_jsonl(a.xml_sections)
    with open(a.out, "w", encoding="utf-8") as w:
        for rec in rows:
            text = rec.get("text","") or ""
            # NEW: spans
            spans = find_spans(text)
            # existing normalization
            refs = normalize_with_lcp(text) if a.mode=="rich" else normalize_local(text)
            w.write(json.dumps({
                "doc_id": rec.get("doc_id"),
                "sec_id": rec.get("sec_id"),
                "heading": rec.get("heading",""),
                "explicit_refs": refs,
                "explicit_ref_spans": spans   # NEW field
            }, ensure_ascii=False) + "\n")
    print(f"Wrote refs → {a.out} ({len(rows)} sections)")

if __name__ == "__main__":
    main()
