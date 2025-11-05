# pipeline/parse_pdf_align_select.py
import argparse, json, os, time, importlib, pandas as pd
from docustitch.parsers.pdf_fallback import (
    lines_to_sections, load_xml_map, score_alignment, dedupe_by_sec_id
)

CANDIDATES = {
    "fitz": "docustitch.parsers.pdf_backends.fitz_backend",
    "pdfminer": "docustitch.parsers.pdf_backends.pdfminer_backend",
    "pdfplumber": "docustitch.parsers.pdf_backends.pdfplumber_backend",
    "pdftotext": None
}

def filter_to_xml_truth(pdf_secs, xml_map):
    """Keep only sections whose sec_id exists in XML (truth set), normalized."""
    allowed = {k.replace(" ","") for k in xml_map.keys()}
    return [s for s in pdf_secs if s["sec_id"].replace(" ","") in allowed]

def fill_reserved_missing(pdf_secs, xml_map):
    """
    Ensure sections that are '[Reserved]' (or effectively reserved) in XML
    are present in the PDF list even if the PDF had no header line for them.
    We detect 'reserved-like' via:
      - heading startswith '[reserved'
      - OR very short body (<= 60 chars) containing 'reserved'
      - OR normalized body equals 'reserved'/'[reserved]' variants
    """
    import re
    have = {s["sec_id"].replace(" ","") for s in pdf_secs}
    added = 0

    def is_reserved_like(x):
        heading = (x.get("heading","") or "").strip().lower()
        text = (x.get("text","") or "")
        norm = re.sub(r"[\s\[\]\.\-–—]+", "", text.lower())
        if heading.startswith("[reserved"):
            return True
        if len(text.strip()) <= 60 and ("reserved" in text.lower()):
            return True
        if norm in {"reserved", "reserved;", "reserved:"}:
            return True
        return False

    for sid_norm, x in xml_map.items():  # keys are normalized in load_xml_map
        if sid_norm not in have and is_reserved_like(x):
            pdf_secs.append({
                "doc_id": x["doc_id"],
                "sec_id": sid_norm,
                "label": sid_norm,
                "heading": x.get("heading","") or "[Reserved]",
                "text": "",
                "hierarchy_path": x.get("hierarchy_path", []),
                "tokens": 1
            })
            added += 1
    return pdf_secs, added


def try_backend(name, module_path, pdf_path, doc_id, xml_map):
    row = dict(backend=name, status="unavailable", runtime_s=None,
               pdf_sections=None, xml_sections=len(xml_map))
    if not module_path:
        return row, None
    try:
        mod = importlib.import_module(module_path)
    except Exception:
        row["status"] = "import_error"
        return row, None
    try:
        t0 = time.time()
        lines = mod.extract_lines(pdf_path)
        secs = lines_to_sections(lines, doc_id)

        # normalize to XML truth + dedupe + fill reserved
        secs = filter_to_xml_truth(secs, xml_map)
        secs = dedupe_by_sec_id(secs)
        secs, added_reserved = fill_reserved_missing(secs, xml_map)

        dur = time.time() - t0
        row.update(status="ok", runtime_s=round(dur, 3), pdf_sections=len(secs))
        row.update(score_alignment(secs, xml_map))
        # helpful: expose how many reserved we added
        row["reserved_added"] = added_reserved
        return row, secs
    except Exception:
        row["status"] = "error"
        return row, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--xml-sections", required=True)
    ap.add_argument("--doc-id", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--report", required=True)
    a = ap.parse_args()

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    xml_map = load_xml_map(a.xml_sections)

    rows=[]; results={}
    for name, mod in CANDIDATES.items():
        row, secs = try_backend(name, mod, a.pdf, a.doc_id, xml_map)
        rows.append(row)
        if secs is not None:
            results[name] = secs

    df = pd.DataFrame(rows)
    df["f1"] = df["f1"].fillna(0)
    df["runtime_s"] = df["runtime_s"].fillna(1e9)
    df["reserved_added"] = df["reserved_added"].fillna(0)

    # Prefer pdfminer on ties for stability
    df["tie_bias"] = df["backend"].apply(lambda b: 0 if b=="pdfminer" else 1)
    winners = df[df["status"]=="ok"].sort_values(by=["f1", "tie_bias", "runtime_s"], ascending=[False, True, True])
    best = winners.iloc[0]["backend"] if not winners.empty else None

    header = not os.path.exists(a.report)
    df.to_csv(a.report, mode="a", header=header, index=False)
    print(df.to_string(index=False))

    if not best:
        raise SystemExit("No PDF backend succeeded.")

    secs = results[best]
    with open(a.out, "w", encoding="utf-8") as f:
        for s in secs:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")
    print(f"`Recommended backend: {best}`")
    print(f"Wrote {len(secs)} sections → {a.out}")

if __name__ == "__main__":
    main()
