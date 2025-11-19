# pipeline/compare_sections.py
from __future__ import annotations
import argparse, json, os, re, csv, difflib
from collections import Counter
from typing import Dict, List, Tuple
from rapidfuzz.distance import Levenshtein

def load_jsonl(path: str) -> Dict[str, dict]:
    m = {}
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            o = json.loads(ln)
            sid = o["sec_id"].replace(" ", "")
            m[sid] = o
    return m

WS = re.compile(r"\s+")
PUNC = re.compile(r"[^\w§\.]+", re.UNICODE)

def norm_text(s: str) -> str:
    s = s or ""
    s = s.replace("\r", "")
    s = WS.sub(" ", s).strip()
    return s

def tokenize(s: str) -> List[str]:
    s = s.lower()
    s = PUNC.sub(" ", s)
    return [t for t in s.split() if t]

def jaccard(a_tokens: List[str], b_tokens: List[str]) -> float:
    if not a_tokens and not b_tokens:
        return 1.0
    A, B = set(a_tokens), set(b_tokens)
    if not A and not B:
        return 1.0
    if not A or not B:
        return 0.0
    return len(A & B) / len(A | B)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml", required=True, help="artifacts/.../sections.jsonl")
    ap.add_argument("--pdf", required=True, help="artifacts/.../sections_pdf.jsonl")
    ap.add_argument("--out-csv", required=True, help="path to write parity_report.csv")
    ap.add_argument("--out-diffdir", required=True, help="directory to write per-sec diffs")
    ap.add_argument("--topn", type=int, default=15, help="how many worst items to dump diffs for")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    os.makedirs(args.out_diffdir, exist_ok=True)

    xml_map = load_jsonl(args.xml)
    pdf_map = load_jsonl(args.pdf)

    commons = sorted(set(xml_map.keys()) & set(pdf_map.keys()))
    missing_in_pdf = sorted(set(xml_map.keys()) - set(pdf_map.keys()))
    only_in_pdf = sorted(set(pdf_map.keys()) - set(xml_map.keys()))

    rows = []
    scored = []

    for sid in commons:
        x = xml_map[sid]; p = pdf_map[sid]
        xt = norm_text(x.get("text","")); pt = norm_text(p.get("text",""))
        x_tokens = tokenize(xt); p_tokens = tokenize(pt)
        # metrics
        len_ratio = (len(pt) / max(1, len(xt)))
        jac = jaccard(x_tokens, p_tokens)
        # Levenshtein normalized similarity (1 - normalized distance)
        lv_dist = Levenshtein.normalized_distance(xt, pt)  # 0..1
        rf_sim = (1.0 - lv_dist) * 100.0                   # 0..100

        row = dict(
            sec_id=sid,
            xml_len=len(xt),
            pdf_len=len(pt),
            len_ratio=round(len_ratio, 3),
            xml_tokens=len(x_tokens),
            pdf_tokens=len(p_tokens),
            jaccard_unigram=round(jac, 3),
            rf_similarity=round(rf_sim, 1),
            xml_heading=x.get("heading",""),
            pdf_heading=p.get("heading",""),
        )
        rows.append(row)
        # score for ranking "worst": low rf_similarity and extreme len_ratio
        scored.append((sid, rf_sim, abs(1.0 - len_ratio)))

    # write CSV
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["sec_id"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # choose top-N worst to diff (lowest similarity, then biggest len_ratio dev)
    scored.sort(key=lambda t: (t[1], -t[2]))  # ascending sim, then descending |1-len_ratio|
    worst = [sid for sid, _, _ in scored[:args.topn]]

    for sid in worst:
        x = xml_map[sid]; p = pdf_map[sid]
        xt = norm_text(x.get("text","")).splitlines()
        pt = norm_text(p.get("text","")).splitlines()
        diff = difflib.unified_diff(
            xt, pt,
            fromfile=f"XML {sid}", tofile=f"PDF {sid}",
            lineterm=""
        )
        with open(os.path.join(args.out_diffdir, f"{sid.replace('§','S')}.diff.txt"),
                  "w", encoding="utf-8") as df:
            for line in diff:
                df.write(line + "\n")

       # Also write small summary file
    summary_path = os.path.join(os.path.dirname(args.out_csv), "parity_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as sf:
        sf.write(f"Common sections: {len(commons)}\n")
        sf.write(f"Missing in PDF:  {len(missing_in_pdf)}\n")
        sf.write(f"Only in PDF:     {len(only_in_pdf)}\n")
        if missing_in_pdf:
            sf.write("\nMissing in PDF (up to 25):\n")
            for s in missing_in_pdf[:25]:
                sf.write(f"  {s}\n")

    # >>> add these prints *inside* main(), here:
    print(f"[compare] Common sections: {len(commons)}")
    print(f"[compare] Missing in PDF:  {len(missing_in_pdf)}")
    if missing_in_pdf:
        print("[compare] First few missing:", ", ".join(list(missing_in_pdf)[:5]))
    print(f"[compare] Only in PDF:     {len(only_in_pdf)}")
    print(f"[compare] CSV:  {args.out_csv}")
    print(f"[compare] Diffs: {args.out_diffdir}")

if __name__ == "__main__":
    main()
