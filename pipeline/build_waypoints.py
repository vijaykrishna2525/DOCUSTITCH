# pipeline/build_waypoints.py
from __future__ import annotations
import argparse, json, os, re, math
from typing import Dict, List, Tuple
import pandas as pd
import yaml

# ---------------------------
# Helpers
# ---------------------------

WS = re.compile(r"\s+")
RX_LOCAL_SEC = re.compile(r"^§\d{1,3}\.\d{1,4}")

def _norm01(s: pd.Series) -> pd.Series:
    if s is None or len(s) == 0:
        return pd.Series(dtype=float)
    s = s.fillna(0).astype(float)
    lo, hi = float(s.min()), float(s.max())
    if math.isclose(hi, lo):
        return pd.Series([0.0]*len(s), index=s.index)
    return (s - lo) / (hi - lo)

def _read_jsonl(path: str) -> List[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            rows.append(json.loads(ln))
    return rows

def _text_ok(x: str) -> str:
    return (x or "").replace("\r", "").strip()

def _count_lex_hits(text: str, heading: str, terms: List[str]) -> int:
    if not terms:
        return 0
    hay = f"{heading}\n{text}".lower()
    c = 0
    for t in terms:
        t = (t or "").strip().lower()
        if not t: 
            continue
        # require full token phrase; tolerate spaces/hyphens
        pat = re.escape(t).replace(r"\ ", r"[\s\-]+")
        if re.search(rf"(?<!\w){pat}(?!\w)", hay, flags=re.I):
            c += 1
    return c

def _heading_bonus(heading: str, patterns: Dict[str,str]) -> float:
    if not heading:
        return 0.0
    h = heading.strip()
    bonus = 0.0
    # definitions / applicability get 1.0; others small bumps
    if patterns.get("definitions_heading") and re.search(patterns["definitions_heading"], h):
        bonus = max(bonus, 1.0)
    if patterns.get("applicability_heading") and re.search(patterns["applicability_heading"], h):
        bonus = max(bonus, 1.0)
    # small extra bumps for common anchor-ish words
    if re.search(r"(?i)\b(purpose|scope|authority|compliance|reporting)\b", h):
        bonus = max(bonus, 0.5)
    return bonus

def _safe_list_len(v) -> int:
    if isinstance(v, (list, tuple)):
        return len(v)
    # strings from parquet could be JSON-like; try to parse
    if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
        try:
            import ast
            vv = ast.literal_eval(v)
            return len(vv) if isinstance(vv, (list, tuple)) else 0
        except Exception:
            return 0
    return 0

# ---------------------------
# Core build
# ---------------------------

def build(args):
    os.makedirs(os.path.dirname(args.out_parquet), exist_ok=True)

    # sections
    secs = _read_jsonl(args.sections)
    df_secs = pd.DataFrame([{
        "sec_id": s.get("sec_id"),
        "heading": s.get("heading",""),
        "text": _text_ok(s.get("text",""))
    } for s in secs]).dropna(subset=["sec_id"]).reset_index(drop=True)

    # lexicon
    with open(args.lexicon, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)
    global_terms = list(y.get("global_terms") or [])
    doc_terms = list((y.get("doc_specific") or {}).get(args.doc_id, []) or [])
    patterns = y.get("patterns") or {}

    # terms parquet (from extract_terms.py)
    term_density = pd.Series(0.0, index=df_secs.index)
    if args.terms and os.path.exists(args.terms):
        TP = pd.read_parquet(args.terms)
        TP["sec_id_norm"] = TP["sec_id"].astype(str).str.replace(" ", "")
        M = df_secs["sec_id"].str.replace(" ","")
        # density = prefer a numeric column if present, else length of top_terms
        if "tfidf_sum" in TP.columns:
            s = TP.set_index("sec_id_norm")["tfidf_sum"]
            term_density = M.map(s).fillna(0.0)
        elif "top_terms" in TP.columns:
            s = TP.set_index("sec_id_norm")["top_terms"].map(_safe_list_len)
            term_density = M.map(s).fillna(0.0)
    term_density = _norm01(pd.Series(term_density))

    # lexicon hits (global + doc_specific)
    lexicon_all = global_terms + doc_terms
    lex_hits = []
    for i, row in df_secs.iterrows():
        lex_hits.append(_count_lex_hits(row["text"], row["heading"], lexicon_all))
    lex_hits = _norm01(pd.Series(lex_hits, index=df_secs.index))

    # citation centrality (in-degree) from explicit edges
    cent = pd.Series(0.0, index=df_secs.index)
    if args.edges and os.path.exists(args.edges):
        E = pd.read_parquet(args.edges)
        indeg = E.groupby("dst_sec_id").size().astype(float) if len(E) else pd.Series(dtype=float)
        cent = df_secs["sec_id"].map(indeg).fillna(0.0)
    cent = _norm01(cent)

    # cross-ref density using xml_refs.jsonl (local §… tokens in the section)
    xref_den = pd.Series(0.0, index=df_secs.index)
    if args.xml_refs and os.path.exists(args.xml_refs):
        R = _read_jsonl(args.xml_refs)
        ref_count = {}
        for r in R:
            sid = (r.get("sec_id") or "").replace(" ","")
            toks = r.get("explicit_refs") or []
            # keep only local §X.Y* style
            n_local = sum(1 for t in toks if isinstance(t,str) and RX_LOCAL_SEC.match(t.strip().replace("Â§","§")))
            ref_count[sid] = n_local
        M = df_secs["sec_id"].str.replace(" ","")
        xref_den = M.map(pd.Series(ref_count)).fillna(0.0)
    xref_den = _norm01(xref_den)

    # heading bonus
    head_bonus = pd.Series([_heading_bonus(h, patterns) for h in df_secs["heading"]], index=df_secs.index)
    head_bonus = _norm01(head_bonus)

    # weights (CLI-overridable)
    w_term = args.w_term
    w_lex  = args.w_lex
    w_cent = args.w_cent
    w_head = args.w_head
    w_xref = args.w_xref

    score = (
        w_term*term_density +
        w_lex*lex_hits +
        w_cent*cent +
        w_head*head_bonus +
        w_xref*xref_den
    )

    out = pd.DataFrame({
        "sec_id": df_secs["sec_id"],
        "score": score.round(6),
        "window": args.window,
        "reason": "blend(term_density,lexicon,centrality,heading,xref)"
    }).sort_values("score", ascending=False, kind="mergesort")

    # keep top-K
    out_top = out.head(args.k).reset_index(drop=True)
    out_top.to_parquet(args.out_parquet, index=False)
    print(f"Wrote {len(out_top)} waypoints → {args.out_parquet}\n")
    print("Top anchors:")
    for _, r in out_top.iterrows():
        sec = r["sec_id"]
        head = df_secs.loc[df_secs["sec_id"]==sec, "heading"].values
        head = head[0] if len(head) else ""
        print(f"  {sec:8s} score={r['score']:.3f}  head={head[:80]}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", required=True, help="artifacts/.../sections.jsonl")
    ap.add_argument("--terms", required=False, default=None, help="artifacts/.../terms.parquet")
    ap.add_argument("--xml-refs", required=False, default=None, help="artifacts/.../xml_refs.jsonl")
    ap.add_argument("--edges", required=False, default=None, help="artifacts/.../edges_explicit.parquet")
    ap.add_argument("--lexicon", required=True, help="artifacts/lexicon.yaml")
    ap.add_argument("--doc-id", required=True, help="cfr_6_37 | cfr_6_115 | cfr_20_408")
    ap.add_argument("--out-parquet", required=True)
    ap.add_argument("--k", type=int, default=12)
    ap.add_argument("--window", type=int, default=1)

    # weights (defaults = 0.45/0.25/0.20/0.05/0.05)
    ap.add_argument("--w-term", type=float, default=0.45)
    ap.add_argument("--w-lex",  type=float, default=0.25)
    ap.add_argument("--w-cent", type=float, default=0.20)
    ap.add_argument("--w-head", type=float, default=0.05)
    ap.add_argument("--w-xref", type=float, default=0.05)

    args = ap.parse_args()
    build(args)

if __name__ == "__main__":
    main()
