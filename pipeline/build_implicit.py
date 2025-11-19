# pipeline/build_implicit.py
from __future__ import annotations
import argparse, json, os, re, sys
from typing import List, Dict, Tuple
import numpy as np
import pandas as pd

try:
    from sentence_transformers import SentenceTransformer
except Exception as e:
    print("ERROR: sentence-transformers is required. pip install sentence-transformers", file=sys.stderr)
    raise

SEC_RX = re.compile(r"^§?\s*(\d{1,3})\.(\d{1,4})")

def _clean(s: str) -> str:
    s = (s or "").replace("Â§", "§")
    return re.sub(r"\s+", " ", s).strip()

def load_sections(path: str) -> pd.DataFrame:
    rows: List[Dict] = []
    with open(path, encoding="utf-8") as f:
        for ln in f:
            o = json.loads(ln)
            rows.append(dict(
                sec_id=_clean((o.get("sec_id") or "")).replace(" ", ""),
                heading=_clean(o.get("heading", "") or ""),
                text=_clean(o.get("text", "") or ""),
            ))
    df = pd.DataFrame(rows)
    df["idx"] = np.arange(len(df))
    return df

def load_gists(path: str) -> pd.DataFrame:
    rows: List[Dict] = []
    with open(path, encoding="utf-8") as f:
        for ln in f:
            o = json.loads(ln)
            rows.append(dict(
                anchor=_clean((o.get("anchor_sec_id") or "")).replace(" ", ""),
                gist=_clean(o.get("gist_text", "") or "")
            ))
    return pd.DataFrame(rows)

def safe_concat_heading_text(heading: str, text: str, max_chars: int = 8000) -> str:
    s = (heading + "\n" + text).strip()
    return s[:max_chars]

def dot_topk(query_vec: np.ndarray, cand_mat: np.ndarray, cand_ix: List[int], k: int) -> List[Tuple[int, float]]:
    """Assumes query_vec and cand_mat rows are L2-normalized → cosine == dot."""
    sims = cand_mat @ query_vec  # (m,)
    order = np.argsort(-sims)[:k]
    return [(cand_ix[j], float(sims[j])) for j in order]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", required=True)
    ap.add_argument("--gists", required=True)
    ap.add_argument("--out-parquet", required=True)
    ap.add_argument("--doc-id", required=True, help="e.g., cfr_6_37")

    # retrieval knobs
    ap.add_argument("--k", type=int, default=5, help="top-k neighbors within window")
    ap.add_argument("--min-sim", type=float, default=0.35)
    ap.add_argument("--window", type=int, default=1, help="±window around anchor index")
    ap.add_argument("--use-gist", action="store_true", help="encode gist as the query instead of section text")
    ap.add_argument("--bidirectional", action="store_true", help="also write reverse edge")
    ap.add_argument("--global-k", type=int, default=0, help="optional extra global top-k (outside window); 0 disables")

    # model/runtime
    ap.add_argument("--model", default="all-MiniLM-L6-v2")
    ap.add_argument("--batch-size", type=int, default=64)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_parquet), exist_ok=True)

    # Load inputs
    df_sec = load_sections(args.sections)
    df_g = load_gists(args.gists)

    # Index map and section corpus
    idx_of: Dict[str, int] = {r.sec_id: int(r.idx) for r in df_sec.itertuples()}
    sec_texts = [safe_concat_heading_text(r.heading, r.text) for r in df_sec.itertuples()]

    model = SentenceTransformer(args.model)
    # Normalize → cosine = dot
    sec_vecs = np.asarray(model.encode(sec_texts, normalize_embeddings=True, batch_size=args.batch_size), dtype=np.float32)

    # Build queries (gist if requested; else section embedding)
    queries: Dict[str, np.ndarray] = {}
    if args.use_gist:
        enc_inputs, order = [], []
        for g in df_g.itertuples():
            if g.anchor in idx_of and g.gist:
                enc_inputs.append(g.gist)
                order.append(g.anchor)
        if enc_inputs:
            gist_vecs = model.encode(enc_inputs, normalize_embeddings=True, batch_size=args.batch_size)
            for a, v in zip(order, gist_vecs):
                queries[a] = v.astype(np.float32)

    # Fallback to section embedding for any anchor missing a gist or when not using gist
    for a in df_g["anchor"]:
        if a in idx_of and a not in queries:
            queries[a] = sec_vecs[idx_of[a]]

    edges = []
    seen: set[Tuple[str, str]] = set()
    n = len(df_sec)

    for t, (anchor, qvec) in enumerate(queries.items(), 1):
        ai = idx_of[anchor]
        lo = max(0, ai - args.window)
        hi = min(n - 1, ai + args.window)
        win_ix = [i for i in range(lo, hi + 1) if i != ai]

        def add_candidates(candidates: List[int], k: int, label: str):
            if not candidates or k <= 0:
                return
            cand_mat = sec_vecs[candidates, :]
            topk = dot_topk(qvec, cand_mat, candidates, k)
            for dst_i, s in topk:
                if s < args.min_sim or dst_i == ai:
                    continue
                src = df_sec.sec_id.iloc[ai]
                dst = df_sec.sec_id.iloc[dst_i]
                key = (src, dst)
                if key in seen:
                    continue
                seen.add(key)
                edges.append(dict(
                    src_doc_id=args.doc_id,
                    src_sec_id=src,
                    dst_sec_id=dst,
                    score=float(s),
                    method=f"implicit_{label}"
                ))
                if args.bidirectional:
                    rkey = (dst, src)
                    if rkey not in seen:
                        seen.add(rkey)
                        edges.append(dict(
                            src_doc_id=args.doc_id,
                            src_sec_id=dst,
                            dst_sec_id=src,
                            score=float(s),
                            method=f"implicit_{label}_bidir"
                        ))

        # window neighbors
        add_candidates(win_ix, args.k, "knn_window")

        # optional global neighbors (outside window)
        if args.global_k > 0:
            far_ix = [i for i in range(n) if (i < lo or i > hi) and i != ai]
            add_candidates(far_ix, args.global_k, "knn_global")

    out = pd.DataFrame(edges)
    out.to_parquet(args.out_parquet, index=False)
    print(f"Wrote {len(out)} implicit edges → {args.out_parquet}")

if __name__ == "__main__":
    main()
