# pipeline/build_gists.py
from __future__ import annotations
import argparse, json, os, math, re, itertools
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np

# --- sentence split (simple, legal-friendly) ---
SENT_SPLIT = re.compile(r"(?<=[\.\?\!\;:])\s+(?=[A-Z\(§])")

def split_sents(text: str) -> List[str]:
    if not text: return []
    # de-hyphenate and normalize spaces
    t = text.replace("\r", "")
    t = re.sub(r"(\w)-\n(\w)", r"\1\2", t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{2,}", "\n", t).strip()
    sents = [s.strip() for s in SENT_SPLIT.split(t) if len(s.strip()) > 1]
    # guard against extreme fragments
    return [s for s in sents if len(s.split()) >= 3]

# --- MMR (Maximal Marginal Relevance) w/ TF-IDF + cosine ---
def mmr(query_vec: np.ndarray,
        cand_vecs: np.ndarray,
        lambda_: float = 0.7,
        topk: int = 5) -> List[int]:
    """
    query_vec: (D,)
    cand_vecs: (N, D)
    returns indices of selected sentences
    """
    if cand_vecs.shape[0] == 0:
        return []
    # similarities to query
    # avoid div by zero
    def norm_rows(X):
        n = np.linalg.norm(X, axis=1, keepdims=True) + 1e-9
        return X / n
    q = query_vec / (np.linalg.norm(query_vec) + 1e-9)
    C = norm_rows(cand_vecs)
    sim_to_q = (C @ q)  # (N,)

    selected = []
    remaining = set(range(cand_vecs.shape[0]))
    while remaining and len(selected) < topk:
        if not selected:
            i = int(np.argmax(sim_to_q))
            selected.append(i)
            remaining.remove(i)
            continue
        # redundancy term = max sim to any already selected sentence
        sel_mat = C[selected]  # (k, D)
        red = C[list(remaining)] @ sel_mat.T  # (|R|, k)
        max_red = red.max(axis=1) if red.size else np.zeros((len(remaining),))
        rem_list = list(remaining)
        mmr_scores = lambda_ * sim_to_q[rem_list] - (1 - lambda_) * max_red
        j = rem_list[int(np.argmax(mmr_scores))]
        selected.append(j)
        remaining.remove(j)
    return selected

def build_gist_for_window(doc_df: pd.DataFrame,
                          anchor_row: pd.Series,
                          window: int,
                          k_sentences: int,
                          lambda_: float) -> Dict:
    """
    doc_df: sections DataFrame with columns [sec_id, heading, text]
    anchor_row: row from waypoints (sec_id, score, window, reason)
    window: +/- window size around anchor
    returns dict gist record
    """
    # collect window sections
    sec_ids = list(doc_df["sec_id"])
    idx_map = {s:i for i,s in enumerate(sec_ids)}
    a_sid = anchor_row["sec_id"]
    if a_sid not in idx_map:
        return None
    a_idx = idx_map[a_sid]
    lo = max(0, a_idx - window); hi = min(len(doc_df)-1, a_idx + window)
    block = doc_df.iloc[lo:hi+1].copy()

    # sentences + bookkeeping
    sent_rows = []
    for _, row in block.iterrows():
        sents = split_sents(row.get("text","") or "")
        for j, s in enumerate(sents):
            sent_rows.append({
                "sec_id": row["sec_id"],
                "heading": row.get("heading",""),
                "sent_idx": j,
                "sent_text": s
            })
    if not sent_rows:
        return None
    S = pd.DataFrame(sent_rows)

    # build TF-IDF on sentences; query = concatenated headings + anchor heading
    from sklearn.feature_extraction.text import TfidfVectorizer
    vec = TfidfVectorizer(min_df=1, max_df=0.9, ngram_range=(1,2))
    X = vec.fit_transform(S["sent_text"].tolist())  # (N, D)

    # query text = anchor heading + neighboring headings to bias toward topical coherence
    neighbor_heads = " ".join(block["heading"].fillna("").tolist())
    q_text = (anchor_row.get("sec_id","") + " " +
              doc_df.loc[doc_df["sec_id"]==a_sid, "heading"].fillna("").iloc[0] + " " +
              neighbor_heads)
    q = vec.transform([q_text]).toarray().ravel()
    C = X.toarray()

    pick = mmr(q, C, lambda_=lambda_, topk=k_sentences)
    picked = S.iloc[pick].copy()
    picked = picked.sort_values(["sec_id","sent_idx"])

    gist_text = " ".join(picked["sent_text"].tolist())
    token_est = len(gist_text.split())

    return {
        "anchor_sec_id": a_sid,
        "window_size": int(window),
        "k_sentences": int(k_sentences),
        "lambda": float(lambda_),
        "gist_text": gist_text,
        "token_estimate": int(token_est),
        "source_spans": [
            dict(sec_id=r.sec_id, sent_idx=int(r.sent_idx), sent_text=r.sent_text)
            for _, r in picked.iterrows()
        ]
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", required=True, help="artifacts/.../sections.jsonl (XML-first)")
    ap.add_argument("--waypoints", required=True, help="artifacts/.../waypoints.parquet")
    ap.add_argument("--out-jsonl", required=True, help="artifacts/.../gists.jsonl")
    ap.add_argument("--k-sentences", type=int, default=6, help="sentences per anchor window")
    ap.add_argument("--mmr-lambda", type=float, default=0.7, help="MMR diversity weight (0..1)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_jsonl), exist_ok=True)

    # load sections
    secs = []
    with open(args.sections, encoding="utf-8") as f:
        for ln in f:
            o = json.loads(ln)
            secs.append(dict(sec_id=o["sec_id"], heading=o.get("heading",""), text=o.get("text","") or ""))
    df = pd.DataFrame(secs)

    # load waypoints
    wps = pd.read_parquet(args.waypoints)

    out = []
    for _, wp in wps.iterrows():
        rec = build_gist_for_window(df, wp, int(wp.get("window",1)), args.k_sentences, args.mmr_lambda)
        if rec:
            out.append(rec)

    with open(args.out_jsonl, "w", encoding="utf-8") as w:
        for r in out:
            w.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"Wrote gists → {args.out_jsonl} ({len(out)} anchors)")

if __name__ == "__main__":
    main()
