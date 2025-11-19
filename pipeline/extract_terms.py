# pipeline/extract_terms.py
# Per-section term extraction using TF-IDF (1–3 grams), with light legal-friendly cleanup.
from __future__ import annotations
import argparse, json, os, re
from typing import List, Tuple

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# --- domain-aware stopwords (extend if needed)
DOMAIN_STOP = {
    # english
    "the","a","an","and","or","of","to","in","for","on","by","with","as","at","from",
    "that","this","these","those","is","are","was","were","be","being","been","it","its",
    "shall","must","may","should","can","will","not","no","any","all","each","such",
    # legal boilerplate
    "section","sections","subsection","subsections","paragraph","paragraphs",
    "subpart","subparts","part","parts","title","chapter","cfr","usc","code",
}

WS = re.compile(r"\s+")
def clean_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\r"," ").replace("Â§","§")
    # keep letters, digits, punctuation that helps phrases; strip random symbols
    s = re.sub(r"[^\w§\.\-/'() ]+", " ", s)
    s = WS.sub(" ", s).strip()
    return s

def load_sections(path: str) -> pd.DataFrame:
    rows = []
    with open(path, encoding="utf-8") as f:
        for ln in f:
            o = json.loads(ln)
            rows.append({
                "sec_id": o["sec_id"],
                "heading": o.get("heading","") or "",
                "text": o.get("text","") or "",
            })
    df = pd.DataFrame(rows)
    # minimal cleanup
    df["heading_clean"] = df["heading"].map(clean_text)
    df["text_clean"]    = df["text"].map(clean_text)
    return df

def build_vectorizer(max_features: int = 20000) -> TfidfVectorizer:
    return TfidfVectorizer(
        lowercase=True,
        ngram_range=(1,3),
        min_df=2,           # ignore very rare noise
        max_df=0.98,        # ignore super-common boilerplate
        max_features=max_features,
        token_pattern=r"(?u)\b[^\W\d_][\w\-']+\b",  # words incl. hyphen/apostrophe
        stop_words=list(DOMAIN_STOP),
        norm="l2",
        sublinear_tf=True,
    )

def top_terms_for_row(row_vec, feature_names: List[str], topk: int = 20) -> List[Tuple[str, float]]:
    # row_vec is a 1xN sparse row
    coo = row_vec.tocoo()
    pairs = list(zip(coo.col, coo.data))
    if not pairs:
        return []
    pairs.sort(key=lambda t: t[1], reverse=True)
    out = []
    for j, w in pairs[:topk]:
        term = feature_names[j]
        # drop tiny ngrams like single letters
        if len(term) < 2: 
            continue
        out.append((term, float(round(w, 6))))
    return out

def main():
    ap = argparse.ArgumentParser(description="Extract per-section terms with TF-IDF")
    ap.add_argument("--sections", required=True, help="path to artifacts/.../sections.jsonl")
    ap.add_argument("--out-parquet", required=True, help="output parquet path")
    ap.add_argument("--topk", type=int, default=25, help="top terms per section")
    ap.add_argument("--max-features", type=int, default=20000)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_parquet), exist_ok=True)

    df = load_sections(args.sections)
    texts = (df["heading_clean"] + " " + df["text_clean"]).tolist()

    vec = build_vectorizer(max_features=args.max_features)
    X = vec.fit_transform(texts)
    feats = vec.get_feature_names_out()

    # doc length proxy → compute term density (nonzero tf-idf terms / total terms)
    # fall back to token count if needed
    token_counts = [max(1, len(t.split())) for t in texts]
    nonzero_counts = (X > 0).sum(axis=1).A1
    term_density = (nonzero_counts / pd.Series(token_counts)).clip(lower=0).astype(float)

    tops = []
    for i in range(X.shape[0]):
        tops.append(top_terms_for_row(X[i], feats, topk=args.topk))

        # --- build output dataframe
    out = pd.DataFrame({
        "sec_id": df["sec_id"].astype(str),
        "heading": df["heading"].astype(str),
        "term_density": term_density.astype(float),
    })

    # Serialize top_terms to JSON strings for Arrow compatibility
    import json as _json
    out["top_terms"] = [
        _json.dumps([{"term": t, "score": w} for (t, w) in (lst or [])], ensure_ascii=False)
        for lst in tops
    ]

    out.to_parquet(args.out_parquet, index=False)
    print(f"Saved: {args.out_parquet} rows: {len(out)}")


if __name__ == "__main__":
    main()
