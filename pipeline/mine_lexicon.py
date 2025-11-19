# pipeline/mine_lexicon.py
from __future__ import annotations
import argparse, json, re, os
from collections import Counter, defaultdict
from typing import Dict
import pandas as pd, yaml

WS = re.compile(r"\s+")
PUNCT_EDGES = re.compile(r"^[^\w§]+|[^\w\)]+$", re.UNICODE)

STOP_WORDS = set("""
a an and are as at be by for from has have in into is it its of on or that the their there these this to under with without such shall may can will must not
""".split())

SOFT_STOP_PHRASES = {
    "reserved","final","age","advance","vehicle","motor","determination",
    "use your benefits","responsibilities"
}

LEGAL_STEMS = {
    "applicability":"applicability","applicable":"applicability",
    "definition":"definition","definitions":"definition",
    "authority":"authority","purpose":"purpose","scope":"scope",
    "penalties":"penalty","penalty":"penalty",
    "compliance":"compliance","requirement":"requirement","requirements":"requirement",
    "reporting":"reporting","report":"reporting","reports":"reporting",
    "confidentiality":"confidentiality","retaliation":"retaliation",
    "eligibility":"eligibility","eligible":"eligibility",
    "payment":"payment","payments":"payment",
    "investigation":"investigation","training":"training",
    "recordkeeping":"recordkeeping","supervision":"supervision",
    "grievance":"grievance",
}

def norm_token(s: str) -> str:
    if not s: return ""
    s = s.replace("Â§","§")      # fix mojibake if present
    s = WS.sub(" ", s).strip()
    s = PUNCT_EDGES.sub("", s)
    return s

def keep_word(w: str) -> bool:
    w = w.lower()
    if not w or w in STOP_WORDS: return False
    if any(ch.isdigit() for ch in w) and len(w) <= 2:  # drop tiny numeric tokens
        return False
    return True

def normalize_keyphrase(s: str) -> str:
    s = norm_token(s).lower()
    s = " ".join(w for w in s.split() if keep_word(w))
    parts = []
    for w in s.split():
        parts.append(LEGAL_STEMS.get(w, w))
    return " ".join(parts)

def looks_useful(phrase: str) -> bool:
    if not phrase: return False
    if phrase in SOFT_STOP_PHRASES: return False
    toks = phrase.split()
    if len(toks) == 1 and phrase not in set(LEGAL_STEMS.values()):
        return False
    if "'" in phrase: return False
    return True

def load_terms_parquet(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    rows = []
    for _, r in df.iterrows():
        try:
            items = json.loads(r["top_terms"]) or []
        except Exception:
            items = []
        for it in items:
            t_raw = it.get("term","")
            t_norm = normalize_keyphrase(t_raw)
            if not t_norm: 
                continue
            rows.append({
                "sec_id": r["sec_id"],
                "term": norm_token(t_raw),
                "norm": t_norm,
                "score": float(it.get("score", 0.0)),
            })
    return pd.DataFrame(rows)

def mine_lexicon(terms_dfs: Dict[str, pd.DataFrame],
                 top_per_doc: int,
                 min_df: int,
                 seed_csv: str | None) -> Dict:
    doc_specific = {}
    global_df = Counter()
    global_scores = defaultdict(list)

    for doc_id, df in terms_dfs.items():
        if df.empty:
            doc_specific[doc_id] = []
            continue
        agg = (df.groupby("norm", dropna=True)["score"]
                 .max().reset_index()
                 .sort_values("score", ascending=False))
        agg = agg[agg["norm"].map(looks_useful)]
        sel = agg["norm"].head(top_per_doc).tolist()
        doc_specific[doc_id] = sel
        for t in sel:
            global_df[t] += 1
            global_scores[t].append(float(agg.loc[agg["norm"]==t,"score"].iloc[0]))

    global_terms = []
    for t, dfreq in global_df.items():
        if dfreq < min_df: 
            continue
        mean_s = sum(global_scores[t])/max(1,len(global_scores[t]))
        global_terms.append((t, dfreq, mean_s))
    global_terms.sort(key=lambda x: (x[1], x[2]), reverse=True)
    global_terms = [t for (t,_,_) in global_terms]

    seeds = []
    if seed_csv and os.path.exists(seed_csv):
        sdf = pd.read_csv(seed_csv)
        for v in sdf["term"].astype(str).tolist():
            nv = normalize_keyphrase(v)
            if nv and nv not in global_terms:
                seeds.append(nv)

    return {
        "version": 1,
        "notes": "Mined terms for waypoint seeding (XML-first).",
        "global_terms": sorted(list(dict.fromkeys(seeds + global_terms))),
        "doc_specific": doc_specific,
        "patterns": {
            "section_label": r"\u00A7{1,2}\s*\d{1,3}\.\d{1,4}(?:\([a-z0-9ivxl]+\))*",
            "definitions_heading": r"(?i)\bdefinitions?\b",
            "applicability_heading": r"(?i)\bapplicability\b",
        }
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfr37", required=True)
    ap.add_argument("--cfr115", required=True)
    ap.add_argument("--cfr408", required=True)
    ap.add_argument("--out-yaml", required=True)
    ap.add_argument("--seed-csv", default="")
    ap.add_argument("--top-per-doc", type=int, default=12)
    ap.add_argument("--min-df", type=int, default=1)
    a = ap.parse_args()

    dfs = {
        "cfr_6_37": load_terms_parquet(a.cfr37),
        "cfr_6_115": load_terms_parquet(a.cfr115),
        "cfr_20_408": load_terms_parquet(a.cfr408),
    }
    yml = mine_lexicon(dfs, a.top_per_doc, a.min_df, a.seed_csv or None)
    os.makedirs(os.path.dirname(a.out_yaml), exist_ok=True)
    with open(a.out_yaml, "w", encoding="utf-8") as f:
        yaml.safe_dump(yml, f, allow_unicode=True, sort_keys=False)
    print(f"Wrote lexicon → {a.out_yaml} (global={len(yml['global_terms'])}, "
          f"docs={[len(v) for v in yml['doc_specific'].values()]})")

if __name__ == "__main__":
    main()
