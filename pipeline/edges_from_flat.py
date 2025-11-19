# pipeline/edges_from_flat.py
from __future__ import annotations
import argparse, re, os
import pandas as pd

CLAUSE_SUFFIX_RX = re.compile(r"(?:\([a-z0-9ivxl]+\))+$", re.I)

def to_base(tok: str) -> str:
    t = (tok or "").replace("Â§","\u00A7").replace(" ","")
    if not t: return ""
    t = re.sub(r"^[^\d\u00A7]+", "", t)  
    if not t: return ""
    if t[0].isdigit():  
        return ""
    core = t[1:] if t.startswith("\u00A7") else t
    core = CLAUSE_SUFFIX_RX.sub("", core)
    return "\u00A7"+core if re.match(r"^\d{1,3}\.\d{1,4}$", core) else ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--flat-csv", required=True)
    ap.add_argument("--out-parquet", required=True)
    ap.add_argument("--min_span_count", type=int, default=1)
    a = ap.parse_args()

    df = pd.read_csv(a.flat_csv, encoding="utf-8")
    df["dst_sec_id"] = df["norm_token"].map(to_base)
    df = df[(df["dst_sec_id"]!="") & (df["match_kind"]!="crossdoc")]

    agg = (df.groupby(["src_sec_id","dst_sec_id"])
             .agg(span_count=("norm_token","size"),
                  examples=("match_text", lambda x: list(dict.fromkeys(x))[:3]))
             .reset_index())

    if a.min_span_count > 1:
        agg = agg[agg["span_count"]>=a.min_span_count]

    os.makedirs(os.path.dirname(a.out_parquet), exist_ok=True)
    agg.to_parquet(a.out_parquet, index=False)
    print(f"Wrote {len(agg)} edges → {a.out_parquet}")

if __name__ == "__main__":
    main()
