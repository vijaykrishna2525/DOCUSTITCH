# pipeline/edges_from_refs.py
import argparse, json, os
import pandas as pd
import re

def load_ids(sections_path):
    ids=set()
    with open(sections_path,"r",encoding="utf-8") as f:
        for ln in f:
            ids.add(json.loads(ln)["sec_id"].replace(" ",""))
    return ids

def load_refs(path):
    rows=[]
    with open(path,"r",encoding="utf-8") as f:
        for ln in f:
            rows.append(json.loads(ln))
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", required=True, help="sections.jsonl (XML canonical)")
    ap.add_argument("--xml-refs", required=True, help="xml_refs.jsonl (from extract_refs.py)")
    ap.add_argument("--out", required=True, help="edges_explicit.parquet")
    a = ap.parse_args()

    os.makedirs(os.path.dirname(a.out), exist_ok=True)

    valid_ids = load_ids(a.sections)
    refs = load_refs(a.xml_refs)

    edges=[]
    for r in refs:
        src = r["sec_id"].replace(" ","")
        if src not in valid_ids:
            continue
        for token in (r.get("explicit_refs") or []):
            t = token.replace(" ","")
            # Use only in-document section targets that look like §X.Y...
            if re.match(r"^§\d{1,3}\.\d{1,3}", t):
                if t in valid_ids and t != src:
                    edges.append((src, t, "explicit", 1.0))
            # ignore cross-docs here (e.g., "20CFR §408.210", "6CFR part 115")
    df = pd.DataFrame(edges, columns=["src_sec_id","dst_sec_id","edge_type","weight"])
    df.to_parquet(a.out, index=False)
    print(f"Wrote {len(df)} edges → {a.out}")

if __name__ == "__main__":
    main()
