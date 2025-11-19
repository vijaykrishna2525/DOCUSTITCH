# pipeline/merge_graph.py
from __future__ import annotations
import argparse, os, pandas as pd
import numpy as np

def _norm_id(x: pd.Series) -> pd.Series:
    return x.astype(str).str.replace(" ", "", regex=False)

def load_edges(explicit_pq: str, implicit_pq: str) -> pd.DataFrame:
    frames = []
    if os.path.exists(explicit_pq):
        e = pd.read_parquet(explicit_pq).copy()
        # normalize ids just in case
        if "src_sec_id" in e: e["src_sec_id"] = _norm_id(e["src_sec_id"])
        if "dst_sec_id" in e: e["dst_sec_id"] = _norm_id(e["dst_sec_id"])
        e["edge_type"] = "explicit"
        frames.append(e[["src_sec_id","dst_sec_id","edge_type"]])

    if os.path.exists(implicit_pq):
        i = pd.read_parquet(implicit_pq).copy()
        if "src_sec_id" in i: i["src_sec_id"] = _norm_id(i["src_sec_id"])
        if "dst_sec_id" in i: i["dst_sec_id"] = _norm_id(i["dst_sec_id"])
        i["edge_type"] = "implicit"
        keep = ["src_sec_id","dst_sec_id","edge_type"] + (["score"] if "score" in i.columns else [])
        frames.append(i[keep])

    if not frames:
        raise FileNotFoundError("No edges found")

    df = pd.concat(frames, ignore_index=True)
    if "score" not in df.columns:
        df["score"] = np.nan
    return df

def make_weight(row, w_explicit=1.0, w_implicit=0.4):
    if row["edge_type"] == "explicit":
        return w_explicit
    s = row.get("score", np.nan)
    if pd.isna(s):
        return w_implicit
    return w_implicit * float(s)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", required=True, help="sections.jsonl for this doc")
    ap.add_argument("--edges-explicit", required=True, help="edges_explicit.parquet")
    ap.add_argument("--edges-implicit", required=True, help="edges_implicit.parquet")
    ap.add_argument("--out-graph", required=True, help="graph_merged.parquet (nodes)")
    ap.add_argument("--out-edges", required=True, help="edges_merged.parquet (edges with weights)")
    args = ap.parse_args()

    # load sections for node list
    S = pd.read_json(args.sections, lines=True)
    S["sec_id"] = _norm_id(S["sec_id"])
    nodes = S[["sec_id","heading"]].drop_duplicates()
    nodes = nodes.rename(columns={"sec_id":"node_sec_id","heading":"node_heading"})

    # load & prep edges
    E = load_edges(args.edges_explicit, args.edges_implicit).copy()
    E = E[E["src_sec_id"] != E["dst_sec_id"]].copy()
    E["weight"] = E.apply(make_weight, axis=1)

    # keep max-weight per (src,dst)
    E = E.sort_values("weight", ascending=False).drop_duplicates(["src_sec_id","dst_sec_id"])

    # degree stats
    indeg = E.groupby("dst_sec_id")["weight"].sum().rename("in_weight")
    outdeg = E.groupby("src_sec_id")["weight"].sum().rename("out_weight")
    deg = nodes.set_index("node_sec_id").join(indeg, how="left").join(outdeg, how="left").fillna(0.0).reset_index()

    # simple centrality
    if len(deg):
        deg["centrality"] = deg["in_weight"] / max(1e-9, deg["in_weight"].max())

    # NEW: add a 'method' alias for compatibility (so downstream code can group on 'method')
    E["method"] = E["edge_type"]  # explicit / implicit

    os.makedirs(os.path.dirname(args.out_graph), exist_ok=True)
    deg.to_parquet(args.out_graph, index=False)
    E.to_parquet(args.out_edges, index=False)
    print(f"Wrote nodes → {args.out_graph} ({len(deg)}), edges → {args.out_edges} ({len(E)})")

if __name__ == "__main__":
    main()
