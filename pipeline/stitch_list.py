# pipeline/stitch_list.py
from __future__ import annotations
import argparse, json, os, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--waypoints", required=True, help="waypoints.parquet")
    ap.add_argument("--edges", required=True, help="edges_merged.parquet")
    ap.add_argument("--graph", required=True, help="graph_merged.parquet (with centrality)")
    ap.add_argument("--k-per-anchor", type=int, default=3, help="neighbors to include per anchor")
    ap.add_argument("--out-json", required=True, help="stitched_list.json")
    args = ap.parse_args()

    W = pd.read_parquet(args.waypoints).sort_values("score", ascending=False)
    E = pd.read_parquet(args.edges)
    G = pd.read_parquet(args.graph).set_index("node_sec_id")

    # rank neighbors by: (explicit-first via weight), then centrality of dst
    E = E.join(G["centrality"], on="dst_sec_id")

    plan = []
    seen = set()

    for _, row in W.iterrows():
        anchor = row["sec_id"]
        if anchor not in seen:
            plan.append({"sec_id": anchor, "role":"anchor"})
            seen.add(anchor)
        neigh = (
            E[E["src_sec_id"]==anchor]
            .assign(prior_score=lambda d: d["weight"] + 0.25*(d["centrality"].fillna(0)))
            .sort_values("prior_score", ascending=False)
        )
        for _, n in neigh.head(args.k_per_anchor).iterrows():
            target = n["dst_sec_id"]
            if target not in seen:
                plan.append({"sec_id": target, "role":"neighbor"})
                seen.add(target)

    os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        for item in plan:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Wrote stitched list → {args.out_json} ({len(plan)} items)")

if __name__ == "__main__":
    main()
