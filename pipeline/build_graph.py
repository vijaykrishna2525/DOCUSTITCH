# pipeline/build_graph.py
from __future__ import annotations
import argparse, json, os
import pandas as pd
import networkx as nx

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--edges", required=True)
    ap.add_argument("--out-graph", required=True)
    ap.add_argument("--out-metrics", required=True)
    a = ap.parse_args()

    df = pd.read_parquet(a.edges)
    G = nx.DiGraph()
    for _, r in df.iterrows():
        G.add_edge(r["src_sec_id"], r["dst_sec_id"], span_count=int(r["span_count"]))

    pr = nx.pagerank(G, alpha=0.85, max_iter=100)
    indeg = dict(G.in_degree()); outdeg = dict(G.out_degree())
    strength = {n: sum(G[u][v].get("span_count",1) for u,v in G.in_edges(n)) for n in G.nodes()}

    nd = (pd.DataFrame({
        "sec_id": list(G.nodes()),
        "pagerank": [pr.get(n,0.0) for n in G.nodes()],
        "in_degree": [indeg.get(n,0) for n in G.nodes()],
        "out_degree":[outdeg.get(n,0) for n in G.nodes()],
        "in_span_strength":[strength.get(n,0) for n in G.nodes()]
    }).sort_values(["pagerank","in_degree","in_span_strength"], ascending=False).reset_index(drop=True))

    os.makedirs(os.path.dirname(a.out_graph), exist_ok=True)
    nd.to_parquet(a.out_graph, index=False)

    metrics = {
        "num_nodes": int(G.number_of_nodes()),
        "num_edges": int(G.number_of_edges()),
        "density": float(nx.density(G)),
        "components": int(nx.number_weakly_connected_components(G)),
        "avg_clustering": float(nx.average_clustering(G.to_undirected()))
    }
    with open(a.out_metrics, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print("Saved:", a.out_graph, a.out_metrics)

if __name__ == "__main__":
    main()
