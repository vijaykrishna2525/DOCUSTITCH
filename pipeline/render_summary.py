# pipeline/render_summary.py
from __future__ import annotations
import argparse, json, os, math
from typing import Dict, List

def load_map(path: str, key: str) -> Dict[str, dict]:
    m={}
    with open(path, encoding="utf-8") as f:
        for ln in f:
            o=json.loads(ln)
            m[o[key]]=o
    return m

def est_tokens(s: str) -> int:
    # cheap token proxy
    return max(1, len((s or "").split()))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", required=True, help="sections.jsonl (for headings)")
    ap.add_argument("--gists", required=True, help="gists.jsonl")
    ap.add_argument("--stitched", required=True, help="stitched_list.json")
    ap.add_argument("--budget", type=int, default=1200, help="token budget")
    ap.add_argument("--out-txt", required=True, help="summary.txt")
    args = ap.parse_args()

    # maps
    sec_map = load_map(args.sections, "sec_id")
    gist_map = load_map(args.gists, "anchor_sec_id")
    order = [json.loads(ln)["sec_id"] for ln in open(args.stitched, encoding="utf-8")]

    used = set()
    picked: List[str] = []
    total = 0

    # prefer anchor gists when available; fallback to section first sentence
    for sid in order:
        if sid in used: 
            continue
        used.add(sid)
        heading = sec_map.get(sid,{}).get("heading","")
        text = ""

        g = gist_map.get(sid)
        if g:
            text = g.get("gist_text","")
        else:
            # fallback: first ~2 sentences from section text
            sec_text = (sec_map.get(sid,{}).get("text","") or "")
            spl = [t.strip() for t in sec_text.split(". ") if t.strip()]
            text = ". ".join(spl[:2]) + ("." if spl else "")

        t = est_tokens(text)
        if total + t > args.budget:
            continue
        picked.append(f"{sid} — {heading}\n{text}\n")
        total += t

    os.makedirs(os.path.dirname(args.out_txt), exist_ok=True)
    with open(args.out_txt, "w", encoding="utf-8") as f:
        f.write("# Stitched Summary\n\n")
        for block in picked:
            f.write(block + "\n")
        f.write(f"\n---\nApprox tokens used: {total} / {args.budget}\n")
    print(f"Wrote → {args.out_txt} ({len(picked)} sections, ~{total} tokens)")

if __name__ == "__main__":
    main()
