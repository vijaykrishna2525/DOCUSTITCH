# pipeline/eval_summaries.py
import os, re, json, argparse
import pandas as pd
from collections import Counter
from rouge_score import rouge_scorer
from sentence_transformers import SentenceTransformer
import numpy as np

def read(path): 
    return open(path, encoding="utf-8").read() if os.path.exists(path) else ""

def mojibake_flags(s:str)->bool:
    return any(x in s for x in ["Â§","â€”","â€“","â€","â€™","â€œ","â€"])

def trigram_redundancy(s:str)->float:
    toks = s.split()
    if len(toks) < 3: return 0.0
    tris = [tuple(toks[i:i+3]) for i in range(len(toks)-2)]
    c = Counter(tris)
    dup = sum(v-1 for v in c.values() if v>1)
    return dup / max(1,len(tris))

def load_stitched_ids_mixed(stitched_path):
    """
    Accepts either:
      - JSON array: [{"sec_id": "§37.41", ...}, "§37.55", ...]
      - JSONL: each line is a JSON object/string with "sec_id" or a plain string
    Returns: list[str] of sec_ids in order.
    """
    out = []
    if not os.path.exists(stitched_path):
        return out
    txt = open(stitched_path, encoding="utf-8").read().strip()
    if not txt:
        return out
    # Try JSON array first
    try:
        obj = json.loads(txt)
        if isinstance(obj, list):
            for x in obj:
                if isinstance(x, dict) and "sec_id" in x:
                    out.append(x["sec_id"])
                elif isinstance(x, str):
                    out.append(x)
        return out
    except json.JSONDecodeError:
        pass
    # Fallback: JSONL
    for ln in txt.splitlines():
        ln = ln.strip()
        if not ln: 
            continue
        try:
            o = json.loads(ln)
            if isinstance(o, dict) and "sec_id" in o:
                out.append(o["sec_id"])
            elif isinstance(o, str):
                out.append(o)
        except Exception:
            # last resort: treat raw line as a sec_id string
            out.append(ln)
    return out

def coverage(summary:str, sec_ids:list)->float:
    if not sec_ids: return 0.0
    s_norm = re.sub(r"\s+","",summary)
    hit=sum(1 for sid in sec_ids if re.sub(r"\s+","",sid) in s_norm)
    return 100.0*hit/len(sec_ids)

def rougeL(summary:str, gists:str)->float:
    if not gists.strip() or not summary.strip(): return 0.0
    sc = rouge_scorer.RougeScorer(['rougeL'], use_stemmer=True)
    return float(sc.score(gists, summary)['rougeL'].fmeasure)

def embed_sim(summary:str, gist_texts:list, model)->float:
    if not gist_texts or not summary.strip(): return 0.0
    vec_s = model.encode([summary], normalize_embeddings=True)
    vec_g = model.encode(gist_texts, normalize_embeddings=True)
    sims = np.dot(vec_g, vec_s[0])
    return float(np.mean(sims))

def load_gists(gists_path):
    out=[]
    if os.path.exists(gists_path):
        for ln in open(gists_path, encoding="utf-8"):
            ln = ln.strip()
            if not ln: 
                continue
            try:
                o=json.loads(ln); g=o.get("gist_text","")
                if g: out.append(g)
            except: 
                pass
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parts", nargs="+", required=True)
    ap.add_argument("--budget", type=int, required=True)
    ap.add_argument("--out-csv", default=None)
    args = ap.parse_args()

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    rows=[]
    for doc in args.parts:
        final_paths = [
            f"artifacts/{doc}/summary_{args.budget}_refined_openrouter.txt",
            f"artifacts/{doc}/summary_{args.budget}_refined_qwen72b.txt",
            f"artifacts/{doc}/summary_{args.budget}_refined_sherlock.txt",
            f"artifacts/{doc}/summary_{args.budget}_refined_hf.txt",
            f"artifacts/{doc}/summary_{args.budget}_refined_none.txt",
        ]
        final_p = next((p for p in final_paths if os.path.exists(p)), "")
        stitched_p = f"artifacts/{doc}/stitched_list.json"
        gists_p    = f"artifacts/{doc}/gists.jsonl"

        if not final_p:
            rows.append(dict(doc=doc, status="MISSING", path="")); continue

        summary = read(final_p)
        cues = re.findall(r"\(§\s*\d+(?:\.\d+)?\)", summary)  # section cues
        gist_texts = load_gists(gists_p)
        gists_all  = "\n".join(gist_texts)

        rows.append(dict(
            doc=doc,
            status="OK",
            path=final_p,
            words=len(summary.split()),
            mojibake="YES" if mojibake_flags(summary) else "no",
            cue_count=len(cues),
            cue_density=round(100.0*len(cues)/max(1,len(summary.split())),2), # cues per 100 words
            coverage=round(coverage(summary, load_stitched_ids_mixed(stitched_p)),1),
            rougeL=round(rougeL(summary, gists_all),3),
            embed_sim=round(embed_sim(summary, gist_texts, model),3),
            redundancy=round(trigram_redundancy(summary),3),
        ))
    df=pd.DataFrame(rows)
    print(df.to_string(index=False))
    if args.out_csv:
        os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
        df.to_csv(args.out_csv, index=False)
        print(f"\n[eval] wrote → {args.out_csv}")

if __name__=="__main__":
    main()
