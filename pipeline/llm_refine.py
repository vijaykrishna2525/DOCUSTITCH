# pipeline/llm_refine.py
# -----------------------------------------------------------------------------
# Refines an extractive draft summary into a concise, faithful synopsis.
#
# Inputs:
#   --doc-id <cfr_*>                 e.g., cfr_6_37
#   --part-prefix <int>              e.g., 37, 115, 408
#   --gists <path>                   artifacts/<doc>/gists.jsonl
#   --stitched <path>                artifacts/<doc>/stitched_list.json
#   --sections <path>                artifacts/<doc>/sections.jsonl   (optional; used for headings)
#   --draft <path>                   artifacts/<doc>/summary_1200.txt (extractive draft)
#   --budget-words <int>             e.g., 600, 900, 1200
#   --out <path>                     where to write refined text
#
# LLM backend (choose one):
#   --backend none                   (default) clean + trim only (no LLM)
#   --backend hf --hf-model <id>     local HuggingFace model (instruction tuned)
#   --backend openai --model <id>    OpenAI-compatible; set --api-key and --base-url if needed
#       [--api-key ENV or arg] [--base-url http://localhost:8000/v1]
#
# Examples (PowerShell):
#   # Clean+trim only (no model)
#   python pipeline\llm_refine.py `
#     --doc-id cfr_6_37 --part-prefix 37 `
#     --gists artifacts\cfr_6_37\gists.jsonl `
#     --stitched artifacts\cfr_6_37\stitched_list.json `
#     --sections artifacts\cfr_6_37\sections.jsonl `
#     --draft artifacts\cfr_6_37\summary_1200.txt `
#     --budget-words 900 --backend none `
#     --out artifacts\cfr_6_37\summary_900_refined.txt
#
#   # Local HF model (requires transformers + a local instruct model)
#   python pipeline\llm_refine.py `
#     --doc-id cfr_6_115 --part-prefix 115 `
#     --gists artifacts\cfr_6_115\gists.jsonl `
#     --stitched artifacts\cfr_6_115\stitched_list.json `
#     --sections artifacts\cfr_6_115\sections.jsonl `
#     --draft artifacts\cfr_6_115\summary_1200.txt `
#     --budget-words 800 --backend hf --hf-model mistralai/Mistral-7B-Instruct-v0.2 `
#     --out artifacts\cfr_6_115\summary_800_refined.txt
#
#   # OpenAI-compatible endpoint (local vLLM/LM Studio/OAI)
#   python pipeline\llm_refine.py `
#     --doc-id cfr_20_408 --part-prefix 408 `
#     --gists artifacts\cfr_20_408\gists.jsonl `
#     --stitched artifacts\cfr_20_408\stitched_list.json `
#     --sections artifacts\cfr_20_408\sections.jsonl `
#     --draft artifacts\cfr_20_408\summary_1200.txt `
#     --budget-words 1000 --backend openai --model gpt-4o-mini `
#     --api-key %OPENAI_API_KEY% --base-url https://api.openai.com/v1 `
#     --out artifacts\cfr_20_408\summary_1000_refined.txt
# -----------------------------------------------------------------------------

from __future__ import annotations
import argparse, json, os, re, sys, textwrap
from typing import List, Dict, Any, Optional

# =========================
# Impeccable System & User Prompts
# =========================

SYS_PROMPT = """You are a meticulous legal summarizer operating fully offline on sensitive documents (Code of Federal Regulations parts). 
Your mandate is to produce a concise, neutral, strictly faithful synopsis of the given CFR part using ONLY the supplied context, gists, 
stitched order, and the draft extractive summary. You must not introduce content not supported by the provided text.

REQUIREMENTS — CONTENT
1) Fidelity: Preserve meaning precisely; do not invent policies, definitions, timelines, numbers, or parties.
2) Scope: Summarize ONLY what pertains to the given CFR part; avoid drifting into other titles/chapters unless explicitly cited in the text.
3) Prioritization: Emphasize purpose/scope, core requirements, responsibilities, obligations, processes (e.g., reporting, audits), enforcement, 
   and defined terms that shape compliance. Secondary details may be compressed.
4) Citations: Use section cues as parentheses like (§{PART_PREFIX}.X) sparingly and only when clarifying where a rule comes from. 
   Do NOT emit full legal citations or URLs. If cross-title citations are mentioned, name them generically (e.g., “referenced in Title 42”) 
   and only if necessary to preserve meaning.
5) No legal advice. No normative language. Neutral, matter-of-fact tone.

REQUIREMENTS — STYLE & STRUCTURE
1) Output format: 
   - Begin with a one-sentence overview of the part’s purpose/scope.
   - Then 3–7 short paragraphs grouping related provisions (eligibility/scope; definitions; duties/controls; procedures; audits/appeals; data, etc.).
   - If useful, use short bullet lists (max 5 bullets per list) to enumerate conditions or steps.
2) Language & clarity:
   - Plain American English; avoid quotations unless the phrase is uniquely necessary to preserve meaning.
   - Define acronyms on first use (e.g., “Department of Homeland Security (DHS)”) when present in context.
   - Use consistent terminology; avoid synonym drift that could create ambiguity.
3) Section cues policy:
   - Use (§{PART_PREFIX}.X) only when needed to disambiguate; do not clutter every sentence with citations.
   - Do not cite nonexistent sections. Do not cite subparagraphs unless they are central (e.g., “(c)” if it changes meaning).
4) Formatting polish:
   - No mojibake: normalize ‘Â§’, curly quotes, dashes, and spacing silently.
   - No headings like “Draft”/“Refined”; no preface; just the final summary text.

REQUIREMENTS — BUDGET CONTROL
1) Hard cap: Do not exceed ~{BUDGET_WORDS} words. If needed, compress by merging duplicative points and removing low-value repetition.
2) Brevity discipline: Prefer short sentences with crisp verbs; remove throat clearing (“this section describes…”).

QUALITY GATES (do silently; do not print the checks)
- Consistency: All stated responsibilities/requirements must appear in the provided context/draft; nothing may be invented.
- Cross-checks: Numbers, thresholds, timelines, and actors (agency/facility/States) must match the text.
- Glue: Ensure smooth transitions; no bullet orphaning; no contradictions.
- Terminology: Definitions used must align with those in the context, or be omitted if uncertain.

If any requirement cannot be satisfied from the input, omit that point rather than guessing. Output only the final summary.
"""

USER_TEMPLATE = """Document ID: {doc_id}
Part prefix: {part_prefix}

Context (anchor gists & stitched cues; normalized excerpts)
-----------------------------------------------------------
{context}

Draft Extractive Summary (to refine)
------------------------------------
{draft}

Task
----
Rewrite the draft into a single, coherent, budget-bounded synopsis that:
- faithfully represents the part’s scope and key obligations;
- removes duplication and tightens phrasing;
- uses section cues sparingly as (§{part_prefix}.X) only where they aid orientation;
- normalizes typography (no “Â§”, smart quotes/dashes fixed);
- fits within ~{budget_words} words;
- uses 3–7 short paragraphs (and brief bullet lists if helpful);
- maintains neutral tone and avoids legal advice.

Output only the refined summary. Do not include rationale, headings, or any text outside the summary.
"""

# =========================
# Utilities
# =========================

def normalize_mojibake(s: str) -> str:
    if not s:
        return ""
    # Common CFR issues
    s = s.replace("Â§", "§")
    s = s.replace("â€”", "—").replace("â€“", "–")
    s = s.replace("â€˜", "‘").replace("â€™", "’")
    s = s.replace("â€œ", "“").replace("â€\x9d", "”").replace("â€ť", "”").replace("â€\x9c", "“")
    s = s.replace("â€", '"')  # fallthrough weirdness
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s+\n", "\n", s)
    s = re.sub(r"\n\s+", "\n", s)
    return s.strip()

def hard_word_cap(text: str, budget_words: int) -> str:
    words = text.split()
    if len(words) <= budget_words:
        return text
    clipped = " ".join(words[:budget_words])
    # try to end at a sentence boundary if close
    m = re.search(r"(.+?[.!?])[^.!?]*$", clipped)
    if m and len(clipped) - len(m.group(1)) <= 40:
        clipped = m.group(1)
    return clipped

def enforce_paren_section_spacing(text: str) -> str:
    text = re.sub(r"\(\s*§\s*", "(§", text)
    text = re.sub(r"\s+\)", ")", text)
    return text

def load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows = []
    with open(path, encoding="utf-8") as f:
        for ln in f:
            if ln.strip():
                rows.append(json.loads(ln))
    return rows

def read_text(path: str) -> str:
    return open(path, encoding="utf-8").read()

def build_context(doc_id: str, part_prefix: int,
                  gists_path: str, stitched_path: str,
                  sections_path: Optional[str],
                  max_items: int = 18) -> str:
    """Build a compact context block from stitched order + gists.
       Accepts stitched as JSON array or JSONL (one object per line).
       Filters to the current part prefix (e.g., 37, 115, 408).
    """
    # --- load stitched (array or JSONL) ---
    stitched_items: List[Dict[str, Any]] = []
    raw = open(stitched_path, encoding="utf-8").read().strip()
    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            stitched_items = obj
        elif isinstance(obj, dict):
            stitched_items = [obj]
    except Exception:
        for ln in raw.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            stitched_items.append(json.loads(ln))

    # normalize and keep only this part (e.g., "§37.x" if part_prefix=37)
    want_prefix = f"§{part_prefix}."
    stitched_ids: List[str] = []
    for it in stitched_items:
        sid = it.get("sec_id") if isinstance(it, dict) else None
        if isinstance(sid, str) and sid.startswith(want_prefix):
            stitched_ids.append(sid)
    stitched_ids = stitched_ids[:max_items]

    # index gists by anchor_sec_id OR sec_id
    gists_map: Dict[str, Dict[str, Any]] = {}
    for row in load_jsonl(gists_path):
        gid = row.get("anchor_sec_id") or row.get("sec_id")
        if gid:
            gists_map[gid] = row

    # optional headings
    head_map: Dict[str, str] = {}
    if sections_path and os.path.exists(sections_path):
        for row in load_jsonl(sections_path):
            sid = row.get("sec_id")
            if sid:
                head_map[sid] = row.get("heading", "") or ""

    # compose lines
    lines = []
    for sid in stitched_ids:
        head = head_map.get(sid, "")
        gist = gists_map.get(sid, {}).get("gist_text", "") or ""
        chunk = [f"{sid} — {head}".strip(" —")] if head else [sid]
        if gist:
            chunk.append(normalize_mojibake(gist))
        lines.append("\n".join(chunk))

    ctx = "\n\n".join(lines)
    return normalize_mojibake(ctx)


# =========================
# Backends
# =========================

def run_none_backend(draft: str, budget_words: int) -> str:
    out = normalize_mojibake(draft)
    out = enforce_paren_section_spacing(out)
    out = hard_word_cap(out, budget_words)
    return out

def run_hf_backend(sys_prompt: str, user_prompt: str, model_id: str, max_new_tokens: int = 500) -> str:
    try:
        from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
        import torch
    except Exception as e:
        raise RuntimeError("Transformers not available. Install `transformers` and try again.") from e

    # ---- SAFETY SETTINGS (Windows-friendly) ----
    # • Force CPU to avoid CUDA/driver/paging pitfalls
    # • No 4-bit quantization (bitsandbytes is flaky on Windows)
    # • Keep memory use low; small model only (≤1–2B)
    tok = AutoTokenizer.from_pretrained(model_id, use_fast=True)
    mdl = AutoModelForCausalLM.from_pretrained(
        model_id,
        device_map="cpu",
        low_cpu_mem_usage=True,
        torch_dtype=None,      # let HF choose FP32 on CPU
    )

    pipe = pipeline(
        "text-generation",
        model=mdl,
        tokenizer=tok,
        device=-1,             # CPU
    )

    if hasattr(tok, "apply_chat_template"):
        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ]
        prompt = tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        prompt = f"[SYSTEM]\n{sys_prompt}\n\n[USER]\n{user_prompt}\n\n[ASSISTANT]\n"

    out = pipe(
        prompt,
        max_new_tokens=max_new_tokens,
        do_sample=False,
        temperature=0.0,
        pad_token_id=tok.eos_token_id,
        eos_token_id=tok.eos_token_id,
        return_full_text=False,   # only the completion
    )[0]["generated_text"].strip()

    return out



def run_openai_backend(sys_prompt: str, user_prompt: str, model: str, api_key: Optional[str], base_url: Optional[str]) -> str:
    import requests

    api_key = api_key or os.environ.get("OPENAI_API_KEY") or os.environ.get("API_KEY")
    if not api_key:
        raise RuntimeError("Missing API key for openai backend. Pass --api-key or set OPENAI_API_KEY.")
    base_url = base_url or os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1"

    url = f"{base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=120)
    if r.status_code >= 300:
        raise RuntimeError(f"OpenAI backend error {r.status_code}: {r.text}")
    data = r.json()
    return (data["choices"][0]["message"]["content"] or "").strip()

# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser(description="Refine extractive CFR summary with strict, budget-aware prompting.")
    ap.add_argument("--doc-id", required=True)
    ap.add_argument("--part-prefix", required=True, type=int)
    ap.add_argument("--gists", required=True)
    ap.add_argument("--stitched", required=True)
    ap.add_argument("--sections", required=False, default=None)
    ap.add_argument("--draft", required=True)
    ap.add_argument("--budget-words", required=True, type=int)
    ap.add_argument("--out", required=True)

    ap.add_argument("--backend", choices=["none", "hf", "openai"], default="none")

    # HF
    ap.add_argument("--hf-model", default=None)
    ap.add_argument("--max-new-tokens", type=int, default=800)

    # OpenAI-compatible
    ap.add_argument("--model", default=None, help="OpenAI-compatible model name")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--base-url", default=None)

    args = ap.parse_args()

    # Build context
    context = build_context(
        doc_id=args.doc_id,
        part_prefix=args.part_prefix,
        gists_path=args.gists,
        stitched_path=args.stitched,
        sections_path=args.sections,
        max_items=18,
    )

    # Read & normalize draft extractive summary
    draft = normalize_mojibake(read_text(args.draft))

    # Bake prompts
    sys_prompt = SYS_PROMPT.replace("{PART_PREFIX}", str(args.part_prefix)).replace("{BUDGET_WORDS}", str(args.budget_words))
    user_prompt = USER_TEMPLATE.format(
        doc_id=args.doc_id,
        part_prefix=args.part_prefix,
        context=context,
        draft=draft,
        budget_words=args.budget_words,
    )

    # Generate/Refine
    if args.backend == "none":
        out = run_none_backend(draft, args.budget_words)
    elif args.backend == "hf":
        if not args.hf_model:
            raise RuntimeError("Provide --hf-model for backend=hf")
        out = run_hf_backend(sys_prompt, user_prompt, args.hf_model, max_new_tokens=args.max_new_tokens)
    else:  # openai
        if not args.model:
            raise RuntimeError("Provide --model for backend=openai")
        out = run_openai_backend(sys_prompt, user_prompt, model=args.model, api_key=args.api_key, base_url=args.base_url)

    # Post-process polish & budget clamp
    out = normalize_mojibake(out).strip()
    out = enforce_paren_section_spacing(out)
    out = hard_word_cap(out, args.budget_words)
    out = out.strip()

    # Write
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(out + "\n")

    print(f"[llm_refine] wrote → {args.out} (≤ ~{args.budget_words} words) using backend={args.backend}")

if __name__ == "__main__":
    main()
