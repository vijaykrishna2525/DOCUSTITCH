# DocuStitch

# **DocuStitch Capstone Project (DAEN 690)**  
*A graph-aware regulatory document analysis and summarization pipeline for CFR legal texts*

---

# Table of Contents
- [About the Project](#about-the-project)
- [Motivation & Problem Statement](#motivation--problem-statement)
- [Features / Highlights](#features--highlights)
- [Tech Stack](#tech-stack)
- [System Architecture](#system-architecture)
- [Pipeline Workflow](#pipeline-workflow)
- [Core Methodology](#core-methodology)
- [Evaluation & Metrics](#evaluation--metrics)
- [Artifacts Generated](#artifacts-generated)
- [Repository Structure](#repository-structure)
- [Installation](#installation)
- [Running the Pipeline](#running-the-pipeline)
- [Limitations](#limitations)
- [Future Work](#future-work)
- [Acknowledgements](#acknowledgements)

---

# **About the Project**

DocuStitch is an end-to-end legal document processing pipeline designed to parse, analyze, and summarize highly structured federal regulations from the **Code of Federal Regulations (CFR)**.

The system integrates:

- XML and PDF parsing  
- Citation extraction  
- Graph construction (explicit + implicit dependencies)  
- Term/lexicon mining  
- Waypoint and gist generation  
- Graph-aware summarization  
- Optional LLM refinement (Qwen 72B)  
- Evaluation metrics (coverage, ROUGE, redundancy)

The project was developed as part of the **DAEN 690 Capstone** at George Mason University.

---

# **Motivation & Problem Statement**

CFR documents are long, hierarchical, highly interdependent, and citation-heavy. Traditional LLM summarization fails due to:

- Length exceeding model context windows  
- Heavy reliance on cross-references  
- Distributed regulatory meaning  
- Non-linear dependency structures  

**Goal:**  
Build a *scalable, reproducible, graph-aware summarization pipeline* capable of extracting regulatory meaning faithfully under strict token budgets.

The pipeline is applied to:

- **6 CFR 37 — REAL ID DRIVER’S LICENSES AND IDENTIFICATION CARDS**
- **6 CFR 115 — SEXUAL ABUSE AND ASSAULT PREVENTION STANDARDS**
- **20 CFR 408 — —SPECIAL BENEFITS FOR CERTAIN WORLD WAR II VETERANS **

---

# **Features / Highlights**

-  Multi-format parsing (XML + PDF)
-  Automated citation extraction from both formats
-  Explicit graph construction (normalized section references)
-  Implicit graph construction using transformer embeddings
-  Merged graph analysis for dependency scoring + centrality
-  Lexicon + term mining (TF-IDF across CFR corpus)
-  Waypoint identification (high-value regulatory sections)
-  MMR-based gist extraction
-  Graph-aware stitching under a token budget
-  Optional LLM refinement (Qwen 72B)
-  Evaluation metrics (coverage, ROUGE-L, redundancy, semantic similarity)

---

# **Tech Stack**

**Languages & Frameworks**  
- Python 3.10+  
- Typer (CLI)  
- pydantic (data validation)

**NLP & ML**  
- sentence-transformers  
- scikit-learn  
- transformers  
- torch  

**PDF & XML Parsing**  
- PyMuPDF  
- pdfminer.six  
- pdfplumber  
- BeautifulSoup4  

**Data Handling**  
- pandas  
- pyarrow  
- numpy  

**Graph Processing**  
- networkx  

**Evaluation**  
- rouge-score  
- cosine similarity  

---

# **System Architecture**

```
                ┌─────────────────────────┐
                │  CFR XML / PDF Inputs   │
                └────────────┬────────────┘
                             │
                 ┌───────────▼────────────┐
                 │  Parsing & Alignment   │
                 └───────────┬────────────┘
                             │
                 ┌───────────▼────────────┐
                 │  Citation Extraction   │
                 └───────────┬────────────┘
                             │
                 ┌───────────▼────────────┐
                 │ Mining Terms & Lexicon │
                 └───────────┬────────────┘
                             │
                 ┌───────────▼────────────┐
                 │ Explicit + Implicit    │
                 │     Graphs             │
                 └───────────┬────────────┘
                             │
                 ┌───────────▼────────────┐
                 │ Waypoints & Gists      │
                 └───────────┬────────────┘
                             │
                 ┌───────────▼────────────┐
                 │ Stitching + Summary    │
                 └───────────┬────────────┘
                             │
                 ┌───────────▼────────────┐
                 │   Evaluation Metrics    │
                 └─────────────────────────┘
```

---

# **Pipeline Workflow**

## **1. Parsing**
Extract structured sections from XML and best-quality text from PDF.  
Outputs:
- `sections.jsonl`
- `sections_pdf.jsonl`
- `parity_report.csv`

---

## **2. Citation Extraction**
Regex + normalization to compute explicit reference edges.  
Outputs:
- `xml_refs.jsonl`
- `pdf_refs.jsonl`
- `explicit_refs_flat.csv`
- `edges_explicit.parquet`

---

## **3. Term Extraction & Lexicon Mining**
TF-IDF across CFR parts to identify regulatory keywords.  
Outputs:
- `terms.parquet`
- `lexicon.yaml`

---

## **4. Graph Construction**
- Explicit graph (citations)  
- Implicit graph (embeddings)  
- Merged graph (weighted union)  

Outputs:
- `edges_implicit.parquet`
- `graph.parquet`
- `graph_merged.parquet`
- `graph_metrics.json`

---

## **5. Waypoints & Gists**
Selective anchor sections + MMR-based gist sentences.  
Outputs:
- `waypoints.parquet`
- `gists.jsonl`

---

## **6. Stitching**
Graph traversal–based summary ordering.  
Output:
- `stitched_list.json`

---

## **7. Summary Generation**
Token-budget-constrained stitched final summary.  
Output:
- `deliverable.json`

---

## **8. LLM Refinement (Optional)**
Refined narrative using Qwen-72B.  
Output:
- `summary_3000_refined_qwen72b.txt`

---

# **Core Methodology**

### Parsing & Normalization
- Cleaning, alignment, hyphen-fixing, header/footer removal  
- Hierarchical section mapping  

### Citation Extraction
- Regex-based detection  
- Canonical section normalization  
- PDF/XML parity comparison  

### Graph Modeling
- Explicit graph = legal citations  
- Implicit graph = embedding similarity  
- Merged graph = structural + semantic signals  

### Summarization
- Lexicon-weighted anchor scoring  
- MMR for gist selection  
- Graph-aware stitching  
- LLM refinement  

---

# **Evaluation & Metrics**

| CFR Part   | Coverage | ROUGE-L | Emb. Sim. | Redundancy |
|-----------|----------|---------|-----------|------------|
| 6 CFR 37  | 76.0%    | 0.239   | 0.625     | 0.091      |
| 6 CFR 115 | 80.6%    | 0.303   | 0.527     | 0.032      |
| 20 CFR 408| 69.0%    | 0.236   | 0.474     | 0.034      |

---

# **Artifacts Generated**

```
sections.jsonl
sections_pdf.jsonl
xml_refs.jsonl
pdf_refs.jsonl
explicit_refs_flat.csv
edges_explicit.parquet
edges_implicit.parquet
edges_merged.parquet
graph.parquet
graph_merged.parquet
graph_metrics.json
terms.parquet
waypoints.parquet
gists.jsonl
stitched_list.json
deliverable.json
summary_3000_refined_qwen72b.txt
parity_report.csv
parity_summary.txt
backend_benchmark.csv
```

Global:
- `artifacts/_eval/summary_eval_3000.csv`
- `artifacts/_eval/summary_eval_3000_report.md`
- `lexicon.yaml`

---

# **Repository Structure**

```
DocuStitch_Capstone/
│── README.md
│── requirements.txt
│── pyproject.toml
│── setup.cfg
│
├── pipeline/
│   ├── eval_summaries.py
│   ├── llm_refine.py
│   ├── merge_graph.py
│   ├── build_implicit.py
│   ├── render_summary.py
│   ├── stitch_list.py
│   ├── build_gists.py
│   ├── build_waypoints.py
│   ├── mine_lexicon.py
│   ├── extract_terms.py
│   ├── build_graph.py
│   ├── edges_from_flat.py
│   ├── compare_sections.py
│   ├── edges_from_refs.py
│   ├── extract_refs.py
│   ├── link_explicit.py
│   ├── parse_pdf_align_select.py
│   ├── parse_xml.py
│   ├── score_refs.py
│   └── summarize.py

│
├── docustitch/
│   ├── parsers/
│   ├── pdf_backends/
│   ├── linking/
│   ├── utils/
│   └── summarization/
│
├── artifacts/
│   ├── cfr_6_37/
│   ├── cfr_6_115/
│   ├── cfr_20_408/
│   ├── _eval/
│   ├── _reports/
│   └── lexicon.yaml
│
└── .venv/
```

---

# **Installation**

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .\.venv\Scripts\activate
pip install -r requirements.txt
```

---

# **Running the Pipeline**
## **Stage-by-Stage Example**

```bash
python pipeline/parse_xml.py --doc-id cfr_6_37
python pipeline/parse_pdf_align_select.py --doc-id cfr_6_37
python pipeline/extract_refs.py --doc-id cfr_6_37
python pipeline/build_graph.py --doc-id cfr_6_37
python pipeline/build_implicit.py --doc-id cfr_6_37
python pipeline/merge_graph.py --doc-id cfr_6_37
python pipeline/extract_terms.py --doc-id cfr_6_37
python pipeline/build_waypoints.py --doc-id cfr_6_37
python pipeline/build_gists.py --doc-id cfr_6_37
python pipeline/stitch_list.py --doc-id cfr_6_37
python pipeline/render_summary.py --doc-id cfr_6_37 --budget 3000
python pipeline/eval_summaries.py --budget 3000
```

---

# **Limitations**

- Implicit graph quality depends on embedding model choice  
- CFR-specific formatting assumptions limit generalization  
- Refinement requires large-model inference endpoints  

---

# **Future Work**

- Graph visualization UI  
- RAG question answering  
- Combined multi-part CFR summarization  
- Custom domain-specific embeddings  
- Supervised summarization models  

---

# **Acknowledgements**

This project uses:

- CFR public datasets  
- PyMuPDF, PDFMiner, pdfplumber  
- BeautifulSoup4  
- SentenceTransformers, Transformers  
- pandas, numpy, networkx  
- Typer, pydantic, rouge-score  
- Qwen-72B for refinement (optional)  
