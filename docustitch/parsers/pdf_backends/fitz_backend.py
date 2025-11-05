import fitz
def extract_lines(pdf_path: str) -> list[str]:
    lines=[]
    with fitz.open(pdf_path) as doc:
        for p in doc:
            lines.extend((p.get_text("text") or "").splitlines())
    return [l.rstrip() for l in lines]
