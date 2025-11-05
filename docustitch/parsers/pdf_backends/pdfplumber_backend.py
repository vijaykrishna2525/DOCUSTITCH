import pdfplumber
def extract_lines(pdf_path: str) -> list[str]:
    lines=[]
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
    return [l.rstrip() for l in lines]
