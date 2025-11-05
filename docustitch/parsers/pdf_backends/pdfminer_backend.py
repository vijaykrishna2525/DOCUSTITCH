from pdfminer.high_level import extract_text
def extract_lines(pdf_path: str) -> list[str]:
    text = extract_text(pdf_path) or ""
    return [l.rstrip() for l in text.splitlines()]
