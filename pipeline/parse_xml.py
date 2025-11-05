# pipeline/parse_xml.py
import argparse, json, os, re, requests
from urllib.parse import urlparse
from docustitch.parsers.xml_parser import parse_xml_text

PART_URL_RX = re.compile(r"(?:^|[-_/])part(\d+)\.xml$", re.IGNORECASE)

def derive_volume_url(part_url: str) -> tuple[str|None, str|None]:
    """
    Given a part-level URL like:
      .../CFR-2025-title6-vol1/xml/CFR-2025-title6-vol1-part37.xml
    return:
      (volume_url, part_num_str) ->
      .../CFR-2025-title6-vol1/xml/CFR-2025-title6-vol1.xml, "37"
    If we can't infer, return (None, None).
    """
    m = PART_URL_RX.search(part_url)
    if not m:
        return None, None
    part_num = m.group(1)
    # swap trailing "-partNNN.xml" with ".xml"
    volume_url = re.sub(r"-part\d+\.xml$", ".xml", part_url, flags=re.IGNORECASE)
    return volume_url, part_num

def filter_by_part(sections: list[dict], part_num: str) -> list[dict]:
    """
    Keep only sections whose label/ID belongs to the requested Part.
    We match the first integer after '§'.
    """
    want = str(part_num).strip()
    out = []
    for s in sections:
        label = (s.get("label") or s.get("sec_id") or "").replace(" ", "")
        # §37.3, §115.10(b), etc. -> capture "37" / "115"
        m = re.search(r"§\s*([0-9]+)", label)
        if m and m.group(1) == want:
            out.append(s)
            continue
        # also accept prefix "37." / "115."
        m2 = re.search(r"§\s*([0-9]+)\.", label)
        if m2 and m2.group(1) == want:
            out.append(s)
    return out

def fetch_text(uri: str) -> str:
    if uri.startswith("http"):
        r = requests.get(uri, timeout=180)
        r.raise_for_status()
        return r.text
    with open(uri, "r", encoding="utf-8") as f:
        return f.read()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml", required=True, help="GovInfo XML URL (part or volume) or local file")
    ap.add_argument("--doc-id", required=True, help="e.g., cfr_6_37")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # 1) Try to parse whatever was passed
    xml_text = fetch_text(args.xml)
    sections = parse_xml_text(xml_text, args.doc_id)
    print(f"[parse_xml] initial parse: {len(sections)} sections")

    # 2) If zero sections AND URL looks like a part URL, auto-fallback:
    #    derive volume URL + part number, fetch volume, parse all, then filter to part.
    if len(sections) == 0 and args.xml.startswith("http"):
        vol_url, part_num = derive_volume_url(args.xml)
        if vol_url and part_num:
            try:
                print(f"[parse_xml] zero sections from part URL; trying volume: {vol_url} (part {part_num})")
                vol_text = fetch_text(vol_url)
                vol_sections = parse_xml_text(vol_text, args.doc_id)
                print(f"[parse_xml] volume parse found {len(vol_sections)} sections; filtering to Part {part_num}")
                sections = filter_by_part(vol_sections, part_num)
                print(f"[parse_xml] after filter: {len(sections)} sections")
            except Exception as e:
                print(f"[parse_xml] volume fallback failed: {e}")

    # 3) Write JSONL
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for s in sections:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")
    print(f"Wrote {len(sections)} sections → {args.out}")

if __name__ == "__main__":
    main()
