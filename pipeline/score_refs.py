import argparse, json, csv, os

def load_map(path):
    m={}
    with open(path,"r",encoding="utf-8") as f:
        for ln in f:
            o=json.loads(ln); m[o["sec_id"].replace(" ","")] = o
    return m

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml-refs", required=True)
    ap.add_argument("--pdf-refs", required=True)
    ap.add_argument("--out-csv", required=True)
    a = ap.parse_args()

    os.makedirs(os.path.dirname(a.out_csv), exist_ok=True)
    xml = load_map(a.xml_refs); pdf = load_map(a.pdf_refs)

    rows=[]
    for sid, x in xml.items():
        xr = set((x.get("explicit_refs") or []))
        pr = set((pdf.get(sid,{}).get("explicit_refs") or []))
        overlap = len(xr & pr)
        only_xml = len(xr - pr); only_pdf = len(pr - xr)
        prec = overlap/len(pr) if pr else (1.0 if not xr else 0.0)
        rec  = overlap/len(xr) if xr else 1.0
        f1 = (2*prec*rec/(prec+rec)) if (prec+rec) else 1.0
        rows.append(dict(sec_id=sid, xml_refs=len(xr), pdf_refs=len(pr),
                         overlap=overlap, only_xml=only_xml, only_pdf=only_pdf,
                         precision=round(prec,3), recall=round(rec,3), f1=round(f1,3)))
    with open(a.out_csv,"w",newline="",encoding="utf-8") as f:
        import csv as _csv
        w=_csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["sec_id"])
        w.writeheader(); w.writerows(rows)
    print(f"Wrote → {a.out_csv} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
