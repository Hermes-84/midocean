from __future__ import annotations
import csv, json, os, re, tempfile
from pathlib import Path
import xml.etree.ElementTree as ET
import boto3
from botocore.config import Config
from build_exports_clean import build_from_source_root

BUCKET='toppoint-xml'; REGION='eu-north-1'; ENDPOINT='https://s3-eu-north-1.amazonaws.com'
SOURCES='''EUR/feed-v4/Products_v4.xml
EUR/feed-v4/ProductTranslations_v4.xml
EUR/feed-v4/ProductPrices_v4.xml
EUR/feed-v4/Colors_v4.xml
EUR/feed-v4/Categories_v4.xml
EUR/feed-v4/Print_v4.xml
EUR/feed-v4/PositionTranslations_v4.xml
EUR/feed-v3/Products_v3.xml
EUR/feed-v3/ProductTranslations_v3.xml
EUR/feed-v3/Print_v3.xml
EUR/feed-v3/PositionTranslations_v3.xml
EUR/feed-v3/colors.xml
EUR/feed-v3/categories.xml
EUR/product_images.xml
EUR/ProductionTimes.xml'''.splitlines()
FOREIGN=('DA_','DE_','EN_','ES_','FI_','FR_','NL_','NO_','PL_','SE_','PT_')

def txt(e,n):
    x=e.find(n) if e is not None else None
    return (x.text or '').strip() if x is not None and x.text else ''

def download(root):
    s=boto3.client('s3',aws_access_key_id=os.environ['TOPPOINT_AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['TOPPOINT_AWS_SECRET_ACCESS_KEY'],region_name=REGION,endpoint_url=ENDPOINT,config=Config(s3={'addressing_style':'path'}))
    for key in SOURCES:
        p=root/key; p.parent.mkdir(parents=True,exist_ok=True); s.download_file(BUCKET,key,str(p))

def read(path):
    with path.open(encoding='utf-8-sig',newline='') as f:
        r=csv.DictReader(f); return r.fieldnames or [],list(r)

def main():
    out=Path('out/toppoint-clean-qa'); out.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory() as t:
        root=Path(t); download(root); src=root/'EUR'; exp=root/'exports'
        result=build_from_source_root(src,exp)
        headers,rows=read(exp/'Products.csv'); dh,drows=read(exp/'DPO PRINT.csv')
        v3=ET.parse(src/'feed-v3'/'Products_v3.xml').getroot()
        social={(txt(n,'Product_Id'),txt(n,'Color_Code')):txt(n,'Social_Compliance') for n in v3.find('Products').findall('Product')}
        assert len(rows)==len(social)==5827
        foreign=[h for h in headers if any(re.search(r'(?:^|__)'+re.escape(x),h) for x in FOREIGN)]
        assert not foreign,foreign[:20]
        assert not {'v4_it__title','v4_it__description','v4_it__search_term'}.intersection(headers)
        mism=[]; social_values=[]
        for r in rows:
            k=(r['products__product__product_id'],r['products__product__color_code']); a=r['products__product__social_compliance']; e=social.get(k,'')
            if a: social_values.append(a)
            if a!=e: mism.append((k,e,a))
        assert not mism,mism[:5]
        assert any('amfori bsci' in x.lower() for x in social_values)
        metal={r['products__product__metal_parts'] for r in rows if r['products__product__metal_parts']}
        assert metal<={'Sì','No'},metal
        audit='v4__Sustainability_Compliance__From_Social_Audited_Factory'; assert audit in headers
        audits={r[audit] for r in rows if r[audit]}; assert audits<={'Sì','No'},audits
        sig={}; dup=[]
        for h in headers:
            s=tuple(r[h] for r in rows)
            if h not in headers[:119] and s in sig: dup.append((h,sig[s]))
            sig.setdefault(s,h)
        assert not dup,dup[:10]
        pr=ET.parse(src/'feed-v4'/'Print_v4.xml').getroot(); source_groups={txt(n,'Print_Group') for n in pr.findall('./Products/Product/Positions/Position')}
        output_groups={r[dh[2]] for r in drows if r[dh[2]]}; assert output_groups<=source_groups
        if 'DPN_DW1' in source_groups: assert 'DPN_DW1' in output_groups
        if 'DPN-DW1' not in source_groups: assert 'DPN-DW1' not in output_groups
        report={'products_rows':len(rows),'products_columns':len(headers),'new_columns':len(headers)-119,'dpo_rows':len(drows),'social_examples':sorted(set(social_values))[:12],'metal_parts_values':sorted(metal),'social_audit_values':sorted(audits),'boolean_columns':result['products']['boolean_columns'],'semantic_duplicates_removed':len(result['products']['semantic_duplicate_columns']),'exact_duplicates_removed':len(result['products']['exact_duplicate_columns']),'foreign_language_columns':foreign}
        (out/'qa.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8'); (out/'headers.txt').write_text('\n'.join(headers),encoding='utf-8')
        print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0
if __name__=='__main__': raise SystemExit(main())
