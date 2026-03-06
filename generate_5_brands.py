"""
generate_5_brands.py — Generate dashboard-style PDFs for first 5 brands.
Run: python generate_5_brands.py
"""
import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.ingestion import load_and_clean, filter_by_date, split_by_brand
from modules.kpi import calculate_kpis
from modules.pdf_generator_html import generate_pdf_html

XLS   = 'extracted/February Monthly Report/Raw_Files_From_Tally/febSalesReportData.xls'
START = '2026-02-01'
END   = '2026-02-28'
OUT   = 'output'

os.makedirs(OUT, exist_ok=True)

print("Loading data...")
df_all    = load_and_clean(XLS)
df_ranged = filter_by_date(df_all, START, END)
brand_data = split_by_brand(df_ranged)

brands_list = list(brand_data.keys())
print(f"Total brands with sales: {len(brands_list)}")
print(f"Generating PDFs for first 5: {brands_list[:5]}\n")

for brand_name in brands_list[:5]:
    try:
        kpis = calculate_kpis(brand_data[brand_name])
        safe = brand_name.replace(' ', '_').replace("'", '').replace('/', '-')
        path = os.path.join(OUT, f"{safe}_DashReport_Feb2026.pdf")
        generate_pdf_html(
            output_path=path,
            brand_name=brand_name,
            kpis=kpis,
            start_date=START,
            end_date=END,
        )
        print(f"  OK  {brand_name:40s}  ->  {os.path.basename(path)}")
    except Exception as exc:
        print(f"  ERR {brand_name}: {exc}")
        import traceback; traceback.print_exc()

print("\nDone. Check the output/ folder.")
