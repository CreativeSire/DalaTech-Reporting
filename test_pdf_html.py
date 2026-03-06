"""Test HTML-based PDF generation."""
import os
from modules.ingestion import load_and_clean, filter_by_date, split_by_brand
from modules.kpi import calculate_kpis
from modules.pdf_generator_html import generate_pdf_html

print("Loading data...")
xls = 'extracted/February Monthly Report/Raw_Files_From_Tally/febSalesReportData.xls'
df_all = load_and_clean(xls)
df_ranged = filter_by_date(df_all, '2026-02-01', '2026-02-28')
brand_data = split_by_brand(df_ranged)

print(f"Brands with sales: {len(brand_data)}")

# Test first brand
test_brand = list(brand_data.keys())[0]
print(f"\nGenerating PDF for: {test_brand}")

kpis = calculate_kpis(brand_data[test_brand])
output_path = f'output/{test_brand.replace(" ", "_")}_Test_Report.pdf'

try:
    generate_pdf_html(
        output_path=output_path,
        brand_name=test_brand,
        kpis=kpis,
        start_date='2026-02-01',
        end_date='2026-02-28',
    )
    print(f"PDF saved to: {output_path}")
    print("SUCCESS!")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
