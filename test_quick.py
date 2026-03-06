"""Quick test of the pipeline"""
from modules.ingestion import load_and_clean, filter_by_date, split_by_brand, get_all_brands
from modules.kpi import calculate_kpis

xls = 'extracted/February Monthly Report/Raw_Files_From_Tally/febSalesReportData.xls'
print('Loading data...')
df_all = load_and_clean(xls)
print(f'Loaded {len(df_all)} rows')

df_ranged = filter_by_date(df_all, '2026-02-01', '2026-02-28')
print(f'After date filter: {len(df_ranged)} rows')

all_brands = get_all_brands(df_ranged)
print(f'Total brands in dataset: {len(all_brands)}')

brand_data = split_by_brand(df_ranged)
print(f'Brands with sales: {len(brand_data)}')

zero_sales = sorted(all_brands - set(brand_data.keys()))
print(f'Brands with zero sales: {len(zero_sales)}')
if zero_sales:
    print(f'  Skipped: {zero_sales}')

# Test first brand
first_brand = list(brand_data.keys())[0]
print(f'\nTesting KPI calculation for: {first_brand}')
kpis = calculate_kpis(brand_data[first_brand])
print(f'  Revenue: N{kpis["total_revenue"]:,.0f}')
print(f'  Stores: {kpis["num_stores"]}')
print(f'  Top store: {kpis["top_store_name"]}')
print('\n✓ Pipeline test PASSED')
