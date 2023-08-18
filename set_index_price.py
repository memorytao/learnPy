import requests
import pandas as pd

stock_symbol = str.upper(input('input stock symbol:'))
main_url = f"https://www.set.or.th/th/market/product/stock/quote/{stock_symbol}/historical-trading"
referer = f"https://www.set.or.th/th/market/product/stock/quote/{stock_symbol}/historical-trading"
price_url = f"https://www.set.or.th/api/set/stock/{stock_symbol}/historical-trading"
landing_page = requests.get(main_url)

resp = requests.get(price_url, cookies=landing_page.cookies,headers={'Referer': referer})

if len(resp.json()) > 1:
    df = pd.DataFrame(resp.json())
    df.to_excel(f'./{stock_symbol}.xlsx', index=False)
else:
    print(" Symbol not found! ")