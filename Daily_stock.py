import pandas as pd
import requests
from tqdm import tqdm
from datetime import datetime
import datetime
import time

from Yfinance import get_twsk_symbols,get_twsk_o_symbols


def get_daily_stock():
    symbols = get_twsk_symbols()

    today = datetime.date.today()
    data = pd.DataFrame()
    df_symbols = pd.DataFrame()
    for i in tqdm(range(len(symbols))):
        cur_symbol = symbols["證券代號"][i]
        url = f"https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.stockList;autoRefresh=1690365661267;fields=avgPrice%2Corderbook;symbols={cur_symbol}.TW?bkt=&device=desktop&ecma=modern&feature=useNewQuoteTabColor%2CenableNewPk&intl=tw&lang=zh-Hant-TW&partner=none&prid=2joh3c9ic1rj5&region=TW&site=finance&tz=Asia%2FTaipei&ver=1.2.1942&returnMeta=true"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
        res = requests.get(url,headers=headers)
        current = res.json()

        try:
            df_stock = pd.DataFrame({
                        'Date':today,
                        'Open':current['data'][0]['regularMarketOpen'],
                        'High':current['data'][0]['regularMarketDayHigh'],
                        'Low':current['data'][0]['regularMarketDayLow'],
                        'Close':current['data'][0]['price'],
                        'Volume':current['data'][0]['volume']
                    },index=[0])
        except:
            df_stock = pd.DataFrame({
                        'Date':today,
                        'Open':'-',
                        'High':'-',
                        'Low':'-',
                        'Close':'-',
                        'Volume':'-'
                    },index=[0])
        data = pd.concat([data, df_stock])
        df_symbols = pd.concat([df_symbols, pd.DataFrame([cur_symbol], columns=['Stock_id'])])
        time.sleep(0.06)
    df = pd.concat([data, df_symbols], join = 'outer', axis = 1)
    df.reset_index(inplace=True,drop=True)
    return df


def get_daily_stock_o():
    symbols = get_twsk_o_symbols()

    today = datetime.date.today()
    data = pd.DataFrame()
    df_symbols = pd.DataFrame()
    for i in tqdm(range(len(symbols))):
        cur_symbol = symbols["證券代號"][i]
        url = f"https://tw.stock.yahoo.com/_td-stock/api/resource/StockServices.stockList;autoRefresh=1690365661267;fields=avgPrice%2Corderbook;symbols={cur_symbol}.TWO?bkt=&device=desktop&ecma=modern&feature=useNewQuoteTabColor%2CenableNewPk&intl=tw&lang=zh-Hant-TW&partner=none&prid=2joh3c9ic1rj5&region=TW&site=finance&tz=Asia%2FTaipei&ver=1.2.1942&returnMeta=true"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
        res = requests.get(url,headers=headers)
        current = res.json()

        try:
            df_stock = pd.DataFrame({
                        'Date':today,
                        'Open':current['data'][0]['regularMarketOpen'],
                        'High':current['data'][0]['regularMarketDayHigh'],
                        'Low':current['data'][0]['regularMarketDayLow'],
                        'Close':current['data'][0]['price'],
                        'Volume':current['data'][0]['volume']
                    },index=[0])
        except:
            df_stock = pd.DataFrame({
                        'Date':today,
                        'Open':'-',
                        'High':'-',
                        'Low':'-',
                        'Close':'-',
                        'Volume':'-'
                    },index=[0])
        data = pd.concat([data, df_stock])
        df_symbols = pd.concat([df_symbols, pd.DataFrame([cur_symbol], columns=['Stock_id'])])
        time.sleep(0.12)
    df = pd.concat([data, df_symbols], join = 'outer', axis = 1)
    df.reset_index(inplace=True,drop=True)
    #df['Date'] = pd.to_datetime(df.Date, format='%Y-%m-%d')
    return df
















