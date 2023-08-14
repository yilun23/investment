import pandas as pd
import yfinance as yf
import time
from tqdm import tqdm
import sqlite3

result = r"C:\Users\Brian\Desktop\\"

# ===============================================================================================================
#取得台股上市資訊
def get_twsk_symbols():
    symbol_link = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data'
    symbols = pd.read_csv(symbol_link)
    return symbols

#取得台股櫃買資訊
def get_twsk_o_symbols():
    symbol_link_o = "https://www.tpex.org.tw/web/stock/trading/intraday_trading/intraday_trading_list_result.php?l=zh-tw&o=data"
    symbols = pd.read_csv(symbol_link_o)
    return symbols
# ===============================================================================================================
def get_twsk():
    #上市
    stock_list = get_twsk_symbols()
    stock_list = stock_list[["證券代號", "證券名稱"]]

    #歷史股票資料
    stock_list.columns = ['Stock_id', 'Name']
    data = pd.DataFrame()
    for i in tqdm(range(len(stock_list))):
        # 抓取股票資料
        stock_id = stock_list.loc[i, 'Stock_id'] + '.TW'
        df = yf.Ticker(stock_id).history(period="max")
        # 增加股票代號
        df['Stock_id'] = stock_list.loc[i, 'Stock_id']
        # 合併
        data = pd.concat([data, df])
        time.sleep(0.8)

    #資料處理
    data.reset_index(inplace=True)
    data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Stock_id']]
    data["Date"] = data["Date"].dt.date
    data = data.round({"Open":3, "High":3, "Low":3, "Close":3})
    return data


def get_twsk_o():
    #櫃買
    stock_list_o = get_twsk_o_symbols()
    stock_list_o = stock_list_o[["證券代號", "證券名稱"]]

    #歷史股票資料
    stock_list_o.columns = ['Stock_id', 'Name']
    data_o = pd.DataFrame()
    for i in tqdm(range(len(stock_list_o))):
        # 抓取股票資料
        stock_id_o = stock_list_o.loc[i, 'Stock_id'] + '.TWO'
        df_o = yf.Ticker(stock_id_o).history(period="max")
        # 增加股票代號
        df_o['Stock_id'] = stock_list_o.loc[i, 'Stock_id']
        # 合併
        data_o = pd.concat([data_o, df_o])
        time.sleep(0.8)

    #資料處理
    data_o.reset_index(inplace=True)
    data_o = data_o[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Stock_id']]
    data_o["Date"] = data_o["Date"].dt.date
    data_o = data_o.round({"Open":3, "High":3, "Low":3, "Close":3})
    return data_o




# ===============================================================================================================
# =============================================================================
# #上市歷史股價
# historicaldata = get_twsk()
# #寫入csv
# historicaldata.to_csv(result + '/twsk__historicaldata.csv', index=False)
#
#
#
#
# #上櫃歷史股價
# historicaldata_o = get_twsk_o()
# #寫入csv
# historicaldata_o.to_csv(result + '/twsk_o_historicaldata.csv', index=False)
# =============================================================================


# =============================================================================
# #新增進資料庫
# conn = sqlite3.connect('money.db')
# data.to_sql('Twsk', conn, if_exists='append', index=False)
# conn.close()
# =============================================================================










# =============================================================================
# historical_data[historical_data["Stock_id"].isin(["2330"])].sort_values(["Date"], ascending=True)
# #historical_data.columns
# a = historical_data[historical_data["Stock_id"].isin(["2330"])].sort_values(["Date"], ascending=True)
# a.to_csv(result + '/a.csv', index=False)
# historical_data.rename(columns={'stock_id': 'Stock_id'})
# #新增列名
# historical_data.reset_index(inplace=True)
# =============================================================================