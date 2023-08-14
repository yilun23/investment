import pandas as pd
import requests
import sqlite3
from tqdm import tqdm

from Yfinance import get_twsk_symbols,get_twsk_o_symbols



def twsk_tunover_analysis(percent):
    #上市均價
    conn = sqlite3.connect('money.db')
    symbols = get_twsk_symbols()
    df = pd.DataFrame()
    for i in tqdm(range(len(symbols))):
        cur_symbol = symbols["證券代號"][i]
        df_stock = pd.read_sql(f"SELECT * FROM Twsk WHERE Stock_id LIKE '{cur_symbol}'", conn)
        df_stock = df_stock.sort_values('Date', ascending=False)
        df_stock.reset_index(inplace=True,drop=True)
        df_stock['Volume'] = df_stock['Volume'].apply(lambda x:0 if x=="-" else x)
        df_today = df_stock.loc[0]
        current = df_stock["Volume"].loc[0]
        #計算個股均價
        df_five = sum(df_stock["Volume"].loc[1:5])/5
        df_ten = sum(df_stock["Volume"].loc[1:10])/10
        df_twenty = sum(df_stock["Volume"].loc[1:20])/20
        df_thirty = sum(df_stock["Volume"].loc[1:30])/30

        if current >= df_five * percent:
            df = pd.concat([df, df_today], axis=1)
            continue
        elif current >= df_ten * percent:
            df = pd.concat([df, df_today], axis=1)
            continue
        elif current >= df_twenty * percent:
            df = pd.concat([df, df_today], axis=1)
            continue
        elif current >= df_thirty * percent:
            df = pd.concat([df_today], axis=1)
            continue
    #dataframe轉向
    df = df.T
    conn.close()
    return df



def twsk_o_tunover_analysis(percent):
    #上市均價
    conn = sqlite3.connect('money.db')
    symbols = get_twsk_o_symbols()
    df = pd.DataFrame()
    for i in tqdm(range(len(symbols))):
        cur_symbol = symbols["證券代號"][i]
        df_stock = pd.read_sql(f"SELECT * FROM Twsk_O WHERE Stock_id LIKE '{cur_symbol}'", conn)
        df_stock = df_stock.sort_values('Date', ascending=False)
        df_stock.reset_index(inplace=True,drop=True)
        df_stock['Volume'] = df_stock['Volume'].apply(lambda x:0 if x=="-" else x)
        df_today = df_stock.loc[0]
        current = df_stock["Volume"].loc[0]
        #計算個股均價
        df_five = sum(df_stock["Volume"].loc[1:5])/5
        df_ten = sum(df_stock["Volume"].loc[1:10])/10
        df_twenty = sum(df_stock["Volume"].loc[1:20])/20
        df_thirty = sum(df_stock["Volume"].loc[1:30])/30

        if current >= df_five * percent:
            df = pd.concat([df, df_today], axis=1)
            continue
        elif current >= df_ten * percent:
            df = pd.concat([df, df_today], axis=1)
            continue
        elif current >= df_twenty * percent:
            df = pd.concat([df, df_today], axis=1)
            continue
        elif current >= df_thirty * percent:
            df = pd.concat([df_today], axis=1)
            continue
    #dataframe轉向
    df = df.T
    conn.close()
    return df
