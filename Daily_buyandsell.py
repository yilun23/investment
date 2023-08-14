import pandas as pd
import requests
import sqlite3
import datetime
import datacompy


year = "2023"
def get_daily_buyandsell_detail(year):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
    url = 'https://fubon-ebrokerdj.fbs.com.tw/Z/ZE/ZEE/ZEE.djhtm'
    r = requests.get(url, headers=headers)
    df = pd.read_html(r.text)[2]
    df = df.rename(columns = df.iloc[1])
    tday = df.iloc[0,0].split(' ')[2].replace('/', '-')
    note = year + "-" + tday
    df = df.drop(index=[0,1])
    df.reset_index(inplace=True,drop=True)
    df.insert(0, '日期', value=datetime.datetime.strptime(note, "%Y-%m-%d").date())
    df.insert(2, '股票代號', value = df['股票名稱'].str.split(' ').str.get(0).apply(lambda x:x[:6]))
    df = df.fillna(0)
    return df




#補資料(暫不需要)舊的沒資料代表不活躍，有新的資料但線圖還不能看出走向。
# =============================================================================
#     conn = sqlite3.connect('share_detail.db')
#     data = pd.read_sql(f"SELECT * FROM share_detail WHERE 日期 < '{note}'", conn)
#     predate = data['日期'].drop_duplicates().sort_values(ascending=False).reset_index(drop=True).head(1)
#     preday = pd.read_sql(f"SELECT * FROM share_detail WHERE 日期 LIKE '{predate[0]}'", conn)
#     compare = datacompy.Compare(df, preday, join_columns='股票代號')
#
#
#
#     ###df1_unq_rows=前面的df獨有的數據, df2_unq_rows=後面的df獨有的數據
#     #尋找今天無資料但最近一天資料有的
#     compare.df2_unq_rows['日期'] = datetime.datetime.strptime(note, "%Y-%m-%d").date()
#     new_df = pd.concat([df, compare.df2_unq_rows])
#     new_df.reset_index(inplace=True,drop=True)
#     new_df.to_sql('share_detail', conn, if_exists='append', index=False)
#
#
#     compare.df1_unq_rows['日期'] = predate[0]
#     new_preday = compare.df1_unq_rows
#     new_preday.reset_index(inplace=True,drop=True)
#     new_preday.to_sql('share_detail', conn, if_exists='append', index=False)
#     conn.close()
# =============================================================================













def get_daily_buy(year, category, mode):
    if mode == 0:
        label = "上市"
    elif mode == 1:
        label = "櫃買"
    dic = {"D":"外資", "DD":"投信", "F":"主力"}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
    conn = sqlite3.connect('money.db')
    conn1 = sqlite3.connect('shares.db')
    botton = ['1']
    #抓取外資買超數據
    for days in botton:
        url = f'https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_{category}_{mode}_{days}.djhtm'
        r = requests.get(url, headers=headers)
        df = pd.read_html(r.text)[2]
        df = df.rename(columns = df.iloc[1])
        tday = df.iloc[0,1].split('：')[1].replace('/', '-')
        note = year + "-" + tday
        df = df.drop(index=[0,1])
        df.reset_index(inplace=True,drop=True)
        if category == "DD" and mode == 0:
            df = df.iloc[:,lambda df: [0,1,2,7]]
        elif category == "F":
            df = df.iloc[:,lambda df: [0,1,2,7]]
        else:
            df = df.iloc[:,lambda df: [0,1,2,5]]
        df['股票代號'] = df['股票名稱'].str.split(' ').str.get(0)
        df['股票代號'] = df['股票代號'].apply(lambda x:x[:6])
        df.columns = ['Rank', 'Stock_name', 'Close', 'Over_volume', 'Stock_id']
        df['Over_volume'] = df.apply(lambda row:int(row['Over_volume'])*1000, axis=1)
        df.insert(0, 'Date', value=datetime.datetime.strptime(note, "%Y-%m-%d").date())
        df = df[['Date', 'Rank', 'Stock_name', 'Stock_id', 'Close', 'Over_volume']]


        #針對股票代號查詢在外流通股數
        df_shares = pd.DataFrame()
        for cur_symbol in df['Stock_id']:
            data = pd.read_sql(f"SELECT * FROM Twsk_all_shares WHERE Stock_id LIKE '{cur_symbol}'", conn)
            df_shares = pd.concat([df_shares, data], axis=0)

        #合併並計算占比
        merge = pd.merge(left=df, right=df_shares, on='Stock_id', how='inner')
        merge['Percent'] = merge.apply(lambda row:0 if float(row['Shares_outstandings']) == 0 else '%.3f%%' % (float(row['Over_volume'])/float(row['Shares_outstandings'])*100), axis=1)
        #寫入SQL
        merge.to_sql('{}買超{}'.format(label, dic[category]), conn1, if_exists='append', index=False)
    conn.close()
    conn1.close()

#賣超
def get_daily_sell(year, category, mode):
    if mode == 0:
        label = "上市"
    elif mode == 1:
        label = "櫃買"
    dic = {"DA":"外資", "DE":"投信", "FA":"主力"}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
    conn = sqlite3.connect('money.db')
    conn1 = sqlite3.connect('shares.db')
    botton = ['1']
    #抓取外資買超數據
    for days in botton:
        url = f'https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_{category}_{mode}_{days}.djhtm'
        r = requests.get(url, headers=headers)
        df = pd.read_html(r.text)[2]
        df = df.rename(columns = df.iloc[1])
        tday = df.iloc[0,1].split('：')[1].replace('/', '-')
        note = year + "-" + tday
        df = df.drop(index=[0,1])
        df.reset_index(inplace=True,drop=True)
        if category == "DD" and mode == 0:
            df = df.iloc[:,lambda df: [0,1,2,7]]
        elif category == "F":
            df = df.iloc[:,lambda df: [0,1,2,7]]
        else:
            df = df.iloc[:,lambda df: [0,1,2,5]]
        df['股票代號'] = df['股票名稱'].str.split(' ').str.get(0)
        df['股票代號'] = df['股票代號'].apply(lambda x:x[:6])
        df.columns = ['Rank', 'Stock_name', 'Close', 'Over_volume', 'Stock_id']
        df['Over_volume'] = df.apply(lambda row:int(row['Over_volume'])*1000, axis=1)
        df.insert(0, 'Date', value=datetime.datetime.strptime(note, "%Y-%m-%d").date())
        df = df[['Date', 'Rank', 'Stock_name', 'Stock_id', 'Close', 'Over_volume']]


        #針對股票代號查詢在外流通股數
        df_shares = pd.DataFrame()
        for cur_symbol in df['Stock_id']:
            data = pd.read_sql(f"SELECT * FROM Twsk_all_shares WHERE Stock_id LIKE '{cur_symbol}'", conn)
            df_shares = pd.concat([df_shares, data], axis=0)

        #合併並計算占比
        merge = pd.merge(left=df, right=df_shares, on='Stock_id', how='inner')
        merge['Percent'] = merge.apply(lambda row:0 if float(row['Shares_outstandings']) == 0 else '%.3f%%' % (float(row['Over_volume'])/float(row['Shares_outstandings'])*100), axis=1)
        #寫入SQL
        merge.to_sql('{}賣超{}'.format(label, dic[category]), conn1, if_exists='append', index=False)
    conn.close()
    conn1.close()
