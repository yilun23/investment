import pandas as pd
import requests
import sqlite3



#買超
def get_buy(category, mode):
    if mode == 0:
        label = "上市"
    elif mode == 1:
        label = "櫃買"
    dic = {"D":"外資", "DD":"投信", "F":"主力"}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
    conn = sqlite3.connect('money.db')
    botton = ['1', '5', '10', '30']
    #抓取外資買超數據
    for days in botton:
        url = f'https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_{category}_{mode}_{days}.djhtm'
        r = requests.get(url, headers=headers)
        df = pd.read_html(r.text)[2]
        df = df.rename(columns = df.iloc[1])
        df = df.drop(index=[0,1])
        df.reset_index(inplace=True,drop=True)
        if category == "DD" and mode == 0:
            df = df.iloc[:, lambda df:[0,1,7]]
        elif category == "F":
            df = df.iloc[:, lambda df:[0,1,7]]
        else:
            df = df.iloc[:, lambda df:[0,1,5]]
        df['股票代號'] = df['股票名稱'].str.split(' ').str.get(0)
        df['股票代號'] = df['股票代號'].apply(lambda x:x[:6])
        df.columns = ['名次', '股票名稱', '買賣超張數', '股票代號']
        df['名次'] = df['名次'].astype(int)
        df['買賣超張數'] = df.apply(lambda row:int(row['買賣超張數'])*1000, axis=1)

        #針對股票代號查詢在外流通股數
        df_shares = pd.DataFrame()
        for cur_symbol in df['股票代號']:
            data = pd.read_sql(f"SELECT * FROM Twsk_all_shares WHERE Stock_id LIKE '{cur_symbol}'", conn)
            df_shares = pd.concat([df_shares, data], axis=0)

        #合併並計算占比
        merge = pd.merge(left=df, right=df_shares, left_on='股票代號', right_on='Stock_id', how='inner')
        merge[f'買超{label}{dic[category]}{days}日'] = merge.apply(lambda row:0 if float(row['Shares_outstandings']) == 0 else '%.3f%%' % (float(row['買賣超張數'])/float(row['Shares_outstandings'])*100), axis=1)
        if category=="D" and days=="1":
            global merge_all
            merge_all = merge.iloc[:, lambda merge: [0,3,6]]
            continue
        else:
            merge = merge.iloc[:, lambda merge: [0,3,6]]
            merge_all = pd.merge(left=merge_all, right=merge, on='股票代號', how='outer')

    conn.close()
    return merge_all
#賣超
def get_sell(category, mode):
    if mode == 0:
        label = "上市"
    elif mode == 1:
        label = "櫃買"
    dic = {"DA":"外資", "DE":"投信", "FA":"主力"}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/111.25 (KHTML, like Gecko) Chrome/99.0.2345.81 Safari/123.36'}
    conn = sqlite3.connect('money.db')
    botton = ['1', '5', '10', '30']
    #抓取外資買超數據
    for days in botton:
        url = f'https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_{category}_{mode}_{days}.djhtm'
        r = requests.get(url, headers=headers)
        df = pd.read_html(r.text)[2]
        df = df.rename(columns = df.iloc[1])
        df = df.drop(index=[0,1])
        df.reset_index(inplace=True,drop=True)
        if category == "DD" and mode == 0:
            df = df.iloc[:, lambda df:[0,1,7]]
        elif category == "F":
            df = df.iloc[:, lambda df:[0,1,7]]
        else:
            df = df.iloc[:, lambda df:[0,1,5]]
        df['股票代號'] = df['股票名稱'].str.split(' ').str.get(0)
        df['股票代號'] = df['股票代號'].apply(lambda x:x[:6])
        df.columns = ['名次', '股票名稱', '買賣超張數', '股票代號']
        df['名次'] = df['名次'].astype(int)
        df['買賣超張數'] = df.apply(lambda row:int(row['買賣超張數'])*1000, axis=1)

        #針對股票代號查詢在外流通股數
        df_shares = pd.DataFrame()
        for cur_symbol in df['股票代號']:
            data = pd.read_sql(f"SELECT * FROM Twsk_all_shares WHERE Stock_id LIKE '{cur_symbol}'", conn)
            df_shares = pd.concat([df_shares, data], axis=0)

        #合併並計算占比
        merge = pd.merge(left=df, right=df_shares, left_on='股票代號', right_on='Stock_id', how='inner')
        merge[f'賣超{label}{dic[category]}{days}日'] = merge.apply(lambda row:0 if float(row['Shares_outstandings']) == 0 else '%.3f%%' % (float(row['買賣超張數'])/float(row['Shares_outstandings'])*100), axis=1)
        if category=="DA" and days=="1":
            global merge_all
            merge_all = merge.iloc[:, lambda merge: [0,3,6]]
            continue
        else:
            merge = merge.iloc[:, lambda merge: [0,3,6]]
            merge_all = pd.merge(left=merge_all, right=merge, on='股票代號', how='outer')

    conn.close()
    return merge_all
