import pandas as pd
import requests
import sqlite3
import datetime


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



