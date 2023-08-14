import pandas as pd
import sqlite3

from Calculate_buyandsell_buyandsell import get_buy,get_sell
from Calculate_volume import twsk_tunover_analysis,twsk_o_tunover_analysis
# ===============================================================================================================


result = r"C:\Users\Brian\Desktop\\"
yday = "11"           #起始日
month = "08"          #起始月
ynote =  month + yday 
#year = "2023"
#ynote = year + '-' + month + '-' + yday 
#ynote = datetime.datetime.strptime(ynote, "%Y-%m-%d").date()


######外資買賣超明細
# ===============================================================================================================
conn = sqlite3.connect('share_detail.db')
data = pd.read_sql("SELECT * FROM share_detail WHERE 日期 BETWEEN '2023-08%' and '2023-09%'", conn)
date_list = data['日期'].drop_duplicates().sort_values(ascending=False).reset_index(drop=True).head(31)

current = pd.read_sql(f"SELECT * FROM share_detail WHERE 日期 LIKE '{date_list[0]}'", conn)
pre_day = pd.read_sql(f"SELECT * FROM share_detail WHERE 日期 LIKE '{date_list[1]}'", conn)
merge = pd.merge(left=current, right=pre_day, on='股票代號', how='inner')
df_merge = merge.iloc[:,0:6]
df_merge.columns = ['日期', '股票名稱', '股票代號', '買賣超張數', '持股張數', '持股比例']
df_merge.insert(len(df_merge.columns),'1日持股', value=merge.iloc[:,len(merge.columns)-1])

for i in range(len(date_list)-2): 
    pre_day = pd.read_sql(f"SELECT * FROM share_detail WHERE 日期 LIKE '{date_list[i+2]}'", conn)
    merge = pd.merge(left=df_merge, right=pre_day, on='股票代號', how='inner')
    df_merge = merge.iloc[:,0:6+int(i+1)]
    df_merge.rename(columns={'日期_x':'日期', '股票名稱_x':'股票名稱', '買賣超張數_x':'買賣超張數', '持股張數_x':'持股張數', '持股比例_x':'持股比例', }, inplace=True)
    df_merge.insert(len(df_merge.columns),f'{i+2}日持股', value=merge.iloc[:,len(merge.columns)-1])
conn.close()

# ===============================================================================================================
# =============================================================================
# backup = pd.read_excel(result + "back.xlsx")
# df_merge = pd.merge(left=current, right=backup, on='股票代號', how='inner')
# ##shares
# shares = current
# shares["Shares_outstandings"] = current.apply(lambda row:0 if int(row['持股張數']) == 0 or float(row['持股比例'].replace("%","")) == 0 else   int(int(row['持股張數']) / (float(row['持股比例'].replace("%",""))/100) * 1000), axis=1)
# shares = shares.iloc[:,lambda shares:[2,6]]
# shares = shares.rename(columns = {"股票代號":"Stock_id"})
# conn = sqlite3.connect('money.db')
# shares.to_sql('Twsk_all_shares', conn, if_exists='append', index=False) 
# conn.close()
# =============================================================================

# ===============================================================================================================


###合併外資、投信、主力一日到三十日買賣超排行
#######外資買賣超#######
#買超
###外資D, 投信DD, 主力F
###上市=0, 櫃買=1
dic = ["D", "DD", "F"]
for key in dic:
    df1 = get_buy(key, 0)
for key in dic:    
    df2 = get_buy(key, 1)
#賣超
###外資DA, 投信DE, 主力FA
###上市=0, 櫃買=1
dic = ["DA", "DE", "FA"]
for key in dic:    
    df3 = get_sell(key, 0)
for key in dic: 
    df4 = get_sell(key, 1)



dfs = [df2, df3, df4]
df_final = pd.merge(left=df_merge, right=df1, on='股票代號', how='outer')
for df in dfs:
    df_final = df_final.merge(df, on='股票代號', how='outer')


wb = pd.ExcelWriter(result + '籌碼明細 {}.xlsx'.format(ynote), engine = 'xlsxwriter')
df_final.to_excel(wb,index = False,sheet_name="籌碼明細 ")


# ===============================================================================================================


#######成交量分析#######括號內填倍率
###上市
Stock_turnover = twsk_tunover_analysis(2)
#寫入csv
Stock_turnover.to_excel(wb,index = False,sheet_name="上市成交量分析")
###櫃買
Stock_o_turnover = twsk_o_tunover_analysis(2)
#寫入csv
Stock_o_turnover.to_excel(wb,index = False,sheet_name="櫃買成交量分析")
wb.close()
















# =============================================================================
# current["股數"] = current.apply(lambda row:0 if int(row['持股張數']) == 0 or float(row['持股比例'].replace("%","")) == 0 else int( int(row['持股張數']) / float(float(row['持股比例'].replace("%",""))/100) * 1000), axis=1)
# df = current[["股票代號", "股數"]]
# df.columns = ["Stock_id", "Shares_outstandings"]
# conn = sqlite3.connect('money.db')
# df.to_sql('Twsk_all_shares', conn, if_exists='append', index=False) 
# =============================================================================














































