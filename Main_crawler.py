import sqlite3

from Yfinance import get_twsk,get_twsk_o
#from Shares_outstandings import get_twsk_shares,get_twsk_o_shares
from Daily_stock import get_daily_stock,get_daily_stock_o
from Daily_buyandsell import get_daily_buy,get_daily_sell,get_daily_buyandsell_detail
result = r"C:\Users\Brian\Desktop\\"

#time_start = "2023-07-19"
#time_end = "2023-07-20"


##############每日成交資訊##############(下午3點)
# ===============================================================================================================

######每日上市成交資訊######
today = get_daily_stock()
#新增進資料庫
conn = sqlite3.connect('money.db')
today.to_sql('Twsk', conn, if_exists='append', index=False) 
conn.close()
# ===============================================================================================================

######每日櫃買成交資訊######
today_o = get_daily_stock_o()
#新增進資料庫
conn = sqlite3.connect('money.db')
today_o.to_sql('Twsk_O', conn, if_exists='append', index=False) 
conn.close()

# ===============================================================================================================





######每日外資買賣超明細######(參數：年)(晚上11點)
# ===============================================================================================================
df_buyandsell_detail = get_daily_buyandsell_detail("2023")

conn = sqlite3.connect('share_detail.db')
df_buyandsell_detail.to_sql('share_detail', conn, if_exists='append', index=False)
conn.close()



# ===============================================================================================================

















#######外資買賣超單次每日######(參數：年, 外資/投信/主力, 上市/櫃買)(晚上六點)
# ===============================================================================================================
#買超
###外資D, 投信DD, 主力F
###上市=0, 櫃買=1
dic = ["D", "DD", "F"]
for key in dic:
    get_daily_buy('2023', key, 0) 
    get_daily_buy('2023', key, 1)

#賣超
###外資DA, 投信DE, 主力FA
###上市=0, 櫃買=1
dic = ["DA", "DE", "FA"]
for key in dic:    
    get_daily_sell('2023', key, 0)
    get_daily_sell('2023', key, 1)

# ===============================================================================================================






























##############歷史股價##############
# ===============================================================================================================
#上市歷史股價
try:
    print('上市歷史股價')
    historicaldata = get_twsk()    
except:
    print("\033[1;31m上市歷史股價資料讀取失敗\033[0m")  
    
#寫入csv
historicaldata.to_csv(result + '/Twsk__historicaldata.csv', index=False)
#新增進資料庫====================================================================================================
conn = sqlite3.connect('money.db')
historicaldata.to_sql('Twsk', conn, if_exists='append', index=False) 
conn.close()
# ===============================================================================================================
#上櫃歷史股價
try:
    print('上櫃歷史股價')
    historicaldata_o = get_twsk_o()
except:
    print("\033[1;31m上櫃歷史股價資料讀取失敗\033[0m")  
    
#寫入csv
historicaldata_o.to_csv(result + '/Twsk_o_historicaldata.csv', index=False)    
#新增進資料庫====================================================================================================
conn = sqlite3.connect('money.db')
historicaldata_o.to_sql('Twsk_O', conn, if_exists='append', index=False) 
conn.close()    
# ===============================================================================================================



























# =============================================================================
# ##############三個月跑一次##############(停用)
# ##############在外流通股數##############
# # ===============================================================================================================
# #上市在外流通
# try:
#     print('上市在外流通')
#     twsk_shares = get_twsk_shares()   
# except:
#     print("\033[1;31m上市在外流通股數資料讀取失敗\033[0m")  
#     
# #寫入csv   
# twsk_shares.to_csv(result + '/Twsk_shares.csv', index=False)
# #新增進資料庫====================================================================================================
# conn = sqlite3.connect('money.db')
# twsk_shares.to_sql('Twsk_shares', conn, if_exists='append', index=False) 
# conn.close()    
# # ===============================================================================================================
# #上櫃在外流通
# try:
#     print('上櫃在外流通')
#     twsk_o_shares = get_twsk_o_shares()
# except:
#     print("\033[1;31m上櫃在外流通股數資料讀取失敗\033[0m")   
#     
# #寫入csv 
# twsk_o_shares.to_csv(result + '/Twsk_o_shares.csv', index=False)
# #新增進資料庫====================================================================================================
# conn = sqlite3.connect('money.db')
# twsk_o_shares.to_sql('Twsk_O_shares', conn, if_exists='append', index=False) 
# conn.close()    
# # ===============================================================================================================
# =============================================================================





