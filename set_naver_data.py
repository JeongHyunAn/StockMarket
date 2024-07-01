import pymysql
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import numpy as np
from tqdm import tqdm

# DB 연결
engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
con = pymysql.connect(user='root',
                    passwd='1234',
                    host='127.0.0.1',
                    db='stock_db',
                    charset='utf8')
mycursor = con.cursor()

# 티커리스트 불러오기
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker)
	and 종목구분 = '보통주';
""", con=engine)

# 지표 쿼리

naver_df = pd.read_sql("""
select Symbol, Date, Take, OperatingProfit, NetProfit, CPS, PCR, PCHigh, PCLow, ROA, ROE, ROIC, NetIncome, PSR, SPS from kor_ref2;
""", con=engine)

query = """
    insert into kor_naver (Date, Symbol, CPS, NetIncome, NetProfit, OperatingProfit, PCHigh, PCLow, PCR, PSR, ROA, ROE, ROIC, SPS, Take)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    Date=new.Date, Symbol=new.Symbol, CPS=new.CPS, NetIncome=new.NetIncome, NetProfit=new.NetProfit, OperatingProfit=new.OperatingProfit, PCHigh=new.PCHigh, PCLow=new.PCLow, PCR=new.PCR, PSR=new.PSR, ROA=new.ROA, ROE=new.ROE, ROIC=new.ROIC, SPS=new.SPS, Take=new.Take
"""

# Create two datetime objects
year_li = [2020, 2021, 2022]
start_mo = [1, 1, 1]
end_mo = [4, 4, 4]

date_df = pd.DataFrame({'Date': ['2019-12-31']})
date_df['Date'] = pd.to_datetime(date_df['Date'], format='%Y-%m-%d')

for i in range(0, len(year_li)):
    start_date = datetime(year_li[i], start_mo[i], start_mo[i]).date()
    end_date = datetime(year_li[i], end_mo[i], start_mo[i]).date()

    # Calculate the number of days between the two dates
    delta = end_date - start_date
    num_days = delta.days

    df = pd.DataFrame({'Date':pd.date_range(str(start_date), periods=num_days)})
    date_df = pd.concat([date_df, df])

start_date = datetime(2023, 1, 1).date()
end_date = datetime(2024, 4, 1).date()

# Calculate the number of days between the two dates
delta = end_date - start_date
num_days = delta.days

df = pd.DataFrame({'Date':pd.date_range(str(start_date), periods=num_days)})
date_df = pd.concat([date_df, df])
date_df.reset_index(drop=True, inplace=True)
# print(date_df)

concat_df = pd.DataFrame()

merging_df = naver_df[naver_df.columns.difference(date_df.columns)]
date_col = naver_df.pop('Date')
merging_df.insert(0, 'Date', date_col)
merging_df['Date'] = pd.to_datetime(merging_df['Date'], format='%Y-%m-%d')

for i in tqdm(range(0, len(ticker_list))):
    ticker = ticker_list['종목코드'][i]

    df = date_df.copy()
    df['Symbol'] = ticker
    extract_df = merging_df[merging_df['Symbol'] == ticker]

    df_merged = pd.merge(df, extract_df, on=['Date', 'Symbol'], how='outer')
    df_merged = df_merged.sort_values(by='Date')
    df_merged.fillna(method='ffill', inplace=True)
    df_merged = df_merged.replace({np.nan: None})
    # print(df_merged)

    args_ref = df_merged.values.tolist()
    mycursor.executemany(query, args_ref)
    con.commit()

con.close()