# 패키지 불러오기
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from functools import reduce

# 파일들 불러오기
kor_cpi = pd.read_csv('./data/kor_CPI_2015_2024.csv', encoding='cp949')
kor_ppi = pd.read_csv('./data/kor_PPI_2015_2024.csv', encoding='cp949')
kor_gdp = pd.read_csv('./data/kor_GDP_2015_2024.csv')
kor_ir = pd.read_csv('./data/kor_interest_2015_2024.csv')

# 데이터 클렌징
kor_cpi['시점'] = kor_cpi['시점'].astype(str).str.replace('.','/')
kor_ppi['시점'] = kor_ppi['시점'].astype(str).str.replace('.','/')

# 날짜 변환
kor_cpi['시점'] = pd.to_datetime(kor_cpi['시점'], format=('%Y/%m'))
kor_ppi['시점'] = pd.to_datetime(kor_ppi['시점'], format=('%Y/%m'))
kor_gdp['변환'] = kor_gdp['변환'].replace(['2020/Q4', '2021/Q1', '2021/Q2', '2021/Q3', '2021/Q4'
                                        , '2022/Q1', '2022/Q2', '2022/Q3', '2022/Q4'
                                        , '2023/Q1', '2023/Q2', '2023/Q3', '2023/Q4'],
                                        ['2020/10', '2021/01', '2021/04', '2021/07', '2021/10'
                                        , '2022/01', '2022/04', '2022/07', '2022/10'
                                        , '2023/01', '2023/04', '2023/07', '2023/10'])
kor_gdp['변환'] = pd.to_datetime(kor_ir['변환'], format=('%Y/%m'))
kor_ir['변환'] = pd.to_datetime(kor_ir['변환'], format=('%Y/%m'))

# 이름 변환
kor_cpi.rename(columns = {'시점': 'Date', '전국': 'CPI'}, inplace=True)
kor_ppi.rename(columns = {'시점': 'Date', '총지수 (2015=100)': 'PPI'}, inplace=True)
kor_gdp.rename(columns = {'변환': 'Date', '원자료': 'GDP'}, inplace=True)
kor_ir.rename(columns = {'변환': 'Date', '원자료': 'InterestRate'}, inplace=True)

# print(kor_cpi.dtypes)
# print(kor_ppi.dtypes)
# print(kor_gdp.dtypes)
# print(kor_ir.dtypes)

kor_marcap = pd.DataFrame()
marcap_list = []

for year in ['15', '16', '17', '18', '19', '20', '21', '22', '23', '24']:
    marcap = pd.read_csv('./data/kor_marcap_20' + year + '.csv')
    marcap_list.append(marcap)

kor_marcap = pd.concat(marcap_list)
kor_marcap['Date'] = pd.to_datetime(kor_marcap['Date'], format='%Y-%m-%d')

# 빈 공간은 제일 최근 값으로 다 채워줌
data_frames = [kor_cpi, kor_ppi, kor_gdp, kor_ir, kor_marcap]
df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['Date'], how='outer'), data_frames)
df_merged['CPI'].fillna(method='ffill', inplace=True)
df_merged['PPI'].fillna(method='ffill', inplace=True)
df_merged['GDP'].fillna(method='ffill', inplace=True)
df_merged['InterestRate'].fillna(method='ffill', inplace=True)

df_merged.drop([0,1], axis=0, inplace=True)
df_merged.reset_index(drop=True, inplace=True)
# print(df_merged.columns.values)
df_merged.rename(columns = {'ChagesRatio': 'ChangeRatio', 'Code': 'Symbol', 'Marcap': 'MarketCap'}, inplace=True)
kor_indicator = df_merged[['Symbol', 'Date', 'Name', 'Market', 'Changes', 'ChangeRatio', 'ChangeCode', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'Stocks', 'MarketCap', 'CPI', 'PPI', 'GDP', 'InterestRate']]

kor_indicator.dropna(subset=['Symbol'], inplace=True)
kor_indicator.reset_index(drop=True, inplace=True)
# print(kor_indicator.columns)

# null_mask = kor_indicator.isnull().any(axis=1)
# null_rows = kor_indicator[null_mask]

# print(null_rows)

# DB 연결
engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
con = pymysql.connect(user='root',
                    passwd='1234',
                    host='127.0.0.1',
                    db='stock_db',
                    charset='utf8')
mycursor = con.cursor()

query = """
    insert into kor_ref (Symbol, Date, Name, Market, Changes, ChangeRatio, ChangeCode, Open, High, Low, Close, Volume, Amount, Stocks, MarketCap, CPI, PPI, GDP, InterestRate)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    Symbol=new.Symbol, Date=new.Date, Name=new.Name, Market=new.Market, Changes=new.Changes, ChangeRatio=new.ChangeRatio, ChangeCode=new.ChangeCode, Open=new.Open, High=new.High, Low=new.Low, Close=new.Close, Volume=new.Volume, Amount=new.Amount, Stocks=new.Stocks, MarketCap=new.MarketCap, CPI=new.CPI, PPI=new.PPI, GDP=new.GDP, InterestRate=new.InterestRate
"""

args = kor_indicator.values.tolist()
mycursor.executemany(query, args)
con.commit()

# DB 연결 종료
engine.dispose()
con.close()