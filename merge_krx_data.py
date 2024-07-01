import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# DB 연결
engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
con = pymysql.connect(user='root',
                    passwd='1234',
                    host='127.0.0.1',
                    db='stock_db',
                    charset='utf8')
mycursor = con.cursor()

# query = """
#     insert into kor_training_data (ChangeRatio, Symbol, Date, Market, Open, High, Low, Close, Volume, Amount, Stocks, MarketCap, CPI, PPI, GDP, InterestRate, CPS, NetIncome, NetProfit, OperatingProfit, PCHigh, PCLow, PCR, PSR, ROA, ROE, ROIC, SPS, Take, EPS, PER, FWD_EPS, FWD_PER, BPS, PBR, DPS, DY, Financial, Insurance, Investment, PrivateEquity, Bank, OtherFinance, Pension, OtherCorporation, Individual, Foreigner, OtherForeigner)
#     values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
#     on duplicate key update
#     ChangeRatio=new.ChangeRatio, Symbol=new.Symbol, Date=new.Date, Market=new.Market, Open=new.Open, High=new.High, Low=new.Low, Close=new.Close, Volume=new.Volume, Amount=new.Amount, Stocks=new.Stocks, MarketCap=new.MarketCap, CPI=new.CPI, PPI=new.PPI, GDP=new.GDP, InterestRate=new.InterestRate, CPS=new.CPS, NetIncome=new.NetIncome, NetProfit=new.NetProfit, OperatingProfit=new.OperatingProfit, PCHigh=new.PCHigh, PCLow=new.PCLow, PCR=new.PCR, PSR=new.PSR, ROA=new.ROA, ROE=new.ROE, ROIC=new.ROIC, SPS=new.SPS, Take=new.Take, EPS=new.EPS, PER=new.PER, FWD_EPS=new.FWD_EPS, FWD_PER=new.FWD_PER, BPS=new.BPS, PBR=new.PBR, DPS=new.DPS, DY=new.DY, Financial=new.Financial, Insurance=new.Insurance, Investment=new.Investment, PrivateEquity=new.PrivateEquity, Bank=new.Bank, OtherFinance=new.OtherFinance, Pension=new.Pension, OtherCorporation=new.OtherCorporation, Individual=new.Individual, Foreigner=new.Foreigner, OtherForeigner=new.OtherForeigner
# """
query = """
    insert into kor_test_data_code (ChangeCode, Symbol, Date, Market, Open, High, Low, Close, Volume, Amount, Stocks, MarketCap, CPI, PPI, GDP, InterestRate, CPS, NetIncome, NetProfit, OperatingProfit, PCHigh, PCLow, PCR, PSR, ROA, ROE, ROIC, SPS, Take, EPS, PER, FWD_EPS, FWD_PER, BPS, PBR, DPS, DY, Financial, Insurance, Investment, PrivateEquity, Bank, OtherFinance, Pension, OtherCorporation, Individual, Foreigner, OtherForeigner)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    ChangeCode=new.ChangeCode, Symbol=new.Symbol, Date=new.Date, Market=new.Market, Open=new.Open, High=new.High, Low=new.Low, Close=new.Close, Volume=new.Volume, Amount=new.Amount, Stocks=new.Stocks, MarketCap=new.MarketCap, CPI=new.CPI, PPI=new.PPI, GDP=new.GDP, InterestRate=new.InterestRate, CPS=new.CPS, NetIncome=new.NetIncome, NetProfit=new.NetProfit, OperatingProfit=new.OperatingProfit, PCHigh=new.PCHigh, PCLow=new.PCLow, PCR=new.PCR, PSR=new.PSR, ROA=new.ROA, ROE=new.ROE, ROIC=new.ROIC, SPS=new.SPS, Take=new.Take, EPS=new.EPS, PER=new.PER, FWD_EPS=new.FWD_EPS, FWD_PER=new.FWD_PER, BPS=new.BPS, PBR=new.PBR, DPS=new.DPS, DY=new.DY, Financial=new.Financial, Insurance=new.Insurance, Investment=new.Investment, PrivateEquity=new.PrivateEquity, Bank=new.Bank, OtherFinance=new.OtherFinance, Pension=new.Pension, OtherCorporation=new.OtherCorporation, Individual=new.Individual, Foreigner=new.Foreigner, OtherForeigner=new.OtherForeigner
"""

# 가격정보 불러오기
price_df = pd.read_sql("""
select * from kor_ref;
""", con=engine)

# 지표 불러오기
indicator_df = pd.read_sql("""
select * from kor_naver;
""", con=engine)

# 투자자 불러오기
trader_df = pd.read_sql("""
select * from kor_krx_data;
""", con=engine)
trader_df = trader_df.drop(['Close'], axis=1)

df_merged = pd.merge(price_df, trader_df, on=['Date', 'Symbol'], how='outer')
df_merged = df_merged.dropna(axis=0, subset=['Close', 'Changes'])

df_merged = pd.merge(df_merged, indicator_df, on=['Date', 'Symbol'], how='outer')
df_merged = df_merged.dropna(axis=0, subset=['Close', 'Changes'])
# df_merged = df_merged[['ChangeRatio', 'Symbol', 'Date', 'Market', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'Stocks', 'MarketCap', 'CPI', 'PPI', 'GDP', 'InterestRate', 'CPS', 'NetIncome', 'NetProfit', 'OperatingProfit', 'PCHigh', 'PCLow', 'PCR', 'PSR', 'ROA', 'ROE', 'ROIC', 'SPS', 'Take', 'EPS', 'PER', 'FWD_EPS', 'FWD_PER', 'BPS', 'PBR', 'DPS', 'DY', 'Financial', 'Insurance', 'Investment', 'PrivateEquity', 'Bank', 'OtherFinance', 'Pension', 'OtherCorporation', 'Individual', 'Foreigner', 'OtherForeigner']]
df_merged = df_merged[['ChangeCode', 'Symbol', 'Date', 'Market', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'Stocks', 'MarketCap', 'CPI', 'PPI', 'GDP', 'InterestRate', 'CPS', 'NetIncome', 'NetProfit', 'OperatingProfit', 'PCHigh', 'PCLow', 'PCR', 'PSR', 'ROA', 'ROE', 'ROIC', 'SPS', 'Take', 'EPS', 'PER', 'FWD_EPS', 'FWD_PER', 'BPS', 'PBR', 'DPS', 'DY', 'Financial', 'Insurance', 'Investment', 'PrivateEquity', 'Bank', 'OtherFinance', 'Pension', 'OtherCorporation', 'Individual', 'Foreigner', 'OtherForeigner']]
df_merged = df_merged.replace({np.nan: 0})
# print(df_merged)

args_ref = df_merged.values.tolist()
mycursor.executemany(query, args_ref)
con.commit()

con.close()