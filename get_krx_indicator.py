import requests as rq
import pymysql
from sqlalchemy import create_engine
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
import time

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
where 기준일 = (select max(기준일) from kor_ticker);
""", con=engine)

# done_list = pd.read_sql("""
# select distinct Symbol from kor_krx_data;
# """, con=engine).stack().tolist()

# print(done_list)

# DB저장 쿼리
query = f"""
    insert into kor_krx_data (Date,Symbol,Close,Fluc,FlucPrice,FlucRatio,EPS,PER,FWD_EPS,FWD_PER,BPS,PBR,DPS,DY,Financial,Insurance,Investment,PrivateEquity,Bank,OtherFinance,Pension,OtherCorporation,Individual,Foreigner,OtherForeigner)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
    Date=new.Date, Symbol=new.Symbol, Close=new.Close, Fluc=new.Fluc, FlucPrice=new.FlucPrice, FlucRatio=new.FlucRatio, EPS=new.EPS, PER=new.PER, FWD_EPS=new.FWD_EPS, FWD_PER=new.FWD_PER, BPS=new.BPS, PBR=new.PBR, DPS=new.DPS, DY=new.DY, Financial=new.Financial, Insurance=new.Insurance, Investment=new.Investment, PrivateEquity=new.PrivateEquity, Bank=new.Bank, OtherFinance=new.OtherFinance, Pension=new.Pension, OtherCorporation=new.OtherCorporation, Individual=new.Individual, Foreigner=new.Foreigner, OtherForeigner=new.OtherForeigner;
"""

empty_df = pd.DataFrame({'TRD_DD': [None], 'Symbol': [None], 'TDD_CLSPRC': [None], 'FLUC_TP_CD': [None], 'CMPPREVDD_PRC': [None], 'FLUC_RT': [None], 'EPS': [None], 'PER': [None], 'FWD_EPS': [None], 'FWD_PER': [None], 'BPS': [None], 'PBR': [None], 'DPS': [None], 'DVD_YLD': [None], 'TRDVAL1': [None], 'TRDVAL2': [None], 'TRDVAL3': [None], 'TRDVAL4': [None], 'TRDVAL5': [None], 'TRDVAL6': [None], 'TRDVAL7': [None], 'TRDVAL8': [None], 'TRDVAL9': [None], 'TRDVAL10': [None], 'TRDVAL11': [None], 'TRDVAL_TOT': [None]})

# krx데이터 url 주소
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

# 투자자별 거래실적 및 지표 가져오는 함수
def get_df(ticker, krx_code, start_date, end_date):
    indicator_opt = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03502',
        'searchType': '2',
        'mktId': 'ALL',
        'tboxisuCd_finder_stkisu0_1': ticker,
        'isuCd': krx_code,
        'strtDd': start_date,
        'endDd': end_date,
        'csvxls_isNo': 'false'
    }

    trader_opt = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT02303',
        'locale': 'ko_KR',
        'trdVolVal': '2',
        'askBid': '3',
        'tboxisuCd_finder_stkisu0_1': ticker,
        'isuCd': krx_code,
        'param1isuCd_finder_stkisu0_1': 'ALL',
        'strtDd': start_date,
        'endDd': end_date,
        'detailView': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }

    json_url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    df_merged = pd.DataFrame()

    indicator_request = rq.post(json_url, indicator_opt, headers=headers).text
    trader_request = rq.post(json_url, trader_opt, headers=headers).text

    indicator_reff = pd.json_normalize(json.loads(indicator_request))
    indicator_df = pd.DataFrame(data=indicator_reff)
    indicator_df = pd.DataFrame(indicator_df['output'][0])
    # print('indicator_df')
    # print(indicator_df)

    trader_reff = pd.json_normalize(json.loads(trader_request))
    trader_df = pd.DataFrame(data=trader_reff)
    trader_df = pd.DataFrame(trader_df['output'][0])
    # print('trader_df')
    # print(trader_df)

    # print('len of indicator_df + ' + str(len(indicator_df)) + ', trader_df : ' + str(len(trader_df)))

    if len(indicator_df) != 0 and len(trader_df) != 0:
        df_merged = pd.merge(indicator_df, trader_df, on=['TRD_DD'], how='outer')
        # print('if')
        # print(df_merged)

    elif len(indicator_df) == 0 and len(trader_df) != 0:
        indicator_df = pd.DataFrame({'TRD_DD': [None], 'TDD_CLSPRC': [None], 'FLUC_TP_CD': [None], 'CMPPREVDD_PRC': [None], 'FLUC_RT': [None], 'EPS': [None],'PER': [None], 'FWD_EPS': [None], 'FWD_PER': [None], 'BPS': [None], 'PBR': [None], 'DPS': [None], 'DVD_YLD': [None]})
        indicator_df['TRD_DD'] = trader_df['TRD_DD']
        df_merged = pd.merge(indicator_df, trader_df, on=['TRD_DD'], how='outer')
        # print('elif 1')
        # print(df_merged)

    elif len(trader_df) == 0 and len(indicator_df) != 0:
        trader_df = pd.DataFrame({'TRD_DD': [None], 'TRDVAL1': [None], 'TRDVAL2': [None], 'TRDVAL3': [None], 'TRDVAL4': [None], 'TRDVAL5': [None], 'TRDVAL6': [None], 'TRDVAL7': [None], 'TRDVAL8': [None], 'TRDVAL9': [None], 'TRDVAL10': [None], 'TRDVAL11': [None], 'TRDVAL_TOT': [None]})
        trader_df['TRD_DD'] = indicator_df['TRD_DD']
        df_merged = pd.merge(indicator_df, trader_df, on=['TRD_DD'], how='outer')
        # print('elif 2')
        # print(df_merged)

    else:
        # print('else')
        df_merged = empty_df.copy()
        # print(df_merged)

    return df_merged

# 오류 발생시 저장할 데이터프레임 생성
error_list = []

for i in tqdm(range(0, len(ticker_list))):
# for i in tqdm(range(0, 1)):

    # 빈 데이터프레임 복사
    data_df = empty_df.copy()

    # 티커 선택
    # ticker = '900300'
    ticker = ticker_list['종목코드'][i]
    # krx_code = 'HK0000312568'
    krx_code = ticker_list['표준코드'][i]

    # if ticker not in done_list:
    #     print(ticker)
    try:
        # df = get_df(ticker, krx_code, '20240404', '20240405')
        # df = df.sort_values(by = 'TRD_DD')
        # df.replace(',','', regex=True, inplace=True)
        # df.replace('-',None, regex=True, inplace=True)
        # data_df = pd.concat([data_df, df]).reset_index(drop=True)

        # data_df['Symbol'] = ticker
        # data_df.insert(1, 'Symbol', data_df.pop('Symbol'))

        # data_df = data_df[['TRD_DD', 'Symbol', 'TDD_CLSPRC', 'FLUC_TP_CD', 'CMPPREVDD_PRC', 'FLUC_RT', 'EPS', 'PER', 'FWD_EPS', 'FWD_PER', 'BPS', 'PBR', 'DPS', 'DVD_YLD', 'TRDVAL1', 'TRDVAL2', 'TRDVAL3', 'TRDVAL4', 'TRDVAL5', 'TRDVAL6', 'TRDVAL7', 'TRDVAL8', 'TRDVAL9', 'TRDVAL10', 'TRDVAL11']]
        # data_df = data_df.rename(columns = {'TRD_DD': 'Date', 'Symbol': 'Symbol', 'TDD_CLSPRC': 'Close', 'FLUC_TP_CD': 'Fluc', 'CMPPREVDD_PRC': 'FlucPrice', 'FLUC_RT': 'FlucRatio', 'EPS': 'EPS', 'PER': 'PER', 'FWD_EPS': 'FWD_EPS', 'FWD_PER': 'FWD_PER', 'BPS': 'BPS', 'PBR': 'PBR', 'DPS': 'DPS', 'DVD_YLD': 'DY', 'TRDVAL1': 'Financial', 'TRDVAL2': 'Insurance', 'TRDVAL3': 'Investment', 'TRDVAL4': 'PrivateEquity', 'TRDVAL5': 'Bank', 'TRDVAL6': 'OtherFinance', 'TRDVAL7': 'Pension', 'TRDVAL8': 'OtherCorporation', 'TRDVAL9': 'Individual', 'TRDVAL10': 'Foreigner', 'TRDVAL11': 'OtherForeigner'})

        # data_df = data_df.dropna(axis=0, subset=['Date'])
        # data_df = data_df.replace({np.nan: None})

        # for year in ['2024', '2023', '2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015']:
        for year in ['2024']:
            # print(year)
            df = get_df(ticker, krx_code, year + '0406', year + '1231')
            if len(df) != 0:
                df['TRD_DD'] = pd.to_datetime(df['TRD_DD'], format='%Y/%m/%d')
                df = df.sort_values(by = 'TRD_DD')
                df.replace(',','', regex=True, inplace=True)
                df.replace('-',None, regex=True, inplace=True)
                data_df = pd.concat([df, data_df]).reset_index(drop=True)
                # print(data_df.columns)
            else:
                pass
            time.sleep(2)

        data_df['Symbol'] = ticker
        data_df.insert(1, 'Symbol', data_df.pop('Symbol'))

        data_df = data_df[['TRD_DD', 'Symbol', 'TDD_CLSPRC', 'FLUC_TP_CD', 'CMPPREVDD_PRC', 'FLUC_RT', 'EPS', 'PER', 'FWD_EPS', 'FWD_PER', 'BPS', 'PBR', 'DPS', 'DVD_YLD', 'TRDVAL1', 'TRDVAL2', 'TRDVAL3', 'TRDVAL4', 'TRDVAL5', 'TRDVAL6', 'TRDVAL7', 'TRDVAL8', 'TRDVAL9', 'TRDVAL10', 'TRDVAL11']]
        # print(data_df.columns)
        data_df = data_df.rename(columns = {'TRD_DD': 'Date', 'Symbol': 'Symbol', 'TDD_CLSPRC': 'Close', 'FLUC_TP_CD': 'Fluc', 'CMPPREVDD_PRC': 'FlucPrice', 'FLUC_RT': 'FlucRatio', 'EPS': 'EPS', 'PER': 'PER', 'FWD_EPS': 'FWD_EPS', 'FWD_PER': 'FWD_PER', 'BPS': 'BPS', 'PBR': 'PBR', 'DPS': 'DPS', 'DVD_YLD': 'DY', 'TRDVAL1': 'Financial', 'TRDVAL2': 'Insurance', 'TRDVAL3': 'Investment', 'TRDVAL4': 'PrivateEquity', 'TRDVAL5': 'Bank', 'TRDVAL6': 'OtherFinance', 'TRDVAL7': 'Pension', 'TRDVAL8': 'OtherCorporation', 'TRDVAL9': 'Individual', 'TRDVAL10': 'Foreigner', 'TRDVAL11': 'OtherForeigner'})
        # print(data_df.columns)
        data_df = data_df.dropna(axis=0, subset=['Date', 'Close'])
        # data_df = data_df.dropna(axis=0, subset=['Date'])
        data_df = data_df.replace({np.nan: None})
        # break;

    except:
        error_list.append(ticker)
        # print('#'*50)
        # print('오류처리')
        # print('#'*50)

        # print('@'*50)
        # print(ticker)
        # print(data_df.columns)
        args = data_df.values.tolist()

        mycursor.executemany(query, args)
        con.commit()

        time.sleep(1)

print('#'*50)
print(error_list)
print('#'*50)

con.close()