# Symbol, Date, CPS, PCHigh, PCLow, PCR, Take, NetProfit, OperatingProfit, ROA, ROE, ROIC, NetIncome, BPS, EPS, PBR, PER, PSR, SPS

import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import requests
import json
import time
from functools import reduce
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

query = """
    insert into kor_ref2 (Symbol, Date, CPS, PCHigh, PCLow, PCR, Take, NetProfit, OperatingProfit, ROA, ROE, ROIC, NetIncome, BPS, EPS, PBR, PER, PSR, SPS)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    Symbol=new.Symbol, Date=new.Date, CPS=new.CPS, PCHigh=new.PCHigh, PCLow=new.PCLow, PCR=new.PCR, Take=new.Take, NetProfit=new.NetProfit, OperatingProfit=new.OperatingProfit, ROA=new.ROA, ROE=new.ROE, ROIC=new.ROIC, NetIncome=new.NetIncome, BPS=new.BPS, EPS=new.EPS, PBR=new.PBR, PER=new.PER, PSR=new.PSR, SPS=new.SPS
"""

url = 'https://navercomp.wisereport.co.kr/company/chart/c1030001.aspx'
concat_list = []

def req_data(num, ticker, frequency):

    params = {
        'cmp_cd': ticker,
        'frq': frequency,
        'rpt': num,
        'finGubun': 'MAIN',
        'chartType': 'svg'
    }

    res = requests.get(url, params=params)
    res = json.loads(res.text)

    return res

def clean_data(res):

    name_li = []
    data_li = []
    for i in range(0, len(res)):
        raw_data = pd.json_normalize(res['chartData' + str(i + 1)]['series'])
        name_li = name_li + raw_data['name'].tolist()
        data_li = data_li + raw_data['data'].tolist()
        # for j in range(0, len(raw_data['data'])):
        #     data_li.append(raw_data['data'][j])
            # print(raw_data['data'][j])

    # print(name_li)
    # print(data_li)

    processed_data = pd.DataFrame()
    processed_data['Date'] = res['chartData2']['categories']
    for i in range(0, len(name_li)):
        processed_data[name_li[i]] = data_li[i]

    processed_data = processed_data[processed_data["Date"].str.contains("2024") == False]
    processed_data = processed_data[np.where((processed_data['Date'].str.len()>1), True, False)]
    processed_data = processed_data.replace({np.nan: None})
    # processed_data = processed_data.dropna(axis=0, subset=['Date'])
    # print(processed_data)

    return processed_data


for i in tqdm(range(1573, len(ticker_list))):
# for i in tqdm(range(0, 1)):
    
    ticker = ticker_list['종목코드'][i]

    # print(ticker)
    # break;

    df_list = []
    ann_data = pd.DataFrame()
    qrt_data = pd.DataFrame()

    for freq in ['Y', 'Q']:
        for num in ['1', '2', '6']:
            res = req_data(num, ticker, freq)
            df = clean_data(res)

            if freq == 'Y':
                df = df[df['Date'].str.contains('2022|2023') == False]
                if num == '1':
                    ann_data['Date'] = df['Date']
                merging_df = df[df.columns.difference(ann_data.columns)]
                merging_df.insert(0, 'Date', df.pop('Date'))
                ann_data = pd.merge(ann_data, merging_df, on=['Date'], how='outer')

            else:
                if num == '1':
                    qrt_data['Date'] = df['Date']
                merging_df = df[df.columns.difference(qrt_data.columns)]
                merging_df.insert(0, 'Date', df.pop('Date'))
                qrt_data = pd.merge(qrt_data, merging_df, on=['Date'], how='outer')

    if len(ann_data) > 0 and len(qrt_data) > 0:
        concat_df = pd.concat([ann_data, qrt_data])
    elif len(ann_data) < 1:
        concat_df = qrt_data
    elif len(qrt_data) < 1:
        concat_df = ann_data
    else:
        pass

    concat_df.fillna(method='ffill', inplace=True)

    concat_df.insert(0, 'Symbol', ticker)
    concat_df['Date'] = pd.to_datetime(concat_df['Date'], format='%Y/%m') + pd.tseries.offsets.MonthEnd()
    concat_df = concat_df.replace({np.nan: None})
    # print(concat_df.columns)

    args_ref = concat_df.values.tolist()
    mycursor.executemany(query, args_ref)
    con.commit()

    time.sleep(1)

con.close()

# kor_data = pd.concat(concat_list)
# kor_data = kor_data.replace({np.nan: None})

# args_ref = kor_data.values.tolist()
# mycursor.executemany(query, args_ref)
# con.commit()