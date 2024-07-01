"""
get_kor_stockvalue.py 코드를 응용해 전 종목 가치지표를 계산해보도록 하자. 먼저 SQL에서 가치지표가 저장될 테이블(kor_value)을 만듬
"""

# 패키지 불러오기
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

# 분기 재무제표 불러오기
kor_fs = pd.read_sql("""
select * from kor_fs
where 공시구분 = 'q'
and 계정 in ('당기순이익', '자본', '영업활동으로인한현금흐름', '매출액');
""", con=engine)

# 티커 리스트 불러오기
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
and 종목구분 = '보통주';
""", con=engine)

engine.dispose()

# TTM 구하기
kor_fs = kor_fs.sort_values(['종목코드', '계정', '기준일'])

# print(kor_fs)
new_column = kor_fs.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(window=4, min_periods=4).sum()
new_column = new_column.reset_index()
kor_fs['ttm'] = new_column['값']

# 자본은 평균 구하기
kor_fs['ttm'] = np.where(kor_fs['계정'] == '자본', kor_fs['ttm'] / 4, kor_fs['ttm'])
kor_fs = kor_fs.groupby(['계정', '종목코드']).tail(1)

kor_fs_merge = kor_fs[['계정', '종목코드', 'ttm']].merge(ticker_list[['종목코드', '시가총액', '기준일']], on='종목코드')
kor_fs_merge['시가총액'] = kor_fs_merge['시가총액'] / 100000000
kor_fs_merge['value'] = kor_fs_merge['시가총액'] / kor_fs_merge['ttm']
kor_fs_merge['value'] = kor_fs_merge['value'].round(4)
kor_fs_merge['지표'] = np.where(
    kor_fs_merge['계정'] == '매출액', 'PSR',
        np.where(kor_fs_merge['계정'] == '영업활동으로인한현금흐름', 'PCR',
            np.where(kor_fs_merge['계정'] == '자본', 'PBR',
                np.where(kor_fs_merge['계정'] == '당기순이익', 'PER', None))))

kor_fs_merge.rename(columns={'value': '값'}, inplace=True)
kor_fs_merge = kor_fs_merge[['종목코드', '기준일', '지표', '값']]
# for i in kor_fs_merge['값']:
#     print('*' * 40)
#     print(i)
#     if 0 == i:
#         print('zero')
#     elif np.nan == i:
#         print('np.nan')
# kor_fs_merge = kor_fs_merge.replace([np.inf, -np.inf, np.nan], None)
kor_fs_merge = kor_fs_merge.replace([np.inf, -np.inf], np.nan)
kor_fs_merge = kor_fs_merge.replace({np.nan: None})
print(kor_fs_merge.head(4))

query = """
    insert into kor_value (종목코드, 기준일, 지표, 값)
    values (%s,%s,%s,%s) as new
    on duplicate key update
    값=new.값
"""

args_fs = kor_fs_merge.values.tolist()
mycursor.executemany(query, args_fs)
con.commit()

ticker_list['값'] = ticker_list['주당배당금'] / ticker_list['종가']
ticker_list['값'] = ticker_list['값'].round(4)
ticker_list['지표'] = 'DY'
dy_list = ticker_list[['종목코드', '기준일', '지표', '값']]
dy_list = dy_list.replace([np.inf, -np.inf, np.nan], None)
dy_list = dy_list[dy_list['값'] != 0]

print(dy_list.tail())

args_dy = dy_list.values.tolist()
mycursor.executemany(query, args_dy)
con.commit()

engine.dispose()
con.close()