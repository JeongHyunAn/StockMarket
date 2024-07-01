from sqlalchemy import create_engine
import pandas as pd
import numpy as np

engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)

value_list = pd.read_sql("""
select * from kor_value
where 기준일 = (select max(기준일) from kor_value);
""", con=engine)

engine.dispose()

value_list.loc[value_list['값'] <= 0, '값'] = np.nan
value_pivot = value_list.pivot(index='종목코드', columns='지표', values='값')
data_bind = ticker_list[['종목코드', '종목명']].merge(value_pivot, how='left', on='종목코드')

print(data_bind.head())