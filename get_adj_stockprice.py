from dateutil.relativedelta import relativedelta
import requests as rq
import pandas as pd
from io import BytesIO
from datetime import date
from get_kor_stocktable import ticker_list

i = 0
ticker = ticker_list['종목코드'][i]
fr = (date.today() + relativedelta(years=-5)).strftime("%Y%m%d")
to = (date.today()).strftime("%Y%m%d")

url = f'''https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1
&startTime={fr}&endTime={to}&timeframe=day'''

data = rq.get(url).content
data_price = pd.read_csv(BytesIO(data))

print(data_price.head())

price = data_price.iloc[:, 0:6]
price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
price = price.dropna()
price['날짜'] = price['날짜'].str.extract('(\d+)')
price['날짜'] = pd.to_datetime(price['날짜'])
price['종목코드'] = ticker

print(price.head())
