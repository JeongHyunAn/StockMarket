from sqlalchemy import create_engine
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import statsmodels.api as sm
import numpy as np

engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

ticker_list = pd.read_sql(
"""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)


price_list = pd.read_sql(
"""
select 날짜, 종가, 종목코드
from kor_price
where 날짜 >= (select (select max(날짜) from kor_price) - interval 1 year);
""", con=engine)

engine.dispose()

# print(price_list.head())

price_pivot = price_list.pivot(index='날짜', columns='종목코드', values='종가')
# print(price_pivot.iloc[0:5, 0:5])

ret_list = pd.DataFrame(data=(price_pivot.iloc[-1] / price_pivot.iloc[0]) - 1, columns=['return'])
data_bind = ticker_list[['종목코드', '종목명']].merge(ret_list, how='left', on='종목코드')
# print(data_bind.head())

momentum_rank = data_bind['return'].rank(axis=0, ascending=False)
# print(data_bind[momentum_rank <= 20])

price_momentum = price_list[price_list['종목코드'].isin(data_bind.loc[momentum_rank <= 20, '종목코드'])]

# plt.rc('font', family='Malgun Gothic')
# g = sns.relplot(data=price_momentum,
#                 x='날짜',
#                 y='종가',
#                 col='종목코드',
#                 col_wrap=5,
#                 kind='line',
#                 facet_kws={
#                     'sharey': False,
#                     'sharex': True
#                 })
# g.set(xticklabels=[])
# g.set(xlabel=None)
# g.set(ylabel=None)
# g.fig.set_figwidth(15)
# g.fig.set_figheight(8)
# plt.subplots_adjust(wspace=0.5, hspace=0.2)
# plt.show()

ret = price_pivot.pct_change().iloc[1:]
ret_cum = np.log(1 + ret).cumsum()

x = np.array(range(len(ret)))
y = ret_cum.iloc[:, 0].values

reg = sm.OLS(y, x).fit()
# reg.summary()

x = np.array(range(len(ret)))
k_ratio = {}

for i in range(0, len(ticker_list)):

    ticker = data_bind.loc[i, '종목코드']

    try:
        y = ret_cum.loc[:, price_pivot.columns == ticker]
        reg = sm.OLS(y, x).fit()
        res = float(reg.params / reg.bse)
    except:
        res = np.nan

    k_ratio[ticker] = res

k_ratio_bind = pd.DataFrame.from_dict(k_ratio, orient='index').reset_index()
k_ratio_bind.columns = ['종목코드', 'K_ratio']

# k_ratio_bind.head()

data_bind = data_bind.merge(k_ratio_bind, how='left', on='종목코드')
k_ratio_rank = data_bind['K_ratio'].rank(axis=0, ascending=False)
# data_bind[k_ratio_rank <= 20]

k_ratio_momentum = price_list[price_list['종목코드'].isin(data_bind.loc[k_ratio_rank <= 20, '종목코드'])]
print('Price list')
print(price_list.head())
print('Momentum rank')
print(data_bind.loc[k_ratio_rank <= 20, '종목코드'])
print('Momentum data')
print(price_list[price_list['종목코드'].isin(data_bind.loc[k_ratio_rank <= 20, '종목코드'])])
# plt.rc('font', family='Malgun Gothic')
# g = sns.relplot(data=k_ratio_momentum,
#                 x='날짜',
#                 y='종가',
#                 col='종목코드',
#                 col_wrap=5,
#                 kind='line',
#                 facet_kws={
#                     'sharey': False,
#                     'sharex': True
#                 })
# g.set(xticklabels=[])
# g.set(xlabel=None)
# g.set(ylabel=None)
# g.fig.set_figwidth(15)
# g.fig.set_figheight(8)
# plt.subplots_adjust(wspace=0.5, hspace=0.2)
# plt.show()