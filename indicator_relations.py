from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

data_bind = pd.read_sql("""
select Symbol, PSR, PCR, PER, PBR, DY from kor_training_data
where Date = '2024-01-23';
""", con=engine)

cols = ['PSR', 'PCR', 'PER', 'PBR', 'DY']

data_bind[cols] = data_bind[cols].apply(pd.to_numeric)
data_bind[data_bind[cols] <= 0] = np.nan

# print(data_bind)

value_rank = data_bind[['PER', 'PBR']].rank(axis = 0)
value_sum = value_rank.sum(axis = 1, skipna = False).rank()
print(data_bind.loc[value_sum <= 20, ['Symbol', 'PER', 'PBR']])

value_list_copy = data_bind.copy()
value_list_copy['DY'] = 1 / value_list_copy['DY']
value_list_copy = value_list_copy[['PER', 'PBR', 'PCR', 'PSR', "DY"]]
value_rank_all = value_list_copy.rank(axis=0)

mask = np.triu(value_rank_all.corr())
fig, ax = plt.subplots(figsize=(10, 6))
sns.heatmap(value_rank_all.corr(),
            annot=True,
            mask=mask,
            annot_kws={"size": 16},
            vmin=0,
            vmax=1,
            center=0.5,
            cmap='coolwarm',
            square=True)
ax.invert_yaxis()
plt.show()