# https://github.com/hyunyulhenry/quant_py/blob/main/data_korea.ipynb

import requests as rq
import json
from io import BytesIO
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re
import pymysql

# 영업일 날짜 받기
url = 'http://finance.naver.com/sise/sise_deposit.nhn'
data = rq.get(url)
data_html = BeautifulSoup(data.content, features="lxml")
parse_day = data_html.select_one('div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text

biz_day = re.findall('[0-9]+', parse_day)
biz_day = ''.join(biz_day)

# KRX에 Request
gen_url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

gen_params = {
    'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
    'locale': 'ko_KR',
    'mktId': 'ALL',
    'trdDd': biz_day,
    'share': '1',
    'money': '1',
    'csvxls_isNo': 'false'
}

headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201'}
krx_request = rq.post(gen_url, gen_params, headers=headers).text

krx_reff = pd.json_normalize(json.loads(krx_request))
kor_ticker = pd.DataFrame(data=krx_reff)
kor_ticker = pd.DataFrame(kor_ticker['OutBlock_1'][0])

# ['ISU_SRT_CD', 'ISU_CD', 'ISU_ABBRV', 'MKT_NM', 'SECT_TP_NM', 'TDD_CLSPRC', 'FLUC_TP_CD', 'CMPPREVDD_PRC', 'FLUC_RT', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'MKTCAP', 'LIST_SHRS', 'MKT_ID']
# ['종목코드', '표준코드', '종목명', '시장구분', '소속부', '종가', '등락', '등락가', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수', '시장코드']
kor_ticker = kor_ticker[['ISU_SRT_CD', 'ISU_CD', 'ISU_ABBRV', 'MKT_NM']]
print(kor_ticker)

kor_ticker = kor_ticker.rename(columns = {'ISU_SRT_CD': '종목코드', 'ISU_CD': '표준코드', 'ISU_ABBRV': '종목명', 'MKT_NM': '시장구분'})

kor_ticker['종목명'] = kor_ticker['종목명'].str.strip()
kor_ticker['기준일'] = biz_day

gen_otp_data = {
    'searchType': '1',
    'mktId': 'ALL',
    'trdDd': biz_day,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
}
headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
krx_ind = rq.post(down_url, {'code': otp}, headers=headers)

krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
krx_ind['종목명'] = krx_ind['종목명'].str.strip()
krx_ind['기준일'] = biz_day

# # print(krx_ind.tail(10))

"""
두 데이터에 공통으로 존재하지 않는 종목 확인
선박펀드, 광물펀드, 해외종목 등의 정보
"""
diff = list(set(kor_ticker['종목명']).symmetric_difference(set(krx_ind['종목명'])))
# print(diff)

"""
스팩 종목은 종목명에 '스팩' 혹은 '제n호' 라는 단어가 들어간다. 따라서 contains() 메서드를 통해 종목명에 '스팩'이 들어가거나 정규 표현식을 이용해 '제n호'라는 문자가 들어간 종목명을 찾는다.
국내 종목 중 종목코드 끝이 0이 아닌 종목은 우선주에 해당한다.
리츠 종목은 종목명이 '리츠'로 끝난다. 따라서 endswith() 메서드를 통해 이러한 종목을 찾는다. (메리츠화재 등의 종목도 중간에 리츠라는 단어가 들어가므로 contains() 함수를 이용하면 안된다.)
"""
# 스펙(SPAC)주 확인
# print(kor_ticker[kor_ticker['종목명'].str.contains('스팩|제[0-9]+호')]['종목명'].values)

# 우선주 확인
# print(kor_ticker[kor_ticker['종목코드'].str[-1:] != '0']['종목명'].values)

# 리츠주 확인
# print(kor_ticker[kor_ticker['종목명'].str.endswith('리츠')]['종목명'].values)

kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                        np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',
                        np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',
                        np.where(kor_ticker['종목명'].isin(diff),  '기타',
                        '보통주'))))
kor_ticker = kor_ticker.reset_index(drop=True)

kor_ticker['기준일'] = pd.to_datetime(kor_ticker['기준일'])

print(kor_ticker.tail(10))

con = pymysql.connect(user='root',
                    passwd='1234',
                    host='127.0.0.1',
                    db='stock_db',
                    charset='utf8')

mycursor = con.cursor()
query = f"""
    insert into kor_ticker (종목코드,표준코드,종목명,시장구분,기준일,종목구분)
    values (%s,%s,%s,%s,%s,%s) as new
    on duplicate key update                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
    종목코드=new.종목코드,표준코드=new.표준코드,종목명=new.종목명,시장구분=new.시장구분,기준일=new.기준일,종목구분=new.종목구분;
"""

args = kor_ticker.values.tolist()

mycursor.executemany(query, args)
con.commit()

con.close()