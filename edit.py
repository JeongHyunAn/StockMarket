(SELECT 'Changes', 'Symbol', 'Date', 'Market', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'Stocks', 'MarketCap', 'CPI', 'PPI', 'GDP', 'InterestRate', 'CPS', 'NetIncome', 'NetProfit', 'OperatingProfit', 'PCHigh', 'PCLow', 'PCR', 'PSR', 'ROA', 'ROE', 'ROIC', 'SPS', 'Take', 'Fluc', 'FlucPrice', 'FlucRatio', 'EPS', 'PER', 'FWD_EPS', 'FWD_PER', 'BPS', 'PBR', 'DPS', 'DY', 'Financial', 'Insurance', 'Investment', 'PrivateEquity', 'Bank', 'OtherFinance', 'Pension', 'OtherCorporation', 'Individual', 'Foreigner', 'OtherForeigner')
UNION
(SELECT Changes, Symbol, Date, Market, Open, High, Low, Close, Volume, Amount, Stocks, MarketCap, CPI, PPI, GDP, InterestRate, CPS, NetIncome, NetProfit, OperatingProfit, PCHigh, PCLow, PCR, PSR, ROA, ROE, ROIC, SPS, Take, Fluc, FlucPrice, FlucRatio, EPS, PER, FWD_EPS, FWD_PER, BPS, PBR, DPS, DY, Financial, Insurance, Investment, PrivateEquity, Bank, OtherFinance, Pension, OtherCorporation, Individual, Foreigner, OtherForeigner
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/kor_training_data.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
FROM kor_training_data
WHERE Symbol='');