import pandas as pd
from datetime import datetime
import yfinance as yf
import ta
import numpy as np
import json

def historical_data(symbol, interval='1d', window1=3, window2=20, alpha1=0.1, alpha2=0.5):
    # start_date and end_date default is 99 years and now if set none
    data_historical = yf.download(symbol, interval=interval, period='max', actions='True')

    # Calculate RSI
    data_historical["RSI"] = ta.momentum.RSIIndicator(data_historical["Close"]).rsi()

    # Calculate Bollinger Bands
    bbands = ta.volatility.BollingerBands(data_historical["Close"])
    data_historical["BB_upper"] = bbands.bollinger_hband()
    data_historical["BB_lower"] = bbands.bollinger_lband()

    # Calculate MACD
    macd = ta.trend.MACD(data_historical["Close"])
    data_historical["MACD"] = macd.macd()
    data_historical["MACD_signal"] = macd.macd_signal()

    # Calculate percentage change
    data_historical['pct_change'] = data_historical['Adj Close'].pct_change()

    # min_periods = 1 let the ma starting with first month
    SMA1 = "SMA-"+str(window1)
    data_historical[SMA1] = data_historical['Adj Close'].rolling(window1,min_periods=1).mean()

    window = 20
    SMA2 = "SMA-"+str(window2)
    data_historical[SMA2] = data_historical['Adj Close'].rolling(window2,min_periods=1).mean()

    data_historical['EMA_'+str(alpha1)] = data_historical['Adj Close'].ewm(alpha=alpha1, adjust=False).mean()
    data_historical['EMA_'+str(alpha2)] = data_historical['Adj Close'].ewm(alpha=alpha2, adjust=False).mean()

    # Shift to the future by one day so that everyday uses the information up to 
    # yesterday to make a trading decision for tmr
    data_historical[SMA1+'-3_shift_1d'] = data_historical[SMA1].shift(1)
    data_historical[SMA2+'-20_shift_1d'] = data_historical[SMA2].shift(1)

    # identify buy signal
    data_historical['signal'] = np.where(data_historical[SMA1+'-3_shift_1d'] > data_historical[SMA2+'-20_shift_1d'], 1, 0)
    # identify sell signal
    data_historical['signal'] = np.where(data_historical[SMA1+'-3_shift_1d'] < data_historical[SMA2+'-20_shift_1d'], -1, data_historical['signal'])

    # calculate instantaneous log return for buy-and-hold straetegy as benchmark
    data_historical['log_return_buy_n_hold'] = np.log(data_historical['Adj Close']).diff()

    # calculate instantaneous log return for trend following straetegy
    data_historical['log_return_trend_follow'] = data_historical['signal'] * data_historical['log_return_buy_n_hold']

    # calculate the cumulative return for buy-and-hold and trend-following strategy
    data_historical['return_buy_n_hold'] = np.exp(data_historical['log_return_buy_n_hold']).cumprod()
    data_historical['return_trend_follow'] = np.exp(data_historical['log_return_trend_follow']).cumprod()

    # derive trading action at each time step; 2 is buy, -2 is sell
    data_historical['action'] = data_historical.signal.diff()

    data_historical['symbol'] = symbol

    return data_historical


def general_info(symbol):
    ticker_obj = yf.Ticker(symbol)

    # change format to meet the requirements of saving into mysql
    ticker_obj.info['companyOfficers'] = json.dumps(ticker_obj.info['companyOfficers'])
    
    df_info = pd.DataFrame([ticker_obj.info])
    df_info['symbol'] = symbol
    df_actions = ticker_obj.actions.reset_index()
    df_actions['symbol'] = symbol
    df_quarterly_income_stmt = ticker_obj.quarterly_income_stmt.transpose().reset_index()
    df_quarterly_income_stmt['symbol'] = symbol
    df_quarterly_balance_sheet = ticker_obj.quarterly_balance_sheet.transpose().reset_index()
    df_quarterly_balance_sheet['symbol'] = symbol
    df_quarterly_cashflow = ticker_obj.quarterly_cashflow.transpose().reset_index()
    df_quarterly_cashflow['symbol'] = symbol
    df_recommendations_summary = ticker_obj.recommendations_summary
    df_recommendations_summary['symbol'] = symbol
    df_upgrades_downgrades = ticker_obj.upgrades_downgrades.reset_index()
    df_upgrades_downgrades['symbol'] = symbol
    df_get_earnings_dates = ticker_obj.get_earnings_dates(limit=1000).reset_index()
    df_get_earnings_dates['symbol'] = symbol
    df_news = pd.DataFrame(ticker_obj.news)
    df_news['thumbnail'] = df_news['thumbnail'].apply(json.dumps)
    df_news['relatedTickers'] = df_news['relatedTickers'].apply(json.dumps)
    df_news['symbol'] = symbol

    return df_info, df_actions, df_quarterly_income_stmt, df_quarterly_balance_sheet, df_quarterly_cashflow, df_recommendations_summary, \
    df_upgrades_downgrades, df_get_earnings_dates, df_news


def get_data(symbol_list):

    # initialise empty dataframes
    df_info_all = pd.DataFrame()
    df_actions_all = pd.DataFrame()
    df_quarterly_income_stmt_all = pd.DataFrame()
    df_quarterly_balance_sheet_all = pd.DataFrame()
    df_quarterly_cashflow_all = pd.DataFrame()
    df_recommendations_summary_all = pd.DataFrame()
    df_upgrades_downgrades_all = pd.DataFrame()
    df_get_earnings_dates_all = pd.DataFrame()
    df_news_all = pd.DataFrame()
    data_historical_all = pd.DataFrame()

    # save data for all symbols
    for symbol in symbol_list:
        print(f'Downloading data for {symbol}')
        df_info, df_actions, df_quarterly_income_stmt, df_quarterly_balance_sheet, df_quarterly_cashflow, df_recommendations_summary, \
        df_upgrades_downgrades, df_get_earnings_dates, df_news = general_info(symbol)
        df_historical = historical_data(symbol, interval='1d', window1=3, window2=20, alpha1=0.1, alpha2=0.5).reset_index()
        data_historical_all = pd.concat([data_historical_all, df_historical], ignore_index=True)
        df_info_all = pd.concat([df_info_all, df_info], ignore_index=True)
        df_actions_all = pd.concat([df_actions_all, df_actions], ignore_index=True)
        df_quarterly_income_stmt_all = pd.concat([df_quarterly_income_stmt_all, df_quarterly_income_stmt], ignore_index=True)
        df_quarterly_balance_sheet_all = pd.concat([df_quarterly_balance_sheet_all, df_quarterly_balance_sheet], ignore_index=True)
        df_quarterly_cashflow_all = pd.concat([df_quarterly_cashflow_all, df_quarterly_cashflow], ignore_index=True)
        df_recommendations_summary_all = pd.concat([df_recommendations_summary_all, df_recommendations_summary], ignore_index=True)
        df_upgrades_downgrades_all = pd.concat([df_upgrades_downgrades_all, df_upgrades_downgrades], ignore_index=True)
        df_get_earnings_dates_all = pd.concat([df_get_earnings_dates_all, df_get_earnings_dates], ignore_index=True)
        df_news_all = pd.concat([df_news_all, df_news], ignore_index=True)

    table_names = {
    'stock_company_info':df_info,
    'stock_actions':df_actions_all,
    'stock_quarterly_income_stmt':df_quarterly_income_stmt_all,
    'stock_quarterly_balance_sheet':df_quarterly_balance_sheet_all,
    'stock_quarterly_cashflow':df_quarterly_cashflow_all,
    'stock_recommendations_summary':df_recommendations_summary_all,
    'stock_upgrades_downgrades':df_upgrades_downgrades_all,
    'stock_get_earnings_dates':df_get_earnings_dates_all,
    'stock_news':df_news_all,
    'stock_data_historical':data_historical_all
    }
    print('All symbol data download finish...')
    return table_names


if __name__ == '__main__':
    print('Please import as a module')

