from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect

# Define the connection URL
# DATABASE_URL = "mysql+mysqlconnector://root:Lxy930719@localhost/finance"
DATABASE_URL = "mysql+mysqlconnector://lxy:Lxy930719~@mydb.cvgm8e2kwp95.us-east-1.rds.amazonaws.com/finance"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)


""" 
TODO:
1. use boto3 to control the start and stop of the rds instance and also check the status
2. save the rds login in access key or password in the environment variable
"""

def save_data(data):
    tables = [
        df_info_all,
        df_actions_all,
        df_quarterly_income_stmt_all,
        df_quarterly_balance_sheet_all,
        df_quarterly_cashflow_all,
        df_recommendations_summary_all,
        df_upgrades_downgrades_all,
        df_get_earnings_dates_all,
        df_news_all,
        data_historical_all
        ]
    table_names = [
        'stock_company_info',
        'stock_actions',
        'stock_quarterly_income_stmt',
        'stock_quarterly_balance_sheet',
        'stock_quarterly_cashflow',
        'stock_recommendations_summary',
        'stock_upgrades_downgrades',
        'stock_get_earnings_dates',
        'stock_news',
        'stock_data_historical'
    ]

    # Insert dataframes into tables
    for table_name, df in zip(table_names, tables):
        try:
            # Write the DataFrame to the MySQL table
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data inserted into table '{table_name}' successfully.")
        except Exception as e:
            print(f"Error inserting data into table '{table_name}': {e}")