from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
import time

load_dotenv()

db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')

db_instance = os.getenv('DB_INSTANCE')

# Define the connection URL
DATABASE_URL = f"mysql+mysqlconnector://{db_username}:{db_password}@{db_instance}"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def save_to_rds(data):

    # Insert dataframes into tables
    for table_name, df in data.items():
        try:
            # Write the DataFrame to the MySQL table
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data inserted into table '{table_name}' successfully.")
        except Exception as e:
            print(f"Error inserting data into table '{table_name}': {e}")

def save_to_csv(data):

    # Insert dataframes into tables
    for table_name, df in data.items():
        try:
            # Write the DataFrame to the MySQL table
            df.to_csv(f'./result/{table_name}.csv', index=False)
            print(f"Data saved in csv table '{table_name}' successfully.")
        except Exception as e:
            print(f"Error saved in csv table '{table_name}': {e}")

if __name__ == '__main__':
    pass
    # print('test')