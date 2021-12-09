# import python libraries
from datetime import datetime
from datetime import date
import pandas as pd
from sqlalchemy import create_engine

def ingest():
    # this function ingests data from the provided csv file and loads it into a local dataframe
    df = pd.read_csv("DATA.csv")
    return df

def clean():
    # this function cleans data before it is transformed
    df = ingest()
    # clean data: check for NAN values and print results
    df_check_nan = df.isnull().values.any()
    print(f"NAN values in dataframe: {df_check_nan}")
    # clean data: check for duplicate records and print results
    df_check_duplicates = df[df.duplicated()]
    print(f"Duplicate records in dataframe: {df_check_duplicates}")
    return df

def transform():
    # this function transforms data before it is exported to a MySQL database
    df = clean()
    # drop the id column - this data will be re-indexed when it is exported
    df = df.drop(columns=["id"])
    # add an import timestamp for each record 
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    df["import_date"] = pd.to_datetime(current_time)
    return df
    
def connect():
    # this function connects to the database so we can export data
    # database information
    user = "root"
    password = "7]DFzwRkV"
    host = "localhost"
    database = "sfl_science"
    port = 3306
    # connect to the server
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}", echo=False)
    return engine

def export():
    # this function exports clean and transformed data to the database
    df = transform()
    engine = connect()
    # export data to the MySQL database
    df.to_sql(name="sfl_tbl", con=engine, if_exists = "append", index=True)
    
export()
    