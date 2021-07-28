import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()

def connect():
    """connect: This function helps to create a connection with an mysql instance

    Args:
        None

    Returns:
        pool (sqlalchemy conection): conexion pool
    """

    db_config = {
        "pool_size": 1,
        "max_overflow": 0,
        "pool_timeout": 30,
        "pool_recycle": 1800,
    }

    pool = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername="mysql+pymysql",
            username=os.getenv("DB_USER"),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            host=os.getenv('DB_HOST_IP'),
            port=os.getenv('DB_PORT')
        ),
        **db_config
    )
    return pool

def df_to_sql(df,table):
    """df_to_sql: This functions takes a pandas Data Frame to update an mysql table

    Args:
        df (pd.DataFrame): table to be send to mysql table
        table (str): target table to be updated

    Returns:
        None
    """
    pool = connect()
    df.to_sql(table,pool,if_exists='append', index=False)
    return None