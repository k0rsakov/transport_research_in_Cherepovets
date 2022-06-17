from sqlalchemy import create_engine
import pandas as pd
from db_parameters import *


class PostgreSQL:
    """

    """
    def __init__(self,
                 host: str,
                 database: str,
                 login: str,
                 password: str,
                 df: pd.DataFrame):
        self.host = host
        self.db = database
        self.login = login
        self.password = password
        self.df = df

    def authorization_pg(self):
        """
        Создание коннектора к БД PostgreSQL
        """
        engine_str = f'postgresql://{self.login}:{self.password}@{self.host}:5432/{self.db}'
        engine = create_engine(engine_str)
        return engine

    def into_pg_table(self,pg_table_name:str):
        """
        Помещение данных в БД PostgreSQL
        """
        dataframe = self.df
        connector = self.authorization_pg()
        dataframe.to_sql(
            name=pg_table_name,
            con=connector,
            chunksize=10000,
            index=False,
            if_exists='append'
        )
