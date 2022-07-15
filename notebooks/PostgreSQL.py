from sqlalchemy import create_engine
import pandas as pd
from db_parameters import *

class PostgreSQL:
    """
    Класс для работы с БД PostgreSQL
    """
    def __init__(self,
                 host: str,
                 database: str,
                 login: str,
                 password: str,
                 port:int = 5432
                 ):
        self.host = host
        self.db = database
        self.login = login
        self.password = password
        self.port = port

    def authorization_pg(self):
        """
        Создание коннектора к БД PostgreSQL
        """
        engine_str = f'postgresql://{self.login}:{self.password}@{self.host}:{self.port}/{self.db}'
        engine = create_engine(engine_str)
        return engine

    def into_pg_table(self,
                      pg_table_name: str,
                      df: pd.DataFrame,
                      schema: str = 'public'
                      ) -> bool:
        """
        Помещение данных в БД PostgreSQL
        """
        try:
            dataframe = df
            connector = self.authorization_pg()
            dataframe.to_sql(
                name=pg_table_name,
                schema=schema,
                con=connector,
                chunksize=10000,
                index=False,
                if_exists='append'
            )
            return True
        except Exception:
            print(f"Can't insert df in {pg_table_name}")

    def execute_to_df(
            self,
            query: str,) -> pd.DataFrame:
        """
        Метод для извлечения df из БД
        """
        try:
            return pd.read_sql(query, self.authorization_pg())
        except Exception:
            print(f"Can't execute df from PostgreSQL")
