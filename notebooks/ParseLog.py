from datetime import datetime
import pandas as pd

def utc_to_str_date(utc_date: str) -> str:
    """
    Принимает на вход строку в формате ddmmyyyy:hhmmss и возвращает строку в формате yyyy-hh-mm hh:mm:ss
    """
    return datetime.strptime(utc_date, '%d%m%Y:%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

class ParseLog:
    """
    """
    def __init__(self,path_to_file):
        self.path = path_to_file
    
    def parse_log_to_raw_df(self):
        """
        Принимает на вход путь к файлу в формате .log и преобразовывает в сырой pd.DataFrame
        """
        with open(self.path) as f:
            line_array = []
            line = f.readline()
            line_array.append(line)
            while line:
                try:
                    line = f.readline()
                    line_array.append(line)
                except Exception:
                    pass

        df = pd.DataFrame([string_line.split(";") for string_line in line_array])

        return df

    def transform_df(self):
        """
        Берёт сырой df из метода parse_log_to_raw_df() и преобразовывает в читаемый df
        """
        df = self.parse_log_to_raw_df()

        # Смена название колонок у сырого df
        new_name_columns = [
            'TIMESTAMP',
            'CLID',
            'UUID',
            'ROUTE',
            'VEHICLE_TYPE',
            'LATITUDE',
            'LONGITUDE',
            'SPEED',
            'DIRECTION',
            'PRODUCTION',
            'GARANGE_NUMBER',
            'REG_NUMBER',
            'ROUTE_DESCR'
        ]
        new_name_columns = list(map(lambda x: x.lower(), new_name_columns))
        df.columns = new_name_columns

        # Сортировка, чтобы убрать NULL-значения
        df = df[df.timestamp != '']

        # Приведение UTC-даты к общему timestamp
        df.timestamp = df.timestamp.apply(lambda x: utc_to_str_date(x))
        df.timestamp = pd.to_datetime(df.timestamp)

        # Смена типов у значений
        df.longitude = df.longitude.astype(float)
        df.latitude = df.latitude.astype(float)
        df.speed = df.speed.astype(float)
        df.direction = df.direction.astype(int)
        df.production = df.production.astype(int)

        return df