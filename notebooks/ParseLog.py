import pandas as pd

class ParseLog:
    """
    """
    def __init__(self,path_to_file):
        self.path = path_to_file
    
    def parse_log_to_raw_df(self):
        """
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

        """
        df = self.parse_log_to_raw_df()

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

        df = df[df.timestamp != '']

        return df