import pandas as pd
import dask.dataframe as dd
from pii_anonymizer.common.constants import FILE_PATH, DELIMITER


class CsvParser:
    def __init__(self, config):
        self.__validate_config(config)
        self.input_path = config[FILE_PATH]
        self.delimiter = (
            config[DELIMITER] if DELIMITER in config and config[DELIMITER] else ","
        )

    def __validate_config(self, config):
        if FILE_PATH not in config or not config[FILE_PATH]:
            raise ValueError("Config 'file_path' needs to be provided for parsing")

    def parse(self):
        try:
            df = dd.read_csv(self.input_path, delimiter=self.delimiter).compute()
        except pd.errors.EmptyDataError:
            return pd.DataFrame({})

        if df.isnull().values.any():
            raise ValueError("Dataframe contains NULL values")

        return df
