import pandas as pd
import dask.dataframe as dd
from pii_anonymizer.common.constants import FILE_PATH, DELIMITER
from pii_anonymizer.common.file_utils import format_glob, get_format

allowed_format = ["csv", "parquet", "parq"]


class InputParser:
    def __init__(self, config):
        self.__validate_config(config)
        self.input_path = format_glob(config[FILE_PATH])

        self.delimiter = (
            config[DELIMITER] if DELIMITER in config and config[DELIMITER] else ","
        )

    def __validate_config(self, config):
        if FILE_PATH not in config or not config[FILE_PATH]:
            raise ValueError("Config 'file_path' needs to be provided for parsing")

    def parse(self):
        try:
            match get_format(self.input_path):
                case "csv":
                    df = dd.read_csv(
                        self.input_path, delimiter=self.delimiter, dtype=str
                    ).compute()
                case "parquet":
                    df = dd.read_parquet(
                        self.input_path, delimiter=self.delimiter, dtype=str
                    ).compute()
        except pd.errors.EmptyDataError:
            return pd.DataFrame({})

        if df.isnull().values.any():
            raise ValueError("Dataframe contains NULL values")

        return df
