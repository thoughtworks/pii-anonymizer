from pyspark.sql import SparkSession
from pii_anonymizer.common.file_utils import format_glob, get_format
from pii_anonymizer.spark.constants import FILE_PATH


class InputParser:
    def __init__(self, spark: SparkSession, config):
        self.__validate_config(config)
        self.input_path = format_glob(config[FILE_PATH])
        self.delimiter = (
            config["delimiter"]
            if "delimiter" in config and config["delimiter"]
            else ","
        )
        self.spark = spark

    def __validate_config(self, config):
        if FILE_PATH not in config or not config[FILE_PATH]:
            raise ValueError("Config 'file_path' needs to be provided for parsing")

    def parse(self):
        format = get_format(self.input_path)
        df = self.spark.read.load(
            self.input_path,
            format=format,
            sep=self.delimiter,
            header="true",
        )

        return df
