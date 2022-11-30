from pyspark.sql import SparkSession, DataFrame
from pii_anonymizer.common.constants import (
    ANONYMIZE,
    ACQUIRE,
    OUTPUT_FILE_NAME,
    OUTPUT_FILE_PATH,
    FILE_PATH,
    OUTPUT_FILE_FORMAT,
)

output_file_format = ["csv", "parquet"]
output_file_format_err_msg = (
    f"Output file format must be {' or '.join(output_file_format)}"
)


class OutputWriter:
    def __init__(self, spark: SparkSession, config):
        self.__validate_config(config)
        self.__validate_output_format(config)
        self.output_path = config[ANONYMIZE][OUTPUT_FILE_PATH]
        self.input_file_name = config[ACQUIRE][FILE_PATH]
        self.output_file_name = config[ANONYMIZE].get(OUTPUT_FILE_NAME, None)
        self.spark = spark

    def __validate_config(self, config):
        if (
            ANONYMIZE not in config
            or not config[ANONYMIZE]
            or OUTPUT_FILE_PATH not in config[ANONYMIZE]
            or not config[ANONYMIZE][OUTPUT_FILE_PATH]
        ):
            raise ValueError(
                "Config 'output_file_path' needs to be provided for parsing"
            )

    def __validate_output_format(self, config):
        self.output_format = config[ANONYMIZE].get(OUTPUT_FILE_FORMAT, "csv")
        if self.output_format not in output_file_format:
            raise ValueError(output_file_format_err_msg)

    def get_output_file_path(self):
        file_name = self.input_file_name.split("/")[-1]
        file_name_no_extension = file_name.split(".")[0]

        if self.output_file_name is None:
            output_file_name = (
                "anonymized"
                if file_name_no_extension == "*"
                else f"{file_name_no_extension}_anonymized"
            )
        else:
            output_file_name = self.output_file_name

        return f"{self.output_path}/{output_file_name}"

    def write(self, df: DataFrame):
        match self.output_format:
            case "csv":
                df.write.mode("overwrite").option("header", "true").csv(
                    self.get_output_file_path()
                )
            case "parquet":
                df.write.mode("overwrite").option("header", "true").parquet(
                    self.get_output_file_path()
                )
