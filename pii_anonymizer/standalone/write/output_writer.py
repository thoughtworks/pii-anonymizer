import pandas as pd
from pandas import DataFrame

from pii_anonymizer.common.constants import (
    ACQUIRE,
    ANONYMIZE,
    FILE_PATH,
    OUTPUT_FILE_NAME,
    OUTPUT_FILE_PATH,
    OUTPUT_FILE_FORMAT,
)

output_file_format = ["csv", "parquet"]
output_file_format_err_msg = (
    f"Output file format must be {' or '.join(output_file_format)}"
)


class OutputWriter:
    def __init__(self, config):
        self.__validate_config(config)
        self.__validate_output_format(config)
        self.input_file_name = config[ACQUIRE][FILE_PATH]
        self.output_path = config[ANONYMIZE][OUTPUT_FILE_PATH]
        self.output_file_name = config[ANONYMIZE].get(OUTPUT_FILE_NAME, None)

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

        return f"{self.output_path}/{output_file_name}.{self.output_format}"

    def write(self, df: DataFrame):
        match self.output_format:
            case "csv":
                df.to_csv(self.get_output_file_path(), index=False)
            case "parquet":
                df.to_parquet(self.get_output_file_path(), index=False)
        print("Anonymized output has been successfully created!")
