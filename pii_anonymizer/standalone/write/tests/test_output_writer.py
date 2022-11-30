from unittest import TestCase
from unittest.mock import patch
import pandas as pd
from pii_anonymizer.standalone.write.output_writer import (
    OutputWriter,
    output_file_format_err_msg,
)


class TestOutputWriter(TestCase):

    # TODO: check acquire file path exists
    def test_invalid_config_gets_caught_during_initialization(self):
        context = {}
        with self.assertRaises(ValueError) as ve:
            OutputWriter(config=context)
        self.assertEqual(
            str(ve.exception),
            "Config 'output_file_path' needs to be provided for parsing",
        )

    def test_invalid_output_format_gets_caught_during_initialization(self):
        context = {
            "acquire": {"file_path": "/anonymizer/test_data.csv", "delimiter": ","},
            "anonymize": {
                "output_file_path": "/anonymizer/output",
                "output_file_format": "invalid_format",
            },
        }
        with self.assertRaises(ValueError) as ve:
            OutputWriter(config=context)
        self.assertEqual(
            str(ve.exception),
            output_file_format_err_msg,
        )

    def test_correct_output_path_is_generated(self):
        output_format_list = ["csv", "parquet"]
        for output_format in output_format_list:
            with self.subTest():
                context = {
                    "acquire": {
                        "file_path": "/anonymizer/test_data.csv",
                        "delimiter": ",",
                    },
                    "anonymize": {
                        "output_file_path": "/anonymizer/output",
                        "output_file_format": output_format,
                    },
                }
                input_file_name = "test_data"
                output_directory = "/anonymizer/output"
                expected = (
                    f"{output_directory}/{input_file_name}_anonymized.{output_format}"
                )
                writer = OutputWriter(config=context)
                self.assertEqual(writer.get_output_file_path(), expected)

    def test_correct_output_path_is_generated_when_reading_multiple_files(self):
        output_format_list = ["csv", "parquet"]
        for output_format in output_format_list:
            with self.subTest():
                context = {
                    "acquire": {
                        "file_path": "./test_data/multiple_csv/*.csv",
                        "delimiter": ",",
                    },
                    "anonymize": {
                        "output_file_path": "/anonymizer/output",
                        "output_file_format": output_format,
                    },
                }
                output_directory = "/anonymizer/output"
                expected = f"{output_directory}/anonymized.{output_format}"
                writer = OutputWriter(config=context)
                self.assertEqual(writer.get_output_file_path(), expected)

    def test_correct_output_path_is_generated_when_filename_is_given(self):
        output_format_list = ["csv", "parquet"]
        for output_format in output_format_list:
            with self.subTest():
                context = {
                    "acquire": {
                        "file_path": f"./test_data/multiple_csv/*.csv",
                        "delimiter": ",",
                    },
                    "anonymize": {
                        "output_file_path": "/anonymizer/output",
                        "output_file_name": "hello",
                        "output_file_format": output_format,
                    },
                }
                output_directory = "/anonymizer/output"
                expected = f"{output_directory}/hello.{output_format}"
                writer = OutputWriter(config=context)
                self.assertEqual(writer.get_output_file_path(), expected)

    @patch("pandas.DataFrame.to_csv")
    @patch("pandas.DataFrame.to_parquet")
    def test_writer_call_correct_methods_on_write(self, mock_to_parquet, mock_to_csv):
        df = pd.read_csv("./test_data/test_data.csv")
        mock_to_csv.return_value = None
        mock_to_parquet.return_value = None

        output_format_list = ["csv", "parquet"]
        for output_format in output_format_list:
            with self.subTest():
                context = {
                    "acquire": {
                        "file_path": "/anonymizer/test_data.csv",
                        "delimiter": ",",
                    },
                    "anonymize": {
                        "output_file_path": "./output",
                        "output_file_format": output_format,
                    },
                }
                OutputWriter(config=context).write(df)
                match output_format:
                    case "csv":
                        mock_to_csv.assert_called()
                    case "parquet":
                        mock_to_parquet.assert_called()
