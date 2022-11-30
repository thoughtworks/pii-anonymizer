from unittest import TestCase
from unittest.mock import MagicMock, call
from pyspark.sql import SparkSession
from pii_anonymizer.spark.write.output_writer import (
    OutputWriter,
    output_file_format_err_msg,
)


class TestOutputWriter(TestCase):
    def setUp(self) -> None:
        self.SPARK = (
            SparkSession.builder.master("local").appName("Test CsvWriter").getOrCreate()
        )

    def test_invalid_config_gets_caught_during_initialization(self):
        context = {}
        with self.assertRaises(ValueError) as ve:
            OutputWriter(self.SPARK, config=context)
        self.assertEqual(
            str(ve.exception),
            "Config 'output_file_path' needs to be provided for parsing",
        )

    def test_invalid_output_format_gets_caught_during_initialization(self):
        context = context = {
            "acquire": {"file_path": "/anonymizer/test_data.csv", "delimiter": ","},
            "anonymize": {
                "output_file_path": "/anonymizer/output",
                "output_file_format": "invalid_format",
            },
        }
        with self.assertRaises(ValueError) as ve:
            OutputWriter(self.SPARK, config=context)
        self.assertEqual(str(ve.exception), output_file_format_err_msg)

    def test_correct_output_path_is_generated(self):
        context = {
            "acquire": {"file_path": "/anonymizer/test_data.csv", "delimiter": ","},
            "anonymize": {"output_file_path": "/anonymizer/output"},
        }
        input_file_name = "test_data"
        output_directory = "/anonymizer/output"
        expected = f"{output_directory}/{input_file_name}_anonymized"
        writer = OutputWriter(spark=self.SPARK, config=context)
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
                expected = f"{output_directory}/anonymized"
                writer = OutputWriter(spark=self.SPARK, config=context)
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
                expected = f"{output_directory}/hello"
                writer = OutputWriter(spark=self.SPARK, config=context)
                self.assertEqual(writer.get_output_file_path(), expected)

    def test_writer_call_correct_methods_on_write(self):
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
                df = MagicMock()
                OutputWriter(self.SPARK, config=context).write(df)
                match output_format:
                    case "csv":
                        kall = (
                            call.write.mode("overwrite")
                            .option("header", "true")
                            .csv("./output/test_data_anonymized")
                        )
                        self.assertEqual(df.mock_calls, kall.call_list())
                    case "parquet":
                        kall = (
                            call.write.mode("overwrite")
                            .option("header", "true")
                            .parquet("./output/test_data_anonymized")
                        )
                        self.assertEqual(df.mock_calls, kall.call_list())
