from unittest import TestCase
from pyspark.sql import SparkSession
from pii_anonymizer.spark.anonymize.anonymizer import Anonymizer


class TestReplaceAnonymizer(TestCase):
    def setUp(self) -> None:
        self.SPARK = (
            SparkSession.builder.master("local")
            .appName("Test PIIDetector")
            .getOrCreate()
        )

    def test_replace_for_single_analyzer_result(self):
        replace_string = "[REPLACED]"
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii", "something else")]
        )
        analyzer_results = ["pii"]
        result = test_data_frame.rdd.map(
            lambda row: Anonymizer.replace(row, replace_string, analyzer_results)
        ).toDF()

        actual = result.collect()[0][0]

        self.assertEqual(actual, f"text containing {replace_string}")

    def test_replace_for_multiple_analyzer_results(self):
        replace_string = "[REPLACED]"
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii1 and pii2", "something else")]
        )
        analyzer_results = ["pii1", "pii2"]

        result = test_data_frame.rdd.map(
            lambda row: Anonymizer.replace(row, replace_string, analyzer_results)
        ).toDF()

        actual = result.collect()[0][0]

        self.assertEqual(
            actual, f"text containing {replace_string} and {replace_string}"
        )
