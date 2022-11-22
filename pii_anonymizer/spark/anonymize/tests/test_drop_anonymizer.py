from unittest import TestCase
from pyspark.sql import SparkSession
from pii_anonymizer.spark.anonymize.anonymizer import Anonymizer


class TestDropAnonymizer(TestCase):
    def setUp(self) -> None:
        self.SPARK = (
            SparkSession.builder.master("local")
            .appName("Test PIIDetector")
            .getOrCreate()
        )

    def test_drop_for_single_analyzer_result(self):
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii", "something else")]
        )

        analyzer_results = ["pii"]
        result = test_data_frame.rdd.map(
            lambda row: Anonymizer.drop(row, analyzer_results)
        ).toDF()

        actual = result.collect()[0][0]

        self.assertEqual(actual, "text containing ")

    def test_drop_for_multiple_analyzer_results(self):
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii1 and pii2", "something else")]
        )
        analyzer_results = ["pii1", "pii2"]

        result = test_data_frame.rdd.map(
            lambda row: Anonymizer.drop(row, analyzer_results)
        ).toDF()

        actual = result.collect()[0][0]

        self.assertEqual(actual, "text containing  and ")
