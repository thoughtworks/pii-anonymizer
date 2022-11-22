from unittest import TestCase
from pyspark.sql import SparkSession
from pii_anonymizer.spark.anonymize.anonymizer import Anonymizer
from hashlib import sha256


class TestHashAnonymizer(TestCase):
    def setUp(self) -> None:
        self.SPARK = (
            SparkSession.builder.master("local")
            .appName("Test PIIDetector")
            .getOrCreate()
        )

    def test_hash_for_single_analyzer_result(self):
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii", "something else")]
        )
        hashed = sha256("pii".encode("utf-8")).hexdigest()
        analyzer_results = ["pii"]
        result = test_data_frame.rdd.map(
            lambda row: Anonymizer.hash(row, analyzer_results)
        ).toDF()

        actual = result.collect()[0][0]

        self.assertEqual(actual, f"text containing {hashed}")

    def test_hash_for_multiple_analyzer_results(self):
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii1 and pii2", "something else")]
        )
        analyzer_results = ["pii1", "pii2"]
        hashed1 = sha256("pii1".encode("utf-8")).hexdigest()
        hashed2 = sha256("pii2".encode("utf-8")).hexdigest()
        result = test_data_frame.rdd.map(
            lambda row: Anonymizer.hash(row, analyzer_results)
        ).toDF()

        actual = result.collect()[0][0]

        self.assertEqual(actual, f"text containing {hashed1} and {hashed2}")
