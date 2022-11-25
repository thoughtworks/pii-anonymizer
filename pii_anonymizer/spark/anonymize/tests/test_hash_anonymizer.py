from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, array
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
            [("text containing pii", "something else")], ["test"]
        )
        hashed = sha256("pii".encode("utf-8")).hexdigest()
        pii_list = ["pii"]
        resultDf = test_data_frame.withColumn("pii_list", array(*map(lit, pii_list)))
        resultDf = resultDf.withColumn(
            "test",
            Anonymizer.hash("test", "pii_list"),
        )
        actual = resultDf.collect()[0][0]

        self.assertEqual(actual, f"text containing {hashed}")

    def test_hash_for_multiple_analyzer_results(self):
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii1 and pii2", "something else")], ["test"]
        )
        pii_list = ["pii1", "pii2"]
        hashed1 = sha256("pii1".encode("utf-8")).hexdigest()
        hashed2 = sha256("pii2".encode("utf-8")).hexdigest()

        resultDf = test_data_frame.withColumn("pii_list", array(*map(lit, pii_list)))
        resultDf = resultDf.withColumn(
            "test",
            Anonymizer.hash("test", "pii_list"),
        )
        actual = resultDf.collect()[0][0]

        self.assertEqual(actual, f"text containing {hashed1} and {hashed2}")
