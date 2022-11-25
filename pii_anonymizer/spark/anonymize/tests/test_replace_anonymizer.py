from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, array
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
            [("text containing pii", "something else")], ["test"]
        )
        pii_list = ["pii"]
        resultDf = test_data_frame.withColumn(
            "pii_list", array(*map(lit, pii_list))
        ).withColumn("replace_string", lit(replace_string))
        resultDf = resultDf.withColumn(
            "test",
            Anonymizer.replace("test", "replace_string", "pii_list"),
        )

        actual = resultDf.collect()[0][0]

        self.assertEqual(actual, f"text containing {replace_string}")

    def test_replace_for_multiple_analyzer_results(self):
        replace_string = "[REPLACED]"
        test_data_frame = self.SPARK.createDataFrame(
            [("text containing pii1 and pii2", "something else")], ["test"]
        )
        pii_list = ["pii1", "pii2"]
        resultDf = test_data_frame.withColumn(
            "pii_list", array(*map(lit, pii_list))
        ).withColumn("replace_string", lit(replace_string))
        resultDf = resultDf.withColumn(
            "test",
            Anonymizer.replace("test", "replace_string", "pii_list"),
        )
        actual = resultDf.collect()[0][0]
        self.assertEqual(
            actual, f"text containing {replace_string} and {replace_string}"
        )
