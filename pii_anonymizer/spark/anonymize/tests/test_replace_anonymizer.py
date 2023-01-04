from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, LongType
from pii_anonymizer.spark.anonymize.anonymizer import Anonymizer


class TestReplaceAnonymizer(TestCase):
    rdd_schema = StructType(
        [
            StructField(
                "test",
                StructType(
                    [
                        StructField("text", StringType()),
                        StructField(
                            "pii",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("end", LongType(), False),
                                        StructField("start", LongType(), False),
                                        StructField("text", StringType(), False),
                                        StructField("type", StringType(), False),
                                    ]
                                ),
                                True,
                            ),
                            nullable=True,
                        ),
                    ]
                ),
                nullable=False,
            ),
            StructField("replace_string", StringType(), False),
        ]
    )

    def setUp(self) -> None:
        self.SPARK = (
            SparkSession.builder.master("local")
            .appName("Test PIIDetector")
            .getOrCreate()
        )

    def test_replace_for_single_analyzer_result(self):
        replace_string = "[REPLACED]"

        test_data_frame = self.SPARK.createDataFrame(
            [
                (
                    {
                        "test": {
                            "text": "text containing pii",
                            "pii": [
                                {
                                    "end": 0,
                                    "start": 0,
                                    "text": "pii",
                                    "type": "NRIC",
                                }
                            ],
                        },
                        "replace_string": replace_string,
                    }
                ),
                (
                    {
                        "test": {"text": "test", "pii": []},
                        "replace_string": replace_string,
                    }
                ),
            ],
            self.rdd_schema,
        )

        test_data_frame = test_data_frame.withColumn(
            "test",
            Anonymizer.replace("test", "replace_string"),
        )

        actual = test_data_frame.collect()[0][0]

        self.assertEqual(actual, f"text containing {replace_string}")

    def test_replace_for_multiple_analyzer_results(self):
        replace_string = "[REPLACED]"

        test_data_frame = self.SPARK.createDataFrame(
            [
                (
                    {
                        "test": {
                            "text": "text containing pii1 and pii2",
                            "pii": [
                                {
                                    "end": 0,
                                    "start": 0,
                                    "text": "pii1",
                                    "type": "NRIC",
                                },
                                {
                                    "end": 0,
                                    "start": 0,
                                    "text": "pii2",
                                    "type": "NRIC",
                                },
                            ],
                        },
                        "replace_string": replace_string,
                    }
                ),
                (
                    {
                        "test": {"text": "test", "pii": []},
                        "replace_string": replace_string,
                    }
                ),
            ],
            self.rdd_schema,
        )

        test_data_frame = test_data_frame.withColumn(
            "test",
            Anonymizer.replace("test", "replace_string"),
        )
        actual = test_data_frame.collect()[0][0]
        self.assertEqual(
            actual, f"text containing {replace_string} and {replace_string}"
        )
