from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, LongType
from pii_anonymizer.spark.anonymize.anonymizer import Anonymizer
from hashlib import sha256


class TestHashAnonymizer(TestCase):
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
            )
        ]
    )

    def setUp(self) -> None:
        self.SPARK = (
            SparkSession.builder.master("local")
            .appName("Test PIIDetector")
            .getOrCreate()
        )

    def test_hash_for_single_analyzer_result(self):
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
                        }
                    }
                ),
                ({"test": {"text": "test", "pii": []}}),
            ],
            self.rdd_schema,
        )
        hashed = sha256("pii".encode("utf-8")).hexdigest()

        test_data_frame = test_data_frame.withColumn(
            "test",
            Anonymizer.hash("test"),
        )
        actual = test_data_frame.collect()[0][0]
        self.assertEqual(actual, f"text containing {hashed}")

    def test_hash_for_multiple_analyzer_results(self):
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
                        }
                    }
                ),
                ({"test": {"text": "test", "pii": []}}),
            ],
            self.rdd_schema,
        )
        hashed1 = sha256("pii1".encode("utf-8")).hexdigest()
        hashed2 = sha256("pii2".encode("utf-8")).hexdigest()

        test_data_frame = test_data_frame.withColumn(
            "test",
            Anonymizer.hash("test"),
        )
        actual = test_data_frame.collect()[0][0]

        self.assertEqual(actual, f"text containing {hashed1} and {hashed2}")
