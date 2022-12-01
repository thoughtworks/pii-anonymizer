from unittest import TestCase

from pii_anonymizer.common.file_utils import get_format, format_glob


class TestFileUtils(TestCase):
    def setUp(self):
        self.get_format = get_format
        self.format_glob = format_glob

    def test_should_format_input_path_to_glob(self):
        test_cases = [
            {"path": "./test_data", "expect": "./test_data/*"},
            {"path": "./test_data/", "expect": "./test_data/*"},
            {"path": "./test_data/*", "expect": "./test_data/*"},
            {"path": "./test_data/*.csv", "expect": "./test_data/*.csv"},
            {
                "path": "./test_data/test_data.csv",
                "expect": "./test_data/test_data.csv",
            },
        ]
        for test_case in test_cases:
            with self.subTest():
                expect = test_case["expect"]
                actual = self.format_glob(test_case["path"])
                self.assertEqual(actual, expect)

    def test_should_return_file_format(self):
        test_cases = [
            {"path": "./test_data/multiple_csv/*", "expect": "csv"},
            {"path": "./test_data/*.csv", "expect": "csv"},
            {"path": "./test_data/test_data.csv", "expect": "csv"},
            {"path": "./test_data/test_data.parquet", "expect": "parquet"},
            {"path": "./test_data/*.parquet", "expect": "parquet"},
            {"path": "./test_data/*.parq", "expect": "parquet"},
        ]

        for test_case in test_cases:
            with self.subTest():
                expect = test_case["expect"]
                actual = self.get_format(test_case["path"])
                self.assertEqual(actual, expect)
