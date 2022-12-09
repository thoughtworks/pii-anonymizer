from unittest import TestCase

from pii_anonymizer.common.analyze.detectors.phone_number_detector import (
    PhoneNumberDetector,
)
from pii_anonymizer.common.analyze.analyzer_result import AnalyzerResult


class TestPhoneNumberDetector(TestCase):
    def setUp(self):
        self.phone_number_detector = PhoneNumberDetector()

    def test_default_property_values_are_correct(self):
        self.assertEqual("PHONE_NUMBER", self.phone_number_detector.name)
        self.assertEqual(
            "([+\(]\d{1,3}[)]?|\d)(\d|[\ .\-x]){4,}\d",
            self.phone_number_detector.pattern,
        )

    def test_invalid_phone_number_does_not_get_detected(self):
        self.assertEqual(len(self.phone_number_detector.execute("S00001I")), 0)

    def __assert_single_result(self, text_to_be_tested, start, end):
        actual = self.phone_number_detector.execute(text_to_be_tested)
        expected = AnalyzerResult(text_to_be_tested, "PHONE_NUMBER", start, end)
        self.assertEqual(len(actual), 1)
        self.assertEqual(expected, actual[0])

    def test_valid_phone_number_gets_detected_correctly(self):
        test_cases = [
            {"text": "65781234", "match": "65781234", "start": 0, "end": 8},
            {"text": "85781234", "match": "85781234", "start": 0, "end": 8},
            {"text": "95781234", "match": "95781234", "start": 0, "end": 8},
            {"text": "6578 1234", "match": "6578 1234", "start": 0, "end": 9},
            {"text": "8578 1234", "match": "8578 1234", "start": 0, "end": 9},
            {"text": "9578 1234", "match": "9578 1234", "start": 0, "end": 9},
            {"text": "+65 65781234", "match": "+65 65781234", "start": 0, "end": 12},
            {"text": "+65 85781234", "match": "+65 85781234", "start": 0, "end": 12},
            {"text": "+65 95781234", "match": "+65 95781234", "start": 0, "end": 12},
            {"text": "+66 65781234", "match": "+66 65781234", "start": 0, "end": 12},
            {"text": "+65 6578 1234", "match": "+65 6578 1234", "start": 0, "end": 13},
            {"text": "+65 8578 1234", "match": "+65 8578 1234", "start": 0, "end": 13},
            {"text": "+65 9578 1234", "match": "+65 9578 1234", "start": 0, "end": 13},
            {"text": "+66 6578 1234", "match": "+66 6578 1234", "start": 0, "end": 13},
            {"text": "(65) 65781234", "match": "(65) 65781234", "start": 0, "end": 13},
            {"text": "(65) 85781234", "match": "(65) 85781234", "start": 0, "end": 13},
            {"text": "(65) 95781234", "match": "(65) 95781234", "start": 0, "end": 13},
            {
                "text": "(65) 6578 1234",
                "match": "(65) 6578 1234",
                "start": 0,
                "end": 14,
            },
            {
                "text": "(65) 8578 1234",
                "match": "(65) 8578 1234",
                "start": 0,
                "end": 14,
            },
            {
                "text": "(65) 9578 1234",
                "match": "(65) 9578 1234",
                "start": 0,
                "end": 14,
            },
            # Faker's
            {"text": "468.168.1559", "match": "468.168.1559", "start": 0, "end": 12},
            {"text": "591-231-6760", "match": "591-231-6760", "start": 0, "end": 12},
            {"text": "055-692-3144", "match": "055-692-3144", "start": 0, "end": 12},
            {"text": "(540)180-1611", "match": "(540)180-1611", "start": 0, "end": 13},
            {
                "text": "899-040-0003x606",
                "match": "899-040-0003x606",
                "start": 0,
                "end": 16,
            },
            {
                "text": "669.511.0962x1518",
                "match": "669.511.0962x1518",
                "start": 0,
                "end": 17,
            },
            {
                "text": "Call now: 02-123-4567 ext 555",
                "match": "02-123-4567",
                "start": 10,
                "end": 21,
            },
            {
                "text": "โทร 02-123-4567 ต่อ 555",
                "match": "02-123-4567",
                "start": 4,
                "end": 15,
            },
        ]
        for test_case in test_cases:
            with self.subTest():
                actual = self.phone_number_detector.execute(test_case["text"])
                expected = AnalyzerResult(
                    test_case["match"],
                    "PHONE_NUMBER",
                    test_case["start"],
                    test_case["end"],
                )
                self.assertEqual(len(actual), 1)
                self.assertEqual(expected, actual[0])
