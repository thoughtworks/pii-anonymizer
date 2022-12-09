from unittest import TestCase
from unittest.mock import patch

from pii_anonymizer.common.analyze.detectors.thai_id_detector import ThaiIdDetector


class TestThaiIdDetector(TestCase):
    def setUp(self):
        self.thai_id_detector = ThaiIdDetector()
        self.valid_ids = ["8113000277484", "6-2005-20034-26-8", "6 2005 20034 26 8"]
        self.invalid_ids = ["8113000277480", "6-2005-20034-26-0", "6 2005 20034 26 0"]

    def test_get_name_returns_the_valid_detector_name(self):
        self.assertEqual(self.thai_id_detector.get_name(), "TH_ID")

    def test_get_pattern_returns_compiled_regex(self):
        actual_value = self.thai_id_detector.get_pattern()
        return_value = "\d.*\d"
        self.assertEqual(return_value, actual_value)

    def test_valid_id(self):
        for id in self.valid_ids:
            with self.subTest():
                self.assertTrue(self.thai_id_detector.validate(id))

    def test_valid_id_gets_detected_correctly(self):
        for id in self.valid_ids:
            with self.subTest():
                self.assertEqual(len(self.thai_id_detector.execute(id)), 1)

    def test_invalid_id_does_not_get_detected(self):
        for id in self.invalid_ids:
            with self.subTest():
                self.assertEqual(len(self.thai_id_detector.execute(id)), 0)
        # self.assertEqual(len(self.thai_id_detector.execute("8113000277484")), 0)
