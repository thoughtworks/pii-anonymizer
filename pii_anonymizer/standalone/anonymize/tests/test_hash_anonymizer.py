from unittest import TestCase
from pii_anonymizer.standalone.anonymize.anonymizer import Anonymizer
from pii_anonymizer.standalone.analyze.utils.analyzer_result import AnalyzerResult
from hashlib import sha256


class TestRedactAnonymizer(TestCase):
    def test_hash_for_single_analyzer_result(self):
        text = "text containing pii"
        hashed = sha256("pii".encode("utf-8")).hexdigest()
        analyzer_results = [AnalyzerResult("pii", "PII_DETECTOR", 16, 18)]
        result = Anonymizer.hash(text, analyzer_results)
        self.assertEqual(result, f"text containing {hashed}")

    def test_hash_for_multiple_analyzer_results(self):
        text = "text containing pii1 and pii2"
        hashed1 = sha256("pii1".encode("utf-8")).hexdigest()
        hashed2 = sha256("pii2".encode("utf-8")).hexdigest()
        analyzer_results = [
            AnalyzerResult("pii1", "PII_DETECTOR", 16, 19),
            AnalyzerResult("pii2", "PII_DETECTOR", 25, 28),
        ]
        result = Anonymizer.hash(text, analyzer_results)
        self.assertEqual(result, f"text containing {hashed1} and {hashed2}")
