import os
from unittest import TestCase
from cryptography.fernet import Fernet

from pii_anonymizer.standalone.anonymize.anonymizer import Anonymizer
from pii_anonymizer.common.analyze.analyzer_result import AnalyzerResult


class TestEncryptAnonymizer(TestCase):
    def test_encrypt_for_single_analyzer_result(self):
        secret = Fernet.generate_key()
        os.environ["PII_SECRET"] = secret.decode()
        fernet = Fernet(secret)

        text = "text containing pii"
        analyzer_results = [AnalyzerResult("pii", "PII_DETECTOR", 16, 18)]
        result = Anonymizer.encrypt(text, analyzer_results)
        encrypted = result[16:]
        decrypted = fernet.decrypt(encrypted).decode()

        self.assertEqual(decrypted, "pii")
        self.assertFalse("pii" in result)
