from typing import List
from hashlib import sha256
from cryptography.fernet import Fernet
import os
from pii_anonymizer.common.analyze.analyzer_result import AnalyzerResult


class Anonymizer:
    @staticmethod
    def replace(text: str, replace_string: str, analyzer_results: List[AnalyzerResult]):
        for result in analyzer_results:
            text = text.replace(result.text, replace_string)
        return text

    @staticmethod
    def hash(text: str, analyzer_results: List[AnalyzerResult]):
        for result in analyzer_results:
            text = text.replace(
                result.text, sha256(result.text.encode("utf-8")).hexdigest()
            )
        return text

    @staticmethod
    def encrypt(text: str, analyzer_results: List[AnalyzerResult]):
        secret = os.getenv("PII_SECRET")

        f = Fernet(secret)

        for result in analyzer_results:
            encrypted_bytes = f.encrypt(result.text.encode())
            ascii_string = encrypted_bytes.decode("ascii")
            text = text.replace(result.text, ascii_string)
        return text
