from pii_anonymizer.standalone.analyze.utils.analyzer_result import AnalyzerResult

from hashlib import sha256


class Anonymizer:
    @staticmethod
    def drop(text: str, analyzer_results: [AnalyzerResult]):
        for result in analyzer_results:
            text = text.replace(result.text, "")
        return text

    @staticmethod
    def redact(text: str, analyzer_results: [AnalyzerResult]):
        for result in analyzer_results:
            text = text.replace(result.text, "[Redacted]")
        return text

    @staticmethod
    def hash(text: str, analyzer_results: [AnalyzerResult]):
        for result in analyzer_results:
            text = text.replace(
                result.text, sha256(result.text.encode("utf-8")).hexdigest()
            )
        return text
