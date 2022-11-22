from pii_anonymizer.standalone.analyze.utils.analyzer_result import AnalyzerResult

from hashlib import sha256


class Anonymizer:
    @staticmethod
    def replace(text: str, replace_string: str, analyzer_results: [AnalyzerResult]):
        for result in analyzer_results:
            text = text.replace(result.text, replace_string)
        return text

    @staticmethod
    def hash(text: str, analyzer_results: [AnalyzerResult]):
        for result in analyzer_results:
            text = text.replace(
                result.text, sha256(result.text.encode("utf-8")).hexdigest()
            )
        return text
