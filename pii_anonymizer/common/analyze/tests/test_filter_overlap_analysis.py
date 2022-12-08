from unittest import TestCase

from pii_anonymizer.common.analyze.analyzer_result import AnalyzerResult
from pii_anonymizer.common.analyze.filter_overlap_analysis import (
    filter_overlapped_analysis,
)


class TestFilterOverlap(TestCase):
    def test_should_keep_analysis_when_no_overlap(self):
        analyzer_results = [
            AnalyzerResult("text", "type", 0, 10),
            AnalyzerResult("text", "type", 11, 20),
        ]

        analysis = filter_overlapped_analysis(analyzer_results)
        self.assertEqual(len(analysis), 2)

    def test_should_keep_longer_matched_when_overlap(self):
        analyzer_results = [
            AnalyzerResult("text", "type", 0, 10),
            AnalyzerResult("longer", "type", 5, 20),
        ]

        analysis = filter_overlapped_analysis(analyzer_results)
        self.assertEqual(len(analysis), 1)
        self.assertEqual(analysis[0].text, "longer")

    def test_should_keep_higher_priority_when_length_is_same(self):
        analyzer_results = [
            AnalyzerResult("THAI_ID", "TH_ID", 0, 7),
            AnalyzerResult("+661234", "PHONE_NUMBER", 3, 10),
        ]

        analysis = filter_overlapped_analysis(analyzer_results)
        self.assertEqual(len(analysis), 1)
        self.assertEqual(analysis[0].type, "TH_ID")
