from typing import List
from pii_anonymizer.common.analyze.analyzer_result import AnalyzerResult

from pii_anonymizer.common.constants import DETECTOR_PRIORITY


def filter_overlapped_analysis(analyzer_results: List[AnalyzerResult]):
    # Compare every element in list
    # If overlap compare overlapping element's distance (start/end)
    # if match is equal check which detector has precedent before remove
    index_to_remove = set()

    for a in range(len(analyzer_results)):
        if a == len(analyzer_results) - 1:
            break
        result_a = analyzer_results[a]
        result_b = analyzer_results[a + 1]
        if (
            result_a.start <= result_b.start <= result_a.end
            or result_a.start <= result_b.end <= result_a.end
        ):
            # Overlap, pick longest match, then lower precendence
            a_distance = result_a.end - result_a.start
            b_distance = result_b.end - result_b.start
            if a_distance > b_distance:
                index_to_remove.add(a + 1)
            elif a_distance < b_distance:
                index_to_remove.add(a)
            else:
                result_a_precedence = DETECTOR_PRIORITY[result_a.type]
                result_b_precedence = DETECTOR_PRIORITY[result_b.type]
                if result_a_precedence < result_b_precedence:
                    index_to_remove.add(a + 1)
                elif result_a_precedence > result_b_precedence:
                    index_to_remove.add(a)

    sorted_index = sorted(index_to_remove, reverse=True)
    for index in sorted_index:
        analyzer_results.pop(index)

    return analyzer_results
