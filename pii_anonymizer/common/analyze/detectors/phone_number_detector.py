from pii_anonymizer.common.analyze.detectors.base_detector import BaseDetector
from pii_anonymizer.common.analyze.regex import RegEx


class PhoneNumberDetector(BaseDetector):
    def __init__(self):
        self.name = "PHONE_NUMBER"

        self.pattern = (
            RegEx()
            .group_start()
            .one_of("+\(")
            .any_digit()
            .range_occurrences(1, 3)
            .one_of(")")
            .zero_or_one_occurrences()
            .pipe()
            .any_digit()
            .group_end()
            .group_start()
            .any_digit()
            .pipe()
            .one_of("\ .\-x")
            .group_end()
            .at_least(4)
            .any_digit()
            .build()
        )

    def get_name(self):
        return self.name

    def get_pattern(self):
        return self.pattern
