from pii_anonymizer.standalone.analyze.detectors.base_detector import BaseDetector
from pii_anonymizer.standalone.analyze.utils.regex import RegEx


class PhoneNumberDetector(BaseDetector):
    def __init__(self):
        self.name = "PHONE_NUMBER"

        self.pattern = (
            RegEx().one_of("0-9+(").extract_digit().range_occurrences(5, 15).build()
        )

    def get_name(self):
        return self.name

    def get_pattern(self):
        return self.pattern
