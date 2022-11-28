import os
import sys

from pii_anonymizer.common.config_validator import validate

sys.path.append(os.path.abspath("."))
import json

from pii_anonymizer.standalone.report.report_generator import ReportGenerator
from pii_anonymizer.standalone.acquire.csv_parser import CsvParser
from pii_anonymizer.standalone.analyze.detectors.pii_detector import PIIDetector
from pii_anonymizer.common.constants import ACQUIRE, REPORT
from pii_anonymizer.standalone.write.output_writer import OutputWriter


class DPFMain:
    def __init__(self, config_file_path):
        with open(config_file_path) as config_file:
            self.config = json.load(config_file)

    # TODO : validate the config for the stages right here
    def run(self):
        validate(self.config)
        parsed_data_frame = CsvParser(config=self.config[ACQUIRE]).parse()
        pii_analysis_report, anonymized_data_frame = PIIDetector(
            self.config
        ).analyze_data_frame(parsed_data_frame)
        if pii_analysis_report.empty:
            print("NO PII VALUES WERE FOUND!")
        else:
            ReportGenerator(config=self.config[REPORT]).generate(
                results_df=pii_analysis_report,
            )
        OutputWriter(config=self.config).write(df=anonymized_data_frame)


# output_directory needs to be obtained from the config json file as a parameter in the 'anonymize' section.
