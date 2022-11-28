import os
import sys

from pii_anonymizer.common.config_validator import validate

sys.path.append(os.path.abspath("."))

import json

from pyspark.sql import SparkSession
from pii_anonymizer.spark.report.report_generator import ReportGenerator
from pii_anonymizer.spark.acquire.csv_parser import CsvParser
from pii_anonymizer.spark.analyze.detectors.pii_detector import PIIDetector
from pii_anonymizer.spark.constants import ACQUIRE, REPORT
from pii_anonymizer.spark.write.output_writer import OutputWriter
from pii_anonymizer.common.get_args import get_args


class Main:
    def __init__(self, config_file_path):
        with open(config_file_path) as config_file:
            self.config = json.load(config_file)

    # TODO : validate the config for the stages right here
    def run(self):
        validate(self.config)
        spark = (
            SparkSession.builder.master("local").appName("PIIDetector").getOrCreate()
        )
        parsed_data_frame = CsvParser(spark, config=self.config[ACQUIRE]).parse()
        pii_analysis_report, redacted_data_frame = PIIDetector(
            self.config
        ).analyze_data_frame(parsed_data_frame)

        report_generator = ReportGenerator(config=self.config[REPORT])
        if report_generator.is_empty_report_dataframe(pii_analysis_report):
            print("NO PII VALUES WERE FOUND!")
        else:
            report_generator.generate(results_df=pii_analysis_report)
        OutputWriter(spark, config=self.config).write(df=redacted_data_frame)


if __name__ == "__main__":
    args = get_args()
    Main(args).run()
