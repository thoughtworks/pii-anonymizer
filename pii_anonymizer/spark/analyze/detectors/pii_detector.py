import importlib
import pkgutil
import inspect
import sys
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, LongType
from pyspark.sql.functions import lit, array, col, udf
from pii_anonymizer.common.analyze.filter_overlap_analysis import (
    filter_overlapped_analysis,
)
from pii_anonymizer.common.constants import ANALYZE, ANONYMIZE
import pii_anonymizer.common.analyze.detectors
from pii_anonymizer.common.analyze.detectors.base_detector import BaseDetector
from pii_anonymizer.spark.anonymize.anonymizer import Anonymizer


class PIIDetector:
    def __init__(self, config):
        self.detectors = self.__get_detector_instances()
        self.config = config

    def __get_detector_modules(self):
        modules = [
            modname
            for importer, modname, ispkg in pkgutil.walk_packages(
                path=pii_anonymizer.common.analyze.detectors.__path__,
                prefix=pii_anonymizer.common.analyze.detectors.__name__ + ".",
            )
            if "tests" not in modname
        ]
        return modules

    def __get_detector_instances(self):
        modules = self.__get_detector_modules()
        detectors = []
        for module in modules:
            importlib.import_module(module)
            classes = inspect.getmembers(sys.modules[module], inspect.isclass)
            for class_name, class_type in classes:
                if class_name != "BaseDetector" and issubclass(
                    class_type, BaseDetector
                ):
                    detectors.append(class_type())
        return detectors

    def __detect_pii_column(self, data):
        results = []
        for detector in self.detectors:
            results += detector.execute(data)

        filtered = filter_overlapped_analysis(results)
        return {"text": data, "pii": filtered}

    def get_analyzer_results(
        self, input_data_frame: DataFrame, excluded_columns: List[str]
    ):
        array_structtype = StructType(
            [
                StructField("end", LongType(), False),
                StructField("start", LongType(), False),
                StructField("text", StringType(), False),
                StructField("type", StringType(), False),
            ]
        )

        for column in input_data_frame.columns:
            with_analysis_struct_type = StructType(
                [
                    StructField("text", StringType()),
                    StructField(
                        "pii", ArrayType(array_structtype, True), nullable=False
                    ),
                ]
            )
            if column in excluded_columns:
                format_excluded = udf(
                    lambda x: {"text": x, "pii": []}, with_analysis_struct_type
                )
                input_data_frame = input_data_frame.withColumn(
                    column, format_excluded(col(column))
                )
            else:
                detectPiiUDF = udf(
                    lambda x: self.__detect_pii_column(x), with_analysis_struct_type
                )
                input_data_frame = input_data_frame.withColumn(
                    column, detectPiiUDF(col(column))
                )

        report_data_frame = None
        report_schema = []
        for column in input_data_frame.columns:
            report_schema.append(
                StructField(column, ArrayType(array_structtype, True), nullable=False)
            )
            if report_data_frame is None:
                report_data_frame = input_data_frame

            report_data_frame = report_data_frame.withColumn(
                column, col(f"{column}.pii")
            )

        report_data_frame = report_data_frame.rdd.toDF(schema=StructType(report_schema))
        return report_data_frame, input_data_frame

    def _get_pii_list(self, row):
        get_analyzer_results_text = lambda x: x.text

        new_row = []
        for cell in row:
            pii_sublist = list(map(get_analyzer_results_text, cell))
            new_row.extend(pii_sublist)
        return new_row

    def get_redacted_text(self, input_data_frame: DataFrame):
        excluded_columns = self.config[ANALYZE].get("exclude", [])

        columns = input_data_frame.columns

        mode = self.config[ANONYMIZE].get("mode")
        value = self.config[ANONYMIZE].get("value", "")

        resultDf = input_data_frame.withColumn("replace_string", lit(value))

        format_excluded = udf(lambda x: x.get("text"), StringType())

        for column in columns:
            if column in excluded_columns:
                resultDf = resultDf.withColumn(
                    column,
                    format_excluded(column),
                )
                continue
            match mode:
                case "replace":
                    resultDf = resultDf.withColumn(
                        column,
                        Anonymizer.replace(column, "replace_string"),
                    )
                case "hash":
                    resultDf = resultDf.withColumn(
                        column,
                        Anonymizer.hash(column),
                    )
                case "encrypt":
                    resultDf = resultDf.withColumn(
                        column,
                        Anonymizer.encrypt(column),
                    )
                case _:
                    resultDf = resultDf.withColumn(
                        column,
                        Anonymizer.replace(column, "replace_string"),
                    )

        resultDf = resultDf.drop("replace_string")
        return resultDf

    def analyze_data_frame(self, input_data_frame: DataFrame):
        excluded_columns = self.config[ANALYZE].get("exclude", [])
        report, with_analysis_data_frame = self.get_analyzer_results(
            input_data_frame, excluded_columns
        )
        redacted = self.get_redacted_text(with_analysis_data_frame)

        return report, redacted
