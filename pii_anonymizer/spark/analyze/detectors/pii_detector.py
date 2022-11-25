import importlib
import pkgutil
import inspect
import sys
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, LongType
from pyspark.sql.functions import lit, array
from pii_anonymizer.common.constants import ANALYZE, ANONYMIZE
from pii_anonymizer.spark.analyze.detectors.base_detector import BaseDetector
import pii_anonymizer.spark.analyze.detectors
from pii_anonymizer.spark.anonymize.anonymizer import Anonymizer


class PIIDetector:
    def __init__(self, config):
        self.detectors = self.__get_detector_instances()
        self.config = config

    def __get_detector_modules(self):
        modules = [
            modname
            for importer, modname, ispkg in pkgutil.walk_packages(
                path=pii_anonymizer.spark.analyze.detectors.__path__,
                prefix=pii_anonymizer.spark.analyze.detectors.__name__ + ".",
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

    def __detect_pii_row(self, row):
        new_row = []
        for element in row:
            results = []
            for detector in self.detectors:
                results += detector.execute(element)
            new_row.append(results)

        return new_row

    def get_analyzer_results(self, input_data_frame: DataFrame):
        columns = input_data_frame.columns

        array_structtype = StructType(
            [
                StructField("end", LongType(), False),
                StructField("start", LongType(), False),
                StructField("text", StringType(), False),
                StructField("type", StringType(), False),
            ]
        )
        result_schema = []
        for column in columns:
            result_schema.append(
                StructField(column, ArrayType(array_structtype, True), nullable=False)
            )

        result = input_data_frame.rdd.map(lambda x: self.__detect_pii_row(x)).toDF(
            schema=StructType(result_schema)
        )

        return result

    def _get_pii_list(self, row):
        get_analyzer_results_text = lambda x: x.text

        new_row = []
        for cell in row:
            pii_sublist = list(map(get_analyzer_results_text, cell))
            new_row.extend(pii_sublist)
        return new_row

    def get_redacted_text(self, input_data_frame: DataFrame, report: DataFrame):
        excluded_columns = self.config[ANALYZE].get("exclude", [])
        pii_list = report.rdd.flatMap(lambda row: self._get_pii_list(row)).collect()
        columns = input_data_frame.columns

        mode = self.config[ANONYMIZE].get("mode")
        value = self.config[ANONYMIZE].get("value", "")

        resultDf = input_data_frame.withColumn(
            "pii_list", array(*map(lit, pii_list))
        ).withColumn("replace_string", lit(value))

        for column in columns:
            if column in excluded_columns:
                continue
            match mode:
                case "replace":
                    resultDf = resultDf.withColumn(
                        column,
                        Anonymizer.replace(column, "replace_string", "pii_list"),
                    )
                case "hash":
                    resultDf = resultDf.withColumn(
                        column,
                        Anonymizer.hash(column, "pii_list"),
                    )
                case _:
                    resultDf = resultDf.withColumn(
                        column,
                        Anonymizer.replace(column, "replace_string", "pii_list"),
                    )

        resultDf = resultDf.drop("replace_string", "pii_list")
        return resultDf

    def analyze_data_frame(self, input_data_frame: DataFrame):
        excluded_columns = self.config[ANALYZE].get("exclude", [])
        selected_columns = [
            col for col in input_data_frame.columns if col not in excluded_columns
        ]
        excluded_data_frame = input_data_frame.select(selected_columns)
        report = self.get_analyzer_results(excluded_data_frame)
        # report = self.get_analyzer_results(input_data_frame)
        redacted = self.get_redacted_text(input_data_frame, report)

        return report, redacted
