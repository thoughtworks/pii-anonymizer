import importlib
import pkgutil
import inspect
import sys

import pii_anonymizer.standalone.analyze.detectors
from pii_anonymizer.standalone.analyze.detectors.base_detector import BaseDetector
from pii_anonymizer.standalone.anonymize.anonymizer import Anonymizer
from pii_anonymizer.standalone.anonymize.anonymizer_result import AnonymizerResult
from pii_anonymizer.common.constants import ANALYZE, ANONYMIZE


# TODO : refactor this to use the annotations instead of the module path.
class PIIDetector:
    def __init__(self, config):
        self.detectors = self.__get_detector_instances()
        self.config = config

    def __get_detector_modules(self):
        modules = [
            modname
            for importer, modname, ispkg in pkgutil.walk_packages(
                path=pii_anonymizer.standalone.analyze.detectors.__path__,
                prefix=pii_anonymizer.standalone.analyze.detectors.__name__ + ".",
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

    # TODO : Should we make this static?
    def analyze_and_anonymize(self, text: str):
        analyzer_results = []
        for detector in self.detectors:
            analyzer_results = analyzer_results + detector.execute(text)

        mode = self.config[ANONYMIZE].get("mode")
        value = self.config[ANONYMIZE].get("value", "")

        match mode:
            case "replace":
                redacted_text = Anonymizer.replace(text, value, analyzer_results)
            case "hash":
                redacted_text = Anonymizer.hash(text, analyzer_results)
            case _:
                redacted_text = Anonymizer.replace(text, value, analyzer_results)

        return AnonymizerResult(redacted_text, analyzer_results)

    def __contains_pii(self, results):
        for result in results:
            if len(result.analyzer_results) > 0:
                return True
        return False

    def exclude_column(self, data):
        exclude_columns = self.config[ANALYZE].get("exclude", [])
        if data.name in exclude_columns:
            return data.map(lambda x: AnonymizerResult(x, []))
        return data.map(self.analyze_and_anonymize)

    def analyze_data_frame(self, input_data_frame):
        result_df = input_data_frame.apply(self.exclude_column)

        return result_df.applymap(lambda x: x.analyzer_results), result_df.applymap(
            lambda x: x.redacted_text
        )
