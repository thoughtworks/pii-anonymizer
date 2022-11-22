from unittest import TestCase

from pii_anonymizer.common.config_validator import validate, anonymize_mode_err_msg


class TestConfigValidator(TestCase):
    def setUp(self):
        self.config_validator = validate

    def test_should_throw_when_invalid_config(self):
        config = {
            "acquire": {"file_path": "./test_data/test_data.csv", "delimiter": ","},
            "analyze": {},
            "report": {"location": "./output", "level": "medium"},
            "anonymize": {"mode": "invalid_mode", "output_file_path": "./output"},
        }
        with self.assertRaises(ValueError) as ve:
            self.config_validator(config)
        self.assertEqual(str(ve.exception), anonymize_mode_err_msg)
