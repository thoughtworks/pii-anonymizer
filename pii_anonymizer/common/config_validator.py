from pii_anonymizer.common.constants import ANONYMIZE, ANALYZE

analyze_err_msg = f"{ANALYZE} key is required in config"
anonymize_err_msg = f"{ANONYMIZE} key is required in config"
anonymize_mode = ["replace", "hash"]
anonymize_mode_err_msg = f"{ANONYMIZE}'s mode must be {' or '.join(anonymize_mode)}"


def validate(config):
    if config.get(ANALYZE) is None:
        return ValueError(analyze_err_msg)
    if config.get(ANONYMIZE) is None:
        return ValueError(anonymize_err_msg)
    # Fallback to default (drop)
    if config[ANONYMIZE].get("mode") is None:
        return
    if config[ANONYMIZE]["mode"] not in anonymize_mode:
        raise ValueError(anonymize_mode_err_msg)
