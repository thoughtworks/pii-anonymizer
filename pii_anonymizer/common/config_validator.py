from pii_anonymizer.common.constants import ANONYMIZE

anonymize_mode = ["redact", "drop"]
anonymize_mode_err_msg = f"{ANONYMIZE}'s mode must be {' or '.join(anonymize_mode)}"


def validate(config):
    # Fallback to default (drop)
    if config[ANONYMIZE].get("mode") is None:
        return

    if config[ANONYMIZE]["mode"] not in anonymize_mode:
        raise ValueError(anonymize_mode_err_msg)
