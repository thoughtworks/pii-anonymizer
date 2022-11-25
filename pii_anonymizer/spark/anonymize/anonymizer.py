from hashlib import sha256
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class Anonymizer:
    @udf(returnType=StringType())
    def replace(text: str, replace_string: str, pii_list):
        for word in pii_list:
            text = text.replace(word, replace_string)
        return text

    @udf(returnType=StringType())
    def hash(text: str, pii_list):
        for word in pii_list:
            text = text.replace(word, sha256(word.encode("utf-8")).hexdigest())
        return text
