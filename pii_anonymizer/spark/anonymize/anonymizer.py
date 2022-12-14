from hashlib import sha256
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
import os


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

    @udf(returnType=StringType())
    def encrypt(text: str, secret: str, pii_list):
        secret = os.getenv("PII_SECRET")
        f = Fernet(secret)

        for word in pii_list:
            encrypted_bytes = f.encrypt(word.encode())
            ascii_string = encrypted_bytes.decode("ascii")
            text = text.replace(word, ascii_string)
        return text
