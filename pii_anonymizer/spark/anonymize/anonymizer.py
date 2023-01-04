from hashlib import sha256
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
import os


class Anonymizer:
    @udf(returnType=StringType())
    def replace(data, replace_string: str):
        pii_list = data["pii"]
        text = data["text"]

        for word in pii_list:
            text = text.replace(word.text, replace_string)
        return text

    @udf(returnType=StringType())
    def hash(data):
        pii_list = data["pii"]
        text = data["text"]

        for pii in pii_list:
            word = pii.text
            text = text.replace(word, sha256(word.encode("utf-8")).hexdigest())
        return text

    @udf(returnType=StringType())
    def encrypt(data: str):
        secret = os.getenv("PII_SECRET")
        f = Fernet(secret)

        pii_list = data["pii"]
        text = data["text"]

        for pii in pii_list:
            word = pii.text
            encrypted_bytes = f.encrypt(word.encode())
            ascii_string = encrypted_bytes.decode("ascii")
            text = text.replace(word, ascii_string)
        return text
