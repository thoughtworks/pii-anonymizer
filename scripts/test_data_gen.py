from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import math

field_names = ["id", "name", "address", "phone_number", "email", "credit_card"]
fake = Faker()

KB_to_generate = 1000 * 500
num_records = math.ceil(KB_to_generate * 7.9)

spark = SparkSession.builder.master("local[*]").appName("PIIDetector").getOrCreate()


def address():
    return fake.address().replace("\n", " ").replace("\r", " ")


df = (
    spark.range(num_records)
    .withColumn("name", udf(fake.name)())
    .withColumn("address", udf(address)())
    .withColumn("email", udf(fake.phone_number)())
    .withColumn("phone_number", udf(fake.ascii_email)())
    .withColumn("credit_card", udf(fake.credit_card_number)())
)
df.write.mode("overwrite").option("header", "true").csv("test_data/generated.csv")
