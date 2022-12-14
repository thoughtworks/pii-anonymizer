import csv
from faker import Faker

field_names = ["id", "name", "address", "phone_number", "email", "credit_card"]
fake = Faker()

data = []
for i in range(1000):
    data.append(
        {
            "id": i + 1,
            "name": fake.name(),
            "address": fake.address().replace("\n", " ").replace("\r", " "),
            "phone_number": fake.phone_number(),
            "email": fake.ascii_email(),
            "credit_card": fake.credit_card_number(),
        }
    )

with open("test_data/generated.csv", "w") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(data)
