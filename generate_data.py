import json
import random

""" Simple script to generate random names that will be used
    to send thru Kafka producer and received by consumer and
    finally be inserted into database.
    It will create a JSON file that has an array of objects
    containing first_name, last_name, email and height in each one.
"""

NAMES = ["John", "Anna", "Tom", "Kate", "Anthony", "Mike", "George", "John"]
LAST_NAMES = ["Smith", "Washington", "Gates",
              "Jobs", "Tate", "Lang", "Bon", "Ackerman"]
EMAILS = ["@gmail.com", "@yahoo.com", "@hotmail.com", "@mail.com"]


with open("accounts.json", "w") as accounts_file:
    json_file = json
    data = []
    # create a 1000 random objects with names, email and height
    for i in range(200):
        datum = {}
        datum["first_name"] = random.choice(NAMES)
        datum["last_name"] = random.choice(LAST_NAMES)
        datum["email"] = datum["first_name"] + "." + \
            datum["last_name"] + \
            str(random.randint(1, 101)) + random.choice(EMAILS)
        datum["height"] = random.randint(150, 199)
        data.append(datum)
    # write everything into file
    json.dump(data, accounts_file, indent=4, sort_keys=True)
