import csv
import os
import random
import string


def random_string(length=10):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def random_number(length=5):
    return "".join(random.choices(string.digits, k=length))


def generate_csv(filename):
    rows = 0
    max_size = 100 * 1024 * 1024
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Column1", "Column2", "Column3"])

        while csvfile.tell() < max_size:
            writer.writerow([random_string(10), random_string(10), random_number(5)])
            rows += 1
    print(f"{filename} generated with {rows} rows.")


generate_csv(f"file.csv")
