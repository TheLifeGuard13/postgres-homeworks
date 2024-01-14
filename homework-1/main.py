"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

EMPLOYEES_PATH = Path(__file__).parent.parent.joinpath("homework-1", "north_data", "employees_data.csv")
CUSTOMERS_PATH = Path(__file__).parent.parent.joinpath("homework-1", "north_data", "customers_data.csv")
ORDERS_PATH = Path(__file__).parent.parent.joinpath("homework-1", "north_data", "orders_data.csv")

load_dotenv()
password = os.getenv("PASSWORD")
conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=password)

with open(EMPLOYEES_PATH) as file:
    reader = csv.reader(file)
    employees_data = [tuple(line) for line in reader]

with open(CUSTOMERS_PATH) as file:
    reader = csv.reader(file)
    customers_data = [tuple(line) for line in reader]

with open(ORDERS_PATH) as file:
    reader = csv.reader(file)
    orders_data = [tuple(line) for line in reader]

cur = conn.cursor()

cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees_data[1:])
cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", customers_data[1:])
cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_data[1:])
conn.commit()

cur.close()
conn.close()
