"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2


conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="-8Nb7AL1F7"
)

with open('./north_data/employees_data.csv') as file:
    reader = csv.reader(file)
    employees_data = []
    for line in reader:
        cor_line = tuple(line)
        employees_data.append(cor_line)

with open('./north_data/customers_data.csv') as file:
    reader = csv.reader(file)
    customers_data = []
    for line in reader:
        cor_line = tuple(line)
        customers_data.append(cor_line)

with open('./north_data/orders_data.csv') as file:
    reader = csv.reader(file)
    orders_data = []
    for line in reader:
        cor_line = tuple(line)
        orders_data.append(cor_line)

cur = conn.cursor()

cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees_data[1:])
cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", customers_data[1:])
cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_data[1:])
conn.commit()

cur.close()
conn.close()
