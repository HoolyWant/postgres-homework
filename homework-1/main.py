"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
import csv
from pathlib import Path

conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='qwerty'
)
my_cwd = os.getcwd()
customers_csv = os.path.join(my_cwd, 'north_data/customers_data.csv')
employees_csv = os.path.join(my_cwd, 'north_data/employees_data.csv')
orders_csv = os.path.join(my_cwd, 'north_data/orders_data.csv')
try:
    with conn:
        with conn.cursor() as cur:

            with open(customers_csv, encoding='utf-8') as customers_file:
                reader = csv.DictReader(customers_file)
                for row in reader:
                    customer_id, company_name, contact_name = row['customer_id'], \
                                                              row['company_name'], \
                                                              row['contact_name']
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (customer_id, company_name, contact_name))

            with open(employees_csv, encoding='utf-8') as employees_file:
                reader = csv.DictReader(employees_file)
                for row in reader:
                    employee_id, first_name, last_name, title, birth_date, notes = int(row['employee_id']), \
                                                                                   row['first_name'], \
                                                                                   row['last_name'], \
                                                                                   row['title'], \
                                                                                   row['birth_date'], \
                                                                                   row['notes']
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (employee_id, first_name, last_name, title, birth_date, notes))

            with open(orders_csv, encoding='utf-8') as orders_file:
                reader = csv.DictReader(orders_file)
                for row in reader:
                    order_id, customer_id, employee_id, order_date, ship_city = row['order_id'], \
                                                                                row['customer_id'], \
                                                                                row['employee_id'], \
                                                                                row['order_date'], \
                                                                                row['ship_city']
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (order_id, customer_id, employee_id, order_date, ship_city))
finally:
    conn.close()
