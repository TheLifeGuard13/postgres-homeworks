import json

import psycopg2
from config import config


def main():
    script_file = "fill_db.sql"
    json_file = "suppliers.json"
    db_name = "homework5"

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({"dbname": db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")

    cur.close()
    conn.commit()
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, encoding="utf-8") as file:
        script = file.read()
    cur.execute(script)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""CREATE TABLE suppliers 
    (supplier_id SERIAL PRIMARY KEY, 
    company_name VARCHAR(100),
    contact VARCHAR(100),
    address VARCHAR(100),
    phone VARCHAR(100),
    fax VARCHAR(100), 
    homepage VARCHAR(100),
    products VARCHAR(200))""")


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей."""
    with open(json_file, encoding="utf-8") as file:
        return json.load(file)


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    list_of_tuples = []
    for one_dict in suppliers:
        company_name = one_dict["company_name"]
        contact = one_dict["contact"]
        address = one_dict["address"]
        phone = one_dict["phone"]
        fax = one_dict["fax"]
        homepage = one_dict["homepage"]
        products = ", ".join(one_dict["products"])
        exit_tuple = (company_name, contact, address, phone, fax, homepage, products)
        list_of_tuples.append(exit_tuple)
    for input_tuple in list_of_tuples:
        cur.execute("""INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)""", input_tuple)


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute("""CREATE TABLE suppliers_short 
        (supplier_id SERIAL PRIMARY KEY, 
        company_name VARCHAR(100))""")
    cur.execute("""CREATE TABLE suppliers_long 
        (some_id SERIAL PRIMARY KEY, 
        company_name VARCHAR(100),
        contact VARCHAR(100),
        address VARCHAR(100),
        phone VARCHAR(100),
        fax VARCHAR(100), 
        homepage VARCHAR(100),
        product VARCHAR(200))""")
    with open(json_file, encoding="utf-8") as file:
        info = json.load(file)
    list_of_tuples = []
    list_of_tuples_two = []
    for one_dict in info:
        company_name_two = one_dict["company_name"]
        exit_tuple_two = (company_name_two,)
        list_of_tuples_two.append(exit_tuple_two)
        for i in range(len(one_dict["products"])):
            company_name = one_dict["company_name"]
            contact = one_dict["contact"]
            address = one_dict["address"]
            phone = one_dict["phone"]
            fax = one_dict["fax"]
            homepage = one_dict["homepage"]
            product = one_dict["products"][i]
            exit_tuple = (company_name, contact, address, phone, fax, homepage, product)
            list_of_tuples.append(exit_tuple)
    for input_tuple in list_of_tuples:
        cur.execute("""INSERT INTO suppliers_long (company_name, contact, address, phone, fax, homepage, product)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""", input_tuple)
    for input_tuple_two in list_of_tuples_two:
        cur.execute("INSERT INTO suppliers_short (company_name) VALUES (%s)", input_tuple_two)
    cur.execute("""
    ALTER TABLE suppliers_long ADD COLUMN supplier_true_id int;
    UPDATE suppliers_long SET supplier_true_id = 
    (SELECT supplier_id FROM suppliers_short WHERE suppliers_long.company_name = suppliers_short.company_name);
    ALTER TABLE products ADD COLUMN supplier_id int;
    UPDATE products SET supplier_id = 
    (SELECT supplier_true_id FROM suppliers_long WHERE suppliers_long.product = products.product_name);
    ALTER TABLE products ADD CONSTRAINT fk_products_product_name FOREIGN KEY (supplier_id) REFERENCES suppliers;
    DROP TABLE suppliers_short;
    DROP TABLE suppliers_long
    """)


if __name__ == "__main__":
    main()
