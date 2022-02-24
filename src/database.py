import sqlite3
import sys
import os


class Database:
    def __init__(self, name: str):
        self.name = name
        self.connection = None
        self.cursor = None
        self.create()

    def create(self):
        self.connect()

    def connect(self):

        if not os.path.exists("db"):
            os.makedirs("db")

        self.connection = sqlite3.connect(f"db/{self.name}.db")
        self.cursor = self.connection.cursor()

    def delete(self):
        os.remove(f"db/{self.name}.db")

    def execute(self, request: str):
        self.cursor.execute(request)

    def commit(self):
        self.connection.commit()

    def push(self, request: str):
        self.execute(request)
        self.commit()

    def disconnect(self):
        self.close()

    def close(self):
        self.connection.close()
        self.connection = None
        self.cursor = None

    def fetch(self, option: str = "one"):
        if option == "one":
            result = self.cursor.fetchone()
        elif option == "all":
            result = self.cursor.fetchall()

        return result

    ## CUSTOM Functions

    # Tables

    def create_table(self, table_name: str, columns: list):
        if self.check_table(table_name) is None:
            self.push(f"CREATE TABLE {table_name} ({', '.join(columns)})")
        else:
            print(f"Table {table_name} already exists")

    def check_table(self, table_name: str):
        self.push(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        return self.fetch()

    def drop_table(self, table_name: str):
        self.push(f"DROP TABLE {table_name}")

    # Queries

    def insert(self, table_name: str, columns: list, values: list):
        self.push(
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        )

    def select(
        self,
        table_name: str,
        columns: list,
        conditions: list = None,
        fetch_option: str = "one",
    ):
        if conditions is None:
            query = f"SELECT {', '.join(columns)} FROM {table_name}"
        else:
            query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE {', '.join(conditions)}"

        self.push(query)

        return self.fetch(fetch_option)
