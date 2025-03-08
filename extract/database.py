import mysql.connector as conn
import pandas as pd
import json
import config
from validation import Validation


class Database:
    def __init__(self, database, read_data):
        # print('*****inside database')

        self.database = database  # Move this line above db_config() to initialize before calling it
        self.table_name = "Orders"
        self.data = read_data

        self.db_config()  # Now self.database is assigned before calling this method
        self.create_table()

    def db_config(self):
        # print("inside config", self.database)

        self.conn = conn.connect(
            host=self.database["host"],
            user=self.database["user"],
            password=self.database["password"],
            database=self.database["database"]
        )
        self.cursor = self.conn.cursor()
        # print("*******connection is successful")

    def add_calculated_columns(self):
        self.data["QuantityOrdered"] = pd.to_numeric(self.data["QuantityOrdered"], errors="coerce")
        self.data["ItemPrice"] = pd.to_numeric(self.data["ItemPrice"], errors="coerce")

        def extract_promotion_discount(value):
            if isinstance(value, dict):
                return float(value.get("Amount", 0))
            elif isinstance(value, str):
                try:
                    discount_data = json.loads(value)
                    return float(discount_data.get("Amount", 0))
                except json.JSONDecodeError:
                    return 0
            return float(value) if pd.notna(value) else 0

        self.data["PromotionDiscount"] = self.data["PromotionDiscount"].apply(extract_promotion_discount)
        self.data["total_sales"] = self.data["QuantityOrdered"] * self.data["ItemPrice"]
        self.data["net_sale"] = self.data["total_sales"] - self.data["PromotionDiscount"]
        self.data = self.data[self.data["net_sale"] > 0]

    def create_table(self):
        """Create table with total_sales and net_sale columns."""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            OrderId VARCHAR(50) PRIMARY KEY,
            OrderItemId BIGINT,
            PromotionDiscount FLOAT,
            batch_id INT,
            total_sales FLOAT, 
            net_sale FLOAT 
        );
        """
        self.cursor.execute(create_table_query)
        self.conn.commit()
        print("Table created successfully.")

    def insert(self):
        """Insert data into the database, ignoring duplicates."""
        insert_query = f"""
        INSERT IGNORE INTO {self.table_name} (OrderId, OrderItemId, PromotionDiscount, batch_id, total_sales, net_sale) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        self.data = self.data.where(pd.notna(self.data), None)
        self.add_calculated_columns()

        data_to_insert = [
            (str(row["OrderId"]), int(row["OrderItemId"]), float(row["PromotionDiscount"]), int(row["batch_id"]),
             float(row["total_sales"]), float(row["net_sale"]))
            for _, row in self.data.iterrows()
        ]

        try:
            for data in data_to_insert:
                self.cursor.execute(insert_query, data)

            self.conn.commit()
            print(f"{self.cursor.rowcount} rows inserted successfully (duplicates ignored).")

        except conn.Error as e:
            print(f"Error inserting data: {e}")

        except Exception as e:
            print(str(e))

    def close_connection(self):
        self.cursor.close()
        self.conn.close()


class Extract:
    def __init__(self, region_paths):
        self.region_paths = region_paths
        self.database = config.database
        self.read_and_insert()

    def read_and_insert(self):
        print("****************insert sheet data inside the databsase*****************")

        for path in self.region_paths:
            # print(f"Reading file: {path}")
            df = pd.read_csv(path)
            df = df.where(pd.notna(df), None)

            db = Database(database=config.database, read_data=df)
            db.insert()

            # db.close_connection()

        print("****************Validation *****************")

        validation = Validation(
            host=self.database["host"],
            user=self.database["user"],
            password=self.database["password"],
            database=self.database["database"]
        )
        validation.validate_all()
        validation.close_connection()


if __name__ == "__main__":
    region_urls = [r"C:\Users\91700\Downloads\order_region_b(in).csv"]
    extractor = Extract(region_urls)
