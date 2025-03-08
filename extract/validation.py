import mysql.connector

class Validation:
    def __init__(self, host, user, password, database, table_name="orders"):

        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.conn.cursor()

        self.table_name = table_name

    def count_total_records(self):
        query = f"SELECT COUNT(*) FROM {self.table_name}"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def total_sales_by_region(self):
        query = f"""
        SELECT Region, SUM(total_sales) AS total_sales_amount
        FROM {self.table_name}
        GROUP BY Region
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def avg_sales_per_transaction(self):

        query = f"SELECT AVG(total_sales) FROM {self.table_name}"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def check_duplicate_order_ids(self):
        query = f"""
        SELECT OrderId, COUNT(*) 
        FROM {self.table_name} 
        GROUP BY OrderId 
        HAVING COUNT(*) > 1
        """
        self.cursor.execute(query)
        duplicates = self.cursor.fetchall()
        return duplicates if duplicates else "No duplicate OrderIds found."

    def validate_all(self):
        print(f"Total Records: {self.count_total_records()}")
        # print(f"Total Sales by Region: {self.total_sales_by_region()}")
        print(f"Average Sales per Transaction: {self.avg_sales_per_transaction()}")
        print(f"Duplicate Order IDs: {self.check_duplicate_order_ids()}")

    def close_connection(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    validator = Validation(
        host="localhost",
        user="root",
        password="12345",
        database="interview"
    )
    
    validator.validate_all()
    validator.close_connection()
