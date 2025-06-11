##          This script generates a Quality Assurance (QA) report from a SQL table containing USDA GATS data. It checks each                ##
##          available year for common data quality issues, including missing or zero values, and abnormally high unit prices.               ##
##          For every year it summarizes total value and quantity by country and HS10 code, it calculates average unit price,               ##
##          and logs any issues found. The final QA report is exported as a CSV to the user's Downloads folder.                             ##
##                                                                                                                                          ##
##                                                   Created by Anthony M. Schauer                                                          ##
##############################################################################################################################################
##############################################################################################################################################


##### Choose a Table to Check and Download ----- OPTIONS: hs10_code_full ; test_table ; countries ; hs10_cleaned 
table = "hs10_cleaned"


##### Step 0: Imports
import pandas as pd
import mysql.connector 
import re
from datetime import datetime
from pathlib import Path
import os


##### Step 1: Create Connection to MySQL 
def connect_to_sql():
    
    try:    
        conn = mysql.connector.connect(
            host="localhost",
            user="amschauer03",      
            password="Anthonyschauer1!",  
            database="usda_gats_db",
            auth_plugin="mysql_native_password"
        )
        print(f"\n✅ Connected to MySQL.")

        return conn 
    
    except mysql.connector.Error as err:
        print(f"❌ MySQL connection error: {err}")


##### Step 2: Get Years
def get_years_from_columns(cursor, table):
    
    cursor.execute(f"SHOW COLUMNS FROM {table}")
    columns = [col[0] for col in cursor.fetchall()]
    
    years = sorted([
        int(re.search(r'\d{4}', col).group())
        for col in columns
        if col.startswith('value') and re.search(r'\d{4}', col)
    ], reverse=True)
    print(f"\n✅ Detected years: {years}")

    return years


##### Step 3: Generate the QA Report 
def generate_qa_report(table):
    
    conn = connect_to_sql()
    cursor = conn.cursor()
    years = get_years_from_columns(cursor, table)
    all_rows = []

    for year in years:
        val_col = f"value{year}"
        qty_col = f"quantity{year}"

        query = f"""
        SELECT country_code, hs10_code,
               SUM({val_col}) AS total_value,
               SUM({qty_col}) AS total_quantity
        FROM {table}
        GROUP BY country_code, hs10_code
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            country, hs10, total_val, total_qty = row
            avg_unit_price = None
            issue_flag = ""

            if total_qty in (0, None):
                issue_flag = "Missing or zero quantity"
            elif total_val in (0, None):
                issue_flag = "Missing or zero value"
            else:
                avg_unit_price = total_val / total_qty
                if avg_unit_price == 0:
                    issue_flag = "Zero avg unit price"
                elif avg_unit_price > 10000:
                    issue_flag = "Abnormally high avg unit price"

            all_rows.append({
                "year": year,
                "country_code": country,
                "hs10_code": hs10,
                "value_sum": total_val,
                "quantity_sum": total_qty,
                "avg_unit_price": avg_unit_price,
                "issue_flag": issue_flag
            })

    df = pd.DataFrame(all_rows)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    downloads_path = Path.home() / "Downloads"
    filename = os.path.join(downloads_path, f"qa_report_{table}_{timestamp}.csv")
    df.to_csv(filename, index=False)
    print(f"\n✅ QA report saved to: {filename}\n")

    cursor.close()
    conn.close()


##### Step 4: Run Lines 
if __name__ == "__main__":
    print("------------------------------------------------------------------------------------------------------------------------------------")
    print(f"\n                      🚀 Running Quanilty Assurance Export | For Table: {table} | Starting at: {datetime.now()}\n")
    generate_qa_report(table) 
    print("------------------------------------------------------------------------------------------------------------------------------------\n")
