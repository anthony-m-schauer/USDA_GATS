##      This script exports a specified table from the usda_gats_db MySQL database that was built using USDA GATS export data to     ##
##      a CSV file. The resulting file includes a timestamp at the top. This script is designed to be run directly with Python.      ##
##                                                                                                                                   ##
##                                                   Created by Anthony M. Schauer                                                   ##
#######################################################################################################################################
#######################################################################################################################################


##### Choose a Table to Download ----- OPTIONS: hs10_full ; test_table ; countries 
table = 'test_table' 


##### Step 0: Imports
import os
import mysql.connector
import pandas as pd
from datetime import datetime
from pathlib import Path  


##### Step 1: Set up Download Information 
csv_filename = f'{table}_csv_download.csv'
downloads_folder = str(Path.home() / "Downloads")
csv_path = os.path.join(downloads_folder, csv_filename)


##### Step 2: Export Function 
def export_usda_table():
    

    ##### Step 3: Connect to SQL
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="amschauer03",      
            password="Anthonyschauer1!",  
            database="usda_gats_db",
            auth_plugin="mysql_native_password"
        )
        cursor = conn.cursor()
        print("\n✅ Connected to SQL Database.")
        
        
        ##### Step 4: Execute SQL Query with Timestamp 
        query = f"SELECT * FROM {table}"
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(f"# Exported on: {timestamp}\n")
        df.to_csv(csv_path, mode='a', index=False)
        print(f"\n✅ Backup successful! Data exported to: {csv_path}")

    except mysql.connector.Error as err:
        print(f"❌ SQL Connection Error: {err}")
    except Exception as e:
        print(f"❌ General Connection Error: {e}")
    

    ##### Step 5: Close Everything 
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("\n✅ Database connection closed.\n")


##### Step 6: Run Lines
if __name__ == "__main__":
    print("------------------------------------------------------------------------------------------------------------------------------------")
    print(f"\n                      🚀 Running Export Table | For Table: {table} | Starting at: {datetime.now()}\n")
    export_usda_table()
    print("------------------------------------------------------------------------------------------------------------------------------------\n")
