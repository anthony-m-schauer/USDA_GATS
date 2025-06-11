##      This script checks a specified table in the usda_gats_db MySQL database for duplicate entries based on given key             ##
##       columns. It prints any duplicates found to the console. Modify the table and keys at the bottom before running.             ##
##                                                                                                                                   ##
##                                                   Created by Anthony M. Schauer                                                   ##
#######################################################################################################################################
#######################################################################################################################################


##### Tables in usda_gats_db Database ----- OPTIONS: hs10_full ; test_table ; countries ; hs10_cleaned
table_list = ["hs10_cleaned"]       # Must be list [] even if just one table

##### Step 0: Imports
import mysql.connector 


##### Step 1: Duplicate Finder Function
def find_duplicates(table_name, key_columns):

 
    ##### Step 2: Connect to SQL
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


        ##### Step 3: Execute SQL Duplicate Query
        key_str = ", ".join(key_columns)
        query = f"""
            SELECT {key_str}, COUNT(*) as count
            FROM {table_name}
            GROUP BY {key_str}
            HAVING COUNT(*) > 1
        """
        cursor.execute(query)
        duplicates = cursor.fetchall()

        if duplicates:
            print(f"\n⚠️  Duplicate entries found in table `{table_name}` based on {key_columns}:")
            for row in duplicates:
                print(row)
        else:
            print(f"\n🎉 No duplicates found in table `{table_name}`.")

    except mysql.connector.Error as err:
        print(f"❌ SQL Connection Error: {err} \n")
    except Exception as e:
        print(f"❌ General Error: {e} \n")


    ##### Step 4: Close Everything
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("\n✅ Database connection closed.\n")


##### Step 5: Run Lines
if __name__ == "__main__":
    for table in table_list:
        if table == "countries":
            keys = ["country_code"]
        else:
            keys = ["country_code", "hs10_code"]

        print("------------------------------------------------------------------------------------------------------------------------------------")
        print(f"\n                      🔍 Checking for duplicates in `{table}`:")
        find_duplicates(table, keys)
        print("------------------------------------------------------------------------------------------------------------------------------------\n")