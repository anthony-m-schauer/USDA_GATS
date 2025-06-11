##            This script checks a specified table in the usda_gats_db MySQL database for outliers in the value columns. It            ##
##             calculates the mean and standard deviation for each column, grouped by HS10 code, and flags any rows that               ##
##              fall significantly outside the expected range. All flagged rows are printed to the console for review.                 ##
##                                                                                                                                     ##
##                                                         Created by Anthony M. Schauer                                               ##
#########################################################################################################################################
#########################################################################################################################################



########################### MAKE SQL CONNECTION PROOF GOD DAMN - oh and also maybe two code folders where one is the build ones and one is for the calcs and eda and next ish



##### Choose a Table to Check ----- OPTIONS: test_table ; hs10_cleaned 
table = "hs10_cleaned"

 
##### Step 0: Imports 
import pandas as pd
import mysql.connector
import time 
from datetime import datetime
import re
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


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
        print(f"❌ SQL connection error: {err}")
        return None
    

##### Step 2: Create Time Keeper
def get_run_time(start_time):
    end_time = datetime.now()
    elapsed = (end_time - start_time)
    total_seconds = int(elapsed.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s.")


##### Step 3: Get Years from Columns
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


##### Step 4: Flag Outliers in DataFrame
def flag_outliers(conn, table, years):
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    print(f"\n✅ Loaded {len(df)} rows from {table}")

    value_cols = [f"value{year}" for year in years]
    df["outlier"] = None  

    for hs10, group in df.groupby("hs10_code"):
        means = group[value_cols].mean()
        stds = group[value_cols].std()

        for idx, row in group.iterrows():
            outlier_years = []
            for year in years:
                col = f"value{year}"
                val = row[col]
                mean = means[col]
                std = stds[col]

                if pd.notna(val) and pd.notna(std) and std > 0:
                    if abs(val - mean) > 3 * std:
                        outlier_years.append(str(year))

            if outlier_years:
                df.at[idx, "outlier"] = ",".join(outlier_years)

    num_flagged = df["outlier"].notna().sum()

    print(f"\n⚠️  Flagged {num_flagged} outlier rows.")
    print(f"👍 {df['outlier'].isnull().sum()} rows left as NULL.\n")

    return df


##### Step 5: Update the Outlier Column in SQL
def update_flag_column(df, conn, table):
    cursor = conn.cursor()
    updated = 0

    for idx, row in df.iterrows():
        
        success = False

        if idx % 2500 == 0 and idx!= 0:
            print(f"Completed {idx} of {len(df)}")
            print(f"⏱️  Current ", end="")
            get_run_time(start_time)

        while not success: 
            try:
                outlier = row["outlier"]
                country_code = row["country_code"]
                hs10_code = row["hs10_code"]

                query = f"""
                    UPDATE {table}
                    SET outlier = %s
                    WHERE country_code = %s AND hs10_code = %s
                """
                cursor.execute(query, (outlier, country_code, hs10_code))
                updated += 1
                success = True 
            
            except Exception as e:
                print(f"❌ Error on row {idx}: {e}")
                print(f"... Retrying ...")
                
                try: 
                    conn = connect_to_sql()
                    cursor = conn.cursor()
                except Exception as e:
                    print(f"❌ Reconnection failed: {e}")
                    time.sleep(30)
               
    conn.commit()
    print(f"\n✅ Updated {updated} rows with outlier flags in `{table}`.")


##### Step 6: Run Lines
if __name__ == "__main__":

    start_time = datetime.now()

    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    print(f"\n                      🚀 Running Outlier Calculation and Column Update | For Table: {table} | Starting at: {datetime.now()}\n")   
    
    conn = connect_to_sql()

    if conn:
        cursor = conn.cursor()
        years = get_years_from_columns(cursor, table)
        df = flag_outliers(conn, table, years)
        update_flag_column(df, conn, table)

        cursor.close()
        conn.close()

        print(f"⏱️  Total", end="")
        get_run_time(start_time)

        print("\n🏁 Done.")
        print("------------------------------------------------------------------------------------------------------------------------------------------------------------\n")    
