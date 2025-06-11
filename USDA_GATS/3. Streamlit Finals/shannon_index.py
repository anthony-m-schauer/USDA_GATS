



##### Choose a Table to Connect ----- OPTIONS: test_table ; hs10_cleaned ----- Example code: 0201206000
table = "hs10_cleaned"

##### Step 0: Imports
import pandas as pd
import mysql.connector
import re
import numpy as np
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

##### Step 1: SQL Connection
def connect_to_sql():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="amschauer03",
            password="Anthonyschauer1!",
            database="usda_gats_db",
            auth_plugin="mysql_native_password"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"❌ SQL connection error: {err}")
        return None

##### Step 2: Get List of Trade Years
def get_years_from_columns(cursor, table):
    cursor.execute(f"SHOW COLUMNS FROM {table}")
    columns = [col[0] for col in cursor.fetchall()]
    
    years = sorted([
        int(re.search(r'\d{4}', col).group())
        for col in columns
        if col.startswith('value') and re.search(r'\d{4}', col)
    ])
    return years

##### Step 3: Calculate Shannon Index
def calculate_shannon_index(hs10_code, table):
    conn = connect_to_sql()
    cursor = conn.cursor()
    years = get_years_from_columns(cursor, table)
    df = pd.read_sql(f"SELECT * FROM {table} WHERE hs10_code = %s", conn, params=(hs10_code,))
    results = []

    if df.empty:
        print(f"⚠️ No data found for HS10 code: {hs10_code}")
        return pd.DataFrame()

    for year in years:
        value_col = f"value{year}"
        if value_col not in df.columns:
            continue

        sub = df[["country_code", value_col]].copy()
        sub = sub[sub[value_col].notna()]
        total = sub[value_col].sum()

        if total == 0 or len(sub) == 0:
            shannon = None
        else:
            proportions = sub[value_col] / total
            shannon = -np.sum(proportions * np.log(proportions)) 

        results.append({
            "Year": year,
            "Shannon Index": round(shannon, 4) if shannon is not None else None
        })

    results = pd.DataFrame(results).sort_values(by="Year", ascending=False)
    cursor.close()
    conn.close()
    
    return results

##### Step 4: Run Full Process
if __name__ == "__main__":
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    print(f"\n                      🚀 Launching Shannon Index Calculator | For Table: {table}\n") 

    hs10_code = input("🔍 Enter an HS10 code to analyze: ").strip()

    shannon_df = calculate_shannon_index(hs10_code, table)
    
    print(f"\n{shannon_df}")

    print("\n🏁 Done.")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------\n")

