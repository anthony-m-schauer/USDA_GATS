


##### Choose a Table to Connect ----- OPTIONS: test_table ; hs10_cleaned ----- Example code: 0201206000
table = "hs10_cleaned"


##### Step 0: Imports 
import pandas as pd
import mysql.connector
import re
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


#### Step 3: Get the Volatility Score
def calculate_volatility_score(hs10_code, table):
    conn = connect_to_sql()
    cursor = conn.cursor()
    years = get_years_from_columns(cursor, table)
    df = pd.read_sql(f"SELECT * FROM {table} WHERE hs10_code = %s", conn, params=(hs10_code,))
    totals = []

    if df.empty:
        print(f"⚠️ No data found for HS10 code: {hs10_code}")
        return pd.DataFrame()

    value_cols = [f"value{year}" for year in years if f"value{year}" in df.columns]
    totals = df[value_cols].sum(axis=0)
    totals.index = [int(col.replace("value", "")) for col in totals.index]
    totals = totals.sort_index()
    pct_changes = totals.pct_change().dropna()
    pct_changes = pct_changes.clip(lower=-2, upper=2) 
    volatility_score = round(pct_changes.std() * 100, 2)

    return volatility_score


##### Step 4: Run Full Process
if __name__ == "__main__":
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    print(f"\n                      🚀 Launching Volatility Score Calculator | For Table: {table}\n") 

    hs10_code = input("🔍 Enter an HS10 code to analyze: ").strip()
    volatility = calculate_volatility_score(hs10_code, table)
    print(f"\n📈 Volatility Score for {hs10_code}: {volatility}")

    print("\n🏁 Done.")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------\n")