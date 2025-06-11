##### Choose a Table to Connect ----- OPTIONS: hs10_full ; test_table ; countries ; hs10_cleaned
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


##### Step 2: Get Top Markets
def get_top_markets(hs10_code, table):
    
    conn = connect_to_sql()

    try:
        query = f"SELECT * FROM {table} WHERE hs10_code = %s"
        df = pd.read_sql(query, conn, params=(hs10_code,))

        if df.empty:
            print("No data found.")
            return None

        value_cols = [col for col in df.columns if re.match(r"value\d{4}", col)]

        df_long = df.melt(id_vars=["country_name"], value_vars=value_cols,
                          var_name="year", value_name="value")
        df_long["year"] = df_long["year"].str.extract(r"(\d{4})").astype(int)

        df_long = df_long[df_long["value"].notnull()]

        def top_n(df, label, recent_years=None):
            if recent_years:
                df = df[df["year"].isin(recent_years)]
            grouped = df.groupby("country_name")["value"].sum().reset_index()
            grouped = grouped.sort_values("value", ascending=False).head(10)
            grouped["period"] = label
            return grouped

        years = sorted(df_long["year"].unique())
        last_10 = years[-10:] if len(years) >= 10 else years
        last_5 = years[-5:] if len(years) >= 5 else years

        top_all = top_n(df_long, "All Time")
        top_10 = top_n(df_long, "Last 10 Years", last_10)
        top_5 = top_n(df_long, "Last 5 Years", last_5)
        conn.close()
        
        return top_all, top_10, top_5

    except Exception as err:
        print(f"❌ Error: {err}")
        return None


##### Step 3: Run Lines
if __name__ == "__main__":
    print("\n📦 USDA GATS — Top Markets\n")
    hs_code = input("Enter an HS-10 code: ").strip()
    
    result = get_top_markets(hs_code, table)

    if result is not None:
        print("\n✅ Top Markets:\n")
        print(result)
    else:
        print("\n⚠️ No results returned.\n")