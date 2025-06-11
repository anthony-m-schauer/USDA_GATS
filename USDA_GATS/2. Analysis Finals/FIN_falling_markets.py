##            This script queries the usda_gats_db MySQL database to identify falling markets for a user-provided            ##
##               HS-10 code. It calculates percent decrease in export value by country and returns the top 10                ##
##                   fastest-declining markets of all time, the last 10 years, and the last 5 years.                         ##
##                                                                                                                           ##
##                                                   Created by Anthony M. Schauer                                           ##
###############################################################################################################################
###############################################################################################################################



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


##### Step 2: Calculate Falling Markets 
def get_falling_markets(hs10_code, table):
    
    conn = connect_to_sql()

    try:
        query = f"SELECT * FROM {table} WHERE hs10_code = %s"
        df = pd.read_sql(query, conn, params=(hs10_code,))

        if df.empty:
            print("No data found.")
            return None

        value_cols = [col for col in df.columns if re.match(r"value\d{4}", col)]
        df_long = df.melt(id_vars=["country_code"], value_vars=value_cols,
                          var_name="year", value_name="value")
        df_long["year"] = df_long["year"].str.extract(r"(\d{4})").astype(int)
        df_long = df_long[df_long["value"].notnull()]

        def falling(df, label, years=None):
            if years:
                df = df[df["year"].isin(years)]
        
            falling = []
            for country, group in df.groupby("country_code"):
                group_sorted = group.sort_values("year")
                start = group_sorted.iloc[0]["value"]
                end = group_sorted.iloc[-1]["value"]
                if start > end and start != 0:
                    fall_pct = round(((start - end) / start) * 100, 2)
                else:
                    fall_pct = None
                falling.append({
                    "country_code": country,
                    "start_value": start,
                    "end_value": end,
                    "percent_fall": fall_pct,
                    "period": label
                })
        
            fall_df = pd.DataFrame(falling)
            fall_df = fall_df.dropna(subset=["percent_fall"])
            fall_df = fall_df.sort_values("percent_fall", ascending=False).head(10)
            return fall_df

        years = sorted(df_long["year"].unique())
        last_10 = years[-10:] if len(years) >= 10 else years
        last_5 = years[-5:] if len(years) >= 5 else years

        fall_all = falling(df_long, "All Time")
        fall_10 = falling(df_long, "Last 10 Years", last_10)
        fall_5 = falling(df_long, "Last 5 Years", last_5)

        return fall_all, fall_10, fall_5

    except Exception as err:
        print(f"❌ Error: {err}")
        return None
    finally:
        conn.close()


##### Step 3: Run Lines
if __name__ == "__main__":
    print("\n📈 USDA GATS — Falling Markets\n")
    hs_code = input("Enter an HS-10 code: ").strip()

    result = get_falling_markets(hs_code, table)

    if result is not None:
        print("\n🚀 Falling Markets by Falling Percent:\n")
        print(result)
    else:
        print("\n⚠️ No results returned.\n")