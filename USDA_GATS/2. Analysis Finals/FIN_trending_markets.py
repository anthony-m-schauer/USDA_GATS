##      This script queries the usda_gats_db MySQL database to identify trending markets for a user-provided HS-10 code. It         ##
##          calculates the percent change in export value by country and then returns the top 10 fastest-growing                    ##
##              markets for three different time periods: all years, the last 10 years, and the last 5 years.                       ##
##                                                                                                                                  ##
##                                                   Created by Anthony M. Schauer                                                  ##
######################################################################################################################################
######################################################################################################################################


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


##### Step 2: Trending Markets Logic
def get_trending_markets(hs10_code, table="hs10_cleaned"):
    
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

        def trending(df, label, years=None):
            if years:
                df = df[df["year"].isin(years)]

            trends = []
            for country, group in df.groupby("country_code"):
                group_sorted = group.sort_values("year")
                start = group_sorted.iloc[0]["value"]
                end = group_sorted.iloc[-1]["value"]
                if start > 0:
                    growth_pct = ((end - start) / start) * 100
                else:
                    growth_pct = None 
                trends.append({
                    "country_code": country,
                    "start_value": start,
                    "end_value": end,
                    "growth_%": growth_pct,
                    "period": label
                })

            trend_df = pd.DataFrame(trends)
            trend_df = trend_df.dropna(subset=["growth_%"])
            trend_df = trend_df.sort_values("growth_%", ascending=False).head(10)
            return trend_df

        years = sorted(df_long["year"].unique())
        last_10 = years[-10:] if len(years) >= 10 else years
        last_5 = years[-5:] if len(years) >= 5 else years

        trend_all = trending(df_long, "All Time")
        trend_10 = trending(df_long, "Last 10 Years", last_10)
        trend_5 = trending(df_long, "Last 5 Years", last_5)

        return pd.concat([trend_all, trend_10, trend_5], ignore_index=True)

    except Exception as err:
        print(f"❌ Error: {err}")
        return None
    finally:
        conn.close()


##### Step 3: Run Standalone
if __name__ == "__main__":
    print("\n📈 USDA GATS — Trending Markets\n")
    hs_code = input("Enter an HS-10 code: ").strip()
    result = get_trending_markets(hs_code)

    if result is not None:
        print("\n🚀 Trending Markets by Growth (%):\n")
        print(result)
    else:
        print("\n⚠️ No results returned.\n")