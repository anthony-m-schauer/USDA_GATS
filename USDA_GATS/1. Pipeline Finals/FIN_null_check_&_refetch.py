##              This script connects to the usda_gats_db MySQL database and performs a check on a given table to                     ##
##                find the rows with null values and uses those rows to query the API and try to to fill them.                       ##
##                                                                                                                                   ##
##                                                   Created by Anthony M. Schauer                                                   ##
#######################################################################################################################################
#######################################################################################################################################


##### Choose a Table to Check ----- OPTIONS: hs10_full ; test_table ; countries 
table = "hs10_full"


##### Step 0: Imports
import pandas as pd
import time 
from datetime import datetime 
import mysql.connector
import requests
import re 


##### Step 1: Set up the API Calls
API_KEY = "iuHeyvauN1CryIF01vofSuMLthfNn1MK7uB8BOae"
base_url = "https://api.fas.usda.gov"
country_url = "/api/gats/countries"
data_url = "/api/gats/censusExports/partnerCode/{partnerCode}/year/{year}/month/{month}" 


##### Step 2: Create Connection to SQL 
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


##### Step 3: Find the years of the columns 
def get_years_from_columns(conn, table):
    cursor = conn.cursor()
    cursor.execute(f"SHOW COLUMNS FROM {table}")
    columns = [col[0] for col in cursor.fetchall()]

    years = set()
    for col in columns:
        match = re.search(r'(?:value|quantity)(\d{4})$', col)
        if match:
            years.add(int(match.group(1)))
    
    years = sorted(years, reverse = True)
    print(f"\n✅ Checking years: {', '.join(map(str, years))}")
    return years


##### Step 4: Find Null Rows  
def find_null_rows(conn, table):
    try:
        cursor = conn.cursor(dictionary=True)
        query = (f"SELECT * FROM {table}")
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        null_rows = df[df.isnull().any(axis=1)]
        
        if null_rows.empty:
            print("\n🎉 No nulls found.")
        else:
            print(f"\n✅ Found {len(null_rows)} rows with nulls.")
            print(null_rows.head())
        
        return null_rows
    
    except Exception as e:
        print(f"❌ Failed to query nulls: {e}")
        return pd.DataFrame()


##### Step 5: Extract Key Null Combos 
def get_null_combos(null_rows, years):
    combos = []

    for _, row in null_rows.iterrows():
        for year in years:
            
            value_col = f"value{year}"
            quantity_col = f"quantity{year}"

            if pd.isnull(row.get(value_col)) or pd.isnull(row.get(quantity_col)):
                combos.append({
                    "country_code": row["country_code"],
                    "hs10_code": row["hs10_code"],
                    "year": int(year)
                })

    print(f"\n🧮 Extracted {len(combos)} unique key/year combos to recheck.")
    print(combos[:6]) 
    
    return combos


##### Step 6: Fetch Data from API
def fetch_data_from_api(partner_code: str, year: str, api_key: str) -> pd.DataFrame:

    all_months_data = []

    for month_num in range(1, 13):
        month = str(month_num).zfill(2)
        data_url = f"/api/gats/censusExports/partnerCode/{partner_code}/year/{year}/month/{month}"
        url = f"{base_url}{data_url}?api_key={api_key}"
        response = requests.get(url)

        while response.status_code == 429:
            print(f"⌛ Rate limit hit for {partner_code}, {month}/{year}. Waiting 30 seconds...")
            time.sleep(30)
            response = requests.get(url)

        if response.status_code == 200:
            try:
                month_data = response.json()
                df = pd.DataFrame(month_data)
                if not df.empty:
                    all_months_data.append(df)
            except Exception as e:
                print(f"⚠️  Failed to parse JSON for {partner_code}, {month}/{year}: {e}")
        else:
            print(f"⚠️  Failed for {partner_code}, {month}/{year}: {response.status_code}")
    
    if not all_months_data:
        return pd.DataFrame()

    full_year_df = pd.concat(all_months_data, ignore_index=True)
    grouped = full_year_df.groupby("hS10Code").agg({
        "value": "sum",
        "quantity1": "sum"
    }).reset_index()

    grouped["year"] = year
    grouped["country_code"] = partner_code
    
    return grouped


##### Step 7: Insert New Data Into SQL
def insert_new_data(conn, table, df, year):
    cursor = conn.cursor()
    inserted_or_updated = 0
    value_col = f"value{year}"
    quantity_col = f"quantity{year}"

    for _, row in df.iterrows():
        try:
            insert_query = f"""
                INSERT INTO {table} (country_code, hs10_code, {value_col}, {quantity_col})
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    {value_col} = VALUES({value_col}),
                    {quantity_col} = VALUES({quantity_col})
            """
            cursor.execute(insert_query, (
                row["country_code"],
                row["hS10Code"],
                row.get("value"),
                row.get("quantity1")
            ))

            inserted_or_updated += 1

        except Exception as e:
            print(f"❌ Error inserting/updating for {row.get('country_code')} - {row.get('hS10Code')}: {e}")
            continue

    conn.commit()
    print(f"✅ {inserted_or_updated} rows inserted or updated for year {year}.")


##### Step 8: Master Function to Run All Steps 
def run_null_refetcher():
    print(f"\n                      🚀 Starting Null Checker | For Table: {table} | Starting at: {datetime.now()}\n")
    start_time = time.time()
    success = False
    current_idx = 0

    while not success:
        try:
            conn = connect_to_sql()
            if conn:
                print("\n✅ Connected to SQL Server.")
            else:
                raise Exception("❌ Failed to connect to SQL Server.")

            years = get_years_from_columns(conn, table)
            null_df = find_null_rows(conn, table)
            conn.close()

            if null_df.empty:
                print("\n🎉 Nothing to fix. Exiting.")
                return

            combos = get_null_combos(null_df, years)
            total_combos = len(combos)

            while current_idx < total_combos:
                combo = combos[current_idx]
                country_code = combo["country_code"]
                hs10_code = combo["hs10_code"]
                year = combo["year"]

                print(f"\n🔄 Trying {country_code}-{hs10_code} for {year} ({current_idx + 1} of {total_combos})")

                try:
                    conn = connect_to_sql()
                    if conn is None:
                        raise Exception("❌ SQL connection failed.")

                    new_data = fetch_data_from_api(country_code, str(year), API_KEY)

                    if not new_data.empty:
                        filtered = new_data[new_data["hS10Code"] == hs10_code]
                        if not filtered.empty:
                            insert_new_data(conn, table, filtered, year)
                            print(f"✅ Inserted {len(filtered)} rows.")
                        else:
                            print(f"⚠️  No matching data found.")
                    else:
                        print(f"📭 No data returned for {country_code}-{hs10_code}-{year}")

                    conn.commit()
                    conn.close()
                    current_idx += 1  

                    if current_idx % 25 == 0:
                        elapsed = time.time() - start_time
                        hours, rem = divmod(elapsed, 3600)
                        minutes, seconds = divmod(rem, 60)
                        print(f"⏱️  Current Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")

                except Exception as e:
                    print(f"❌ Minor Error: {e}")
                    print("... Retrying combo in 2 minutes ...")
                    time.sleep(120)
                    continue  

            success = True
            print(f"\n✅ All nulls rechecked successfully.")
            elapsed = time.time() - start_time
            hours, rem = divmod(elapsed, 3600)
            minutes, seconds = divmod(rem, 60)
            print(f"🎯 Final Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            print(f"🏁 Finished at {datetime.now().strftime('%m/%d/%Y, %H:%M:%S')}")

        except Exception as e:
            print(f"\n🛑 General Error: {e}")
            print("🟡 Retrying entire loop in 10 minutes ...")
            time.sleep(600)


##### Step 9: Run Lines
if __name__ == "__main__":
    run_null_refetcher()