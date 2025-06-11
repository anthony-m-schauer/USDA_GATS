##          This code is designed to take a years input of a specific year and access the GATS data from the USDA website            ##
##          to download all the export values in dollars and quantity in the respective units for each of the hs10-codes.            ##
##                                                                                                                                   ##
##                                                   Created by Anthony M. Schauer                                                   ##
#######################################################################################################################################
#######################################################################################################################################

# Choose years to download. ***EACH MUST BE A STRING*** 
years = ["2016", "2015"]     # Completed through 2017

##### Step 0: Imports
import pandas as pd
import requests
import time 
from datetime import datetime 
import mysql.connector

##### Step 1: Set up the API Calls
API_KEY = "iuHeyvauN1CryIF01vofSuMLthfNn1MK7uB8BOae"
base_url = "https://api.fas.usda.gov" 
country_url = "/api/gats/countries"
data_url = "/api/gats/censusExports/partnerCode/{partnerCode}/year/{year}/month/{month}" 
start_time = time.time()
print(f"\n      Started at: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}")

##### Step 2: Create the Country Code List
url = f"{base_url}{country_url}?api_key={API_KEY}"
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data) 
    print("\n✅ API data received.")
    country_codes = df["countryCode"].dropna().unique().tolist()
    del country_codes[:20]    # Changes the country_codes list to not have the first 20 as they do not work with the API 
else:
    print("❌ Error retrieving API data:", response.status_code)
    print("Exit.")
    exit()

##### Step 3: Create Connection to MySQL 
def connect_to_sql():
    try:    
        conn = mysql.connector.connect(
            host="localhost",
            user="amschauer03",      
            password="Anthonyschauer1!",  
            database="usda_gats_db"
        )
        cursor = conn.cursor()
        print("\n✅ Connected to SQL Server.")
        return conn, cursor
    except mysql.connector.Error as err:
        print(f"❌ SQL connection error: {err}")
        print("Retying in  minutes...") 
        time.sleep(66)

##### Step 4: Create Connection to Yearly Export Data
def get_yearly_export_data(partner_code: str, year: str, api_key: str) -> pd.DataFrame:
    all_months_data = []
    
    for month_num in range(1, 13):
        month = str(month_num).zfill(2)
        data_url = f"/api/gats/censusExports/partnerCode/{partner_code}/year/{year}/month/{month}"
        url = f"{base_url}{data_url}?api_key={api_key}"
        response = requests.get(url)
        while response.status_code == 429:
            print(f"\n⏱️  Rate limit hit for {partner_code}, {month}/{year}. Waiting 2 minutes...")
            time.sleep(120)
            response = requests.get(url)
        if response.status_code == 200:
            month_data = response.json()
            df = pd.DataFrame(month_data)
            if not df.empty:
                all_months_data.append(df)
        else:
            print(f"⚠️  Failed for {partner_code}, {month}/{year}: {response.status_code}")
    
    if not all_months_data:
        print(f"\n⛔ No data found for {partner_code} in {year}.")
        return pd.DataFrame()
    
    full_year_df = pd.concat(all_months_data, ignore_index=True)
    grouped = full_year_df.groupby("hS10Code").agg({
        "value": "sum",
        "quantity1": "sum"
    }).reset_index()

    grouped["year"] = year
    grouped["country_code"] = partner_code
    
    return grouped
    

##### Step 5: Insert Each Country Data  
for year in years:
    
    success = False

    while not success:    
        try:
            value_col = f"value{year}"
            quantity_col = f"quantity{year}"
            
            conn, cursor = connect_to_sql()

            for idx, country in enumerate(country_codes, start=1):
                               
                try:
                    df_run = get_yearly_export_data(country, year, API_KEY)
                    time.sleep(3)
                    # print(f"\n✅ Data returned for {country} in {year} ({idx} of {len(country_codes)})") 
                    # (Optional) print(df_run.head())   # Prints a summary of the data being entered into SQL

                    if idx % 25 == 0:
                        elapsed = time.time() - start_time 
                        hours, rem = divmod(elapsed, 3600)
                        minutes, seconds = divmod(rem, 60)
                        print(f"⏱️  Current Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")

                    if not df_run.empty:
                        insert_data = [
                            (row["country_code"], row["hS10Code"], row["value"], row["quantity1"])
                            for _, row in df_run.iterrows()
                        ]
                        cursor.executemany(f"""
                            INSERT INTO hs10_full (country_code, hs10_code, {value_col}, {quantity_col})
                            VALUES (%s, %s, %s, %s)
                         ON DUPLICATE KEY UPDATE
                             {value_col} = VALUES({value_col}),
                             {quantity_col} = VALUES({quantity_col})
                        """, insert_data)
                        print(f"\n✅ Data returned for {country} in {year} ({idx} of {len(country_codes)})") 

                except mysql.connector.Error as err:
                    print(f"\n❌ Error for {country} in {year} ({idx} of {len(country_codes)}): {err}")
                    elapsed = time.time() - start_time 
                    hours, rem = divmod(elapsed, 3600)
                    minutes, seconds = divmod(rem, 60)
                    print(f"⏱️  Current Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
                    print(f"🎯 Error at {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}")
                    print(f".........Retrying in 1 minute")
                    time.sleep(60)
                    print(f"Retrying now..........")
                    conn, cursor = connect_to_sql()
                    idx = idx - 1
                    print(f"\n{idx}")
                    continue 

            print(f"\n📦 Finished inserting data for {year}.") 
            conn.commit()
            print(f"🎯  Finalized at {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}")
            print("✅ Saved into MySQL Database.")
            elapsed = time.time() - start_time 
            hours, rem = divmod(elapsed, 3600)
            minutes, seconds = divmod(rem, 60)
            print(f"⏱️  Current Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            success = True

        except Exception as e:
            print(f"\n❌ General Error in {year}: {e}")
            elapsed = time.time() - start_time 
            hours, rem = divmod(elapsed, 3600)
            minutes, seconds = divmod(rem, 60)
            print(f"⏱️  Current Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            print(f"🎯 Error at {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}")
            # conn, cursor = connect_to_sql()
            continue 

##### Step 6: Close Everything & Final Prints 
conn.commit()
cursor.close()
conn.close()

end_time = time.time()
elapsed = (end_time - start_time)
hours, remainder = divmod(elapsed, 3600)
minutes, seconds = divmod(remainder, 60)

print("\n✅ All country data inserted into MySQL."); 
print(f"⏱️  Total Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s.")
print(f"🎯  Finalized at {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}")