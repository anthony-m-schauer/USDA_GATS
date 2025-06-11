import requests
import pandas as pd
import mysql.connector

# Step 1: Call the API  
partnerCode = "CH"                                    # Country code for imports
year = "2024"                                          # Desired year
month = "01"                                           # Desired month number (2 digits)
data_url = f"/api/gats/censusExports/partnerCode/{partnerCode}/year/{year}/month/{month}"                     # Specifc url for data group
API_KEY = "iuHeyvauN1CryIF01vofSuMLthfNn1MK7uB8BOae"   # Unique API key

if not data_url.startswith("/"):
    data_url = "/" + data_url
base_url = "https://api.fas.usda.gov"
url = f"{base_url}{data_url}?api_key={API_KEY}"
response = requests.get(url)

# Step 2: Make the request
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data) 
    print("✅ API data received.")
    print(df.head())
else:
    print("❌ Error retrieving API data:", response.status_code)
    exit()


# # Step 4: Connect to MySQL 
# conn = mysql.connector.connect(
#     host="localhost",
#     user="amschauer03",      
#     password="Anthonyschauer1!",  
#     database="usda_gats_db"
# )
# cursor = conn.cursor()

# # Step 6: Insert data into table 
# for index, row in df_run.iterrows():
#     cursor.execute("""
#         INSERT INTO test_table (country_code, hs10_code, value2024, quantity2024)
#         VALUES (%s, %s, %s, %s)
#         ON DUPLICATE KEY UPDATE
#             country_code = VALUES(country_code),
#             hs10_code = VALUES(hs10_code)
#     """, (row["countryCode"], row["hS10Code"], row["value"], row["quantity1"]))
#     print(f"Inserting: {row['countryCode']}, {row['hS10Code']}, {row['value']}, {row['quantity1']}")     # need names from GATS api to be exact

# if response.status_code == 200:
#        month_data = response.json()
#        df = pd.DataFrame(month_data)
#        # print(f"\n📅 Data for {partner_code} - {year}-{month}: {df.columns.tolist()}")          # Prints heading of fetched data  
#        # print(df.head())
#        if not df.empty:
#            all_months_data.append(df)
#    else:
#        print(f"⚠️ Failed for {partner_code}, {month}/{year}: {response.status_code