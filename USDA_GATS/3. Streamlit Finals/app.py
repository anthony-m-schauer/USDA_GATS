


##### How To Run --->       streamlit run "3. Streamlit Finals\app.py"        Example code: 0201206000


##### Step 0: Imports 
import streamlit as st
import pandas as pd
import mysql.connector
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# User Imports
table = "hs10_cleaned"
from column_summary import get_column_summary 
from outlier_markets import get_outlier_markets
from top_markets import get_top_markets
from trending_markets import get_trending_markets
from falling_markets import get_falling_markets
from volatility_score import calculate_volatility_score
from hhi_index import calculate_hhi
from shannon_index import calculate_shannon_index
from percent_index import calculate_percents_index


##### Step 1: Connect to MySQL 
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


##### Step 2: Get HS10 Data 
def get_data_by_hs10(hs10_code):
    query = f"""
        SELECT * FROM {table}
        WHERE hs10_code = %s
    """
    conn = connect_to_sql()
    if conn:
        try:
            df = pd.read_sql(query, conn, params=(hs10_code,))
            conn.close()
            return df
        
        except Exception as e:
            st.error(f"Query failed: {e}")
            return pd.DataFrame()
    
    return pd.DataFrame()


##### Step 3: Create Streamlit App 
st.set_page_config(page_title="USDA Trade Explorer", layout="wide")
st.title("📦 USDA Agricultural Trade Explorer")
st.subheader("Search by HS-10 Code")

# User HS10 Prompt 
with st.form("hs10_form"):
    hs10_input = st.text_input("Enter a 10-digit HS code:", max_chars=10)
    submitted = st.form_submit_button("Search")

if submitted:
    if hs10_input.strip():
        st.subheader(f"🔍 Showing all results for HS-10: `{hs10_input}`")
        result_df = get_data_by_hs10(hs10_input.strip())
        
        if not result_df.empty:

            
            ##### Step 4: Add Calculations 

            # All Data 
            st.dataframe(result_df)
            
            # Column Summary 
            st.subheader("📊 Column Summaries") 
            summary = get_column_summary(result_df)
            summary_df = pd.DataFrame(summary)
            ordered_cols = ['column', 'type', 'nulls', 'unique', 'mean', 'min', 'max', 'sample_values']
            summary_df = summary_df.reindex(columns=[col for col in ordered_cols if col in summary_df.columns])
            st.dataframe(summary_df)

            # Outlier Markets 
            st.subheader("🟨 Outlier Markets")
            outlier_df = get_outlier_markets(hs10_input.strip(), table)
            col1, col2, col3 = st.columns([1, 3, 1])
            with col2:
                st.dataframe(outlier_df)

            # Top Markets 
            st.subheader("🌎 Top Export Markets")
            markets_df = get_top_markets(hs10_input.strip(), table)
            if markets_df: 
                top_all, top_10, top_5 = markets_df
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown("**All Time**")
                    st.dataframe(top_all)
                with col2:
                    st.markdown("**Last 10 Years**")
                    st.dataframe(top_10)
                with col3:
                    st.markdown("**Last 5 Years**")
                    st.dataframe(top_5)
            else:
                st.info("No market data available for this code.")

            # Trending Markets 
            st.subheader("📈 Top Trending Markets")
            trending_df = get_trending_markets(hs10_input.strip(), table)
            if trending_df:
                trend_all, trend_10, trend_5 = trending_df
                col1, col2, col3 = st.columns(3)
                with col1: 
                    st.markdown("**All Time**")
                    st.dataframe(trend_all)
                with col2:
                    st.markdown("**Last 10 Years**")
                    st.dataframe(trend_10)
                with col3: 
                    st.markdown("**Last 5 Years**")
                    st.dataframe(trend_5)
            else:
                st.info("No market data available for this code.")

            # Falling Markets 
            st.subheader("📉 Top Falling Markets")
            falling_df = get_falling_markets(hs10_input.strip(), table)
            if falling_df:
                fall_all, fall_10, fall_5 = falling_df
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown("**All Time**")
                    st.dataframe(fall_all)
                with col2: 
                    st.markdown("**Last 10 Years**")
                    st.dataframe(fall_10)
                with col3: 
                    st.markdown("**Last 5 Years**")
                    st.dataframe(fall_5)
            else:
                st.info("No market data available for this code.")

            # Volatility  Score
            st.subheader("⚡ Volatility Score")
            col1, col2, col3 = st.columns([10, 1, 10])
            with col2:
                volatility_df = calculate_volatility_score(hs10_input.strip(), table)
                st.metric(label=" ", value=f"{volatility_df:.2f}")
            with st.expander("**How is the Volatility Score calulated?**"):
                st.markdown("The Volatility Score is calulated by finding the standard deviation of the percent changes. \n" 
                "**Note:** Volatility is capped at ±200% to prevent extreme outliers from dominating.")
            with st.expander("**📊 Volatility Score Interpretation Guide:**"):
                st.markdown("- 🟢 **< 10%** – Very stable market  \n"
                "- 🔵 **10%–30%** – Moderately stable  \n"
                "- 🟠 **30%–60%** – Volatile market  \n"
                "- 🔴 **> 60%** – Highly volatile / unstable market  \n\n")
            
            # Herfindahl Index 
            st.subheader("🧾 Herfindahl Index")
            with st.expander("**What is the Herfindahl Index?**"):
                st.markdown("The Herfindahl-Hirschman Index (HHI) is a measure of how concentrated or competitive a market is. "
                "It ranges from 0 to 1 (or 0 to 10,000 in some presentations), where lower values indicate a more competitive market with many small players, "
                "and higher values indicate a more concentrated market with fewer dominant firms.  \n\n"
                "**📊 HHI Interpretation Guide:**  \n"
                "- 🔹 **HHI < 0.15** – Highly competitive market  \n"
                "- 🔸 **0.15 ≤ HHI < 0.25** – Moderately concentrated  \n"
                "- 🔺 **HHI ≥ 0.25** – Highly concentrated / less competitive  \n\n")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown("**Herfindahl-Hirschman Index**")
                index_cols = ['Year', 'HHI Index']
                hhi_df = calculate_hhi(hs10_input.strip(), table)
                hhi_df = hhi_df.reindex(columns=[col for col in index_cols if col in hhi_df.columns])
                st.dataframe(hhi_df)
            
            # Shannon Entropy Index
            st.subheader("🔢 Shannon Index")
            with st.expander("**What is the Shannon Entropy Index?**"):
                st.markdown("The Shannon Entropy Index is a measure of diversity or uncertainty in a market. "
                "It reflects how evenly distributed market shares are among participants. The value increases with the number of firms "
                "and the evenness of their market shares. A higher entropy value means a more diverse market (many firms with similar shares)" 
                "while a lower value suggests a more concentrated market (few dominant players).\n\n"
                "**📊 Shannon Entropy Interpretation Guide:**  \n"
                "- 🔹 **Entropy < 1.0** – Very low diversity (market is dominated by a few firms)  \n"
                "- 🔸 **1.0 ≤ Entropy < 2.0** – Moderate diversity  \n"
                "- 🔺 **Entropy ≥ 2.0** – High diversity (competitive and evenly distributed market)  \n\n"
                "*Note: The maximum entropy depends on the number of firms (max = log₂(n))")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown("**Shannon Entropy Index**")
                index_cols = ['Year', 'Shannon Index']
                shannon_df = calculate_shannon_index(hs10_input.strip(), table)
                shannon_df = shannon_df.reindex(columns=[col for col in index_cols if col in shannon_df.columns])
                st.dataframe(shannon_df)

            # Market Percent 
            st.subheader("💯 Market Percent Index")
            with st.expander("**What is the Market Percent Index?**"):
                st.markdown("The Market Percent Index is another way to interpret the share of a total market a single " 
                "HS-10 code makes up. It is the percent a single codes makes up of the total global market for a given year.")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown("**Percent Index**")
                index_cols = ['Year', 'Percent Total Market']
                percent_df = calculate_percents_index(hs10_input.strip(), table)
                percent_df = percent_df.reindex(columns=[col for col in index_cols if col in percent_df.columns])
                st.dataframe(percent_df)
        
        else:
            st.warning("No data found for that HS-10 code.")
    
    else:
        st.warning("Please enter a valid HS-10 code.")

# streamlit run "3. Streamlit Finals\app.py"
