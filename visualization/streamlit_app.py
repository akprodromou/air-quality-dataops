# streamlit_app.py
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# Path to your dbt-created DuckDB file
DB_PATH = "../data/air_quality_weather.duckdb"

st.title("PM2.5 Predictions for Thessaloniki")

st.header("What are PM2.5?")
st.markdown("""
<p style="line-height:1.4; margin-bottom:10px;">
They are <b>fine inhalable particles</b>, with diameters that are generally <b>2.5 micrometers and smaller</b>.
</p>
<p style="line-height:1.4; margin-bottom:10px;">
They can be made up of hundreds of different chemicals: some are emitted directly from sources such as construction sites, unpaved roads, or fires. Most PM2.5 particles form in the atmosphere as a result of <b>complex reactions of chemicals</b> emitted from power plants, industries, and automobiles.
</p>
<p style="line-height:1.4; margin-bottom:10px;">
They are so small that they can be inhaled and cause serious health problems.
</p>
""", unsafe_allow_html=True)

st.subheader("How small is 2.5 micrometers?")

st.markdown("""
Think about a single human hair: the average hair is about **70 micrometers** in diameter – making it **30 times larger** than the largest fine particle!
""")

## Load data
# Connect to DuckDB
con = duckdb.connect(DB_PATH, read_only=True)

## Load the tables
# Define DataFrame for current measurements
df_current = con.execute("SELECT * FROM analysis_air_quality").df()
# Define DataFrame for predictions
df_predictions = con.execute("SELECT * FROM analysis_weather").df()

## Current Predictions

# Convert reading_date to datetime
df_current["reading_date"] = pd.to_datetime(df_current["reading_date"], dayfirst=True)

# Get the latest measurement for each pollutant (parameter)
idx = df_current.groupby("parameter")["reading_date"].idxmax()
latest_df = df_current.loc[idx, ["parameter", "value", "unit"]]

# Create a table with pollutant + unit as column header
latest_df["column_name"] = latest_df["parameter"].str.upper() + " (" + latest_df["unit"] + ")"

# Fix: Create the DataFrame properly without an extra index
final_table = pd.DataFrame([latest_df.set_index("column_name")["value"].to_dict()])

# Streamlit display
st.subheader("Current Air Quality Metrics")
html_table = final_table.round(1).to_html(index=False, classes='centered-table')
st.markdown(f"""
<style>
    .centered-table {{
        margin: auto;
        text-align: center;
    }}
    .centered-table td, .centered-table th {{
        text-align: center !important;
        padding: 8px;
    }}
</style>
{html_table}
""", unsafe_allow_html=True)

## Future Predictions

# Convert timestamp
df_predictions["timestamp"] = pd.to_datetime(
    df_predictions["reading_date"].astype(str) + " " + df_predictions["reading_time"].astype(str)
)
df_predictions_sorted = df_predictions.sort_values("timestamp")

# Create Plotly line chart
fig = px.line(
    df_predictions_sorted,
    x="timestamp",
    y="predicted_pm25",
    title="Predicted PM2.5 over the next 7 days",
    labels={"predicted_pm25": "PM2.5 (µg/m³)", "timestamp": "Time"}
)

# Add horizontal lines for AQG targets
fig.add_hline(y=15, line_dash="dash", line_color="red",
              annotation_text="Recommended Short-Term (24h) AQG", 
              annotation_position="top left")
fig.add_hline(y=5, line_dash="dash", line_color="green",
              annotation_text="Recommended Annual AQG", 
              annotation_position="bottom left")

# Show chart in Streamlit
st.plotly_chart(fig, use_container_width=True)
