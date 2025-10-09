# streamlit_app.py
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path
import datetime

# ---------------------
# Config / Paths
# ---------------------
st.set_page_config(page_title="Air Quality 7-Day Forecast", layout="centered")

# CSS code
st.markdown("""
<style>
    .centered-table {
        margin: auto;
        text-align: center;
    }
    .centered-table td, .centered-table th {
        text-align: center !important;
        padding: 8px;
    }

    /* Blink animation for active circle */
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.65; }
        100% { opacity: 1; }
    }
            
    .blink-circle {
        animation: pulse 3s infinite ease-in-out; 
    }
            
    .circle-animated {
        animation: pulse 3s infinite ease-in-out; 
    }
</style>
""", unsafe_allow_html=True)

BASE_DIR = Path(__file__).resolve().parent.parent  # project root
DB_PATH = BASE_DIR / "data" / "air_quality_weather.duckdb"  # duckdb file
FORECAST_CSV = BASE_DIR / "data" / "forecasts" / "forecast_results.csv"

# Connect to DuckDB
con = duckdb.connect(DB_PATH, read_only=True)

# list of expected pollutant columns (used as defaults)
EXPECTED_POLLUTANTS = ["pm10_value", "pm25_value", "o3_value", "no2_value", "so2_value"]

# ---------------------
# Utility functions
# ---------------------
@st.cache_data(ttl=300)
def load_forecast_csv(path: Path):
    """Load forecast CSV if present; normalize column names and types."""
    if not path.exists():
        return None
    df = pd.read_csv(path)
    # try to find a date-like column and normalize to 'forecast_day'
    if "forecast_day" not in df.columns:
        possible_date_cols = [c for c in df.columns if ("date" in c.lower() or "day" in c.lower())]
        if possible_date_cols:
            df = df.rename(columns={possible_date_cols[0]: "forecast_day"})
    if "forecast_day" not in df.columns:
        # nothing we can do automatically
        return None

    df["forecast_day"] = pd.to_datetime(df["forecast_day"])
    # coerce expected pollutant columns to numeric if they exist
    for p in EXPECTED_POLLUTANTS:
        if p in df.columns:
            df[p] = pd.to_numeric(df[p], errors="coerce")
    df = df.sort_values("forecast_day").reset_index(drop=True)
    return df

@st.cache_data(ttl=300)
def load_forecast_from_duckdb(db_path: Path):
    """Try to read a forecast/results table from DuckDB. Returns DataFrame or None."""
    if not db_path.exists():
        return None
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        # Try common table names that might hold predictions
        candidate_tables = ["forecast_results", "predicted_pollutants_7day", "predictions", "predicted_forecast"]
        for t in candidate_tables:
            try:
                df = con.execute(f"SELECT * FROM {t} ORDER BY forecast_day").fetchdf()
                if "forecast_day" in df.columns or "reading_date" in df.columns:
                    # normalize to 'forecast_day'
                    if "reading_date" in df.columns and "forecast_day" not in df.columns:
                        df = df.rename(columns={"reading_date": "forecast_day"})
                    df["forecast_day"] = pd.to_datetime(df["forecast_day"])
                    for p in EXPECTED_POLLUTANTS:
                        if p in df.columns:
                            df[p] = pd.to_numeric(df[p], errors="coerce")
                    return df.sort_values("forecast_day").reset_index(drop=True)
            except Exception:
                continue
    except Exception:
        return None
    return None

# ---------------------
# Plots
# ---------------------

# Current Reading

st.title("Air Quality Monitoring - Thessaloniki")

st.subheader("Overview")
st.markdown("""
This project is part of an end-to-end **Air Quality DataOps pipeline** for Thessaloniki, Greece. It automates the **collection, transformation, and prediction** of pollutant concentrations using open data from the **OpenAQ API** and a trained **Random Forest forecasting model**.

Key components include:
- **Data Ingestion**: Automated retrieval of pollutant data (NO₂, PM₁₀, PM₂.₅, O₃, CO) from OpenAQ.
- **Data Transformation (dbt + DuckDB)**: Cleaning, standardizing, and structuring data for analysis.
- **Predictive Modeling**: A Random Forest model trained on historical air quality data is used to forecast air quality for the next 7 days.
- **Visualization App (Streamlit)**: Displays daily and hourly pollutant forecasts, health risk levels, and trends.

This app shows the **latest 7-day forecast** produced by the pipeline, helping to monitor pollution dynamics and assess air quality trends in Thessaloniki.
""")

## Load the tables
# Define DataFrame for current measurements
df_current = con.execute("SELECT * FROM analysis_air_quality").df()
# Define DataFrame for predictions
df_predictions = con.execute("SELECT * FROM analysis_weather").df()

## Current Predictions

# Convert reading_date to datetime
df_current["reading_date"] = pd.to_datetime(df_current["reading_date"], dayfirst=True)

# Get the latest measurement for each pollutant (parameter)
# For each unique parameter, give the row index where reading_date is the latest
idx = df_current.groupby("parameter")["reading_date"].idxmax()

# idx is a pandas Series, so it can be passed into loc
# select the rows at those indices
latest_df = df_current.loc[idx, ["parameter", "value", "unit"]]

# Create a table with pollutant + unit as column header
latest_df["column_name"] = latest_df["parameter"].str.upper() + " (" + latest_df["unit"] + ")"
# Fix: Create the DataFrame properly without an extra index
final_table = pd.DataFrame([latest_df.set_index("column_name")["value"].to_dict()]).T
final_table.columns = ["Current Reading (µg/m³)"] 
final_table.index.name = "Pollutant"

no2_limits = [0, 40, 90, 120, 230, 340, 1000]
ozone_limits = [0, 50, 100, 130, 240, 380, 800]
pm25_limits = [0, 10, 20, 25, 50, 75, 800]
pm10_limits = [0, 20, 40, 50, 100, 150, 1200] 
so2_limits = [0, 100, 200, 350, 500, 750, 1250]

very_good_hex = '#188c39'
good_hex = '#7cb324'
medium_hex = '#ceb000'
poor_hex = '#dc9a00'
very_poor_hex = '#db6d00'
extremely_poor_hex = '#ca0000'

bin_list = [no2_limits, ozone_limits, pm10_limits, pm25_limits, so2_limits]

verbal_labels = ["Very Good", "Good", "Medium", "Poor", "Very Poor", "Extremely Poor"]


def get_bin_color(parameter, value):
    # Map parameter name to its limits
    limits_dict = {
        'NO2 (µg/m³)': no2_limits,
        'O3 (µg/m³)': ozone_limits,
        'PM10 (µg/m³)': pm10_limits,
        'PM25 (µg/m³)': pm25_limits,
        'SO2 (µg/m³)': so2_limits
    }
    colors = [very_good_hex, good_hex, medium_hex, poor_hex, very_poor_hex, extremely_poor_hex]

    limits = limits_dict.get(parameter)
    if limits is None:
        return ''  # fallback

    # Determine which bin the value falls into
    for i in range(len(limits)-1):
        if limits[i] <= value < limits[i+1]:
            return colors[i]
    return colors[-1]  # if value >= highest limit

def get_bin_index(parameter, value):
    limits_dict = {
        'NO2 (µg/m³)': no2_limits,
        'O3 (µg/m³)': ozone_limits,
        'PM10 (µg/m³)': pm10_limits,
        'PM25 (µg/m³)': pm25_limits,
        'SO2 (µg/m³)': so2_limits
    }
    limits = limits_dict.get(parameter)
    if limits is None:
        return None

    for i in range(len(limits)-1):
        if limits[i] <= value < limits[i+1]:
            return i
    return len(limits)-2  # last bin if value >= highest limit

def get_verbal_status(parameter, value):
    bin_idx = get_bin_index(parameter, value)
    if bin_idx is None:
        return ""
    return verbal_labels[bin_idx]


grey_hex = '#d3d3d3'  # light grey for inactive bins
colors = [very_good_hex, good_hex, medium_hex, poor_hex, very_poor_hex, extremely_poor_hex]
def generate_bin_circles(parameter, value):
    bin_idx = get_bin_index(parameter, value)
    circles = []
    for i in range(6):  # always 6 circles
        color = colors[i] if i == bin_idx else grey_hex
        # add blink-circle class only for the active circle
        cls = "blink-circle" if i == bin_idx else ""
        circles.append(f'<span class="{cls}" style="display:inline-block;width:12px;height:12px;border-radius:50%;margin:1px;background-color:{color}"></span>')
    return ''.join(circles)

# Streamlit display

html_table = final_table.round(1).to_html(index=True, classes='centered-table')
# convert index → column, name it 'Pollutant'
final_table_reset = final_table.reset_index().rename(columns={'column_name': 'Pollutant'})  # if needed
# ensure the reading column has the desired name
final_table_reset.columns = ['Pollutant', 'Reading<br>(µg/m³)']

final_table_reset['Index'] = final_table_reset.apply(
    lambda row: generate_bin_circles(row['Pollutant'], row['Reading<br>(µg/m³)']),
    axis=1
)

# New column for verbal status
final_table_reset['Status'] = final_table_reset.apply(
    lambda row: get_verbal_status(row['Pollutant'], row['Reading<br>(µg/m³)']),
    axis=1
)

# render without showing the index (we already have Pollutant as a column)
# Keep Pollutant, Reading, Status
html_table = final_table_reset[['Pollutant', 'Reading<br>(µg/m³)', 'Index', 'Status']].round(1).to_html(
    index=False, classes='centered-table', escape=False
)

## Ring Creation

## Ring Creation
# Compute the max value
max_value = final_table['Current Reading (µg/m³)'].max()
max_pollutant = final_table['Current Reading (µg/m³)'].idxmax()
max_status = get_verbal_status(max_pollutant, max_value)
max_color = get_bin_color(max_pollutant, max_value)

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
    .table-ring-container {{
        display: flex;
        flex-direction: row;
        align-items: center;
        gap: 50px;
        justify-content: center;
    }}
    .ring-container {{
        display: flex;
        justify-content: center;
        align-items: center;
    }}
    .table-heading {{
        text-align: left;  /* Align left */
        font-size: 22px;
        font-weight: bold;
        margin-bottom: 15px;
        color: white;  /* Adjust color as needed */
        padding-left: 117.5px;  /* Optional: add left padding for spacing */
    }}
</style>

<div class="table-heading">Current Air Quality Reading</div>  

<div class="table-ring-container">
    <div>{html_table}</div>
    <div class="ring-container" style="flex-direction: column; display: flex; align-items: center;">
        <div style="margin-bottom: 10px; font-weight: bold; color: white;">Air Quality Index (AQI)</div>
        <svg width="110" height="110">
            <circle 
                cx="55" 
                cy="55" 
                r="45" 
                fill="none" 
                stroke="{max_color}" 
                stroke-width="3" 
                stroke-dasharray="2,2"
                class="circle-animated"
            />
            <circle 
                cx="55" 
                cy="55" 
                r="53" 
                fill="none" 
                stroke="{max_color}" 
                stroke-width="3" 
                stroke-dasharray="4,2"                
            />
            <text x="55" y="60" text-anchor="middle" font-size="12" font-family="Arial" fill="white">{max_status}</text>
        </svg>
    </div>
</div>
""", unsafe_allow_html=True)

## Ring end

# Get today's date
today = datetime.date.today().strftime("%d-%m-%Y")

# Caption with reference
st.markdown(f"""
<style>
.custom-caption {{
    font-size: 12px !important;
    color: #a5a5a5 !important;
    margin: 0 !important;
    padding: 0 !important;
    line-height: 1.2 !important;
}}
</style>

<p class="custom-caption">
Data provided by European Environment Agency - Air Quality Download Service API | Updated: {today}
</p>
""", unsafe_allow_html=True)

# ---------------------
# Forecast
# ---------------------

forecast_df = load_forecast_csv(FORECAST_CSV)
if forecast_df is None:
    st.info("No forecast CSV found — attempting to read from DuckDB...")
    forecast_df = load_forecast_from_duckdb(DB_PATH)

if forecast_df is None:
    st.warning(
        "No forecast data available. Please run `rf_forecast.py` to generate "
        "forecasts (which will save to `data/forecasts/forecast_results.csv`), "
        "or ensure a forecast table exists in the DuckDB file."
    )
    st.stop()



# ---------------------
# Sidebar controls
# ---------------------

st.markdown("""
<style>  
            
/* Targets the first span (i.e. the box) inside any element with data-baseweb="checkbox" */
[data-baseweb="checkbox"] > span:first-child {
    background-color: #2196f3 !important; 
    border-color: #2196f3 !important; 
}
</style>
""", unsafe_allow_html=True)

# --- sidebar pollutant selector with working "Select All" ---
# (assumes `st`, `forecast_df`, and `EXPECTED_POLLUTANTS` are defined)

COLUMN_MAPPING = {
    "forecast_day": "Date",
    "pm10_value": "PM10 (µg/m³)",
    "pm25_value": "PM2.5 (µg/m³)",
    "o3_value": "Ozone (µg/m³)",
    "no2_value": "NO₂ (ppb)",
    "so2_value": "SO₂ (ppb)"
}

st.sidebar.header("Controls")

# which pollutant columns are available
available_pollutants = [p for p in EXPECTED_POLLUTANTS if p in forecast_df.columns]
if not available_pollutants:
    numeric_cols = forecast_df.select_dtypes("number").columns.tolist()
    available_pollutants = [c for c in numeric_cols if c != "forecast_day"]

# initialize select_all flag the first time
if "select_all_pollutants" not in st.session_state:
    st.session_state["select_all_pollutants"] = True

# callback when the Select All checkbox is changed by the user
def _toggle_all():
    new_val = st.session_state["select_all_pollutants"]
    for p in available_pollutants:
        st.session_state[f"checkbox_{p}"] = new_val

# callback when any individual checkbox changes -> update Select All state
def _update_select_all():
    all_checked = all(st.session_state.get(f"checkbox_{p}", False) for p in available_pollutants)
    st.session_state["select_all_pollutants"] = all_checked

selected = []
with st.sidebar.expander("Select pollutants to display", expanded=True):
    # Select All checkbox with on_change
    st.checkbox(
        "Select All",
        value=st.session_state["select_all_pollutants"],
        key="select_all_pollutants",
        on_change=_toggle_all,
    )

    # individual pollutant checkboxes (display friendly name, keep original column name)
    for pollutant in available_pollutants:
        display_name = COLUMN_MAPPING.get(pollutant, pollutant)
        cb_key = f"checkbox_{pollutant}"
        default_val = st.session_state.get(cb_key, st.session_state["select_all_pollutants"])

        is_selected = st.checkbox(
            display_name,
            value=default_val,
            key=cb_key,
            on_change=_update_select_all,
        )
        if is_selected:
            selected.append(pollutant)



# Date range selector
min_date = forecast_df["forecast_day"].min().date()
max_date = forecast_df["forecast_day"].max().date()
date_range = [min_date, max_date]
if isinstance(date_range, list) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date


# ---------------------
# Filter data
# ---------------------

# Define the new, user-friendly column names
COLUMN_MAPPING = {
    "forecast_day": "Date",
    "pm10_value": "PM10 (µg/m³)",
    "pm25_value": "PM2.5 (µg/m³)",
    "o3_value": "Ozone (µg/m³)",
    "no2_value": "NO₂ (ppb)",
    "so2_value": "SO₂ (ppb)"
}

# --- Data Filtering and Preparation ---
# Filter by date range
mask = (forecast_df["forecast_day"].dt.date >= start_date) & (forecast_df["forecast_day"].dt.date <= end_date)
df_filtered = forecast_df.loc[mask].copy()

# Filter by selected pollutants
# Keep forecast_day column + only the selected pollutants
columns_to_keep = ["forecast_day"] + [p for p in selected if p in df_filtered.columns]
df_filtered = df_filtered[columns_to_keep]

st.subheader("Forecast table (7-day)")
st.markdown("Daily Average Values")

# 1. Prepare data for display
df_filtered_display = df_filtered.copy()
df_filtered_display["forecast_day"] = df_filtered_display["forecast_day"].dt.strftime("%d-%m-%Y")

# 2. Round numeric pollutant columns to 1 decimal place
for p in EXPECTED_POLLUTANTS:
    if p in df_filtered_display.columns:
        df_filtered_display[p] = df_filtered_display[p].round(1)

# 3. RENAME THE COLUMNS
# Create a dictionary with only the columns present in the DataFrame
rename_map = {old_name: new_name for old_name, new_name in COLUMN_MAPPING.items() if old_name in df_filtered_display.columns}

df_filtered_display.rename(columns=rename_map, inplace=True)

# Convert DataFrame to list of dicts and display
st.dataframe(df_filtered_display.to_dict(orient="records"), use_container_width=True)


# Individual pollutant bars
st.write("### Individual pollutant forecasts")

# Define AQI category limits and colors
limit_dict = {
    "no2_value": [0, 40, 90, 120, 230, 340, 1000],
    "o3_value": [0, 50, 100, 130, 240, 380, 800],
    "pm10_value": [0, 20, 40, 50, 100, 150, 1200],
    "pm25_value": [0, 10, 20, 25, 50, 75, 800],
    "so2_value": [0, 100, 200, 350, 500, 750, 1250],
}

aqi_colors = [
    '#188c39',  # Very good
    '#7cb324',  # Good
    '#ceb000',  # Medium
    '#dc9a00',  # Poor
    '#db6d00',  # Very poor
    '#ca0000',  # Extremely poor
]

def get_color(value, limits):
    """Return the color corresponding to the AQI bin."""
    for i in range(len(limits) - 1):
        if limits[i] <= value < limits[i + 1]:
            return aqi_colors[i]
    return aqi_colors[-1]  # last category if above all thresholds


cols = st.columns(len(selected), gap="large") 
for i, pollutant in enumerate(selected):
    with cols[i]:
        # Choose correct limits
        limits = limit_dict.get(pollutant)
        if limits is None:
            st.warning(f"No AQI limits defined for {pollutant}. Using default grey bars.")
            bar_colors = ['#999999'] * len(df_filtered)
        else:
            bar_colors = [get_color(v, limits) if pd.notna(v) else '#d3d3d3'
                          for v in df_filtered[pollutant]]

        fig_p = px.bar(
            df_filtered,
            x="forecast_day",
            y=pollutant,
            labels={pollutant: "µg/m³"},
            color=bar_colors,  # Color per bar
            color_discrete_map="identity",
            height=200 
        )

        fig_p.update_layout(
            title_text=pollutant.replace("_value", "").upper(),
            xaxis_title=None,
            yaxis_title=None,
            showlegend=False,
            bargap=.2,
            yaxis=dict(
                # ticks='outside',
                # ticklen=10,
                ticklabelstandoff=-5
            )
        )

        fig_p.update_traces(
            marker=dict(
                cornerradius=5  # Adjust the radius value (in pixels) as needed
            )
        )

        # Make y-axis start from 0
        fig_p.update_yaxes(
            rangemode="tozero"
        )

        st.plotly_chart(fig_p, use_container_width=True)



# ---------------------
# Download / Export
# ---------------------
csv_bytes = df_filtered.to_csv(index=False).encode("utf-8")
st.download_button("Download filtered forecast CSV", data=csv_bytes, file_name="forecast_results_filtered.csv", mime="text/csv")

## Future Predictions

# Convert timestamp
df_predictions["timestamp"] = pd.to_datetime(
    df_predictions["reading_date"].astype(str) + " " + df_predictions["reading_time"].astype(str)
)
df_predictions_sorted = df_predictions.sort_values("timestamp")

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

svg_code = """
<svg viewBox="0 0 450 300" xmlns="http://www.w3.org/2000/svg">
  <style>
    .st0{fill:none;stroke:#ffffff;stroke-width:2;stroke-miterlimit:10;}
    .label{fill:white; font-size:10px; }
  </style>

  <!-- Big circle (hair) -->
  <circle cx="204" cy="184" r="70" class="st0"/>
  <!-- Label for big circle -->
  <text x="10" y="70" class="label">Human Hair (~70 µm)</text>
  
  <!-- Small circle (PM2.5) -->
  <circle cx="298.5" cy="251" r="2.5" class="st0"/>
  <!-- Label for small circle -->
  <text x="365" y="207.5" class="label">PM2.5 (~2.5 µm)</text>
  
  <!-- Arrow from small circle -->
  <path d="M303,242 Q328,210,353,206" stroke="white" stroke-width="1" fill="none" />
  <line x1="351" y1="201" x2="359" y2="204" stroke="white" stroke-width="1" />
  <line x1="359" y1="204" x2="353" y2="210" stroke="white" stroke-width="1" />
  
  <!-- Arrow from big circle -->
  <path d="M160,118 Q130,90 96,87" stroke="white" stroke-width="1" fill="none" />
  <line x1="96" y1="92.5" x2="89" y2="85.5" stroke="white" stroke-width="1" />
  <line x1="89" y1="85.5" x2="97" y2="80.5" stroke="white" stroke-width="1" />
</svg>
"""

st.markdown(svg_code, unsafe_allow_html=True)

st.write("---")
st.caption(
    """
**Data Sources**  
- **Air Quality:** European Environment Agency – [Air Quality Download Service API](https://www.eea.europa.eu/data-and-maps/data/aqereporting-9)  
- **Weather Predictions:** [Open-Meteo API](https://open-meteo.com/)  
- **Historic Weather Data:** [CLIMPACT](https://data.climpact.gr/en/dataset/497dc26d-45e0-4ad5-b8f3-5f8890f65129)  
"""
)