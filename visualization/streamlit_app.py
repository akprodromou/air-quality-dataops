# streamlit_app.py
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
import datetime
import math
from components import create_table_with_ring, create_pm_particles_diagram, generate_bin_circles, get_color
from utils import load_forecast_csv, load_forecast_from_duckdb, get_bin_color, get_bin_index, get_verbal_status
from config import (
    VERBAL_LABELS, COLUMN_MAPPING, LIMIT_DICT,
    DB_PATH, FORECAST_CSV, EXPECTED_POLLUTANTS
)
from components import load_css


# Config / Paths

st.set_page_config(page_title="AQ Forecast Pipeline - Thessaloniki", layout="centered")

load_css('visualization/styles.css')

# Connect to DuckDB
con = duckdb.connect(DB_PATH, read_only=True)

# Import notebook tables for visualization
df_monthly = pd.read_csv("data/forecasts/monthly_mean_pollutants.csv")
corr_subset = pd.read_csv("data/forecasts/correlation_matrix.csv", index_col=0)
df_metrics = pd.read_csv("data/forecasts/model_metrics.csv")

# Intro

st.title("Air Quality Monitoring - Thessaloniki")

st.header("Overview")
st.markdown("""
This project is part of an end-to-end **Air Quality DataOps pipeline** for Thessaloniki, Greece. It automates the **collection,
transformation, and prediction** of pollutant concentrations using open data from the **European Environment Agency, the CLIMPACT initiative,
OpenAQ and Open-Meteo APIs**, with the aim of demonstrating a pipeline for monitoring pollution dynamics and assessing air quality trends in cities.

It follows a 4-step process:
1. **Data Ingestion**: Latest pollutant data readings (NO₂, PM₁₀, PM₂.₅, O₃, CO) are retrieved from OpenAQ.
2. **Data Transformation (dbt + DuckDB)**: The data is cleaned, standardized and structured for analysis.
3. **Predictive modelling**: A Random Forest model trained on historical air quality (EEA) and weather data (CLIMPACT) is used to
    forecast air quality for the next 7 days, based on the respective Open Meteo weather predictions.
4. **Visualization**: Current readings and daily pollutant forecasts for the following week are presented to the users,
    providing pollutant information and associated health risk levels.
""")

st.header("Dashboard")

st.subheader("Current Readings (Hourly Values)")

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
latest_df["column_name"] = (
    latest_df["parameter"].str.upper() + " (" + latest_df["unit"] + ")"
)
# Create the DataFrame without an extra index
final_table = pd.DataFrame([latest_df.set_index("column_name")["value"].to_dict()]).T
final_table.columns = ["Current Reading (µg/m³)"]
final_table.index.name = "Pollutant"


html_table = final_table.round(1).to_html(index=True, classes="centered-table")
# convert index → column, name it 'Pollutant'
# Reset index so that 'Pollutant' becomes a proper column
final_table_reset = final_table.reset_index().rename(columns={"index": "Pollutant"})

# Rename reading column for HTML display
final_table_reset = final_table_reset.rename(
    columns={"Current Reading (µg/m³)": "Reading<br>(µg/m³)"}
)

# Generate the indicator circles
final_table_reset["Index"] = final_table_reset.apply(
    lambda row: generate_bin_circles(row["Pollutant"], row["Reading<br>(µg/m³)"]),
    axis=1,
)

# Add verbal status column
final_table_reset["Status"] = final_table_reset.apply(
    lambda row: get_verbal_status(row["Pollutant"], row["Reading<br>(µg/m³)"]), axis=1
)

# Compute the bin index for each pollutant
final_table_reset["bin_index"] = final_table_reset.apply(
    lambda row: get_bin_index(row["Pollutant"], row["Reading<br>(µg/m³)"]), axis=1
)

# render without showing the index (we already have Pollutant as a column)
# Keep Pollutant, Reading, Status
html_table = (
    final_table_reset[["Pollutant", "Reading<br>(µg/m³)", "Index", "Status"]]
    .round(1)
    .to_html(index=False, classes="centered-table", escape=False)
)

## Ring Creation

# Find the pollutant with the highest bin index
max_bin_idx = final_table_reset["bin_index"].max()
max_pollutant_row = final_table_reset.loc[final_table_reset["bin_index"].idxmax()]

# Extract details
max_pollutant = max_pollutant_row["Pollutant"]
max_value = max_pollutant_row["Reading<br>(µg/m³)"]
max_status = VERBAL_LABELS[max_bin_idx]  # directly from the bin index
max_color = get_bin_color(max_pollutant, max_value)

# Rings to display current Index
st.markdown(
    create_table_with_ring(html_table, max_color, max_status),
    unsafe_allow_html=True
)

# Get today's date
today = datetime.date.today().strftime("%d-%m-%Y")

# Caption with reference
st.caption(f"""
Hourly values (source: European Environment Agency - Air Quality Download Service API)
({today})
""")

# Forecast

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


# Sidebar controls

st.sidebar.header("Controls")

# Determine available pollutant columns 
available_pollutants = [p for p in EXPECTED_POLLUTANTS if p in forecast_df.columns]
if not available_pollutants:
    numeric_cols = forecast_df.select_dtypes("number").columns.tolist()
    available_pollutants = [c for c in numeric_cols if c != "forecast_day"]

# Initialize Select All flag
if "select_all_pollutants" not in st.session_state:
    st.session_state["select_all_pollutants"] = True

# Initialize individual checkboxes the first time
for pollutant in available_pollutants:
    cb_key = f"checkbox_{pollutant}"
    if cb_key not in st.session_state:
        # Default: selected if Select All is True
        st.session_state[cb_key] = st.session_state["select_all_pollutants"]

# sidebar toggle

# Callbacks related to the select_all_pollutants state
def _toggle_all():
    """Triggered when 'Select All' checkbox changes."""
    new_val = st.session_state["select_all_pollutants"]
    for p in available_pollutants:
        st.session_state[f"checkbox_{p}"] = new_val

def _update_select_all():
    """Triggered when an individual pollutant checkbox changes."""
    all_checked = all(
        st.session_state.get(f"checkbox_{p}", False) for p in available_pollutants
    )
    st.session_state["select_all_pollutants"] = all_checked

# Sidebar UI: Pollutant selection
selected = []
with st.sidebar.expander("Select pollutants to display", expanded=True):
    st.checkbox(
        "Select All",
        key="select_all_pollutants",
        on_change=_toggle_all,
    )

    for pollutant in available_pollutants:
        display_name = COLUMN_MAPPING.get(pollutant, pollutant)
        cb_key = f"checkbox_{pollutant}"
        default_val = st.session_state.get(
            cb_key, st.session_state["select_all_pollutants"]
        )
        is_selected = st.checkbox(
            display_name,
            key=cb_key,
            on_change=_update_select_all,
        )
        if is_selected:
            selected.append(pollutant)

# --- Date range selector ---
min_date = forecast_df["forecast_day"].min().date()
max_date = forecast_df["forecast_day"].max().date()
date_range = [min_date, max_date]

if isinstance(date_range, list) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# Color scale legend

st.sidebar.header("Pollutant index scaling")

st.sidebar.markdown("""
<div class="aqi-scale">
    <div class="aqi-step-row">
        <div class="aqi-step" style="background-color:#188c39;"></div>
        <span class="aqi-label">Very good</span>
    </div>
    <div class="aqi-step-row">
        <div class="aqi-step" style="background-color:#7cb324;"></div>
        <span class="aqi-label">Good</span>
    </div>
    <div class="aqi-step-row">
        <div class="aqi-step" style="background-color:#ceb000;"></div>
        <span class="aqi-label">Medium</span>
    </div>
    <div class="aqi-step-row">
        <div class="aqi-step" style="background-color:#dc9a00;"></div>
        <span class="aqi-label">Poor</span>
    </div>
    <div class="aqi-step-row">
        <div class="aqi-step" style="background-color:#db6d00;"></div>
        <span class="aqi-label">Very poor</span>
    </div>
    <div class="aqi-step-row">
        <div class="aqi-step" style="background-color:#ca0000;"></div>
        <span class="aqi-label">Extremely poor</span>
    </div>
</div>
""", unsafe_allow_html=True)


# Filter data

# Data Filtering and Preparation
# Filter by date range
mask = (forecast_df["forecast_day"].dt.date >= start_date) & (
    forecast_df["forecast_day"].dt.date <= end_date
)
df_filtered = forecast_df.loc[mask].copy()

# Filter by selected pollutants
# Keep forecast_day column + only the selected pollutants
columns_to_keep = ["forecast_day"] + [p for p in selected if p in df_filtered.columns]
df_filtered = df_filtered[columns_to_keep]

# Individual pollutant bars
st.subheader("Forecasts (Daily Averages)")

if selected:
    chart_index = 0
    charts_per_row = 2
    num_rows = math.ceil(len(selected) / charts_per_row)
    df_filtered['date_label'] = pd.to_datetime(df_filtered['forecast_day']).dt.strftime('%d/%m')
else:
    st.warning("Please select at least one pollutant to display.")
for row in range(num_rows):
    # Create columns for this row
    cols = st.columns(charts_per_row)
    
    # Fill the columns in this row
    for col_idx in range(charts_per_row):
        if chart_index >= len(selected):
            break  # No more charts to display
            
        pollutant = selected[chart_index]
        
        with cols[col_idx]:
            # Choose correct limits
            limits = LIMIT_DICT.get(pollutant)
            if limits is None:
                st.warning(
                    f"No AQI limits defined for {pollutant}. Using default grey bars."
                )
                bar_colors = ["#999999"] * len(df_filtered)
                lower_limit = None
            else:
                bar_colors = [
                    get_color(v, limits) if pd.notna(v) else "#d3d3d3"
                    for v in df_filtered[pollutant]
                ]
                lower_limit = limits[1] if len(limits) > 1 else None

            fig_p = px.bar(
                df_filtered,
                x="date_label",
                y=pollutant,
                labels={pollutant: "µg/m³"},
                color=bar_colors,
                color_discrete_map="identity",
                height=250,
            )

            # Add red dashed line for lower limit
            if lower_limit is not None:
                fig_p.add_hline(
                    y=lower_limit,
                    line_dash="dash",
                    line_color="#ba8875",
                    line_width=2
                )

            # Format y-axis to 1 decimal
            fig_p.update_yaxes(tickformat=".1f")

            fig_p.update_layout(
                title_text=pollutant.replace("_value", "").upper(),
                xaxis_title=None,
                yaxis_title=None,
                showlegend=False,
                bargap=0.7,
                yaxis=dict(
                    ticklabelstandoff=-5
                ),
                xaxis=dict(
                    # show all ticks
                    tickmode='linear',
                    # one tick per data point
                    dtick=1
                )
            )

            fig_p.update_traces(
                hovertemplate="Day: %{x}<br>" + pollutant.replace("_value", "").upper() + ": %{y:.1f} µg/m³<extra></extra>",
                marker=dict(
                    cornerradius=4
                )
            )

            # Make y-axis start from 0
            fig_p.update_yaxes(rangemode="tozero")
            config = {'width': 'stretch'}

            st.plotly_chart(fig_p, config=config)
        
        chart_index += 1

st.markdown("""
<p style="line-height: 1.5; font-size: 0.688rem; color: rgba(145, 146, 149, 1); margin-bottom: 1rem;">
Bar colors for each pollutant correspond to the Pollutant index scaling provided on the sidebar on the left, e.g. 'Good', 'Very Good' etc.<br>
The dashed line corresponds to the threshold between the "Very Good" and "Good" levels for each pollutant.
</p>
""", unsafe_allow_html=True)

# ---------------------
# Download / Export
# ---------------------
csv_bytes = df_filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download filtered forecast CSV",
    data=csv_bytes,
    file_name="forecast_results_filtered.csv",
    mime="text/csv",
)

## Future Predictions

# Convert timestamp
df_predictions["timestamp"] = pd.to_datetime(
    df_predictions["reading_date"].astype(str)
    + " "
    + df_predictions["reading_time"].astype(str)
)
df_predictions_sorted = df_predictions.sort_values("timestamp")


st.subheader("Forecast table (Daily Averages)")

# 1. Prepare data for display
df_filtered_display = df_filtered.copy()
df_filtered_display["forecast_day"] = df_filtered_display["forecast_day"].dt.strftime(
    "%d-%m-%Y"
)

# 2. Round numeric pollutant columns to 1 decimal place
for p in EXPECTED_POLLUTANTS:
    if p in df_filtered_display.columns:
        df_filtered_display[p] = df_filtered_display[p].round(1)

# 3. Rename the columns
# Create a dictionary with only the columns present in the DataFrame
rename_map = {
    old_name: new_name
    for old_name, new_name in COLUMN_MAPPING.items()
    if old_name in df_filtered_display.columns
}

df_filtered_display.rename(columns=rename_map, inplace=True)

# 4. Convert DataFrame to list of dicts and display
st.dataframe(df_filtered_display.to_dict(orient="records"), width='stretch')

# add a caption
st.caption("Daily Average Values (predicted)")

st.markdown("""
---
""")

st.header("1. Understanding the Data")

st.markdown("""
The air quality of a region is determined by the concentration of several key pollutants at ground level. The table below gives a 
brief description of each pollutant, their main sources and the respective EU air quality standards and objectives:
""")

# Data for pollutants
pollutants = pd.DataFrame({
    "Pollutant": ["PM₂.₅", "PM₁₀", "NO₂", "O₃", "SO₂"],
    "Description": [
        "Fine particulate matter (diameter ≤ 2.5 µm). Penetrates deep into lungs and bloodstream.",
        "Coarse particulate matter (diameter ≤ 10 µm). Causes respiratory irritation and reduced lung function.",
        "Nitrogen dioxide - mainly from traffic and industrial combustion. Affects lungs and contributes to smog.",
        "Ozone - forms through sunlight reacting with other pollutants. Causes coughing and throat irritation.",
        "Sulphur dioxide - produced from burning fossil fuels. Causes respiratory problems and acid rain."
    ],
    "EU Limit (µg/m³)": ["20 (annual)", "40 (annual)", "40 (annual)", "120 (8h avg)", "125 (24h avg)"],
    "Main Sources": [
        "Vehicle emissions, residential heating, industrial processes",
        "Road dust, construction sites, agriculture, industrial activities",
        "Traffic emissions, power plants, industrial combustion",
        "Photochemical reactions involving NO₂ and VOCs under sunlight",
        "Burning of coal and oil in power plants and industries"
    ]
})

# Pollutants descriptions table
# Render table
st.markdown(pollutants.to_html(index=False, classes='centered-table'), unsafe_allow_html=True)

# Optional info note
st.info("""
EU limit values apply over different periods of time (daily, annual, or 8-hour) because the observed health impacts 
        associated with the various pollutants occur over different exposure times. 
""")

st.caption("""
Source: [EU air quality standards](https://environment.ec.europa.eu/topics/air/air-quality/eu-air-quality-standards_en)
""")

st.subheader("1.1. How small are PM2.5?")
st.markdown(
"""
PM2.5 are <b>fine inhalable particles</b>, with diameters that are generally <b>2.5 micrometers and smaller</b>.
They are so small that they can be inhaled and cause serious health problems.
Think about a single human hair: the average hair is about <b>70 micrometers</b> in diameter - making it <b>30 times larger</b> than the largest fine particle!
""",
    unsafe_allow_html=True,
)

# Display PM particles diagram
st.markdown(
    create_pm_particles_diagram(), 
    unsafe_allow_html=True  # ← This too!
)

st.subheader("1.2. Data Sources")

st.markdown(
"""
* **Air Quality** data are sourced from the **European Environment Agency (EEA)**, an agency of the European Union that provides insights 
on the state of Europe's environment. Its aim is to support Europe's environment and climate policies through the data it provides.
The specific data is pulled from its [Air Quality Download Service API](https://www.eea.europa.eu/data-and-maps/data/aqereporting-9).
    * Historic data covers the period between 01/01/2023-31/12/2023.
    * For the forecast model, the latest reading - falling within the last 24hrs - is used.
* **Weather Forecasts** are sourced from [Open-Meteo](https://open-meteo.com/), which provides an open-source weather API 
and offers free access for non-commercial use.
    * The forecast covers the next 7 days from the date of the request.
* **Historic Weather Data** was downloaded from [CLIMPACT](https://data.climpact.gr/en/dataset/497dc26d-45e0-4ad5-b8f3-5f8890f65129),
an initiative on climate change to coordinate a nation-wide network of institutions responsible for the integration, harmonization, 
and optimization of existing climate services, early warning systems and measurements from relevant national infrastructures in Greece. 
    * Historic data covers the period between 01/01/2023-31/12/2023.
""")

st.subheader("1.3. Measurement Units")

st.markdown(
"""
| Variable | Unit | Description |
| :--- | :--- | :--- |
| Air Pollutants | µg/m³ | Micrograms per cubic meter |
| Mean temperature | °C (degrees Celsius) | Average air temperature over the day. |
| Mean relative humidity | % (percent) | Average percentage of humidity in the air. |
| Accumulated rainfall (precipitation) | mm (millimeters) | Total rainfall for the day. |
| Mean wind speed | m/s (meters per second) | Average wind speed during the day. |
| Dominant wind direction | ° (degrees) | Wind direction measured clockwise from north (0°), e.g., 90° = east wind. |
---
""")

# Main app layout
st.header("2. Methodology")



st.markdown("""
The project follows a modular **data operations (DataOps)** architecture designed to ensure **daily automation**, 
            **traceability**, and **scalability** across multiple data sources. The pipeline integrates raw air quality 
            and meteorological data, transforms them into structured analytical tables, and prepares them for the 
            predictive model and dashboard visualization.
""")
st.subheader("2.1. Data Ingestion")
st.markdown("""
Two external APIs are queried daily through an **Airflow Directed Acyclic Graph (DAG)**:

- **OpenAQ API:** retrieves near real-time pollutant concentrations (PM₂.₅, PM₁₀, NO₂, O₃, SO₂).  
- **Open-Meteo API:** provides weather data (temperature, humidity, wind speed, precipitation, wind direction).  

Each ingestion task stores the raw JSON responses in the project’s `ingestion/raw_data` directory, ensuring reproducibility and versioning.
""")
st.subheader("2.2. Transformation Layer")
st.markdown("""
Raw files are processed using **dbt (Data Build Tool)**, which performs sequential transformations through three layers:

- **Staging (`stg_`):** converts the raw JSON into flattened, queryable tables.  
- **Intermediate Cleaning (`stg_openaq_data`):** standardizes column names, units, and timestamps.  
- **Marts (`analysis_air_quality`):** deduplicates data, harmonizes metrics between sources, and outputs analysis-ready tables.  

The marts layer serves as the **single source of truth** for subsequent modeling and visualization.

""")
st.subheader("2.3. Orchestration and Scheduling")
st.markdown("""
All ingestion and transformation tasks are orchestrated through an **Apache Airflow DAG**, which:

- Defines dependencies between tasks (Ingestion → Transformation → Forecasting).  
- Runs automatically on a **daily schedule** (`@daily`).  
- Includes retry logic and error handling for robust execution.  

This orchestration ensures that both air quality and weather datasets are refreshed before model execution.
""")
st.subheader("2.4. Integration with Forecasting and Visualization")
st.markdown("""
The **Random Forest forecast script** consumes the cleaned data from the marts tables and the most recent air quality readings for each station.  
After predictions are generated, the results are passed to the **Streamlit application**, which visualizes both the **latest observed values** and the **7-day forecast** for each pollutant.

The resulting pipeline ensures an automated workflow, from raw data acquisition to presenting the information through visuals.
""")

st.markdown("The data processing and forecasting architecture flow chart is provided below:")

# Flow chart insert code

# wrap the function that follows to display its content inside a modal dialog
@st.dialog("Data Analysis & Forecast Pipeline", width="large", dismissible=True)
def show_flow_chart():
    st.image("visualization/images/flow_chart.png", width='content')
    st.caption("Detailed flow of the Data Analysis and Forecast Pipelines.")
    st.markdown("Click outside or press ESC to close.")

# Button to trigger the dialog
if st.button("View Flow Chart"):
    show_flow_chart()

# End of Flow Chart

st.markdown("""
---
""")

st.header("3. Predictive Modeling")

st.markdown("""
Predictions in this project were made using a machine learning model trained on importing past meteorological and 
            air quality data for Thessaloniki (2023).
""")
st.subheader("3.1. Data Integration")
st.markdown("""
    - Historical air quality data (PM₂.₅, PM₁₀, NO₂, O₃, SO₂) were retrieved from the European Environment Agency.
    - Corresponding meteorological variables (temperature, humidity, wind speed, rainfall, wind direction) were
            imported from the CLIMPACT and Open-Meteo datasets.
    - Both datasets were harmonized on a daily timescale and merged by date.
""")
st.subheader("3.2. Feature Engineering")
st.markdown("""
    - Pollutant values were aggregated from **hourly to daily means**.
    - A month variable was added to capture seasonal effects.
    - **Temporal autocorrelation** was modeled by including each pollutant’s previous-day mean as a
            new predictor (e.g., `pm25_value_prev`).
    - **Wind direction** was converted from compass points to numerical degrees.
""")
st.subheader("3.3. Data Cleaning & Validation")
st.markdown("""
    - Outliers and invalid readings were replaced with NaN and removed post-merge.
    - Multicollinearity was examined using **Variance Inflation Factor (VIF)** to ensure model stability.
""")
st.subheader("3.4. Data Exploration")
st.markdown("""
   At this stage, the relationships between **meteorological** and **pollutant** variables was examined:
""")

fig = px.imshow(
    corr_subset,
    text_auto=".2f",
    color_continuous_scale="RdBu_r",
    aspect="auto",
    zmin=-1,
    zmax=1
)

fig.update_layout(
    title="Correlation Heatmap",
    xaxis_title="Targets",
    yaxis_title="Predictors",
    coloraxis_colorbar=dict(
        title="Correlation",
        orientation="h",
        tickmode="array",
        tickvals=[-1, -0.5, 0, 0.5, 1],
        x=0,
        xanchor="center",
        y=-0.25,
        yanchor="top",
        len=0.40,
        thickness=10
    )
)

st.plotly_chart(fig, use_container_width=True)


st.markdown("""
    - Bivariate Correlation Analysis: The strongest correlations were found between each pollutant’s *current and previous-day values* 
            (O₃: 0.88, SO₂: 0.85, PM₂.₅: 0.78, PM₁₀: 0.70), indicating **temporal autocorrelation**, i.e. pollutant values in a time series are correlated with their own past values.
        - Temperature showed a moderate positive correlation with **O₃** (0.38), consistent with photochemical lower ground ozone formation on warmer days.
        - Wind speed showed a moderate negative correlations with most pollutants, reflecting its role in **pollutant dispersion**.
        - **Relative humidity** and **precipitation** were weakly correlated overall, indicating limited impact on daily pollutant levels.
        - Seasonal Effects: Summer months (July-August) were strongly associated with higher **O₃** concentrations, driven by strong sunlight and heat. 
            Winter months (December-January) correlated positively with **PM₁₀**, likely due to increased **domestic heating** activities. 
""")

# Rename columns for display
df_monthly = df_monthly.rename(columns=COLUMN_MAPPING)

# Select only the mapped columns for the y-axis
y_columns = [col for col in df_monthly.columns if col in COLUMN_MAPPING.values()]

fig = px.line(
    df_monthly,
    x='month',
    y=y_columns,
    title='Yearly Mean Pollutant Concentration (µg/m³)',
    labels={'value': 'Concentration (µg/m³)', 'variable': 'Pollutant', 'month': 'Month'}
)

st.plotly_chart(fig, use_container_width=True)
            
st.markdown("""
    - Scatterplot Analysis: Scatter plots confirmed positive associations between **O₃ and temperature**, and negative associations between **NO₂ and wind speed**.  

    - Multicollinearity Assessment: Variance Inflation Factor (VIF) analysis revealed high collinearity among certain predictors 
            (e.g., *previous-day pollutants* and *relative humidity*, VIF > 20). However, as the subsequent modeling phase employed a **Random Forest Regressor**, 
            which is *non-linear and robust to multicollinearity*, no variables were excluded.  
""")
st.subheader("3.5. Model Selection & Training")
st.markdown("""
   - Given the **non-linear** and **interdependent** nature of pollutant behavior, a **Random Forest Regressor** was chosen over linear models.  
   - The model was trained on 70% of the dataset and tested on the remaining 30%, using meteorological, temporal, and lag variables as predictors.  
""")
st.subheader("3.6. Evaluation Metrics")
st.markdown("""
   - Model performance was evaluated with **R²** and **Root Mean Squared Error (RMSE)** for each pollutant: 
    """)

# Rename pollutant column
df_metrics_display = df_metrics.copy()
df_metrics_display['pollutant'] = df_metrics_display['pollutant'].map(COLUMN_MAPPING)
# Set pollutant as index
df_metrics_display[['R2', 'RMSE']] = df_metrics_display[['R2', 'RMSE']].round(2)
df_metrics_display = df_metrics_display.set_index('pollutant')
df_metrics_display.index.name = "Pollutant"

# Center the table using columns
col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    st.dataframe(df_metrics_display, width=230)

st.markdown(""" 
    Results show better predictability for pollutants driven by **meteorological processes** (O₃, PM₂.₅, SO₂) and lower accuracy for **localized emissions** (NO₂, PM₁₀).
""")

st.markdown("""
---
""")

st.header("4. Findings")
st.subheader("4.1. Conclusions")

st.markdown("""
This Air Quality DataOps pipeline provides real-time monitoring and predictive modelling of air quality indicators based on meteorological input for Thessaloniki, Greece.
Its purpose is to demonstrate a case study for environmental monitoring while providing insights into pollutant dynamics through data analysis.
The pipeline ingests transforms raw environmental data using open-source APIs, makes future estimates using machine learning techniques
and renders the results to an easy to understand web-page that can inform citizens about upcoming air quality conditions.
""")
st.subheader("4.2. Limitations")
st.markdown("""
While the model performs good for a pipeline demonstration, several limitations should be taken into account if a complete model is to be built:
- Traffic-related pollutants (NO₂, PM₁₀) require additional predictors such as traffic volume, day-of-week patterns, proximity to major roads etc. to improve forecast accuracy
- Spatial heterogeneity within Thessaloniki is not captured; the current model treats the city as a single monitoring zone
- Short-term events (e.g., dust storms, wildfires, industrial incidents) are not accounted for in the forecasts
""")














