# streamlit_app.py
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Path to the dbt-created DuckDB file
DB_PATH = "./data/air_quality_weather.duckdb"

st.set_page_config(layout="centered")

st.title("Air Quality Monitoring - Thessaloniki")

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
</style>
""", unsafe_allow_html=True)


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
st.subheader("Current Air Quality Metrics")
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
    # title="Predicted PM2.5 over the next 7 days",
    labels={"predicted_pm25": "PM2.5 (µg/m³)", "timestamp": "Time"}
)

# Add horizontal lines for AQG targets
fig.add_hline(y=15, line_dash="dash", line_color="red",
              annotation_text="Recommended Short-Term (24h) AQG", 
              annotation_position="top left")
fig.add_hline(y=5, line_dash="dash", line_color="green",
              annotation_text="Recommended Annual AQG", 
              annotation_position="bottom left")

fig.update_yaxes(
    range=[0, df_predictions_sorted["predicted_pm25"].max() * 1.1],
    title_text=""  # hide the y-axis label
)

fig.update_layout(
    title_text='Predicted PM2.5 (µg/m³) over the next 7 days - Thessaloniki',
    title_x=0.04, 
    title_y=0.90,
    title_font=dict(size=20)  
)

# Show chart in Streamlit
st.plotly_chart(fig, use_container_width=True)

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






# fig = go.Figure()

# # Human hair circle
# fig.add_trace(go.Scatter(
#     x=[0.5], y=[0.5],
#     mode="markers",
#     marker=dict(size=200, color="rgba(139,69,19,0)", line=dict(color="saddlebrown", width=4)),
#     hovertext="Human Hair (~70 µm)",
#     hoverinfo="text"
# ))

# # PM2.5 particle
# fig.add_trace(go.Scatter(
#     x=[0.55], y=[0.55],
#     mode="markers",
#     marker=dict(size=10, color="rgba(128,128,128,0)", line=dict(color="gray", width=3)),
#     hovertext="PM2.5 (~2.5 µm)",
#     hoverinfo="text"
# ))

# # Layout
# fig.update_layout(
#     xaxis=dict(visible=False, range=[0, 1]),
#     yaxis=dict(visible=False, range=[0, 1]),
#     width=400,
#     height=400,
#     margin=dict(l=20, r=20, t=20, b=20),
#     plot_bgcolor="black"
# )

# st.plotly_chart(fig, use_container_width=False)



# # Create figure
# fig = go.Figure()

# # Draw human hair (large circle) - left side
# fig.add_shape(
#     type="circle",
#     x0=0.05, y0=0.3, x1=0.35, y1=0.6,  # Large circle on left
#     line=dict(color="saddlebrown", width=4),
#     xref="x",
#     yref="y"
# )

# # Draw PM2.5 particle (small circle) - right side, next to large circle
# fig.add_shape(
#     type="circle",
#     x0=0.38, y0=0.3, x1=0.40, y1=0.32,  # Small circle on right
#     line=dict(color="gray", width=3),
#     xref="x",
#     yref="y"
# )

# # Curved line from large circle to its label
# fig.add_shape(
#     type="path",
#     path="M 0.275 0.6 Q 0.325 0.725, 0.45 0.80",  # Curve from top of large circle to label
#     line=dict(color="saddlebrown", width=2),
#     xref="x", yref="y"
# )

# # Human hair label
# fig.add_annotation(
#     x=0.5, y=0.835,
#     text="Human Hair (~70 µm)",
#     showarrow=False,
#     font=dict(color="saddlebrown", size=14, weight="bold"),
#     xref="x", yref="y"
# )

# # Curved line from small circle to its label
# fig.add_shape(
#     type="path",
#     path="M 0 0.115 Q 0.1 0.215, 0.2 0.24",  # Curve from small circle to label
#     line=dict(color="gray", width=2),
#     xref="x", yref="y"
# )

# # PM2.5 label
# fig.add_annotation(
#     x=0.65, y=0.25,
#     text="PM2.5 (~2.5 µm)",
#     showarrow=False,
#     font=dict(color="gray", size=14, weight="bold"),
#     xref="x", yref="y"
# )

# # Layout
# fig.update_layout(
#     xaxis=dict(visible=False, range=[0, 1]),
#     yaxis=dict(visible=False, range=[0, 1]),
#     width=400,
#     height=400,
#     margin=dict(l=20, r=20, t=20, b=20)
# )

# # Streamlit
# st.plotly_chart(fig, use_container_width=False)