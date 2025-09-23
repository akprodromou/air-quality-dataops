# visualizations/visualize_air_quality.py
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

# Get the project root directory
project_root = Path(__file__).parent.parent
data_path = project_root / "data"

conn = duckdb.connect(str(data_path / "air_quality.duckdb"))

# Use relative paths for local execution
df = conn.execute("""
    SELECT 
        result_item.datetime.utc AS datetime_utc,
        result_item.value AS value,
        result_item.coordinates.latitude AS latitude,
        result_item.coordinates.longitude AS longitude,
        result_item.sensorsId AS sensors_id,
        result_item.locationsId AS locations_id
    FROM read_json('data_ingestion/raw_data/openaq_data_thessaloniki_*.json') as raw_data
    CROSS JOIN UNNEST(raw_data.results) AS t(result_item)
    ORDER BY datetime_utc, sensors_id
""").fetchdf()

# Convert datetime column to proper datetime type
df['datetime_utc'] = pd.to_datetime(df['datetime_utc'])

# Map sensor IDs to pollutant names
sensor_to_pollutant = {
    12079197: 'CO',
    8622634: 'NO₂',
    8622640: 'O₃',
    8622643: 'PM10',
    8622646: 'PM2.5',
    8622649: 'SO₂'
}

# Add pollutant column
df['pollutant'] = df['sensors_id'].map(sensor_to_pollutant)

print(f"Data loaded: {len(df)} records from {df['sensors_id'].nunique()} sensors")
print(f"Date range: {df['datetime_utc'].min()} to {df['datetime_utc'].max()}")
print(f"Pollutants: {', '.join(df['pollutant'].unique())}")

# Create a figure with 3 subplots (stacked vertically)
fig, axes = plt.subplots(3, 1, figsize=(10, 15))
fig.suptitle('Air Quality Data - Thessaloniki Agia Sofia', fontsize=18, y=0.98)


# 1. Time series plot by sensor
ax1 = axes[0]
for pollutant in df['pollutant'].unique():
    pollutant_data = df[df['pollutant'] == pollutant]
    ax1.plot(pollutant_data['datetime_utc'], pollutant_data['value'], 
             marker='o', markersize=4, label=pollutant, linewidth=2)

ax1.set_title('Air Quality Values Over Time')
ax1.set_xlabel('Time (UTC)')
ax1.set_ylabel('Value')
ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax1.grid(True, alpha=0.3)
ax1.tick_params(axis='x', rotation=0)

# 2. Box plot by sensor
ax2 = axes[1]
sns.boxplot(x='pollutant', y='value', data=df, ax=ax2)
ax2.set_title('Value Distribution by Pollutant')
ax2.set_xlabel('Pollutant')
ax2.set_ylabel('Value')
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=0)


# 3. Latest values by sensor (bar chart)
ax3 = axes[2]
latest_data = df.loc[df.groupby('sensors_id')['datetime_utc'].idxmax()]
bars = ax3.bar(latest_data['pollutant'], latest_data['value'], 
               color='lightcoral', edgecolor='darkred', linewidth=1.5)
ax3.set_title('Latest Values by Pollutant')
ax3.set_xlabel('Pollutant')
ax3.set_ylabel('Value')
plt.setp(ax3.xaxis.get_majorticklabels(), rotation=0)

# Add value labels on bars
for bar, value in zip(bars, latest_data['value']):
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
             f'{value:.1f}', ha='center', va='bottom')



plt.tight_layout(rect=[0, 0, 1, 0.95])  # leave space for suptitle
