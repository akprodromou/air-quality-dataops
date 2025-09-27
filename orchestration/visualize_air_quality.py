import pandas as pd
import duckdb
import plotly.express as px
from pathlib import Path
import os

def generate_visualizations(output_dir="visualizations"):  
    """
    Generate air quality visualizations.
    
    Args:
        output_dir: Directory to save visualizations. If None, uses default location.
    """
    # Handle the output_dir parameter
    if output_dir is None:
        # Fallback to default location
        output_dir = Path("/tmp/airflow_visualizations")
    else:
        output_dir = Path(output_dir)

    # Ensure directory exists
    output_dir.mkdir(exist_ok=True, parents=True)

    print(f"Generating visualizations in: {output_dir}")
    
    # ---- Project paths ----
    project_root = Path(__file__).parent.parent  
    data_path = project_root / "data"

    try:
        conn = duckdb.connect(str(data_path / "air_quality.duckdb"))

        # Updated query to use absolute path for the JSON files
        ingestion_path = project_root / "ingestion/raw_data"
        json_pattern = str(ingestion_path / "openaq_data_thessaloniki_*.json")
        
        print(f"Looking for data files at: {json_pattern}")
        
        df = conn.execute(f"""
            SELECT 
                result_item.datetime.utc AS datetime_utc,
                result_item.value AS value,
                result_item.coordinates.latitude AS latitude,
                result_item.coordinates.longitude AS longitude,
                result_item.sensorsId AS sensors_id,
                result_item.locationsId AS locations_id
            FROM read_json('{json_pattern}') as raw_data
            CROSS JOIN UNNEST(raw_data.results) AS t(result_item)
            ORDER BY datetime_utc, sensors_id
        """).fetchdf()

        if df.empty:
            print("⚠ Warning: No data found. Creating placeholder visualizations.")
            # Create placeholder visualizations when no data is available
            create_placeholder_visualizations(output_dir)
            conn.close()
            return str(output_dir)

        print(f"✓ Found {len(df)} data records")
        
        df['datetime_utc'] = pd.to_datetime(df['datetime_utc'])

        sensor_to_pollutant = {
            12079197: 'CO',
            8622634: 'NO₂',
            8622640: 'O₃',
            8622643: 'PM10',
            8622646: 'PM2.5',
            8622649: 'SO₂'
        }
        df['pollutant'] = df['sensors_id'].map(sensor_to_pollutant)

        # ----- Interactive Time Series -----
        fig_time = px.line(
            df, 
            x='datetime_utc', 
            y='value', 
            color='pollutant', 
            title='Air Quality Values Over Time',
            labels={'datetime_utc': 'Time (UTC)', 'value': 'Value'}
        )

        # ----- Interactive Box Plot -----
        fig_box = px.box(
            df, 
            x='pollutant', 
            y='value', 
            title='Value Distribution by Pollutant'
        )

        # ----- Interactive Latest Values -----
        latest_data = df.loc[df.groupby('sensors_id')['datetime_utc'].idxmax()]
        fig_latest = px.bar(
            latest_data, 
            x='pollutant', 
            y='value', 
            title='Latest Values by Pollutant',
            text='value'
        )

        # ----- Write HTML files to output directory -----
        fig_time.write_html(output_dir / "time_series.html")
        fig_box.write_html(output_dir / "box_plot.html")
        fig_latest.write_html(output_dir / "latest_values.html")

        conn.close()

        # print(f"✓ Successfully created {len(html_files)} visualization files:")
        # for file in html_files:
        #     print(f"  - {file.name} ({file.stat().st_size} bytes)")

    except Exception as e:
        print(f"Error generating visualizations: {e}")
        print("Creating placeholder visualizations instead...")
        create_placeholder_visualizations(output_dir)

    print(f"Visualizations saved successfully to {output_dir}")
    return str(output_dir)


def create_placeholder_visualizations(output_dir):
    """
    Create placeholder HTML files when data is not available.
    """
    output_dir = Path(output_dir)
    
    placeholder_files = {
        "time_series.html": "Air Quality Time Series - No data available yet",
        "box_plot.html": "Air Quality Distribution - No data available yet", 
        "latest_values.html": "Latest Air Quality Values - No data available yet"
    }
    
    for filename, title in placeholder_files.items():
        with open(output_dir / filename, 'w') as f:
            f.write(f"""
            <!DOCTYPE html>
            <html>
            <head><title>{title}</title></head>
            <body>
                <h1>{title}</h1>
                <p>Data visualization will appear here once data ingestion is complete.</p>
                <p>Run the data ingestion task first to populate the database.</p>
            </body>
            </html>
            """)
    
    print(f"✓ Created {len(placeholder_files)} placeholder visualization files")