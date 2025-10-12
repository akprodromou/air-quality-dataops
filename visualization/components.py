# components.py
import streamlit as st
from utils import get_bin_index
from config import (
    COLORS, GREY_HEX, AQI_COLORS
)

def create_aqi_ring(max_color, max_status):
    """Generate AQI ring SVG component"""
    return f"""
<div class="ring-container" style="flex-direction: column; display: flex; align-items: center;">
  <div style="margin-bottom: 10px; font-weight: bold; color: white;">Air Quality Index (AQI)</div>
  <svg width="110" height="110">
    <circle cx="55" cy="55" r="45" fill="none" stroke="{max_color}" stroke-width="3" stroke-dasharray="2,2" class="circle-animated"/>
    <circle cx="55" cy="55" r="53" fill="none" stroke="{max_color}" stroke-width="3" stroke-dasharray="4,2"/>
    <text x="55" y="60" text-anchor="middle" font-size="12" fill="white">{max_status}</text>
  </svg>
</div>
"""


def create_pm_particles_diagram():
    """Generate PM particles comparison SVG"""
    return """
    <svg viewBox="0 0 450 300" xmlns="http://www.w3.org/2000/svg">
      <style>
        .st0{fill:none;stroke:#ffffff;stroke-width:2;stroke-miterlimit:10;}
        .label{fill:white; font-size:10px; }
      </style>

      <!-- Big circle (hair) -->
      <circle cx="204" cy="184" r="70" class="st0"/>
      <text x="10" y="70" class="label">Human Hair (~70 µm)</text>
      
      <!-- Small circle (PM2.5) -->
      <circle cx="298.5" cy="251" r="2.5" class="st0"/>
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


def create_table_with_ring(html_table, max_color, max_status):
    """Combine table and AQI ring in a flex container"""
    ring_html = create_aqi_ring(max_color, max_status)
    return f"""
<div class="table-ring-container">
    {html_table}
    {ring_html}
</div>
"""

# Load css file
def load_css(file_path):
    with open(file_path) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Generate circles to display the index
def generate_bin_circles(parameter, value):
    bin_idx = get_bin_index(parameter, value)
    circles = []
    for i in range(6):  # always 6 circles
        color = COLORS[i] if i == bin_idx else GREY_HEX
        # add blink-circle class only for the active circle
        cls = "blink-circle" if i == bin_idx else ""
        circles.append(
            f'<span class="{cls}" style="display:inline-block;width:12px;height:12px;border-radius:50%;margin:1px;background-color:{color}"></span>'
        )
    return "".join(circles)

def get_color(value, limits):
    """Return the color corresponding to the AQI bin."""
    for i in range(len(limits) - 1):
        if limits[i] <= value < limits[i + 1]:
            return AQI_COLORS[i]
    return AQI_COLORS[-1]  # last category if above all thresholds