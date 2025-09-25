#!/usr/bin/env python3
"""
Test script to verify visualization function works correctly.
Run this from the project root directory.
"""

import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path.cwd()))

try:
    from orchestration.visualize_air_quality import generate_visualizations
    print("✓ Successfully imported generate_visualizations function")
    
    # Test the function
    print("Testing visualization generation...")
    result_dir = generate_visualizations()
    print(f"✓ Success! Files saved to: {result_dir}")
    
    # List the files that were created
    result_path = Path(result_dir)
    html_files = list(result_path.glob("*.html"))
    
    if html_files:
        print(f"✓ Created {len(html_files)} HTML files:")
        for file in html_files:
            print(f"  - {file.name}")
    else:
        print("⚠ No HTML files found in output directory")
        
except ImportError as e:
    print(f"✗ Import error: {e}")
    print("Make sure you're running from the project root directory")
    print("Current working directory:", Path.cwd())
    
except Exception as e:
    print(f"✗ Error running visualization: {e}")
    print("This might be expected if you don't have data yet")