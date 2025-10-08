import os
from dotenv import load_dotenv
from pyairtable import Api
import plotly.graph_objects as go
import plotly.express as px
from collections import Counter
from datetime import datetime

load_dotenv()

api = Api(os.getenv("AIRTABLE_TOKEN"))
base = api.base(os.getenv("AIRTABLE_BASE_ID"))
firms_table = base.table("Venture Capital Firms")
therapeutic_table = base.table("Therapeutic Areas")
geography_table = base.table("Geographic Regions")

def fetch_lookup_tables():
    """Fetch all lookup tables and create ID->Name mappings"""
    print("Fetching lookup tables...")
    
    therapeutic_map = {}
    geo_map = {}
    
    # Use correct field name: "Therapeutic Area Name"
    for rec in therapeutic_table.all():
        rec_id = rec["id"]
        name = rec.get("fields", {}).get("Therapeutic Area Name", "Unknown")
        therapeutic_map[rec_id] = name
    
    # Use correct field name: "Region Name"
    for rec in geography_table.all():
        rec_id = rec["id"]
        name = rec.get("fields", {}).get("Region Name", "Unknown")
        geo_map[rec_id] = name
    
    print(f"  Loaded {len(therapeutic_map)} therapeutic areas")
    print(f"  Loaded {len(geo_map)} geographic regions")
    
    return therapeutic_map, geo_map

def fetch_all_data():
    print("Fetching VC firms...")
    records = firms_table.all()
    print(f"Fetched {len(records)} VC firms")
    return records

def analyze_data(records, therapeutic_map, geo_map):
    """Extract analytics from records"""
    therapeutics = []
    geographies = []
    countries = []
    verified_count = 0
    has_website = 0
    has_description = 0
    
    for rec in records:
        fields = rec.get("fields", {})
        
        # Therapeutic areas - convert IDs to names
        therapeutic_ids = fields.get("Therapeutic Areas of Focus", [])
        for tid in therapeutic_ids:
            name = therapeutic_map.get(tid, "Unknown")
            therapeutics.append(name)
        
        # Geography - convert IDs to names
        geo_ids = fields.get("Geography Focus", [])
        for gid in geo_ids:
            name = geo_map.get(gid, "Unknown")
            geographies.append(name)
        
        # Countries - convert IDs to names
        country_ids = fields.get("Headquarters Country", [])
        for cid in country_ids:
            name = geo_map.get(cid, "Unknown")
            countries.append(name)
        
        # Data quality metrics
        if fields.get("Verified"):
            verified_count += 1
        if fields.get("Website"):
            has_website += 1
        if fields.get("Description"):
            has_description += 1
    
    return {
        "total_firms": len(records),
        "therapeutics": Counter(therapeutics),
        "geographies": Counter(geographies),
        "countries": Counter(countries),
        "verified_count": verified_count,
        "has_website": has_website,
        "has_description": has_description,
    }

def create_dashboard(analytics):
    """Generate HTML dashboard with Plotly charts"""
    
    # Chart 1: Top Therapeutic Areas
    therapeutic_top = analytics["therapeutics"].most_common(15)
    if therapeutic_top:
        fig1 = go.Figure(data=[
            go.Bar(
                x=[name for name, _ in therapeutic_top],
                y=[count for _, count in therapeutic_top],
                marker_color='rgb(55, 83, 109)',
                text=[count for _, count in therapeutic_top],
                textposition='auto',
            )
        ])
        fig1.update_layout(
            title="Top 15 Therapeutic Areas",
            xaxis_title="Therapeutic Area",
            yaxis_title="Number of VCs",
            height=500,
            xaxis={'tickangle': -45}
        )
        chart1_html = fig1.to_html(include_plotlyjs='cdn', div_id="chart1")
    else:
        chart1_html = "<p>No therapeutic area data available</p>"
    
    # Chart 2: Geographic Distribution
    geo_top = analytics["geographies"].most_common(15)
    if geo_top:
        fig2 = px.pie(
            names=[name for name, _ in geo_top],
            values=[count for _, count in geo_top],
            title="Geographic Focus Distribution (Top 15)"
        )
        fig2.update_traces(textposition='inside', textinfo='percent+label')
        fig2.update_layout(height=500)
        chart2_html = fig2.to_html(include_plotlyjs=False, div_id="chart2")
    else:
        chart2_html = "<p>No geography data available</p>"
    
    # Chart 3: Data Quality Metrics
    fig3 = go.Figure(data=[
        go.Bar(
            x=['Verified', 'Has Website', 'Has Description'],
            y=[
                analytics["verified_count"],
                analytics["has_website"],
                analytics["has_description"]
            ],
            marker_color=['#2ecc71', '#3498db', '#9b59b6'],
            text=[
                f"{analytics['verified_count']}/{analytics['total_firms']}",
                f"{analytics['has_website']}/{analytics['total_firms']}",
                f"{analytics['has_description']}/{analytics['total_firms']}"
            ],
            textposition='auto',
        )
    ])
    fig3.update_layout(
        title="Data Quality Metrics",
        yaxis_title="Count",
        height=400,
    )
    chart3_html = fig3.to_html(include_plotlyjs=False, div_id="chart3")
    
    # Chart 4: Country Distribution
    country_top = analytics["countries"].most_common(15)
    if country_top:
        fig4 = go.Figure(data=[
            go.Bar(
                x=[name for name, _ in country_top],
                y=[count for _, count in country_top],
                marker_color='rgb(26, 118, 255)',
                text=[count for _, count in country_top],
                textposition='auto',
            )
        ])
        fig4.update_layout(
            title="Top Countries by VC Headquarters",
            xaxis_title="Country",
            yaxis_title="Number of VCs",
            height=400,
            xaxis={'tickangle': -45}
        )
        chart4_html = fig4.to_html(include_plotlyjs=False, div_id="chart4")
    else:
        chart4_html = "<p>No country data available</p>"
    
    # HTML Template
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Biotech VC Analytics Dashboard</title>
        <meta charset="utf-8">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            .header {{
                background: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                margin-bottom: 30px;
                text-align: center;
            }}
            h1 {{
                color: #2d3748;
                margin: 0 0 10px 0;
                font-size: 2.5em;
            }}
            .subtitle {{
                color: #718096;
                font-size: 1.1em;
            }}
            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }}
            .stat-card {{
                background: white;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                text-align: center;
            }}
            .stat-number {{
                font-size: 2.5em;
                font-weight: bold;
                color: #667eea;
                margin: 10px 0;
            }}
            .stat-label {{
                color: #718096;
                font-size: 0.95em;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}
            .charts {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
                gap: 20px;
            }}
            .chart-container {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            }}
            .footer {{
                text-align: center;
                color: white;
                margin-top: 40px;
                opacity: 0.8;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🧬 Biotech VC Analytics Dashboard</h1>
                <p class="subtitle">Last updated: {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</p>
            </div>
            
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-label">Total VC Firms</div>
                    <div class="stat-number">{analytics['total_firms']}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Verified</div>
                    <div class="stat-number">{analytics['verified_count']}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Therapeutic Areas</div>
                    <div class="stat-number">{len(analytics['therapeutics'])}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Countries</div>
                    <div class="stat-number">{len(analytics['countries'])}</div>
                </div>
            </div>
            
            <div class="charts">
                <div class="chart-container">{chart1_html}</div>
                <div class="chart-container">{chart2_html}</div>
                <div class="chart-container">{chart3_html}</div>
                <div class="chart-container">{chart4_html}</div>
            </div>
            
            <div class="footer">
                <p>Generated by Python + Airtable API + Plotly</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

def main():
    print("\n=== GENERATING ANALYTICS DASHBOARD ===\n")
    
    # Fetch lookup tables first
    therapeutic_map, geo_map = fetch_lookup_tables()
    
    # Fetch firm data
    records = fetch_all_data()
    
    # Analyze
    print("\nAnalyzing data...")
    analytics = analyze_data(records, therapeutic_map, geo_map)
    
    # Generate dashboard
    print("Creating visualizations...")
    html = create_dashboard(analytics)
    
    # Save to file
    output_file = "vc_dashboard.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"\n✅ Dashboard created: {output_file}")
    print(f"📊 Total VCs: {analytics['total_firms']}")
    print(f"✓ Verified: {analytics['verified_count']}")
    print(f"🧬 Therapeutic Areas: {len(analytics['therapeutics'])}")
    print(f"🌍 Countries: {len(analytics['countries'])}")
    print(f"\nOpen {output_file} in your browser to view!\n")

if __name__ == "__main__":
    main()
