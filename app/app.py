import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import os
import json
from math import radians, sin, cos, sqrt, atan2
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(
    page_title="RMC Retail Site Selection",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'About': "RMC Retail Site Selection Platform - Powered by Geospatial Intelligence"
    }
)

# Dark theme professional styling
st.markdown("""
<style>
    /* Import professional font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Global dark theme */
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        font-family: 'Inter', sans-serif;
    }

    /* Header styling */
    .main-header {
        background: linear-gradient(90deg, #1e40af 0%, #7c3aed 100%);
        padding: 2rem 2rem 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }

    .logo-title {
        display: flex;
        align-items: center;
        gap: 1rem;
        margin-bottom: 0.5rem;
    }

    .rmc-logo {
        background: white;
        color: #1e40af;
        font-weight: 800;
        font-size: 2rem;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        letter-spacing: 2px;
    }

    h1 {
        color: #ffffff !important;
        font-weight: 700 !important;
        font-size: 2.5rem !important;
        margin: 0 !important;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }

    .tagline {
        color: #e0e7ff;
        font-size: 1.1rem;
        font-weight: 300;
        margin-top: 0.5rem;
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
        background-color: #1e293b;
        padding: 0.5rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    }

    .stTabs [data-baseweb="tab"] {
        padding: 1rem 2rem;
        background-color: transparent;
        border-radius: 8px;
        color: #94a3b8;
        font-weight: 500;
        font-size: 1rem;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background-color: #334155;
        color: #e0e7ff;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(90deg, #1e40af 0%, #7c3aed 100%) !important;
        color: white !important;
    }

    /* Section headers */
    h2, h3 {
        color: #f1f5f9 !important;
        font-weight: 600 !important;
        margin-top: 2rem !important;
    }

    /* Metric cards */
    [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
    }

    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
        font-weight: 500 !important;
        font-size: 0.9rem !important;
    }

    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #475569;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
    }

    /* Dataframe styling */
    .stDataFrame {
        background-color: #1e293b;
        border-radius: 8px;
    }

    /* Buttons */
    .stButton > button {
        background: linear-gradient(90deg, #1e40af 0%, #7c3aed 100%);
        color: white;
        font-weight: 600;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-size: 1rem;
        box-shadow: 0 4px 16px rgba(30, 64, 175, 0.4);
        transition: all 0.3s;
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(30, 64, 175, 0.6);
    }

    /* Slider */
    .stSlider {
        padding: 1rem 0;
    }

    /* Text and captions */
    p, .stCaption {
        color: #cbd5e1 !important;
    }

    /* Warning/info boxes */
    .stWarning, .stInfo, .stSuccess {
        background-color: #1e293b;
        border-radius: 8px;
    }

    /* Spinner */
    .stSpinner > div {
        border-color: #7c3aed !important;
    }

    /* Hide sidebar completely */
    [data-testid="stSidebar"] {
        display: none;
    }
    [data-testid="collapsedControl"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Get SQL connection using SDK's built-in SQL connector
@st.cache_resource
def get_connection():
    """Get SQL connection using WorkspaceClient's SQL connector"""
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    warehouse_id = http_path.split("/")[-1] if http_path else "148ccb90800933a1"

    # Get the access token from Databricks Apps forwarded headers
    try:
        headers = st.context.headers
        user_token = headers.get("X-Forwarded-Access-Token") if headers else None
    except Exception as e:
        st.error(f"❌ Failed to read headers: {e}")
        return None

    if not user_token:
        st.error("❌ No access token found.")
        return None

    try:
        # Use WorkspaceClient to get SQL warehouse connection
        cfg = Config(
            host=f"https://{hostname}",
            token=user_token,
            auth_type="pat"
        )
        client = WorkspaceClient(config=cfg)

        # Get SQL warehouse connector from the client
        return client.warehouses.get(id=warehouse_id)

    except Exception as e:
        st.error(f"❌ Connection error: {e}")
        import traceback
        with st.expander("Full Error"):
            st.code(traceback.format_exc())
        return None

def get_user_token():
    """Get authentication token - uses PAT from environment variable"""
    token = os.getenv("DATABRICKS_TOKEN")
    if token:
        return token
    else:
        st.error("No DATABRICKS_TOKEN configured.")
        return None

@st.cache_data(ttl=600)
def query(_token, sql_query):
    """Execute SQL query using Databricks SQL Connector"""
    from databricks import sql as dbsql

    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "e2-demo-west.cloud.databricks.com")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/75fd8278393d07eb")

    if not _token:
        st.error("No authentication token available")
        return pd.DataFrame()

    try:
        st.sidebar.write(f"🔍 Using SQL Connector")
        st.sidebar.write(f"🔍 Host: {hostname}")
        st.sidebar.write(f"🔍 HTTP Path: {http_path}")

        with dbsql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=_token
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)

                # Convert numeric columns
                for col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass

                st.sidebar.success("✅ Query successful!")
                return df

    except Exception as e:
        st.error(f"Query failed: {e}")
        st.sidebar.error(f"❌ Error: {str(e)}")
        import traceback
        with st.expander("Full Error"):
            st.code(traceback.format_exc())
        return pd.DataFrame()

def distance_miles(lat1, lon1, lat2, lon2):
    R = 3959
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

# Header with branding
st.markdown("""
<div class="main-header">
    <div class="logo-title">
        <div class="rmc-logo">RMC</div>
        <div>
            <h1>Retail Site Selection Platform</h1>
            <div class="tagline">Geospatial Intelligence for Strategic Expansion</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# Tabs
tab1, tab2, tab3 = st.tabs(["Current Network", "Expansion Candidates", "Network Optimizer"])

with tab1:
    st.header("Current Store Network")

    # Get user token for OAuth
    user_token = get_user_token()
    if not user_token:
        st.error("Unable to authenticate. Please ensure you're logged in to Databricks.")
        st.stop()

    with st.spinner("Loading store data..."):
        stores = query(user_token, """
            SELECT s.store_number, s.city, s.state, s.annual_sales,
                   e.latitude, e.longitude,
                   e.total_population, e.total_poi_count,
                   e.male_18_to_24, e.female_18_to_24, e.male_45_to_54, e.female_45_to_54,
                   e.income_100k_125k, e.income_125k_150k, e.income_150k_200k, e.income_200k_plus,
                   e.bachelors_degree, e.masters_degree,
                   e.distance_to_valuemart_miles, e.distance_to_quickshop_market_miles,
                   e.poi_count_amenity, e.poi_count_leisure, e.poi_count_shop, e.poi_count_tourism,
                   e.poi_count_office, e.poi_count_public_transport,
                   r.address, r.zip_code
            FROM retail_consumer_goods.geospatial_site_selection.gold_rmc_retail_location_sales s
            JOIN retail_consumer_goods.geospatial_site_selection.gold_rmc_retail_locations_grocery_isochrones_features e
                ON s.store_number = e.store_number
            JOIN retail_consumer_goods.geospatial_site_selection.rmc_retail_locations_grocery r
                ON s.store_number = r.store_number
        """)

        # Load isochrones separately if needed
        try:
            isochrones = query(user_token, """
                SELECT store_number, ST_AsGeoJSON(geometry) as isochrone_geojson
                FROM retail_consumer_goods.geospatial_site_selection.gold_rmc_retail_locations_grocery_isochrones_features
            """)
        except:
            # If isochrone query fails, create empty dataframe
            isochrones = pd.DataFrame()

    if not stores.empty:
        # Merge isochrone data if available
        if not isochrones.empty:
            stores = stores.merge(isochrones, on='store_number', how='left')
        else:
            stores['isochrone_geojson'] = None

        try:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Stores", f"{len(stores):,}")
            col2.metric("Total Annual Sales", f"${stores['annual_sales'].sum():,.0f}")
            col3.metric("Average Sales per Store", f"${stores['annual_sales'].mean():,.0f}")
            col4.metric("Avg Population per Trade Area", f"{stores['total_population'].mean():,.0f}")
        except Exception as e:
            st.error(f"Error displaying metrics: {e}")
            with st.expander("Debug Data"):
                st.write("Data types:", stores.dtypes)
                st.write("First few rows:", stores.head())
            # Continue to show data table anyway

        # Load MA state boundary
        ma_boundary = query(user_token, """
            SELECT ST_AsGeoJSON(geometry) as geometry_geojson
            FROM retail_consumer_goods.geospatial_site_selection.bronze_census_states
            WHERE state_abbr = 'MA'
        """)

        # Create 2-column layout: map (left) + table (right)
        map_col, table_col = st.columns([2, 1])

        with map_col:
            st.subheader("Store Locations")

            # Get toggle state from session state
            if 'show_trade_areas' not in st.session_state:
                st.session_state.show_trade_areas = False

            m = folium.Map(
                location=[stores['latitude'].mean(), stores['longitude'].mean()],
                zoom_start=9,
                tiles='https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
                attr='CartoDB'
            )

            # Add MA state boundary overlay
            if not ma_boundary.empty and ma_boundary.iloc[0]['geometry_geojson']:
                folium.GeoJson(
                    json.loads(ma_boundary.iloc[0]['geometry_geojson']),
                    style_function=lambda x: {
                        'color': '#6366f1',
                        'weight': 2,
                        'fillOpacity': 0,
                        'dashArray': '5, 5'
                    },
                    name='Massachusetts Boundary'
                ).add_to(m)

            # Add isochrones if toggle is on
            if st.session_state.show_trade_areas and 'isochrone_geojson' in stores.columns:
                for _, store in stores.iterrows():
                    if pd.notna(store.get('isochrone_geojson')) and store.get('isochrone_geojson'):
                        try:
                            folium.GeoJson(
                                json.loads(store['isochrone_geojson']),
                                style_function=lambda x: {
                                    'color': '#3b82f6',
                                    'weight': 1,
                                    'fillOpacity': 0.1,
                                    'fillColor': '#3b82f6'
                                }
                            ).add_to(m)
                        except:
                            pass

            for _, store in stores.iterrows():
                # Enhanced tooltip with all requested fields
                tooltip_text = f"""
                <div style="font-family: Arial; font-size: 12px;">
                    <b>Store {store['store_number']}</b><br/>
                    {store['address']}<br/>
                    {store['city']}, {store['state']} {store['zip_code']}<br/>
                    <hr style="margin: 5px 0;">
                    <b>Annual Sales:</b> ${store['annual_sales']:,.0f}<br/>
                    <b>Population:</b> {store['total_population']:,.0f}<br/>
                    <b>POI Count:</b> {store['total_poi_count']:,.0f}
                </div>
                """

                folium.CircleMarker(
                    location=[store['latitude'], store['longitude']],
                    radius=8,
                    popup=tooltip_text,
                    tooltip=tooltip_text,
                    color='#10b981',
                    fill=True,
                    fillColor='#34d399',
                    fillOpacity=0.8,
                    weight=2
                ).add_to(m)
            st_folium(m, width=None, height=500)

            # Toggle below the map
            st.session_state.show_trade_areas = st.checkbox("Show Trade Areas", value=st.session_state.show_trade_areas)

        with table_col:
            st.subheader("Locations by Sales")
            # Create a cleaner table display with column config
            sales_df = stores[['store_number', 'city', 'annual_sales']].copy()
            sales_df = sales_df.sort_values('annual_sales', ascending=False).reset_index(drop=True)

            st.dataframe(
                sales_df,
                column_config={
                    "store_number": st.column_config.TextColumn("Store #", width="small"),
                    "city": st.column_config.TextColumn("City", width="medium"),
                    "annual_sales": st.column_config.NumberColumn(
                        "Annual Sales",
                        format="$%d",
                        width="medium"
                    ),
                },
                hide_index=True,
                use_container_width=True,
                height=500
            )

        st.markdown("<h3 style='text-align: center; margin-bottom: 1rem;'>Sales Performance Drivers</h3>", unsafe_allow_html=True)

        # Calculate derived metrics based on model formula
        stores['young_adults'] = stores['male_18_to_24'] + stores['female_18_to_24'] + stores['male_45_to_54'] + stores['female_45_to_54']
        stores['high_income'] = stores['income_100k_125k'] + stores['income_125k_150k'] + stores['income_150k_200k'] + stores['income_200k_plus']
        stores['higher_education'] = stores['bachelors_degree'] + stores['masters_degree']

        # Calculate values
        young_adults_val = f"{stores['young_adults'].mean():,.0f}"
        high_income_val = f"{stores['high_income'].mean():,.0f}"
        higher_ed_val = f"{stores['higher_education'].mean():,.0f}"
        poi_val = f"{stores['total_poi_count'].mean():,.0f}"

        # Clean card-based metrics
        driver_cols = st.columns(5)

        with driver_cols[0]:
            st.markdown("""
            <div style="background: #1e293b; border-radius: 12px; padding: 24px; border-left: 4px solid #3b82f6; height: 160px; display: flex; flex-direction: column; justify-content: center;">
                <div style="color: #94a3b8; font-size: 14px; font-weight: 600; text-transform: uppercase;">Competitors</div>
                <div style="color: #64748b; font-size: 11px; margin-bottom: 12px;">Closest Proximity</div>
                <div style="color: #f87171; font-size: 22px; font-weight: 700; margin-bottom: 4px;">ValueMart</div>
                <div style="color: #f87171; font-size: 22px; font-weight: 700;">QuickShop</div>
            </div>
            """, unsafe_allow_html=True)

        with driver_cols[1]:
            st.markdown(f"""
            <div style="background: #1e293b; border-radius: 12px; padding: 24px; border-left: 4px solid #3b82f6; height: 160px; display: flex; flex-direction: column; justify-content: center;">
                <div style="color: #94a3b8; font-size: 14px; font-weight: 600; text-transform: uppercase;">Young Adults</div>
                <div style="color: #64748b; font-size: 11px; margin-bottom: 12px;">Avg People per Trade Area</div>
                <div style="color: #ffffff; font-size: 36px; font-weight: 700;">{young_adults_val}</div>
            </div>
            """, unsafe_allow_html=True)

        with driver_cols[2]:
            st.markdown(f"""
            <div style="background: #1e293b; border-radius: 12px; padding: 24px; border-left: 4px solid #3b82f6; height: 160px; display: flex; flex-direction: column; justify-content: center;">
                <div style="color: #94a3b8; font-size: 14px; font-weight: 600; text-transform: uppercase;">High Income HH</div>
                <div style="color: #64748b; font-size: 11px; margin-bottom: 12px;">Avg Households per Trade Area</div>
                <div style="color: #ffffff; font-size: 36px; font-weight: 700;">{high_income_val}</div>
            </div>
            """, unsafe_allow_html=True)

        with driver_cols[3]:
            st.markdown(f"""
            <div style="background: #1e293b; border-radius: 12px; padding: 24px; border-left: 4px solid #3b82f6; height: 160px; display: flex; flex-direction: column; justify-content: center;">
                <div style="color: #94a3b8; font-size: 14px; font-weight: 600; text-transform: uppercase;">Higher Education</div>
                <div style="color: #64748b; font-size: 11px; margin-bottom: 12px;">Avg People per Trade Area</div>
                <div style="color: #ffffff; font-size: 36px; font-weight: 700;">{higher_ed_val}</div>
            </div>
            """, unsafe_allow_html=True)

        with driver_cols[4]:
            st.markdown(f"""
            <div style="background: #1e293b; border-radius: 12px; padding: 24px; border-left: 4px solid #3b82f6; height: 160px; display: flex; flex-direction: column; justify-content: center;">
                <div style="color: #94a3b8; font-size: 14px; font-weight: 600; text-transform: uppercase;">Points of Interest</div>
                <div style="color: #64748b; font-size: 11px; margin-bottom: 12px;">Avg Count per Trade Area</div>
                <div style="color: #ffffff; font-size: 36px; font-weight: 700;">{poi_val}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.warning("No data available. Ensure tables exist and permissions are granted.")

with tab2:
    # Get user token for OAuth
    user_token = get_user_token()
    if not user_token:
        st.error("Unable to authenticate. Please ensure you're logged in to Databricks.")
        st.stop()

    with st.spinner("Loading candidate data..."):
        candidates = query(user_token, """
            SELECT store_number, city, state, latitude, longitude,
                   predicted_annual_sales, total_population, total_poi_count,
                   commute_under_10_min
            FROM retail_consumer_goods.geospatial_site_selection.gold_seed_points_expansion_top_25
        """)

    if not candidates.empty:
        # Create 2-column layout: metrics (left) + filters (right)
        metrics_col, filters_col = st.columns([1, 2])

        with metrics_col:
            st.markdown("### Expansion Candidate Locations")
            st.metric("Total Expansion Locations", f"{len(candidates):,}")
            st.metric("Avg Predicted Sales per Location", f"${candidates['predicted_annual_sales'].mean():,.0f}")

        with filters_col:
            st.markdown("### Expansion Location Filters")

            min_sales = st.slider(
                "Minimum Annual Sales per location for expansion feasibility",
                min_value=int(candidates['predicted_annual_sales'].min()),
                max_value=int(candidates['predicted_annual_sales'].max()),
                value=int(candidates['predicted_annual_sales'].min())
            )

            min_population = st.slider(
                "Target Population accessible to New Location (Per Trade Area)",
                min_value=int(candidates['total_population'].min()),
                max_value=int(candidates['total_population'].max()),
                value=int(candidates['total_population'].min())
            )

        # Apply filters
        filtered = candidates[candidates['predicted_annual_sales'] >= min_sales]
        filtered = filtered[filtered['total_population'] >= min_population]

        st.caption(f"Showing {len(filtered)} of {len(candidates)} expansion locations")

        st.subheader("Expansion Locations")

        # Add legend before map
        st.markdown("""
        <div style="background: #1e293b; padding: 12px; border-radius: 8px; margin-bottom: 16px;">
            <div style="font-weight: 600; margin-bottom: 8px; color: #f1f5f9;">Map Legend</div>
            <div style="display: flex; gap: 24px; flex-wrap: wrap;">
                <div style="display: flex; align-items: center; gap: 8px;">
                    <div style="width: 12px; height: 12px; border-radius: 50%; background: #34d399; border: 2px solid #10b981;"></div>
                    <span style="color: #e2e8f0; font-size: 14px;">Current RMC Locations</span>
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <div style="width: 12px; height: 12px; border-radius: 50%; background: #60a5fa; border: 2px solid #3b82f6;"></div>
                    <span style="color: #e2e8f0; font-size: 14px;">Expansion Candidates</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Load MA state boundary
        ma_boundary = query(user_token, """
            SELECT ST_AsGeoJSON(geometry) as geometry_geojson
            FROM retail_consumer_goods.geospatial_site_selection.bronze_census_states
            WHERE state_abbr = 'MA'
        """)

        m = folium.Map(
            location=[filtered['latitude'].mean(), filtered['longitude'].mean()],
            zoom_start=9,
            tiles='https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
            attr='CartoDB'
        )

        # Add MA state boundary overlay
        if not ma_boundary.empty and ma_boundary.iloc[0]['geometry_geojson']:
            folium.GeoJson(
                json.loads(ma_boundary.iloc[0]['geometry_geojson']),
                style_function=lambda x: {
                    'color': '#6366f1',
                    'weight': 2,
                    'fillOpacity': 0,
                    'dashArray': '5, 5'
                },
                name='Massachusetts Boundary'
            ).add_to(m)

        # Add current RMC locations (always shown)
        current_stores = query(user_token, """
            SELECT s.store_number, s.city, s.state, s.annual_sales,
                   e.latitude, e.longitude,
                   r.address, r.zip_code
            FROM retail_consumer_goods.geospatial_site_selection.gold_rmc_retail_location_sales s
            JOIN retail_consumer_goods.geospatial_site_selection.gold_rmc_retail_locations_grocery_isochrones_features e
                ON s.store_number = e.store_number
            JOIN retail_consumer_goods.geospatial_site_selection.rmc_retail_locations_grocery r
                ON s.store_number = r.store_number
        """)
        if not current_stores.empty:
                for _, store in current_stores.iterrows():
                    tooltip_text = f"""
                    <div style="font-family: Arial; font-size: 12px;">
                        <b>Current Store {store['store_number']}</b><br/>
                        {store['address']}<br/>
                        {store['city']}, {store['state']} {store['zip_code']}<br/>
                        <hr style="margin: 5px 0;">
                        <b>Annual Sales:</b> ${store['annual_sales']:,.0f}
                    </div>
                    """
                    folium.CircleMarker(
                        location=[store['latitude'], store['longitude']],
                        radius=6,
                        popup=tooltip_text,
                        tooltip=tooltip_text,
                        color='#10b981',
                        fill=True,
                        fillColor='#34d399',
                        fillOpacity=0.8,
                        weight=2
                    ).add_to(m)

        # Add expansion candidates (blue markers)
        for _, candidate in filtered.iterrows():
            tooltip_text = f"""
            <div style="font-family: Arial; font-size: 12px;">
                <b>Expansion Location {candidate['store_number']}</b><br/>
                {candidate['city']}, {candidate['state']}<br/>
                <hr style="margin: 5px 0;">
                <b>Predicted Sales:</b> ${candidate['predicted_annual_sales']:,.0f}
            </div>
            """
            folium.CircleMarker(
                location=[candidate['latitude'], candidate['longitude']],
                radius=8,
                popup=tooltip_text,
                tooltip=tooltip_text,
                color='#3b82f6',
                fill=True,
                fillColor='#60a5fa',
                fillOpacity=0.8,
                weight=2
            ).add_to(m)
        st_folium(m, width=None, height=500)

        # Button below the map
        if st.button(f"Optimize Filtered Locations ({len(filtered)} locations)", type="primary", use_container_width=True):
            st.session_state['optimization_candidates'] = filtered.copy()
            st.success(f"✓ {len(filtered)} locations selected for optimization. Go to Network Optimizer tab to run optimization.")
    else:
        st.warning("No data available. Ensure tables exist and permissions are granted.")

with tab3:
    st.header("Network Optimization")

    # Get user token for OAuth
    user_token = get_user_token()
    if not user_token:
        st.error("Unable to authenticate. Please ensure you're logged in to Databricks.")
        st.stop()

    # Check if using pre-selected candidates from Tab 2
    using_preselected = 'optimization_candidates' in st.session_state and st.session_state['optimization_candidates'] is not None

    with st.spinner("Loading optimization data..."):
        existing = query(user_token, """
            SELECT e.latitude, e.longitude
            FROM retail_consumer_goods.geospatial_site_selection.gold_rmc_retail_locations_grocery_isochrones_features e
        """)

        if using_preselected:
            candidates = st.session_state['optimization_candidates']
            st.info(f"Using {len(candidates)} pre-selected locations from Expansion Candidates tab")
            if st.button("Clear Selection & Use All Candidates"):
                st.session_state['optimization_candidates'] = None
                st.rerun()
        else:
            candidates = query(user_token, """
                SELECT store_number, city, state, latitude, longitude, predicted_annual_sales,
                       total_population
                FROM retail_consumer_goods.geospatial_site_selection.gold_seed_points_expansion_top_25
            """)

    if not existing.empty and not candidates.empty:
        st.subheader("Optimization Parameters")
        col1, col2, col3 = st.columns(3)
        with col1:
            max_stores = st.number_input("Maximum New Stores", min_value=1, max_value=20, value=5)
        with col2:
            min_dist_new = st.number_input("Minimum Distance Between New Stores (miles)", min_value=1.0, max_value=10.0, value=3.0, step=0.5)
        with col3:
            min_dist_existing = st.number_input("Minimum Distance from Existing Stores (miles)", min_value=1.0, max_value=10.0, value=2.0, step=0.5)

        if st.button("Run Optimization", type="primary", use_container_width=True):
            with st.spinner("Optimizing network..."):
                selected = []
                for _, candidate in candidates.sort_values('predicted_annual_sales', ascending=False).iterrows():
                    if len(selected) >= max_stores:
                        break

                    # Check distance constraints
                    too_close_existing = any(distance_miles(candidate.latitude, candidate.longitude, row.latitude, row.longitude) < min_dist_existing
                                           for _, row in existing.iterrows())
                    if too_close_existing:
                        continue

                    if selected:
                        too_close_selected = any(distance_miles(candidate.latitude, candidate.longitude, s['latitude'], s['longitude']) < min_dist_new
                                               for s in selected)
                        if too_close_selected:
                            continue

                    selected.append(candidate.to_dict())

                selected_df = pd.DataFrame(selected)

                # Store results in session state
                st.session_state['optimization_results'] = selected_df
                st.session_state['optimization_existing'] = existing
                st.session_state['optimization_candidates'] = candidates

        # Display results if they exist in session state
        if 'optimization_results' in st.session_state and st.session_state['optimization_results'] is not None:
            selected_df = st.session_state['optimization_results']
            existing = st.session_state['optimization_existing']
            candidates = st.session_state['optimization_candidates']

            st.success(f"Optimization complete: {len(selected_df)} locations selected")

            col1, col2, col3 = st.columns(3)
            col1.metric("Locations Selected", f"{len(selected_df)}")
            col2.metric("Total Predicted Revenue", f"${selected_df['predicted_annual_sales'].sum():,.0f}")
            col3.metric("Average Revenue per Location", f"${selected_df['predicted_annual_sales'].mean():,.0f}")

            st.subheader("Optimized Network Map")
            m = folium.Map(
                location=[candidates['latitude'].mean(), candidates['longitude'].mean()],
                zoom_start=9,
                tiles='https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
                attr='CartoDB'
            )
            # Add existing stores
            for _, store in existing.iterrows():
                folium.CircleMarker(
                    location=[store['latitude'], store['longitude']],
                    radius=6,
                    tooltip="Existing Store",
                    color='#10b981',
                    fill=True,
                    fillColor='#34d399',
                    fillOpacity=0.6,
                    weight=2
                ).add_to(m)
            # Add recommended new locations
            for _, location in selected_df.iterrows():
                folium.CircleMarker(
                    location=[location['latitude'], location['longitude']],
                    radius=9,
                    popup=f"<b>New Location {location['store_number']}</b><br/>City: {location['city']}<br/>Predicted Sales: ${location['predicted_annual_sales']:,.0f}",
                    tooltip=f"Recommended: {location['city']}",
                    color='#f59e0b',
                    fill=True,
                    fillColor='#fbbf24',
                    fillOpacity=0.9,
                    weight=3
                ).add_to(m)
            st_folium(m, width=None, height=500)

            st.caption("Green: Existing Stores | Gold: Recommended New Locations")

            st.subheader("Recommended Locations")

            # Create a styled Plotly table
            display_df = selected_df[['store_number', 'predicted_annual_sales', 'total_population']].sort_values('predicted_annual_sales', ascending=False).reset_index(drop=True)

            # Format the values for display
            formatted_sales = ['${:,.0f}'.format(val) for val in display_df['predicted_annual_sales']]
            formatted_pop = ['{:,.0f}'.format(val) for val in display_df['total_population']]

            fig = go.Figure(data=[go.Table(
                header=dict(
                    values=['<b>Store #</b>', '<b>Predicted Annual Sales</b>', '<b>Trade Area Population</b>'],
                    fill_color='#1e40af',
                    align='left',
                    font=dict(color='white', size=14, family='Inter')
                ),
                cells=dict(
                    values=[
                        display_df['store_number'],
                        formatted_sales,
                        formatted_pop
                    ],
                    fill_color=[['#1e293b', '#334155'] * len(display_df)],
                    align='left',
                    font=dict(color='#f1f5f9', size=13, family='Inter'),
                    height=35
                )
            )])

            fig.update_layout(
                height=min(400, len(display_df) * 40 + 60),
                margin=dict(l=0, r=0, t=0, b=0),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )

            st.plotly_chart(fig, use_container_width=True)

            # Save to Delta Table button
            if st.button("Save Results to Delta Table", type="secondary", use_container_width=True):
                with st.spinner("Saving to Delta table..."):
                    try:
                        # Get the store numbers from selected results
                        store_numbers = selected_df['store_number'].tolist()
                        store_numbers_str = ','.join([f"'{sn}'" for sn in store_numbers])

                        # Create the expansion_locations_final table by joining with enriched data
                        save_query = f"""
                        CREATE OR REPLACE TABLE retail_consumer_goods.geospatial_site_selection.gold_expansion_locations_final AS
                        SELECT e.*
                        FROM retail_consumer_goods.geospatial_site_selection.gold_seed_point_isochrones_features e
                        WHERE e.store_number IN ({store_numbers_str})
                        """

                        query(user_token, save_query)
                        st.success(f"✓ Saved {len(store_numbers)} locations to retail_consumer_goods.geospatial_site_selection.gold_expansion_locations_final")
                    except Exception as e:
                        st.error(f"Failed to save results: {e}")
    else:
        st.warning("No data available. Ensure tables exist and permissions are granted.")
