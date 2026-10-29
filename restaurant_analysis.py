
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# =========================================================
# 1. PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="Restaurant Intelligence",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# =========================================================
# 2. THEME SWITCHER
# =========================================================

with st.sidebar:
    st.markdown("## RESTAURANT.")
    st.caption("INTELLIGENCE PLATFORM")

    theme = st.selectbox(
        "Appearance",
        ["Light Mode", "Dark Mode"],
        index=0
    )

is_dark = theme == "Dark Mode"

if is_dark:
    BG = "#101114"
    SIDEBAR = "#17191e"
    CARD = "#1c1f25"
    TEXT = "#f4f4f5"
    MUTED = "#a0a3ab"
    BORDER = "#30333b"
    GRID = "#30333b"
    CHART_TEMPLATE = "plotly_dark"
    ACCENT = "#d6d6d6"
else:
    BG = "#f5f6f8"
    SIDEBAR = "#ffffff"
    CARD = "#ffffff"
    TEXT = "#171717"
    MUTED = "#777b84"
    BORDER = "#e6e8ed"
    GRID = "#eceef2"
    CHART_TEMPLATE = "plotly_white"
    ACCENT = "#252525"


# =========================================================
# 3. DYNAMIC CSS
# =========================================================

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {{
    font-family: 'DM Sans', sans-serif;
}}

.stApp {{
    background: {BG};
    color: {TEXT};
}}

[data-testid="stHeader"] {{
    background: {BG};
}}

[data-testid="stSidebar"] {{
    background: {SIDEBAR};
    border-right: 1px solid {BORDER};
}}

[data-testid="stSidebar"] * {{
    color: {TEXT};
}}

.brand {{
    font-size: 27px;
    font-weight: 800;
    letter-spacing: -1.5px;
    color: {TEXT};
}}

.brand-sub {{
    font-size: 10px;
    letter-spacing: 2px;
    color: {MUTED};
    margin-bottom: 25px;
}}

.main-heading {{
    font-size: 36px;
    font-weight: 800;
    letter-spacing: -1.5px;
    color: {TEXT};
}}

.sub-heading {{
    color: {MUTED};
    font-size: 14px;
    margin-bottom: 22px;
}}

.eyebrow {{
    color: {MUTED};
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 2px;
}}

.kpi-card {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 15px;
    padding: 21px;
    min-height: 125px;
    box-shadow: 0 3px 12px rgba(0,0,0,0.04);
}}

.kpi-label {{
    font-size: 11px;
    color: {MUTED};
    font-weight: 700;
    letter-spacing: 1px;
}}

.kpi-value {{
    color: {TEXT};
    font-size: 28px;
    font-weight: 800;
    margin-top: 12px;
    letter-spacing: -1px;
}}

.kpi-note {{
    color: {MUTED};
    font-size: 11px;
    margin-top: 5px;
}}

.section-heading {{
    font-size: 20px;
    font-weight: 800;
    color: {TEXT};
    margin-top: 15px;
}}

.section-caption {{
    font-size: 12px;
    color: {MUTED};
    margin-bottom: 14px;
}}

div[data-testid="stPlotlyChart"] {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 14px;
    padding: 8px;
}}

div[data-testid="stDataFrame"] {{
    border: 1px solid {BORDER};
    border-radius: 12px;
}}

[data-testid="stMetric"] {{
    background: {CARD};
    border: 1px solid {BORDER};
    border-radius: 12px;
    padding: 15px;
}}

hr {{
    border-color: {BORDER};
}}

.footer {{
    text-align: center;
    color: {MUTED};
    font-size: 11px;
    padding: 25px 0;
}}

#MainMenu {{visibility: hidden;}}
footer {{visibility: hidden;}}
</style>
""", unsafe_allow_html=True)


# =========================================================
# 4. PROJECT PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

ORDERS_FILE = BASE_DIR / "Orders.csv"
RESTAURANTS_FILE = BASE_DIR / "Restaurants.csv"

OUTPUT_DIR = BASE_DIR / "output"


# =========================================================
# 5. SPARK SESSION
# =========================================================

@st.cache_resource
def create_spark_session():

    return (
        SparkSession.builder
        .appName("Restaurant Intelligence Dashboard")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )


# =========================================================
# 6. LOAD DATA USING PYSPARK
# =========================================================

@st.cache_data(show_spinner="Processing data with Apache Spark...")

def load_data():

    if not ORDERS_FILE.exists():
        raise FileNotFoundError("Orders.csv was not found.")

    if not RESTAURANTS_FILE.exists():
        raise FileNotFoundError("Restaurants.csv was not found.")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ORDERS_FILE.as_uri())
    )

    restaurants = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RESTAURANTS_FILE.as_uri())
    )

    joined = (
        orders.alias("o")
        .join(
            restaurants.alias("r"),
            col("o.Restaurant ID") == col("r.RestaurantID"),
            "inner"
        )
        .select(
            "o.*",
            col("r.RestaurantName"),
            col("r.Cuisine"),
            col("r.Zone"),
            col("r.Category")
        )
    )

    return (
        orders.toPandas(),
        restaurants.toPandas(),
        joined.toPandas()
    )


# =========================================================
# 7. LOAD DATA AND HANDLE ERRORS
# =========================================================

try:
    orders_pd, restaurants_pd, data = load_data()

except Exception as error:
    st.error("Unable to load your datasets.")
    st.exception(error)
    st.stop()


# =========================================================
# 8. DATA PREPARATION
# =========================================================

data["Order Amount"] = pd.to_numeric(
    data["Order Amount"], errors="coerce"
)

data["Delivery Time Taken (mins)"] = pd.to_numeric(
    data["Delivery Time Taken (mins)"], errors="coerce"
)

data["Order Date"] = pd.to_datetime(
    data["Order Date"], errors="coerce"
)


# =========================================================
# 9. SIDEBAR NAVIGATION
# =========================================================

with st.sidebar:

    st.markdown(
        f'<div class="brand">RESTAURANT<span style="color:{ACCENT}">.</span></div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="brand-sub">BUSINESS ANALYTICS</div>',
        unsafe_allow_html=True
    )

    st.markdown("### Navigation")

    page = st.radio(
        "Pages",
        [
            "Overview",
            "Restaurant Insights",
            "Cuisine Analysis",
            "Payment Analysis",
            "Data Quality",
            "Reports"
        ],
        label_visibility="collapsed"
    )

    st.divider()

    st.markdown("### Data Filters")

    cuisines = sorted(data["Cuisine"].dropna().unique().tolist())

    selected_cuisines = st.multiselect(
        "Cuisine",
        cuisines,
        default=cuisines
    )

    zones = sorted(data["Zone"].dropna().unique().tolist())

    selected_zones = st.multiselect(
        "Restaurant Zone",
        zones,
        default=zones
    )

    payments = sorted(
        data["Payment Mode"].dropna().unique().tolist()
    )

    selected_payments = st.multiselect(
        "Payment Mode",
        payments,
        default=payments
    )

    st.divider()

    st.caption("POWERED BY APACHE SPARK")
    st.caption("PYTHON · PYSPARK · STREAMLIT")


# =========================================================
# 10. FILTER DATA
# =========================================================

filtered_data = data[
    data["Cuisine"].isin(selected_cuisines)
    & data["Zone"].isin(selected_zones)
    & data["Payment Mode"].isin(selected_payments)
].copy()


# =========================================================
# 11. COMMON FUNCTIONS
# =========================================================

def money(value):
    return f"₹{value:,.0f}"


def kpi_card(label, value, note):

    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-note">{note}</div>
    </div>
    """, unsafe_allow_html=True)


def section_title(title, caption):

    st.markdown(f"""
    <div class="section-heading">{title}</div>
    <div class="section-caption">{caption}</div>
    """, unsafe_allow_html=True)


def format_chart(fig, height=370):

    fig.update_layout(
        template=CHART_TEMPLATE,
        height=height,
        font=dict(
            family="DM Sans",
            color=TEXT
        ),
        paper_bgcolor=CARD,
        plot_bgcolor=CARD,
        margin=dict(l=15, r=20, t=35, b=15),
        legend=dict(
            orientation="h",
            y=1.08,
            x=1,
            xanchor="right"
        )
    )

    fig.update_xaxes(
        showgrid=False,
        color=MUTED
    )

    fig.update_yaxes(
        gridcolor=GRID,
        color=MUTED
    )

    return fig


# =========================================================
# 12. COMMON METRICS
# =========================================================

total_orders = len(filtered_data)

total_revenue = filtered_data["Order Amount"].sum()

average_order = (
    filtered_data["Order Amount"].mean()
    if total_orders else 0
)

average_delivery = (
    filtered_data["Delivery Time Taken (mins)"].mean()
    if total_orders else 0
)

active_restaurants = filtered_data["RestaurantName"].nunique()


# =========================================================
# 13. MAIN HEADER
# =========================================================

st.markdown(
    '<div class="eyebrow">BUSINESS ANALYTICS / RESTAURANT DATA</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="main-heading">Restaurant Intelligence</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="sub-heading">Understand orders, revenue, customer '
    'preferences and restaurant performance.</div>',
    unsafe_allow_html=True
)

st.divider()


# =========================================================
# 14. OVERVIEW
# =========================================================

if page == "Overview":

    st.markdown("### Performance Overview")

    a, b, c, d = st.columns(4)

    with a:
        kpi_card("TOTAL REVENUE", money(total_revenue),
                 "Revenue from selected orders")

    with b:
        kpi_card("TOTAL ORDERS", f"{total_orders:,}",
                 "Customer order volume")

    with c:
        kpi_card("AVERAGE ORDER", money(average_order),
                 "Average order value")

    with d:
        kpi_card("RESTAURANTS", f"{active_restaurants}",
                 "Active in selected data")

    st.write("")

    left, right = st.columns([1.6, 1])

    with left:

        section_title(
            "Revenue Trend",
            "Daily revenue performance"
        )

        trend_data = filtered_data.dropna(
            subset=["Order Date"]
        ).copy()

        if not trend_data.empty:

            trend_data["Date"] = (
                trend_data["Order Date"].dt.date
            )

            trend = (
                trend_data.groupby("Date")["Order Amount"]
                .sum()
                .reset_index()
            )

            fig = px.area(
                trend,
                x="Date",
                y="Order Amount",
                markers=True
            )

            fig.update_traces(
                line_color=ACCENT,
                fillcolor=(
                    "rgba(40,40,40,0.08)"
                    if not is_dark
                    else "rgba(220,220,220,0.10)"
                )
            )

            st.plotly_chart(
                format_chart(fig),
                use_container_width=True
            )
        else:
            st.info("No date information available.")

    with right:

        section_title(
            "Payment Distribution",
            "Order share by payment method"
        )

        payment = (
            filtered_data["Payment Mode"]
            .value_counts()
            .reset_index()
        )

        payment.columns = ["Payment Mode", "Orders"]

        fig = px.pie(
            payment,
            names="Payment Mode",
            values="Orders",
            hole=0.65
        )

        fig.update_traces(
            textposition="inside",
            textinfo="percent+label"
        )

        st.plotly_chart(
            format_chart(fig),
            use_container_width=True
        )

    section_title(
        "Top Performing Restaurants",
        "Top 10 restaurants by revenue"
    )

    top = (
        filtered_data.groupby("RestaurantName")
        .agg(
            Revenue=("Order Amount", "sum"),
            Orders=("Order Amount", "count")
        )
        .reset_index()
        .sort_values("Revenue", ascending=False)
        .head(10)
    )

    if not top.empty:

        fig = px.bar(
            top.sort_values("Revenue"),
            x="Revenue",
            y="RestaurantName",
            orientation="h",
            text="Revenue"
        )

        fig.update_traces(
            marker_color=ACCENT,
            texttemplate="₹%{x:,.0f}",
            textposition="outside"
        )

        st.plotly_chart(
            format_chart(fig, 420),
            use_container_width=True
        )

    if not filtered_data.empty:

        st.markdown("### Business Highlights")

        best_restaurant = (
            filtered_data.groupby("RestaurantName")["Order Amount"]
            .sum().idxmax()
        )

        best_cuisine = (
            filtered_data.groupby("Cuisine")["Order Amount"]
            .sum().idxmax()
        )

        best_payment = (
            filtered_data["Payment Mode"].value_counts().idxmax()
        )

        x, y, z = st.columns(3)

        with x:
            kpi_card("TOP RESTAURANT", best_restaurant,
                     "Highest revenue")

        with y:
            kpi_card("TOP CUISINE", best_cuisine,
                     "Highest revenue")

        with z:
            kpi_card("POPULAR PAYMENT", best_payment,
                     "Most used payment method")


# =========================================================
# 15. RESTAURANT INSIGHTS
# =========================================================

elif page == "Restaurant Insights":

    section_title(
        "Restaurant Performance",
        "Compare restaurant revenue, orders and delivery time."
    )

    summary = (
        filtered_data.groupby("RestaurantName")
        .agg(
            Total_Orders=("Order Amount", "count"),
            Total_Revenue=("Order Amount", "sum"),
            Average_Order=("Order Amount", "mean"),
            Average_Delivery=(
                "Delivery Time Taken (mins)", "mean"
            )
        )
        .reset_index()
        .sort_values("Total_Revenue", ascending=False)
    )

    st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True
    )

    if not summary.empty:

        fig = px.bar(
            summary.head(15).sort_values("Total_Revenue"),
            x="Total_Revenue",
            y="RestaurantName",
            orientation="h",
            text="Total_Orders"
        )

        fig.update_traces(marker_color=ACCENT)

        st.plotly_chart(
            format_chart(fig, 480),
            use_container_width=True
        )


# =========================================================
# 16. CUISINE ANALYSIS
# =========================================================

elif page == "Cuisine Analysis":

    section_title(
        "Cuisine Intelligence",
        "Discover the most popular and profitable cuisines."
    )

    summary = (
        filtered_data.groupby("Cuisine")
        .agg(
            Total_Orders=("Order Amount", "count"),
            Total_Revenue=("Order Amount", "sum"),
            Average_Order=("Order Amount", "mean")
        )
        .reset_index()
        .sort_values("Total_Revenue", ascending=False)
    )

    st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True
    )

    if not summary.empty:

        fig = px.bar(
            summary,
            x="Cuisine",
            y="Total_Revenue",
            text="Total_Revenue"
        )

        fig.update_traces(marker_color=ACCENT)

        st.plotly_chart(
            format_chart(fig, 420),
            use_container_width=True
        )


# =========================================================
# 17. PAYMENT ANALYSIS
# =========================================================

elif page == "Payment Analysis":

    section_title(
        "Payment Intelligence",
        "Analyze customer payment preferences and revenue."
    )

    summary = (
        filtered_data.groupby("Payment Mode")
        .agg(
            Total_Orders=("Order Amount", "count"),
            Total_Revenue=("Order Amount", "sum")
        )
        .reset_index()
        .sort_values("Total_Orders", ascending=False)
    )

    st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True
    )

    if not summary.empty:

        fig = px.bar(
            summary,
            x="Payment Mode",
            y="Total_Revenue",
            text="Total_Revenue"
        )

        fig.update_traces(marker_color=ACCENT)

        st.plotly_chart(
            format_chart(fig, 400),
            use_container_width=True
        )


# =========================================================
# 18. DATA QUALITY
# =========================================================

elif page == "Data Quality":

    section_title(
        "Data Quality & Validation",
        "Check missing values, duplicate records and join consistency."
    )

    missing = orders_pd.isnull().sum().reset_index()
    missing.columns = ["Column", "Missing Values"]

    duplicate_count = orders_pd["Order ID"].duplicated().sum()

    unmatched_count = len(
        orders_pd.merge(
            restaurants_pd,
            left_on="Restaurant ID",
            right_on="RestaurantID",
            how="left",
            indicator=True
        ).query('_merge == "left_only"')
    )

    a, b, c = st.columns(3)

    with a:
        kpi_card(
            "MISSING VALUES",
            str(int(missing["Missing Values"].sum())),
            "Orders dataset"
        )

    with b:
        kpi_card(
            "DUPLICATE ORDER IDS",
            str(duplicate_count),
            "Repeated identifiers"
        )

    with c:
        kpi_card(
            "UNMATCHED ORDERS",
            str(unmatched_count),
            "No restaurant match"
        )

    section_title(
        "Missing Value Report",
        "Missing values by column"
    )

    st.dataframe(
        missing,
        use_container_width=True,
        hide_index=True
    )

    st.markdown("### Dataset Validation")

    st.write("Source orders:", len(orders_pd))
    st.write("Source restaurants:", len(restaurants_pd))
    st.write("Joined orders:", len(data))

    st.write(
        "All orders matched:",
        "Yes" if len(data) == len(orders_pd) else "No"
    )


# =========================================================
# 19. REPORTS AND DOWNLOADS
# =========================================================

elif page == "Reports":

    section_title(
        "Download Center",
        "Export analysis reports as CSV files."
    )

    restaurant_report = (
        filtered_data.groupby("RestaurantName")
        .agg(
            Total_Orders=("Order Amount", "count"),
            Total_Revenue=("Order Amount", "sum"),
            Average_Order=("Order Amount", "mean"),
            Average_Delivery=(
                "Delivery Time Taken (mins)", "mean"
            )
        )
        .reset_index()
    )

    cuisine_report = (
        filtered_data.groupby("Cuisine")
        .agg(
            Total_Orders=("Order Amount", "count"),
            Total_Revenue=("Order Amount", "sum")
        )
        .reset_index()
    )

    payment_report = (
        filtered_data.groupby("Payment Mode")
        .agg(
            Total_Orders=("Order Amount", "count"),
            Total_Revenue=("Order Amount", "sum")
        )
        .reset_index()
    )

    a, b, c = st.columns(3)

    with a:
        st.markdown("**Restaurant Report**")

        st.download_button(
            "Download CSV",
            restaurant_report.to_csv(index=False),
            "restaurant_analysis.csv",
            "text/csv",
            use_container_width=True
        )

    with b:
        st.markdown("**Cuisine Report**")

        st.download_button(
            "Download CSV",
            cuisine_report.to_csv(index=False),
            "cuisine_analysis.csv",
            "text/csv",
            use_container_width=True
        )

    with c:
        st.markdown("**Payment Report**")

        st.download_button(
            "Download CSV",
            payment_report.to_csv(index=False),
            "payment_analysis.csv",
            "text/csv",
            use_container_width=True
        )

    st.divider()

    st.download_button(
        "Download Complete Filtered Dataset",
        filtered_data.to_csv(index=False),
        "restaurant_orders_filtered.csv",
        "text/csv",
        use_container_width=True
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    restaurant_report.to_csv(
        OUTPUT_DIR / "restaurant_analysis.csv",
        index=False
    )

    cuisine_report.to_csv(
        OUTPUT_DIR / "cuisine_analysis.csv",
        index=False
    )

    payment_report.to_csv(
        OUTPUT_DIR / "payment_analysis.csv",
        index=False
    )

    st.success("Reports saved successfully!")


# =========================================================
# 20. FOOTER
# =========================================================

st.divider()

st.markdown("""
<div class="footer">
    RESTAURANT INTELLIGENCE PLATFORM<br>
    POWERED BY PYTHON · APACHE SPARK · PYSPARK · STREAMLIT
</div>
""", unsafe_allow_html=True)
