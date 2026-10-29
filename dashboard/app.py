
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(
    page_title="Restaurant Analytics",
    page_icon="🍽️",
    layout="wide"
)

BASE_DIR = Path(__file__).resolve().parent.parent

restaurant_file = BASE_DIR / "restaurant_results.csv"
cuisine_file = BASE_DIR / "cuisine_results.csv"
payment_file = BASE_DIR / "payment_results.csv"

st.title("Restaurant Order Analytics Dashboard")
st.caption("Powered by Apache Hadoop HDFS and PySpark")

try:
    restaurant = pd.read_csv(restaurant_file)
    cuisine = pd.read_csv(cuisine_file)
    payment = pd.read_csv(payment_file)
except Exception as e:
    st.error(f"Could not load analysis CSV files: {e}")
    st.stop()

total_orders = int(restaurant["Total Orders"].sum())
total_revenue = float(restaurant["Total Revenue"].sum())
avg_order_value = total_revenue / total_orders if total_orders else 0
avg_delivery = (
    restaurant["Average Delivery Time"] * restaurant["Total Orders"]
).sum() / total_orders if total_orders else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Orders", f"{total_orders:,}")
c2.metric("Total Revenue", f"₹{total_revenue:,.0f}")
c3.metric("Average Order Value", f"₹{avg_order_value:,.2f}")
c4.metric("Avg. Delivery Time", f"{avg_delivery:.1f} min")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Restaurant Revenue")
    restaurant_chart = restaurant.sort_values("Total Revenue", ascending=True)
    fig = px.bar(
        restaurant_chart,
        x="Total Revenue",
        y="RestaurantName",
        orientation="h",
        labels={
            "Total Revenue": "Revenue (₹)",
            "RestaurantName": "Restaurant"
        }
    )
    st.plotly_chart(fig, width="stretch")

with right:
    st.subheader("Cuisine Revenue")
    fig = px.pie(
        cuisine,
        names="Cuisine",
        values="Total Revenue",
        hole=0.4
    )
    st.plotly_chart(fig, width="stretch")

left, right = st.columns(2)

with left:
    st.subheader("Orders by Payment Method")
    fig = px.bar(
        payment,
        x="Payment Mode",
        y="Total Orders",
        color="Payment Mode"
    )
    st.plotly_chart(fig, width="stretch")

with right:
    st.subheader("Revenue by Payment Method")
    fig = px.bar(
        payment,
        x="Payment Mode",
        y="Total Revenue",
        color="Payment Mode"
    )
    st.plotly_chart(fig, width="stretch")

st.subheader("Restaurant Analysis Data")
st.dataframe(
    restaurant.sort_values("Total Revenue", ascending=False),
    width="stretch",
    hide_index=True
)

st.subheader("Cuisine Analysis Data")
st.dataframe(cuisine, width="stretch", hide_index=True)

st.subheader("Payment Analysis Data")
st.dataframe(payment, width="stretch", hide_index=True)

st.success("Dashboard loaded from your exported PySpark analysis results.")
