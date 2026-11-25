import streamlit as st
import pandas as pd
import plotly.express as px
import os


st.set_page_config(page_title="Instacart Market Basket Analysis", layout="wide", page_icon="🛒")

st.title("🛒 Instacart Market Basket Analysis Dashboard")
st.markdown("This dashboard visualizes insights from the Instacart dataset, highlighting purchasing behaviors, popular items, and user engagement.")

@st.cache_data
def load_data(filename):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(base_dir, "data", "summary", filename)
    if os.path.exists(data_path):
        return pd.read_csv(data_path, header=None, names=['category', 'metric_value'])
    else:
        st.error(f"Data file not found: {data_path}")
        return pd.DataFrame()

df_busiest_hours = load_data("Busiest_Hours.csv")
df_busiest_days = load_data("Busiest_Days.csv")
df_top_products = load_data("Top_10_Products.csv")
df_top_dept = load_data("Top_Departments.csv")
df_top_aisles = load_data("Top_10_Aisles.csv")
df_reorder_dept = load_data("Reorder_Ratio_Dept.csv")
df_reorder_aisle = load_data("Reorder_Ratio_Aisle.csv")
df_top_reordered = load_data("Top_Reordered_Products.csv")
df_days_prior = load_data("Days_Since_Prior_Order.csv")
df_user_dist = load_data("User_Order_Distribution.csv")


tab1, tab2, tab3, tab4 = st.tabs([
    "📅 Temporal Analysis", 
    "🛍️ Product & Category", 
    "🔁 Reorder Behavior", 
    "👥 User Engagement"
])


with tab1:
    st.header("When do people order?")
    col1, col2 = st.columns(2)
    
    with col1:
        if not df_busiest_hours.empty:
            st.subheader("Busiest Hours of the Day")
            df_busiest_hours['category'] = df_busiest_hours['category'].astype(int)
            df_busiest_hours = df_busiest_hours.sort_values('category')
            fig_hours = px.line(
                df_busiest_hours, x='category', y='metric_value', markers=True,
                labels={'category': 'Hour of Day (0-23)', 'metric_value': 'Total Orders'},
                title="Orders by Hour (Peak: 10 AM - 3 PM)"
            )
            fig_hours.update_xaxes(dtick=1)
            st.plotly_chart(fig_hours, width='stretch')
            
    with col2:
        if not df_busiest_days.empty:
            st.subheader("Busiest Days of the Week")
            day_map = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday"}
            df_busiest_days['category_int'] = df_busiest_days['category'].astype(int)
            df_busiest_days = df_busiest_days.sort_values('category_int')
            df_busiest_days['Day'] = df_busiest_days['category_int'].map(day_map)
            
            fig_days = px.bar(
                df_busiest_days, x='Day', y='metric_value',
                labels={'Day': 'Day of Week', 'metric_value': 'Total Orders'},
                title="Orders by Day (Peak: Weekends)"
            )
            st.plotly_chart(fig_days, width='stretch')


with tab2:
    st.header("What do people buy?")
    col1, col2 = st.columns(2)
    
    with col1:
        if not df_top_products.empty:
            st.subheader("Top 10 Most Popular Products")
            df_top_products = df_top_products.sort_values('metric_value', ascending=True)
            fig_prod = px.bar(
                df_top_products, x='metric_value', y='category', orientation='h',
                labels={'category': 'Product', 'metric_value': 'Purchases'},
                title="Fresh Produce Dominates Purchases"
            )
            st.plotly_chart(fig_prod, width='stretch')
            
        if not df_top_aisles.empty:
            st.subheader("Top 10 Aisles by Sales")
            df_top_aisles = df_top_aisles.sort_values('metric_value', ascending=True)
            fig_aisle = px.bar(
                df_top_aisles, x='metric_value', y='category', orientation='h',
                labels={'category': 'Aisle', 'metric_value': 'Purchases'}
            )
            st.plotly_chart(fig_aisle, width='stretch')
            
    with col2:
        if not df_top_dept.empty:
            st.subheader("Sales by Department")
            fig_dept = px.treemap(
                df_top_dept, path=['category'], values='metric_value',
                labels={'category': 'Department', 'metric_value': 'Purchases'},
                title="Department Volume Share"
            )
            st.plotly_chart(fig_dept, width='stretch')

with tab3:
    st.header("Habitual vs. Exploratory Purchasing")
    
    if not df_top_reordered.empty:
        st.subheader("Top 10 Most Reordered Products")
        df_top_reordered = df_top_reordered.sort_values('metric_value', ascending=True)
        fig_reordered_prod = px.bar(
            df_top_reordered, x='metric_value', y='category', orientation='h',
            labels={'category': 'Product', 'metric_value': 'Reorder Count'}
        )
        st.plotly_chart(fig_reordered_prod, width='stretch')

    col1, col2 = st.columns(2)
    with col1:
        if not df_reorder_dept.empty:
            st.subheader("Reorder Ratio by Department")
            df_reorder_dept = df_reorder_dept.sort_values('metric_value', ascending=True)
            fig_reorder_dept = px.bar(
                df_reorder_dept, x='metric_value', y='category', orientation='h',
                labels={'category': 'Department', 'metric_value': 'Reorder Ratio'},
                title="Staples (Dairy/Produce) vs Occasional (Pantry/Personal Care)"
            )
            fig_reorder_dept.update_xaxes(tickformat=".0%")
            st.plotly_chart(fig_reorder_dept, width='stretch')

    with col2:
        if not df_reorder_aisle.empty:
            st.subheader("Top & Bottom Aisles by Reorder Ratio")
            df_reorder_aisle = df_reorder_aisle.sort_values('metric_value', ascending=False)
            top_10 = df_reorder_aisle.head(10)
            bottom_5 = df_reorder_aisle.tail(5)
            combined_aisles = pd.concat([top_10, bottom_5]).sort_values('metric_value', ascending=True)
            
            fig_reorder_aisle = px.bar(
                combined_aisles, x='metric_value', y='category', orientation='h',
                labels={'category': 'Aisle', 'metric_value': 'Reorder Ratio'},
                title="Highest (e.g. Milk) vs Lowest (e.g. Spices)"
            )
            fig_reorder_aisle.update_xaxes(tickformat=".0%")
            st.plotly_chart(fig_reorder_aisle, width='stretch')

with tab4:
    st.header("How engaged are users?")
    col1, col2 = st.columns(2)
    
    with col1:
        if not df_days_prior.empty:
            st.subheader("Days Since Prior Order")
            st.markdown("Shows how long users wait before reordering. Note the weekly spikes (7, 14, 21, 28 days) and the 30+ day cap.")
            def sort_days(x):
                if '30+' in str(x): return 999
                try: return float(x)
                except: return -1
            
            df_days_prior['sort_key'] = df_days_prior['category'].apply(sort_days)
            df_days_prior = df_days_prior.sort_values('sort_key')
            
            fig_days_prior = px.bar(
                df_days_prior, x='category', y='metric_value',
                labels={'category': 'Days Since Prior Order', 'metric_value': 'Order Count'},
                title="Reorder Gap Distribution"
            )
            st.plotly_chart(fig_days_prior, width='stretch')
            
    with col2:
        if not df_user_dist.empty:
            st.subheader("User Engagement Distribution")
            st.markdown("How many total orders do users place? (Dataset is capped at 100 orders per user)")
            df_user_dist['category_int'] = df_user_dist['category'].astype(int)
            df_user_dist = df_user_dist.sort_values('category_int')
            
            fig_user_dist = px.bar(
                df_user_dist, x='category_int', y='metric_value',
                labels={'category_int': 'Total Orders by User', 'metric_value': 'Number of Users'},
                title="Count of Users by Total Orders"
            )
            st.plotly_chart(fig_user_dist, width='stretch')
