import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import tempfile

# Set page config
st.set_page_config(
    page_title="Iceberg Metadata Insights",
    page_icon="❄️",
    layout="wide"
)

# App title and description
st.title("❄️ Iceberg Metadata Insights")
st.markdown("Key insights into your Iceberg tables' metadata and snapshots.")

# Initialize DuckDB with required extensions
@st.cache_resource
def initialize_duckdb():
    conn = duckdb.connect(database=':memory:')

    # Install and load required extensions
    conn.execute("INSTALL aws;")
    conn.execute("LOAD aws;")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("INSTALL iceberg;")
    conn.execute("LOAD iceberg;")
    conn.execute("INSTALL parquet;")
    conn.execute("LOAD parquet;")

    # Load AWS credentials
    conn.execute("CALL load_aws_credentials();")

    return conn

# Get DuckDB connection
conn = initialize_duckdb()

# Sidebar for input options
st.sidebar.header("Iceberg Table Source")
input_option = st.sidebar.radio("Select input method:", ["Upload Metadata File", "S3 Path", "Local Path"])

metadata_path = None

if input_option == "Upload Metadata File":
    uploaded_file = st.sidebar.file_uploader("Choose an Iceberg metadata JSON file", type=["json"])
    if uploaded_file is not None:
        # Save uploaded file to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp_file:
            temp_file.write(uploaded_file.getvalue())
            metadata_path = temp_file.name
        st.sidebar.success(f"File uploaded successfully!")
elif input_option == "S3 Path":
    default_s3_path = "s3://<BUCKET>/warehouse/sales/metadata/00001-cae4181c-e8ec-4d6a-97b6-c4732cc9c434.metadata.json"
    metadata_path = st.sidebar.text_input("Enter S3 path to metadata file:", value=default_s3_path)
else:
    default_local_path = "data/iceberg/lineitem_iceberg"
    metadata_path = st.sidebar.text_input("Enter local path to Iceberg table:", value=default_local_path)

# Add button to analyze
analyze_button = st.sidebar.button("Analyze Table")

# Add link to documentation
st.sidebar.markdown("---")
st.sidebar.markdown("[DuckDB Iceberg Documentation](https://duckdb.org/docs/stable/extensions/iceberg/overview.html)")

# Helper functions for insights
def extract_operation_metrics(snapshots_df, manifest_df):
    """Extract metrics about operations (add/delete)"""
    if manifest_df.empty or snapshots_df.empty:
        return pd.DataFrame()

    operations = []

    for idx, snapshot in snapshots_df.iterrows():
        seq_num = snapshot['sequence_number']
        snapshot_manifests = manifest_df[manifest_df['manifest_sequence_number'] == seq_num]

        if 'status' in snapshot_manifests.columns:
            adds = snapshot_manifests[snapshot_manifests['status'] == 'ADDED']['record_count'].sum()
            deletes = snapshot_manifests[snapshot_manifests['status'] == 'DELETED']['record_count'].sum()

            operations.append({
                'snapshot_id': snapshot['snapshot_id'],
                'timestamp': snapshot['timestamp'] if 'timestamp' in snapshot else pd.NaT,
                'sequence_number': seq_num,
                'added_records': adds,
                'deleted_records': deletes,
                'net_change': adds - deletes,
                'manifest_count': len(snapshot_manifests)
            })

    return pd.DataFrame(operations)

def calculate_snapshot_intervals(snapshots_df):
    """Calculate time intervals between snapshots"""
    if 'timestamp' not in snapshots_df.columns or len(snapshots_df) <= 1:
        return pd.DataFrame()

    sorted_df = snapshots_df.sort_values('timestamp').reset_index(drop=True)
    intervals = []

    for i in range(1, len(sorted_df)):
        prev = sorted_df.iloc[i-1]
        curr = sorted_df.iloc[i]

        time_diff = (curr['timestamp'] - prev['timestamp']).total_seconds()
        hours_diff = time_diff / 3600
        days_diff = time_diff / (3600 * 24)

        intervals.append({
            'previous_snapshot': prev['snapshot_id'],
            'current_snapshot': curr['snapshot_id'],
            'previous_time': prev['timestamp'],
            'current_time': curr['timestamp'],
            'interval_seconds': time_diff,
            'interval_hours': hours_diff,
            'interval_days': days_diff
        })

    return pd.DataFrame(intervals)

# Main content
if metadata_path and analyze_button:
    try:
        # Get snapshot data
        try:
            snapshots_df = conn.execute(f"""
                SELECT * FROM iceberg_snapshots('{metadata_path}')
            """).fetchdf()

            # Convert timestamp to datetime if it's not already
            if not snapshots_df.empty and 'timestamp_ms' in snapshots_df.columns:
                snapshots_df['timestamp'] = pd.to_datetime(snapshots_df['timestamp_ms'])
        except Exception as e:
            st.error(f"Error fetching snapshots: {str(e)}")
            snapshots_df = pd.DataFrame()

        # Get manifest data
        try:
            manifest_df = conn.execute(f"""
                SELECT * FROM iceberg_metadata('{metadata_path}')
            """).fetchdf()
        except Exception as e:
            st.error(f"Error fetching metadata: {str(e)}")
            manifest_df = pd.DataFrame()

        # Get schema information
        try:
            schema_df = conn.execute(f"""
                SELECT * FROM iceberg_schema('{metadata_path}')
            """).fetchdf()
        except Exception as e:
            schema_df = pd.DataFrame()

        # Calculate insights
        if not snapshots_df.empty and not manifest_df.empty:
            operations_df = extract_operation_metrics(snapshots_df, manifest_df)
            intervals_df = calculate_snapshot_intervals(snapshots_df)

        # Key Metrics Section - Only the requested metrics
        st.header("Key Metrics")
        metric_cols = st.columns(4)

        with metric_cols[0]:
            st.metric("Total Snapshots", len(snapshots_df) if not snapshots_df.empty else 0)

        with metric_cols[1]:
            if not operations_df.empty:
                write_ops = operations_df[operations_df['added_records'] > 0].shape[0]
                st.metric("Write Operations", write_ops)
            else:
                st.metric("Write Operations", "N/A")

        with metric_cols[2]:
            if not operations_df.empty:
                delete_ops = operations_df[operations_df['deleted_records'] > 0].shape[0]
                st.metric("Delete Operations", delete_ops)
            else:
                st.metric("Delete Operations", "N/A")

        with metric_cols[3]:
            if not operations_df.empty:
                avg_manifests = operations_df['manifest_count'].mean()
                st.metric("Avg Manifests/Snapshot", f"{avg_manifests:.1f}")
            else:
                st.metric("Avg Manifests/Snapshot", "N/A")

        # Snapshots Timeline Section - Simplified
        st.header("Snapshots Timeline")
        if not snapshots_df.empty and 'timestamp' in snapshots_df.columns and len(snapshots_df) > 1:
            # Create simple timeline visualization
            fig = px.scatter(
                snapshots_df.sort_values('timestamp'),
                x='timestamp',
                y=[1] * len(snapshots_df),  # All points on same level
                size=[10] * len(snapshots_df),  # Same size for all points
                color_discrete_sequence=['blue'],
                labels={"y": ""},
                title="Snapshot Timeline"
            )

            # Add connecting lines
            fig.add_trace(
                go.Scatter(
                    x=snapshots_df.sort_values('timestamp')['timestamp'],
                    y=[1] * len(snapshots_df),
                    mode='lines',
                    line=dict(color='lightblue', width=1),
                    showlegend=False
                )
            )

            # Improve layout
            fig.update_layout(
                height=300,
                yaxis=dict(
                    showticklabels=False,
                    showgrid=False,
                    zeroline=False,
                    range=[0.5, 1.5]  # Fix y-axis range
                ),
                xaxis=dict(
                    title="Time"
                ),
                hovermode="x unified",
                margin=dict(l=20, r=20, t=40, b=20),
            )

            # Customize hover information
            fig.update_traces(
                hovertemplate="<b>Snapshot ID:</b> %{customdata}<br><b>Time:</b> %{x}<extra></extra>",
                customdata=snapshots_df.sort_values('timestamp')['snapshot_id']
            )

            st.plotly_chart(fig, use_container_width=True)

            # Display snapshot interval statistics
            if not intervals_df.empty:
                interval_cols = st.columns(3)

                with interval_cols[0]:
                    avg_hours = intervals_df['interval_hours'].mean()
                    st.metric("Avg Interval", f"{avg_hours:.1f} hours")

                with interval_cols[1]:
                    min_hours = intervals_df['interval_hours'].min()
                    st.metric("Min Interval", f"{min_hours:.1f} hours")

                with interval_cols[2]:
                    max_hours = intervals_df['interval_hours'].max()
                    st.metric("Max Interval", f"{max_hours:.1f} hours")

                # Show histogram of snapshot intervals
                fig = px.histogram(
                    intervals_df,
                    x='interval_hours',
                    nbins=min(10, len(intervals_df)),
                    title="Distribution of Snapshot Intervals",
                    labels={"interval_hours": "Interval (hours)"},
                    color_discrete_sequence=['lightblue']
                )

                fig.update_layout(
                    xaxis_title="Hours between snapshots",
                    yaxis_title="Count",
                    bargap=0.1,
                    height=300
                )

                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Insufficient data to show snapshots timeline. Need multiple snapshots with timestamps.")

        # Recent Snapshots Table
        expander = st.expander("View Recent Snapshots")
        with expander:
            if not snapshots_df.empty:
                if 'timestamp' in snapshots_df.columns:
                    recent_snapshots = snapshots_df.sort_values('timestamp', ascending=False).head(5)
                    st.dataframe(recent_snapshots, use_container_width=True)
                else:
                    st.dataframe(snapshots_df.head(5), use_container_width=True)
            else:
                st.info("No snapshot data available.")

    except Exception as e:
        st.error(f"Error analyzing Iceberg table: {str(e)}")
        st.error("Please check if the metadata path is correct and accessible.")
else:
    st.info("Please provide an Iceberg table path and click 'Analyze Table' to begin analysis.")

# Clean up temporary file if created
if input_option == "Upload Metadata File" and 'temp_file' in locals():
    try:
        os.unlink(temp_file.name)
    except:
        pass
