import os
from datetime import datetime

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv


# --------------------------------------------------
# Environment
# --------------------------------------------------

load_dotenv(".env")


# --------------------------------------------------
# Page setup
# --------------------------------------------------

st.set_page_config(
    page_title="AI Log Monitoring Dashboard",
    page_icon="🚨",
    layout="wide"
)

st.title("🚨 AI-Powered Cloud Log Monitoring")
st.caption(
    "Real-time anomaly detection, AI incident analysis, "
    "and PostgreSQL incident monitoring"
)

st.info("🔄 Dashboard automatically refreshes every 5 seconds")


# --------------------------------------------------
# PostgreSQL
# --------------------------------------------------

def load_incidents():

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    query = """
    SELECT
        id,
        block_id,
        event_id,
        log_level,
        component,
        events_seen,
        anomaly_probability,
        source_ip,
        destination_ip,
        severity,
        summary,
        likely_cause,
        recommended_action,
        raw_message,
        created_at
    FROM incidents
    ORDER BY created_at DESC;
    """

    df = pd.read_sql(
        query,
        connection
    )

    connection.close()

    return df


# --------------------------------------------------
# Live dashboard
# --------------------------------------------------

@st.fragment(run_every="5s")
def live_dashboard():

    try:

        incidents = load_incidents()

        st.caption(
            f"Last refreshed: "
            f"{datetime.now().strftime('%H:%M:%S')}"
        )

        if incidents.empty:

            st.warning("No incidents found.")
            return


        # ------------------------------------------
        # KPI cards
        # ------------------------------------------

        total_incidents = len(incidents)

        high_incidents = len(
            incidents[
                incidents["severity"].isin(
                    ["High", "Critical"]
                )
            ]
        )

        avg_probability = (
            incidents["anomaly_probability"].mean()
        )

        unique_blocks = (
            incidents["block_id"].nunique()
        )


        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Total Incidents",
            total_incidents
        )

        col2.metric(
            "High / Critical",
            high_incidents
        )

        col3.metric(
            "Average Anomaly Probability",
            f"{avg_probability:.2%}"
        )

        col4.metric(
            "Unique Blocks",
            unique_blocks
        )


        st.divider()


        # ------------------------------------------
        # Filters
        # ------------------------------------------

        st.subheader("🔎 Incident Filters")

        filter1, filter2 = st.columns(2)

        severity_options = [
            "All"
        ] + sorted(
            incidents["severity"]
            .dropna()
            .unique()
            .tolist()
        )

        selected_severity = filter1.selectbox(
            "Severity",
            severity_options
        )

        event_options = [
            "All"
        ] + sorted(
            incidents["event_id"]
            .dropna()
            .unique()
            .tolist()
        )

        selected_event = filter2.selectbox(
            "Event ID",
            event_options
        )


        filtered = incidents.copy()

        if selected_severity != "All":

            filtered = filtered[
                filtered["severity"]
                == selected_severity
            ]

        if selected_event != "All":

            filtered = filtered[
                filtered["event_id"]
                == selected_event
            ]


        # ------------------------------------------
        # Severity chart
        # ------------------------------------------

        st.subheader("📊 Incidents by Severity")

        severity_counts = (
            filtered["severity"]
            .fillna("Unknown")
            .value_counts()
        )

        st.bar_chart(
            severity_counts
        )


        st.divider()


        # ------------------------------------------
        # Incident table
        # ------------------------------------------

        st.subheader("📋 Detected Incidents")

        display_columns = [
            "id",
            "block_id",
            "event_id",
            "severity",
            "component",
            "anomaly_probability",
            "source_ip",
            "destination_ip",
            "created_at"
        ]

        st.dataframe(
            filtered[display_columns],
            use_container_width=True,
            hide_index=True
        )


        st.divider()


        # ------------------------------------------
        # Incident details
        # ------------------------------------------

        st.subheader("🔍 Incident Details")

        block_options = (
            filtered["block_id"]
            .dropna()
            .tolist()
        )

        if block_options:

            selected_block = st.selectbox(
                "Select Block ID",
                block_options
            )

            incident = filtered[
                filtered["block_id"]
                == selected_block
            ].iloc[0]


            detail1, detail2 = st.columns(2)

            with detail1:

                st.write(
                    "**Severity:**",
                    incident["severity"]
                )

                st.write(
                    "**Event ID:**",
                    incident["event_id"]
                )

                st.write(
                    "**Component:**",
                    incident["component"]
                )

                st.write(
                    "**Anomaly Probability:**",
                    f"{incident['anomaly_probability']:.2%}"
                )

                st.write(
                    "**Events Seen:**",
                    incident["events_seen"]
                )


            with detail2:

                st.write(
                    "**Source IP:**",
                    incident["source_ip"]
                )

                st.write(
                    "**Destination IP:**",
                    incident["destination_ip"]
                )

                st.write(
                    "**Block ID:**",
                    incident["block_id"]
                )

                st.write(
                    "**Detected At:**",
                    incident["created_at"]
                )


            st.markdown("### 🤖 AI Analysis")

            st.write("**Summary**")

            st.info(
                incident["summary"]
                or "Not available"
            )

            st.write("**Likely Cause**")

            st.write(
                incident["likely_cause"]
                or "Not available"
            )

            st.write("**Recommended Action**")

            st.success(
                incident["recommended_action"]
                or "Not available"
            )


            with st.expander(
                "View Raw Log Message"
            ):

                st.code(
                    incident["raw_message"]
                    or "Not available"
                )


    except Exception as error:

        st.error(
            f"Could not load incidents: {error}"
        )


live_dashboard()