# app.py — Flow Multi-Tab Dashboard (period-aware)
# Tabs: Transactions, Active Accounts, Accounts Created, New/Active/Total Accounts,
#       Fees, Supply, Contracts, Contract Deployers
# First release implements the Transactions tab.

import pandas as pd
import numpy as np
import altair as alt
import streamlit as st
import snowflake.connector
from datetime import datetime, timezone

# ---------------- Page ----------------
st.set_page_config(page_title="State of Flow Dashboard", page_icon="🟢", layout="wide")

# ---------------- Palette ----------------
COLORS = {
    "cadence": "#16a34a",      # green-600
    "cadence_soft": "#86efac", # green-300
    "evm": "#2563eb",          # blue-600
    "evm_soft": "#93c5fd",     # blue-300
    "bars": "#cbd5e1",         # slate-300
    "bars_overlay": "#94a3b8", # slate-400
    "line_cum": "#0f172a",     # slate-900
}

# ---------------- Utils ----------------
def now_local() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")

def to_float(df: pd.DataFrame, cols: list[str]):
    if df is None or df.empty:
        return df
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    return df

def download_btn(df: pd.DataFrame, label: str, fname: str):
    if df is None or df.empty:
        return
    st.download_button(label, df.to_csv(index=False).encode("utf-8"),
                       file_name=fname, mime="text/csv")

# Replace {{Period}} token in SQL safely (string replacement)
def render_sql(sql: str, period_key: str) -> str:
    return sql.replace("{{Period}}", period_key)

# ---------------- Snowflake ----------------
REQUIRED = ["SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ROLE"]
missing = [k for k in REQUIRED if k not in st.secrets]
if missing:
    st.error(f"Missing secrets: {', '.join(missing)}. Add them in Settings → Secrets.")
    st.stop()

@st.cache_resource
def get_conn():
    params = dict(
        user=st.secrets["SNOWFLAKE_USER"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        role=st.secrets["SNOWFLAKE_ROLE"],
    )
    if "SNOWFLAKE_PASSWORD" in st.secrets:
        params["password"] = st.secrets["SNOWFLAKE_PASSWORD"]
    if "SNOWFLAKE_AUTHENTICATOR" in st.secrets:
        params["authenticator"] = st.secrets["SNOWFLAKE_AUTHENTICATOR"]
    conn = snowflake.connector.connect(**params)
    wh = st.secrets.get("SNOWFLAKE_WAREHOUSE")
    if wh:
        cur = conn.cursor()
        try:
            cur.execute(f'USE WAREHOUSE "{wh}"')
        finally:
            cur.close()
    return conn

@st.cache_data(show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    cur = get_conn().cursor()
    try:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()

def q(sql: str) -> pd.DataFrame:
    return run_query(sql)

# ---------------- Sidebar ----------------
with st.sidebar:
    st.title("Flow Dashboard")
    st.caption("Pick a time period (applies to all tabs).")

    # Global period control (default: last_3_months)
    PERIOD_LABELS = {
        "all_time": "All time",
        "last_3_months": "Last 3 months",
        "last_month": "Last month",
        "last_week": "Last week",
        "last_24h": "Last 24 hours",
    }
    period = st.selectbox(
        "Period",
        options=list(PERIOD_LABELS.keys()),
        format_func=lambda k: PERIOD_LABELS[k],
        index=1  # default to "last_3_months"
    )

    if st.button("🔄 Force refresh"):
        run_query.clear()
        st.rerun()

    st.caption(f"Last updated: {now_local()}")

# ---------------- SQL (Transactions tab) ----------------
SQL_TX_KPIS = """
WITH time_periods AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time'       THEN '2020-01-01'::DATE
            WHEN 'last_year'      THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months'  THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month'     THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week'      THEN CURRENT_DATE - INTERVAL '1 WEEK'
            ELSE CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date
),
cadence_txs AS (
    SELECT count(distinct tx_id) as cadence_transactions
    FROM flow.core.fact_transactions ft CROSS JOIN time_periods tp
    WHERE block_timestamp >= tp.start_date
),
evm_txs AS (
    SELECT count(distinct tx_hash) as evm_transactions
    FROM flow.core_evm.fact_transactions ft CROSS JOIN time_periods tp
    WHERE block_timestamp >= tp.start_date
)
SELECT
    c.cadence_transactions as "Cadence",
    e.evm_transactions     as "EVM",
    (c.cadence_transactions + e.evm_transactions) as total_transactions
FROM cadence_txs c CROSS JOIN evm_txs e;
"""

SQL_TX_OVER_TIME = """
WITH base_transactions AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time'       THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year'      THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_3_months'  THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_month'     THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_week'      THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_24h'       THEN DATE_TRUNC('hour',  block_timestamp)
        END as time_stamp,
        COUNT(DISTINCT tx_id) as total_transactions
    FROM flow.core.fact_transactions
    WHERE block_timestamp >=
        CASE '{{Period}}'
            WHEN 'all_time'       THEN '2020-01-01'::DATE
            WHEN 'last_year'      THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months'  THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month'     THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week'      THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h'       THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1

    UNION ALL

    SELECT
        CASE '{{Period}}'
            WHEN 'all_time'       THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year'      THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_3_months'  THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_month'     THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_week'      THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_24h'       THEN DATE_TRUNC('hour',  block_timestamp)
        END as time_stamp,
        COUNT(DISTINCT tx_hash) as total_transactions
    FROM flow.core_evm.fact_transactions
    WHERE block_timestamp >=
        CASE '{{Period}}'
            WHEN 'all_time'       THEN '2020-01-01'::DATE
            WHEN 'last_year'      THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months'  THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month'     THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week'      THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h'       THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1
),
aggregated_transactions AS (
    SELECT time_stamp, SUM(total_transactions) as total_transactions
    FROM base_transactions
    GROUP BY 1
),
final_metrics AS (
    SELECT
        time_stamp,
        total_transactions AS Transactions,
        LAG(total_transactions) OVER (ORDER BY time_stamp) as prev_period_transactions,
        CONCAT(
            total_transactions, ' (',
            COALESCE(total_transactions - LAG(total_transactions) OVER (ORDER BY time_stamp), 0),
            ')'
        ) AS transactions_diff,
        COALESCE(
            ((total_transactions - LAG(total_transactions) OVER (ORDER BY time_stamp)) /
             NULLIF(LAG(total_transactions) OVER (ORDER BY time_stamp), 0)) * 100, 0
        ) AS pcg_diff,
        SUM(total_transactions) OVER (ORDER BY time_stamp) AS Total_Transactions,
        AVG(total_transactions) OVER (
            ORDER BY time_stamp ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_avg
    FROM aggregated_transactions
    WHERE time_stamp <
        CASE '{{Period}}'
            WHEN 'all_time'       THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year'      THEN DATE_TRUNC('week',  current_date)
            WHEN 'last_3_months'  THEN DATE_TRUNC('week',  current_date)
            WHEN 'last_month'     THEN DATE_TRUNC('day',   current_date)
            WHEN 'last_week'      THEN DATE_TRUNC('day',   current_date)
            WHEN 'last_24h'       THEN DATE_TRUNC('hour',  current_date)
        END
)
SELECT * FROM final_metrics
ORDER BY time_stamp DESC;
"""

SQL_TX_DIST_BY_TYPE = """
WITH base_transactions AS (
    SELECT
        block_timestamp,
        CASE '{{Period}}'
            WHEN 'all_time'       THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year'      THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_3_months'  THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_month'     THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_week'      THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_24h'       THEN DATE_TRUNC('hour',  block_timestamp)
        END as time_stamp,
        'Cadence' as type,
        tx_id as transaction_id
    FROM flow.core.fact_transactions
    WHERE block_timestamp >=
        CASE '{{Period}}'
            WHEN 'all_time'       THEN '2020-01-01'::DATE
            WHEN 'last_year'      THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months'  THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month'     THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week'      THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h'       THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    UNION ALL
    SELECT
        block_timestamp,
        CASE '{{Period}}'
            WHEN 'all_time'       THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year'      THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_3_months'  THEN DATE_TRUNC('week',  block_timestamp)
            WHEN 'last_month'     THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_week'      THEN DATE_TRUNC('day',   block_timestamp)
            WHEN 'last_24h'       THEN DATE_TRUNC('hour',  block_timestamp)
        END as time_stamp,
        'EVM' as type,
        tx_hash as transaction_id
    FROM flow.core_evm.fact_transactions
    WHERE block_timestamp >=
        CASE '{{Period}}'
            WHEN 'all_time'       THEN '2020-01-01'::DATE
            WHEN 'last_year'      THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months'  THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month'     THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week'      THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h'       THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
),
time_based_aggregation AS (
    SELECT time_stamp, type, COUNT(DISTINCT transaction_id) as total_transactions
    FROM base_transactions
    WHERE time_stamp <
        CASE '{{Period}}'
            WHEN 'all_time'       THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year'      THEN DATE_TRUNC('week',  current_date)
            WHEN 'last_3_months'  THEN DATE_TRUNC('week',  current_date)
            WHEN 'last_month'     THEN DATE_TRUNC('day',   current_date)
            WHEN 'last_week'      THEN DATE_TRUNC('day',   current_date)
            WHEN 'last_24h'       THEN DATE_TRUNC('hour',  current_date)
        END
    GROUP BY 1, 2
),
period_comparison AS (
    SELECT
        time_stamp,
        type,
        total_transactions as current_period_transactions,
        LAG(total_transactions) OVER (PARTITION BY type ORDER BY time_stamp) as previous_period_transactions,
        SUM(total_transactions) OVER (PARTITION BY type ORDER BY time_stamp) as cumulative_transactions
    FROM time_based_aggregation
)
SELECT
    time_stamp as period,
    type,
    current_period_transactions as transactions,
    CONCAT(
        current_period_transactions, ' (',
        COALESCE(current_period_transactions - previous_period_transactions, 0), ')'
    ) as transactions_diff,
    CASE
        WHEN previous_period_transactions > 0 THEN
            ((current_period_transactions - previous_period_transactions)::float / previous_period_transactions) * 100
        ELSE 0
    END as pcg_diff,
    cumulative_transactions as total_transactions
FROM period_comparison
ORDER BY period DESC;
"""

# ---------------- Layout ----------------
st.title("🟢 State of Flow — Overview Dashboard")

tabs = st.tabs([
    "Transactions", "Active Accounts", "Accounts Created",
    "New/Active/Total Accounts", "Fees", "Supply",
    "Contracts", "Contract Deployers"
])

# ==========================
# Transactions tab
# ==========================
with tabs[0]:
    st.subheader("Transactions")

    # ---- KPIs (EVM + Cadence / EVM / Cadence) ----
    kpis = q(render_sql(SQL_TX_KPIS, period))
    if kpis is not None and not kpis.empty:
        # Snowflake returns uppercase unless quoted; handle both
        cols = {c.upper(): c for c in kpis.columns}
        total = float(kpis.iloc[0][cols.get("TOTAL_TRANSACTIONS", "TOTAL_TRANSACTIONS")])
        cad   = float(kpis.iloc[0][cols.get("CADENCE", "Cadence")])
        evm   = float(kpis.iloc[0][cols.get("EVM", "EVM")])

        c1, c2, c3 = st.columns(3)
        c1.metric("Transactions [EVM + Cadence]", f"{int(total):,}")
        c2.metric("Transactions [EVM]", f"{int(evm):,}")
        c3.metric("Transactions [Cadence]", f"{int(cad):,}")
        download_btn(kpis, "⬇️ Download KPIs", f"tx_kpis_{period}.csv")
    else:
        st.info("No KPI data for the selected period.")

    st.markdown("### Transactions Over Time — EVM & Cadence (bars + rolling avg)")

    # ---- Over-time (bars + rolling avg bar overlay + cumulative line) ----
    ts = q(render_sql(SQL_TX_OVER_TIME, period))
    if ts is None or ts.empty:
        st.info("No time series for the selected period.")
    else:
        # Normalize names
        ts.columns = [c.upper() for c in ts.columns]
        ts = ts.rename(columns={
            "TIME_STAMP": "TS",
            "TRANSACTIONS": "TX",
            "TOTAL_TRANSACTIONS": "CUM_TX",
            "ROLLING_AVG": "ROLLING",
        })
        ts["TS"] = pd.to_datetime(ts["TS"], errors="coerce")
        to_float(ts, ["TX", "CUM_TX", "ROLLING"])

        base = alt.Chart(ts).properties(height=320)

        # Behind bar: raw transactions
        bars_raw = base.mark_bar(color=COLORS["bars"], opacity=0.6).encode(
            x=alt.X("TS:T", title="Time"),
            y=alt.Y("TX:Q", title="Transactions"),
            tooltip=[
                alt.Tooltip("TS:T", title="Time"),
                alt.Tooltip("TX:Q", title="Tx", format=",.0f"),
                alt.Tooltip("ROLLING:Q", title="Rolling(4)", format=",.1f"),
                alt.Tooltip("CUM_TX:Q", title="Cumulative", format=",.0f"),
            ],
        )

        # Front bar: rolling average
        bars_roll = base.mark_bar(color=COLORS["bars_overlay"], opacity=0.9, size=6).encode(
            x="TS:T",
            y="ROLLING:Q",
        )

        # Cumulative total line
        line_cum = base.mark_line(color=COLORS["line_cum"], strokeWidth=2).encode(
            x="TS:T",
            y=alt.Y("CUM_TX:Q", axis=alt.Axis(title="Cumulative tx", orient="right")),
            tooltip=[alt.Tooltip("CUM_TX:Q", title="Cumulative", format=",.0f")],
        )

        st.altair_chart(alt.layer(bars_raw, bars_roll, line_cum).resolve_scale(y="independent"),
                        use_container_width=True)
        download_btn(ts, "⬇️ Download time series", f"tx_over_time_{period}.csv")

    # ---- Distribution by type ----
    st.markdown("### Distribution of Transactions — Cadence vs EVM")

    dist = q(render_sql(SQL_TX_DIST_BY_TYPE, period))
    if dist is None or dist.empty:
        st.info("No distribution data for the selected period.")
    else:
        dist.columns = [c.upper() for c in dist.columns]
        dist = dist.rename(columns={"PERIOD": "TS", "TYPE": "TYPE", "TRANSACTIONS": "TX"})
        dist["TS"] = pd.to_datetime(dist["TS"], errors="coerce")
        to_float(dist, ["TX"])

        # 100% stacked area over time
        area = (
            alt.Chart(dist)
            .mark_area(opacity=0.85)
            .encode(
                x=alt.X("TS:T", title="Time"),
                y=alt.Y("sum(TX):Q", stack="normalize", axis=alt.Axis(format="%"), title="Share"),
                color=alt.Color(
                    "TYPE:N",
                    scale=alt.Scale(domain=["Cadence", "EVM"], range=[COLORS["cadence"], COLORS["evm"]]),
                    legend=alt.Legend(orient="top")
                ),
                tooltip=[
                    alt.Tooltip("TS:T", title="Time"),
                    "TYPE:N",
                    alt.Tooltip("TX:Q", title="Tx", format=",.0f"),
                ],
            )
            .properties(height=280)
        )

        # Pie for latest period
        latest_ts = dist["TS"].max()
        pie_df = (dist[dist["TS"] == latest_ts]
                  .groupby("TYPE", as_index=False)["TX"].sum())
        pie = (
            alt.Chart(pie_df)
            .mark_arc(innerRadius=60)
            .encode(
                theta=alt.Theta("TX:Q"),
                color=alt.Color(
                    "TYPE:N",
                    scale=alt.Scale(domain=["Cadence", "EVM"], range=[COLORS["cadence"], COLORS["evm"]]),
                    legend=None
                ),
                tooltip=["TYPE:N", alt.Tooltip("TX:Q", title="Tx", format=",.0f")],
            )
            .properties(height=280)
        )

        colA, colB = st.columns([2, 1])
        with colA:
            st.markdown("**100% Stacked Area — Transaction Share Over Time**")
            st.altair_chart(area, use_container_width=True)
        with colB:
            st.markdown(f"**Share (latest period)**")
            st.altair_chart(pie, use_container_width=True)

        download_btn(dist, "⬇️ Download distribution", f"tx_distribution_{period}.csv")

    # ---- Methodology & Links (as requested) ----
    st.markdown("---")
    st.markdown("### Methodology")
    st.markdown(
        "- This dashboard aggregates transactions from **Flow Cadence** (core) and **Flow EVM**.\n"
        "- Time buckets adapt to the selected period:\n"
        "  - All time → monthly, 3 months / last year → weekly, last month / week → daily, last 24h → hourly.\n"
        "- Rolling average uses a trailing 4-bucket window on the selected granularity.\n"
        "- **Data source**: original Flipside data **replicated (volked) to Snowflake**; all queries here read from the Snowflake replica.\n"
        "- Transactions are counted **distinctly** per chain (Cadence: `tx_id`, EVM: `tx_hash`)."
    )

    st.markdown("### Flow Blockchain Links")
    st.markdown(
        "- Flow Docs • Flow Port • Flow Foundation • Flow GitHub\n"
        "- Flow Core Explorer • Flow EVM Explorer\n"
        "- Ecosystem dashboards & analytics references"
    )

    st.markdown("### The State of Flow Blockchain")
    st.markdown(
        "This section summarizes network activity across execution environments (Cadence & EVM), "
        "highlighting throughput trends and the composition of transactions over time. "
        "Use the period selector to switch context quickly."
    )

# ==========================
# Other tabs (scaffold)
# ==========================
with tabs[1]:
    st.subheader("Active Accounts")
    st.info("Coming next: period-aware DAU (total / Cadence / EVM), moving averages, and splits.")

with tabs[2]:
    st.subheader("Accounts Created")
    st.info("Coming next: new accounts over time, moving averages, and change vs prior window.")

with tabs[3]:
    st.subheader("New / Active / Total Accounts")
    st.info("Coming next: combined view with time alignment and cumulative totals.")

with tabs[4]:
    st.subheader("Fees")
    st.info("Coming next: average / total fees (FLOW & USD), success share, and volatility bands.")

with tabs[5]:
    st.subheader("Supply")
    st.info("Coming next: FLOW supply breakdown & deltas.")

with tabs[6]:
    st.subheader("Contracts")
    st.info("Coming next: deployments, active contracts, and event activity (period-aware).")

with tabs[7]:
    st.subheader("Contract Deployers")
    st.info("Coming next: top deployers, velocity, and retention by cohort.")
