# app.py — Flow Multi-Tab Dashboard (period-aware)
# Tabs: Transactions, Active Accounts, Accounts Created, New/Active/Total Accounts,
#       Fees, Supply, Contracts, Contract Deployers

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
    "rolling_bar": "#cbd5e1",  # slate-300
    "tx_bar": "#64748b",       # slate-500 (transactions stacked on top)
    "line_cum": "#0f172a",     # slate-900 (cumulative)
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

def render_sql(sql: str, period_key: str) -> str:
    return sql.replace("{{Period}}", period_key)

# ---- footer shown on all tabs ----
def render_footer():
    st.divider()
    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("### Flow Blockchain Links")
        st.markdown(
            "- Website: https://www.flow.com\n"
            "- Twitter: https://twitter.com/flow_blockchain\n"
            "- Discord: http://chat.onflow.org/\n"
            "- GitHub: https://github.com/onflow\n"
            "- Status Page: https://status.onflow.org/\n"
            "- Flow Blockchain Roadmap: https://flow.com/flow-roadmap\n"
            "- Flow Blockchain Forum: https://forum.flow.com/\n"
            "- Flow Blockchain Primer: https://flow.com/primer\n"
            "- Flow Blockchain Primer: PDF"
        )
    with c2:
        st.markdown("### The State of Flow Blockchain")
        st.markdown(
            "Flow is a layer one blockchain with a next-gen smart contract language (Cadence) and full EVM equivalence "
            "(any Solidity, smart contract, or protocol that works on Ethereum or an EVM L2 works on Flow). The network was "
            "designed to enable consumer crypto applications that scale to mainstream audiences and seamlessly connect to digital ecosystems. "
            "It is based on a unique, multi-role architecture, and designed to scale without sharding, allowing for massive improvements in speed and throughput "
            "while preserving a developer-friendly, ACID-compliant environment.\n\n"
            "Flow is the only blockchain designed from the ground up for consumer applications, empowering developers to build innovative projects "
            "that scale to massive audiences at low cost.\n\n"
            "Flow is architected with a clear vision for the future of consumer crypto, and meticulously designed to address the shortcomings of earlier blockchains "
            "to ensure scalability, usability, and efficiency are not simply an afterthought. By incorporating critical protocol-level primitives, such as account abstraction "
            "and on-chain randomness, as well a powerful transaction model and scalability without sharding, Flow sets itself apart as a blockchain made for those interested "
            "in building apps for widespread adoption and developer-friendly innovation. To date, over 45-million consumer accounts have connected to experiences from the NBA, NFL, "
            "and Disney on Flow.\n\n"
            "Flow is creating an open ecosystem where where software developers, content creators, and consumers alike are appropriately incentivized and rewarded for the value "
            "they contribute to the network\n\n"
            "Flow encourages developers to innovate and build without facing the challenges of high fees and network congestion experienced on other blockchains. Developers on Flow "
            "enjoy both a great user experience and access to liquidity and infrastructure. The Flow ecosystem includes transformative features like user-friendly wallets, "
            "wallet-less onboarding, and sponsored transactions — facilitating a smoother user experience and encouraging broader adoption.\n\n"
            "For more information on the Flow blockchain check out their Primer.\n\n"
            "This dashboard will explore Flow blockchains's recent Crescendo upgrade introducing EVM equivalence, along with Cadence. Below is a list of metrics featured in this dashboard:\n\n"
            "• Transactions including EVM and Cadence\n"
            "• Active wallets including EVM and Cadence\n"
            "• Accounts created over-time\n"
            "• Fees over-time\n"
            "• Token supply including staked, locked, circulating, liquid and total\n"
            "• Contracts deployed and deployers\n"
        )

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
    current_period_transactions as transactions
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

    st.markdown("### Transactions Over Time — EVM & Cadence (stacked bars + cumulative)")

    # ---- Over-time (STACKED bars: Rolling bottom + Transactions on top) + cumulative line ----
    ts = q(render_sql(SQL_TX_OVER_TIME, period))
    if ts is None or ts.empty:
        st.info("No time series for the selected period.")
    else:
        ts.columns = [c.upper() for c in ts.columns]
        ts = ts.rename(columns={
            "TIME_STAMP": "TS",
            "TRANSACTIONS": "TX",
            "TOTAL_TRANSACTIONS": "CUM_TX",
            "ROLLING_AVG": "ROLLING",
        })
        ts["TS"] = pd.to_datetime(ts["TS"], errors="coerce")
        to_float(ts, ["TX", "CUM_TX", "ROLLING"])

        # reshape for stacked bars
        stack_df = pd.DataFrame({
            "TS": pd.concat([ts["TS"], ts["TS"]], ignore_index=True),
            "Series": ["Rolling avg"] * len(ts) + ["Transactions"] * len(ts),
            "Value": pd.concat([ts["ROLLING"], ts["TX"]], ignore_index=True),
        })
        # ensure stacking order: bottom = Rolling avg (order 1), top = Transactions (order 2)
        stack_df["series_order"] = np.where(stack_df["Series"].eq("Rolling avg"), 1, 2)

        base = alt.Chart(stack_df).properties(height=320)

        stacked = base.mark_bar().encode(
            x=alt.X("TS:T", title="Time"),
            y=alt.Y("sum(Value):Q", title="Transactions"),
            color=alt.Color(
                "Series:N",
                scale=alt.Scale(
                    domain=["Rolling avg", "Transactions"],
                    range=[COLORS["rolling_bar"], COLORS["tx_bar"]],
                ),
                legend=alt.Legend(orient="top"),
            ),
            order=alt.Order("series_order:Q"),
            tooltip=[
                alt.Tooltip("TS:T", title="Time"),
                alt.Tooltip("Series:N"),
                alt.Tooltip("sum(Value):Q", title="Value", format=",.1f"),
            ],
        )

        # Overlay cumulative line (right axis)
        line_base = alt.Chart(ts).mark_line(color=COLORS["line_cum"], strokeWidth=2).encode(
            x="TS:T",
            y=alt.Y("CUM_TX:Q", axis=alt.Axis(title="Cumulative tx", orient="right")),
            tooltip=[alt.Tooltip("CUM_TX:Q", title="Cumulative", format=",.0f")],
        )

        st.altair_chart(alt.layer(stacked, line_base).resolve_scale(y="independent"),
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
            st.markdown("**Share (latest period)**")
            st.altair_chart(pie, use_container_width=True)

        download_btn(dist, "⬇️ Download distribution", f"tx_distribution_{period}.csv")

    # ---- Methodology (your wording) ----
    st.markdown("### Methodology")
    st.markdown(
        "This query calculates daily transaction counts for two different sources (flow.core.fact_transactions and "
        "flow.core_evm.fact_transactions) and compares these counts across consecutive days. It outputs the total "
        "transaction count per day, the difference from the previous day, the percentage change, and a cumulative transaction total.\n\n"
        "The output provides a detailed view of daily transaction trends, highlighting fluctuations and offering a clear cumulative summary. "
        "It enables analysis of growth patterns and volatility in transactions across days and identifies notable increases or decreases with the percentage difference calculation."
    )

    # footer (links + state) side-by-side
    render_footer()

# ==========================
# Other tabs (scaffold + shared footer)
# ==========================
with tabs[1]:
    st.subheader("Active Accounts")
    st.info("Coming next: period-aware DAU (total / Cadence / EVM), moving averages, and splits.")
    render_footer()

with tabs[2]:
    st.subheader("Accounts Created")
    st.info("Coming next: new accounts over time, moving averages, and change vs prior window.")
    render_footer()

with tabs[3]:
    st.subheader("New / Active / Total Accounts")
    st.info("Coming next: combined view with time alignment and cumulative totals.")
    render_footer()

with tabs[4]:
    st.subheader("Fees")
    st.info("Coming next: average / total fees (FLOW & USD), success share, and volatility bands.")
    render_footer()

with tabs[5]:
    st.subheader("Supply")
    st.info("Coming next: FLOW supply breakdown & deltas.")
    render_footer()

with tabs[6]:
    st.subheader("Contracts")
    st.info("Coming next: deployments, active contracts, and event activity (period-aware).")
    render_footer()

with tabs[7]:
    st.subheader("Contract Deployers")
    st.info("Coming next: top deployers, velocity, and retention by cohort.")
    render_footer()
