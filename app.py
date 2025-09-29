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

def qp(sql: str, period_key: str | None = None, *, default_period: str = "last_3_months") -> pd.DataFrame:
    """
    Query helper that renders {{Period}} and executes the SQL.
    - period_key: optional explicit period (e.g., "last_week").
    - If not provided, uses st.session_state["period_key"] or default_period.
    """
    pk = period_key or st.session_state.get("period_key") or default_period
    sql_to_run = sql.replace("{{Period}}", pk) if "{{Period}}" in sql else sql

    cur = get_conn().cursor()
    try:
        cur.execute(sql_to_run)
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall()
    finally:
        cur.close()

    return pd.DataFrame(rows, columns=cols)

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

# ---------- SQL: Active Accounts ----------
SQL_ACTIVE_ACCOUNTS_TOTAL = """
WITH time_periods AS (
    SELECT CASE '{{Period}}'
        WHEN 'all_time' THEN '2020-01-01'::DATE
        WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
        WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
        WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
        WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
        ELSE CURRENT_DATE - INTERVAL '1 DAY'
    END AS start_date, CURRENT_DATE AS end_date
)
SELECT COUNT(DISTINCT from_address) AS total_active_addresses
FROM flow.core.ez_transaction_actors ft
       LATERAL FLATTEN(INPUT => b.actors) a
CROSS JOIN time_periods tp
WHERE ft.block_timestamp >= tp.start_date
  AND ft.block_timestamp <  tp.end_date
"""

SQL_ACTIVE_ACCOUNTS_EVM = """
WITH time_periods AS (
    SELECT CASE '{{Period}}'
        WHEN 'all_time' THEN '2020-01-01'::DATE
        WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
        WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
        WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
        WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
        ELSE CURRENT_DATE - INTERVAL '1 DAY'
    END AS start_date, CURRENT_DATE AS end_date
)
SELECT COUNT(DISTINCT from_address) AS total_active_addresses
FROM flow.core_evm.fact_transactions ft
CROSS JOIN time_periods tp
WHERE ft.block_timestamp >= tp.start_date
  AND ft.block_timestamp <  tp.end_date
"""

SQL_ACTIVE_ACCOUNTS_TIMESERIES = """
WITH base_data AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', b.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', b.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', b.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', b.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', b.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', b.block_timestamp)
        END AS time_stamp,
        'Cadence' AS type,
        COUNT(DISTINCT CAST(a.value AS VARCHAR)) AS active_users
    FROM flow.core.ez_transaction_actors b,
         LATERAL FLATTEN(INPUT => b.actors) a
    WHERE b.block_timestamp >= CASE '{{Period}}'
        WHEN 'all_time' THEN '2020-01-01'::DATE
        WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
        WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
        WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
        WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
        WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
    END
    GROUP BY 1,2

    UNION ALL

    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp,
        'EVM' AS type,
        COUNT(DISTINCT from_address) AS active_users
    FROM flow.core_evm.fact_transactions
    WHERE block_timestamp >= CASE '{{Period}}'
        WHEN 'all_time' THEN '2020-01-01'::DATE
        WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
        WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
        WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
        WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
        WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
    END
    GROUP BY 1,2
),
aggregated_data AS (
    SELECT time_stamp AS day, type, active_users
    FROM base_data
    WHERE time_stamp < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END

    UNION ALL

    SELECT time_stamp AS day, 'EVM + Cadence' AS type, SUM(active_users) AS active_users
    FROM base_data
    GROUP BY 1,2
),
final_data AS (
  SELECT
    day,
    type,
    active_users,
    CASE
      WHEN type = 'EVM + Cadence' THEN AVG(active_users) OVER (
        PARTITION BY type
        ORDER BY day
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      )
      ELSE NULL
    END AS rolling_avg
  FROM aggregated_data
)
SELECT 
  day,
  type,
  active_users,
  rolling_avg
FROM final_data
ORDER BY day DESC, type
"""

SQL_ACTIVE_ACCOUNTS_WEEK_TABLE = """
WITH weekly_periods AS (
    SELECT 
        CASE WHEN num=0 THEN DATE_TRUNC('week', CURRENT_DATE)
             ELSE dateadd('week', -num, current_date) END AS start_date,
        CASE WHEN num=0 THEN CURRENT_TIMESTAMP
             ELSE dateadd('week', -num+1, current_date) END AS end_date,
        num AS weeks_ago,
        TO_CHAR(CASE WHEN num=0 THEN DATE_TRUNC('week', CURRENT_DATE)
                     ELSE dateadd('week', -num, current_date) END,'YYYY-MM-DD') || ' to ' ||
        TO_CHAR(CASE WHEN num=0 THEN CURRENT_DATE
                     ELSE dateadd('week', -num+1, current_date) END,'YYYY-MM-DD') AS date_range
    FROM (SELECT row_number() over (order by seq4()) - 1 AS num
          FROM table(generator(rowcount => 12)))
),
cadence_accounts AS (
    SELECT wp.weeks_ago, wp.date_range,
           COUNT(DISTINCT first_tx.value) AS new_accounts
    FROM weekly_periods wp
    LEFT JOIN (
        SELECT a.value, MIN(block_timestamp) AS first_tx_time
        FROM flow.core.ez_transaction_actors,
             LATERAL FLATTEN(INPUT => actors) a
        GROUP BY a.value
    ) first_tx
      ON first_tx.first_tx_time >= wp.start_date AND first_tx.first_tx_time < wp.end_date
    GROUP BY wp.weeks_ago, wp.date_range
),
evm_accounts AS (
    SELECT wp.weeks_ago, wp.date_range,
           COUNT(DISTINCT from_address) AS new_accounts
    FROM weekly_periods wp
    LEFT JOIN (
        SELECT from_address, MIN(block_timestamp) AS first_tx_time
        FROM flow.core_evm.fact_transactions
        GROUP BY from_address
    ) t
      ON t.first_tx_time >= wp.start_date AND t.first_tx_time < wp.end_date
    GROUP BY wp.weeks_ago, wp.date_range
),
combined_metrics AS (
    SELECT 
        wp.weeks_ago, wp.date_range,
        c.new_accounts AS cadence_accounts,
        ROUND(((c.new_accounts - LAG(c.new_accounts) OVER (ORDER BY wp.weeks_ago DESC)) /
              NULLIF(LAG(c.new_accounts) OVER (ORDER BY wp.weeks_ago DESC),0) * 100),2) AS cadence_pct_change,
        e.new_accounts AS evm_accounts,
        ROUND(((e.new_accounts - LAG(e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC)) /
              NULLIF(LAG(e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC),0) * 100),2) AS evm_pct_change,
        (c.new_accounts + e.new_accounts) AS total_accounts,
        ROUND((((c.new_accounts + e.new_accounts) -
              LAG(c.new_accounts + e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC)) /
              NULLIF(LAG(c.new_accounts + e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC),0) * 100),2) AS total_pct_change
    FROM weekly_periods wp
    LEFT JOIN cadence_accounts c ON wp.weeks_ago = c.weeks_ago
    LEFT JOIN evm_accounts e     ON wp.weeks_ago = e.weeks_ago
)
SELECT 
    '# transacting wallets on Cadence' AS "New Addresses Created",
    MAX(CASE WHEN weeks_ago=0 THEN cadence_accounts::STRING END) AS "Current Week",
    MAX(CASE WHEN weeks_ago=1 THEN cadence_accounts::STRING END) AS "Last Week",
    MAX(CASE WHEN weeks_ago=2 THEN cadence_accounts::STRING END) AS "2 Weeks Ago",
    MAX(CASE WHEN weeks_ago=3 THEN cadence_accounts::STRING END) AS "3 Weeks Ago",
    MAX(CASE WHEN weeks_ago=4 THEN cadence_accounts::STRING END) AS "4 Weeks Ago",
    MAX(CASE WHEN weeks_ago=5 THEN cadence_accounts::STRING END) AS "5 Weeks Ago",
    MAX(CASE WHEN weeks_ago=6 THEN cadence_accounts::STRING END) AS "6 Weeks Ago",
    MAX(CASE WHEN weeks_ago=7 THEN cadence_accounts::STRING END) AS "7 Weeks Ago",
    MAX(CASE WHEN weeks_ago=8 THEN cadence_accounts::STRING END) AS "8 Weeks Ago",
    MAX(CASE WHEN weeks_ago=9 THEN cadence_accounts::STRING END) AS "9 Weeks Ago",
    MAX(CASE WHEN weeks_ago=10 THEN cadence_accounts::STRING END) AS "10 Weeks Ago",
    MAX(CASE WHEN weeks_ago=11 THEN cadence_accounts::STRING END) AS "11 Weeks Ago"
FROM combined_metrics
UNION ALL
SELECT 'Cadence % Change',
    MAX(CASE WHEN weeks_ago=0 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=1 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=2 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=3 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=4 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=5 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=6 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=7 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=8 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=9 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=10 THEN cadence_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=11 THEN cadence_pct_change::STRING END)
FROM combined_metrics
UNION ALL
SELECT '# transacting wallets on EVM',
    MAX(CASE WHEN weeks_ago=0 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=1 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=2 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=3 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=4 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=5 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=6 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=7 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=8 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=9 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=10 THEN evm_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=11 THEN evm_accounts::STRING END)
FROM combined_metrics
UNION ALL
SELECT 'EVM % Change',
    MAX(CASE WHEN weeks_ago=0 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=1 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=2 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=3 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=4 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=5 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=6 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=7 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=8 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=9 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=10 THEN evm_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=11 THEN evm_pct_change::STRING END)
FROM combined_metrics
UNION ALL
SELECT '# total transacting wallets',
    MAX(CASE WHEN weeks_ago=0 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=1 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=2 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=3 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=4 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=5 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=6 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=7 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=8 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=9 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=10 THEN total_accounts::STRING END),
    MAX(CASE WHEN weeks_ago=11 THEN total_accounts::STRING END)
FROM combined_metrics
UNION ALL
SELECT 'Total % Change',
    MAX(CASE WHEN weeks_ago=0 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=1 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=2 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=3 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=4 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=5 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=6 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=7 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=8 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=9 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=10 THEN total_pct_change::STRING END),
    MAX(CASE WHEN weeks_ago=11 THEN total_pct_change::STRING END)
FROM combined_metrics
"""

# ---------- SQL: Accounts Created ----------
SQL_ACCOUNTS_CREATED_NUMBER = """
WITH base_data AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp,
        actor_address
    FROM (
        SELECT b.block_timestamp, CAST(a.value AS VARCHAR) AS actor_address
        FROM flow.core.ez_transaction_actors b,
             LATERAL FLATTEN(INPUT => b.actors) a
        WHERE b.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
        UNION ALL
        SELECT block_timestamp, from_address AS actor_address
        FROM flow.core_evm.fact_transactions
        WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    )
    WHERE time_stamp < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
),
new_accounts AS (
    SELECT COUNT(DISTINCT actor_address) AS daily_new_accounts
    FROM (
        SELECT time_stamp, actor_address,
               ROW_NUMBER() OVER (PARTITION BY actor_address ORDER BY time_stamp) AS rn
        FROM base_data
    )
    WHERE rn = 1
)
SELECT daily_new_accounts AS total_new_accounts_last_24h
FROM new_accounts
"""

SQL_ACCOUNTS_CREATED_TIMESERIES = """
WITH base_data AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp,
        actor_address
    FROM (
        SELECT b.block_timestamp, CAST(a.value AS VARCHAR) AS actor_address
        FROM flow.core.ez_transaction_actors b,
             LATERAL FLATTEN(INPUT => b.actors) a
        WHERE b.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
        UNION ALL
        SELECT block_timestamp, from_address AS actor_address
        FROM flow.core_evm.fact_transactions
        WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    )
    WHERE time_stamp < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
),
new_accounts AS (
    SELECT time_stamp AS day,
           COUNT(DISTINCT actor_address) AS daily_new_accounts
    FROM (
        SELECT time_stamp, actor_address,
               ROW_NUMBER() OVER (PARTITION BY actor_address ORDER BY time_stamp) AS rn
        FROM base_data
    )
    WHERE rn = 1
    GROUP BY time_stamp
)
SELECT
    day,
    daily_new_accounts,
    SUM(daily_new_accounts) OVER (ORDER BY day) AS total_new_accounts,
    AVG(daily_new_accounts) OVER (ORDER BY day ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling_avg_new_accounts
FROM new_accounts
ORDER BY day DESC
"""

# ---------- SQL: New / Active / Total Accounts ----------
SQL_NAT_ACTIVE_NUMBER = SQL_ACTIVE_ACCOUNTS_TOTAL  # reuse

SQL_NAT_NEW_NUMBER = SQL_ACCOUNTS_CREATED_NUMBER   # reuse

SQL_NAT_TOTAL_NUMBER = """
WITH time_periods AS (
    SELECT '2020-01-01'::DATE AS start_date
),
new_accounts_last_24h AS (
    SELECT COUNT(DISTINCT CAST(a.value AS VARCHAR)) AS new_accounts_24h
    FROM flow.core.ez_transaction_actors b,
         LATERAL FLATTEN(INPUT => b.actors) a, time_periods tp
    WHERE b.block_timestamp >= tp.start_date
      AND b.block_timestamp <= CURRENT_DATE

    UNION ALL

    SELECT COUNT(DISTINCT from_address) AS new_accounts_24h
    FROM flow.core_evm.fact_transactions ft
    CROSS JOIN time_periods tp
    WHERE ft.block_timestamp >= tp.start_date
      AND ft.block_timestamp <= CURRENT_DATE
),
new_num AS (
    SELECT SUM(new_accounts_24h) AS total_new_accounts_last_24h
    FROM new_accounts_last_24h
)
SELECT total_new_accounts_last_24h
FROM new_num
"""

SQL_NAT_TIMESERIES = """
WITH base_data AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', b.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', b.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', b.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', b.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', b.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', b.block_timestamp)
        END AS time_stamp,
        'Cadence' AS type,
        COUNT(DISTINCT CAST(a.value AS VARCHAR)) AS active_users
    FROM flow.core.ez_transaction_actors b,
         LATERAL FLATTEN(INPUT => b.actors) a
    WHERE b.block_timestamp >= CASE '{{Period}}'
        WHEN 'all_time' THEN '2020-01-01'::DATE
        WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
        WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
        WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
        WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
        WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
    END
    GROUP BY 1,2

    UNION ALL

    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp,
        'EVM' AS type,
        COUNT(DISTINCT from_address) AS active_users
    FROM flow.core_evm.fact_transactions
    WHERE block_timestamp >= CASE '{{Period}}'
        WHEN 'all_time' THEN '2020-01-01'::DATE
        WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
        WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
        WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
        WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
        WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
    END
    GROUP BY 1,2
),
aggregated_data AS (
    SELECT time_stamp AS day, SUM(active_users) AS active_users
    FROM base_data
    GROUP BY 1
),
final_data AS (
    SELECT day, active_users,
           AVG(active_users) OVER (ORDER BY day ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling_avg_active_users
    FROM aggregated_data
),
base_data2 AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp,
        actor_address
    FROM (
        SELECT b.block_timestamp, CAST(a.value AS VARCHAR) AS actor_address
        FROM flow.core.ez_transaction_actors b,
             LATERAL FLATTEN(INPUT => b.actors) a
        WHERE b.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END

        UNION ALL

        SELECT block_timestamp, from_address AS actor_address
        FROM flow.core_evm.fact_transactions
        WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    )
    WHERE time_stamp < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
),
new_accounts AS (
    SELECT time_stamp AS day,
           COUNT(DISTINCT actor_address) AS daily_new_accounts
    FROM (
        SELECT time_stamp, actor_address,
               ROW_NUMBER() OVER (PARTITION BY actor_address ORDER BY time_stamp) AS rn
        FROM base_data2
    )
    WHERE rn = 1
    GROUP BY time_stamp
),
final_data2 AS (
    SELECT
        day,
        daily_new_accounts,
        SUM(daily_new_accounts) OVER (ORDER BY day) AS total_new_accounts,
        AVG(daily_new_accounts) OVER (ORDER BY day ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling_avg_new_accounts
    FROM new_accounts
    ORDER BY day DESC
)
SELECT 
    IFNULL(x.day,y.day) AS date,
    x.active_users,
    x.rolling_avg_active_users,
    y.daily_new_accounts AS new_accounts,
    y.rolling_avg_new_accounts,
    y.total_new_accounts
FROM final_data x
JOIN final_data2 y ON x.day = y.day
ORDER BY date DESC
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
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate

    c1, c2 = st.columns(2)
    try:
        n_total = qp(SQL_ACTIVE_ACCOUNTS_TOTAL)
        c1.metric("Active Accounts — EVM + Cadence", f"{int(n_total.iloc[0,0]):,}")
    except Exception as e:
        c1.info("No data.")

    try:
        n_evm = qp(SQL_ACTIVE_ACCOUNTS_EVM)
        c2.metric("Active Accounts — EVM", f"{int(n_evm.iloc[0,0]):,}")
    except Exception:
        c2.info("No data.")

    # Timeseries: bars (total) + lines (Cadence/EVM) + rolling avg
    import altair as alt
    try:
        df = qp(SQL_ACTIVE_ACCOUNTS_TIMESERIES).copy()
        df["day"] = pd.to_datetime(df["DAY"])
        df["active_users"] = pd.to_numeric(df["ACTIVE_USERS"])
        df["rolling_avg"] = pd.to_numeric(df["ROLLING_AVG"])

        total_df   = df[df["TYPE"]=="EVM + Cadence"]
        cadence_df = df[df["TYPE"]=="Cadence"]
        evm_df     = df[df["TYPE"]=="EVM"]

        bars = alt.Chart(total_df).mark_bar().encode(
            x=alt.X("day:T", title="Date"),
            y=alt.Y("active_users:Q", title="Active Accounts"),
            tooltip=["day:T","active_users:Q"]
        ).properties(height=320)

        line_cadence = alt.Chart(cadence_df).mark_line(point=False, strokeWidth=2, color=CADENCE_COLOR).encode(
            x="day:T", y="active_users:Q", tooltip=["day:T","active_users:Q"]
        )

        line_evm = alt.Chart(evm_df).mark_line(point=False, strokeWidth=2, color=EVM_COLOR).encode(
            x="day:T", y="active_users:Q", tooltip=["day:T","active_users:Q"]
        )

        roll = alt.Chart(total_df.dropna(subset=["rolling_avg"])).mark_line(strokeDash=[6,4], color=NEUTRAL_LINE, strokeWidth=2).encode(
            x="day:T", y=alt.Y("rolling_avg:Q", title="Active Accounts (roll avg)"),
            tooltip=["day:T","rolling_avg:Q"]
        )

        st.altair_chart(bars + line_cadence + line_evm + roll, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not render active accounts timeseries: {e}")

    st.markdown("#### Table of New Addresses Created over the past 12 weeks")
    try:
        w = q(SQL_ACTIVE_ACCOUNTS_WEEK_TABLE)
        st.dataframe(w, use_container_width=True, hide_index=True)
    except Exception:
        st.info("No weekly table data.")

    with st.expander("Methodology"):
        st.markdown("""
This query calculates the daily count of active users by day from two sources: **flow.core.ez_transaction_actors** and **flow.core_evm.fact_transactions**. By aggregating unique active users across both sources, it provides a consolidated view of user activity each day.

- **Daily Active Users**: Counts distinct users (actors and EVM addresses) per day.
- **Aggregated Active User Totals**: Sum across sources for total daily activity.

This offers a straightforward way to monitor user engagement across **Flow Cadence** and **Flow EVM**.
        """)
    render_footer()

with tabs[2]:
    st.subheader("Accounts Created")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    # KPI
    try:
        k = qp(SQL_ACCOUNTS_CREATED_NUMBER)
        st.metric("Accounts Created — EVM + Cadence", f"{int(k.iloc[0,0]):,}")
    except Exception:
        st.info("No data.")

    # Timeseries
    try:
        df = qp(SQL_ACCOUNTS_CREATED_TIMESERIES).copy()
        df["DAY"] = pd.to_datetime(df["DAY"])
        df["DAILY_NEW_ACCOUNTS"] = pd.to_numeric(df["DAILY_NEW_ACCOUNTS"])
        df["TOTAL_NEW_ACCOUNTS"] = pd.to_numeric(df["TOTAL_NEW_ACCOUNTS"])
        df["ROLLING_AVG_NEW_ACCOUNTS"] = pd.to_numeric(df["ROLLING_AVG_NEW_ACCOUNTS"])

        import altair as alt
        bars = alt.Chart(df).mark_bar(color=TOTAL_COLOR).encode(
            x=alt.X("DAY:T", title="Date"),
            y=alt.Y("DAILY_NEW_ACCOUNTS:Q", title="Daily New Accounts"),
            tooltip=["DAY:T","DAILY_NEW_ACCOUNTS:Q"]
        ).properties(height=320)

        rolling = alt.Chart(df.dropna(subset=["ROLLING_AVG_NEW_ACCOUNTS"])).mark_line(
            strokeDash=[6,4], strokeWidth=2, color=NEUTRAL_LINE
        ).encode(
            x="DAY:T", y="ROLLING_AVG_NEW_ACCOUNTS:Q", tooltip=["DAY:T","ROLLING_AVG_NEW_ACCOUNTS:Q"]
        )

        cum = alt.Chart(df).mark_line(strokeWidth=2, color=CADENCE_COLOR).encode(
            x="DAY:T", y=alt.Y("TOTAL_NEW_ACCOUNTS:Q", title="Cumulative New Accounts"),
            tooltip=["DAY:T","TOTAL_NEW_ACCOUNTS:Q"]
        )

        st.altair_chart(bars + rolling + cum, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not render Accounts Created chart: {e}")

    with st.expander("Methodology"):
        st.markdown("""
This query tracks daily **new accounts** across both **Cadence** and **EVM**, counting only the **first appearance** of each unique account/address.  

- **Daily New Accounts**: first-time actors/addresses per day  
- **Cumulative Account Growth**: running total over time  

This highlights daily and cumulative growth trends across **Flow Cadence** and **Flow EVM**.
        """)
    render_footer()

with tabs[3]:
    st.subheader("New / Active / Total Accounts")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    c1, c2, c3 = st.columns(3)
    try:
        n_active = qp(SQL_NAT_ACTIVE_NUMBER)
        c1.metric("Active Accounts — EVM + Cadence", f"{int(n_active.iloc[0,0]):,}")
    except Exception: c1.info("—")

    try:
        n_new = qp(SQL_NAT_NEW_NUMBER)
        c2.metric("Accounts Created — EVM + Cadence", f"{int(n_new.iloc[0,0]):,}")
    except Exception: c2.info("—")

    try:
        n_total = q(SQL_NAT_TOTAL_NUMBER)  # no {{Period}} here
        c3.metric("Total Accounts — EVM + Cadence (all-time)", f"{int(n_total.iloc[0,0]):,}")
    except Exception: c3.info("—")

    # Combined chart: New vs Active vs Total (bars + lines)
    try:
        df = qp(SQL_NAT_TIMESERIES).copy()
        df["DATE"] = pd.to_datetime(df["DATE"])
        for col in ["ACTIVE_USERS","ROLLING_AVG_ACTIVE_USERS","NEW_ACCOUNTS","ROLLING_AVG_NEW_ACCOUNTS","TOTAL_NEW_ACCOUNTS"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        import altair as alt
        bars_new = alt.Chart(df).mark_bar(color=EVM_COLOR, opacity=0.8).encode(
            x=alt.X("DATE:T", title="Date"),
            y=alt.Y("NEW_ACCOUNTS:Q", title="New / Active / Total Accounts"),
            tooltip=["DATE:T","NEW_ACCOUNTS:Q"]
        ).properties(height=340)

        line_active = alt.Chart(df).mark_line(color=CADENCE_COLOR, strokeWidth=2).encode(
            x="DATE:T", y="ACTIVE_USERS:Q", tooltip=["DATE:T","ACTIVE_USERS:Q"]
        )

        line_total = alt.Chart(df).mark_line(color=TOTAL_COLOR, strokeWidth=2).encode(
            x="DATE:T", y="TOTAL_NEW_ACCOUNTS:Q", tooltip=["DATE:T","TOTAL_NEW_ACCOUNTS:Q"]
        )

        roll_new = alt.Chart(df.dropna(subset=["ROLLING_AVG_NEW_ACCOUNTS"])).mark_line(
            strokeDash=[6,4], color=NEUTRAL_LINE, strokeWidth=2
        ).encode(x="DATE:T", y="ROLLING_AVG_NEW_ACCOUNTS:Q")

        st.altair_chart(bars_new + line_active + line_total + roll_new, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not render combined chart: {e}")

    with st.expander("Methodology"):
        st.markdown("""
This combines **new** and **active** account views across **Cadence** and **EVM**.

**Daily New Accounts**  
First appearance of each unique account or address across both environments.  
- *daily_new_accounts*: first-time users per day  
- *total_new_accounts*: cumulative new users over time

**Daily Active Accounts**  
Unique accounts transacting on a given day (regardless of first seen date).  
- *daily_active_accounts*: engagement per day

Together, these show acquisition (new), engagement (active), and long-term adoption (total).
        """)
    render_footer()

with tabs[4]:
    st.subheader("Fees")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    st.info("Coming next: average / total fees (FLOW & USD), success share, and volatility bands.")
    render_footer()

with tabs[5]:
    st.subheader("Supply")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    st.info("Coming next: FLOW supply breakdown & deltas.")
    render_footer()

with tabs[6]:
    st.subheader("Contracts")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    st.info("Coming next: deployments, active contracts, and event activity (period-aware).")
    render_footer()

with tabs[7]:
    st.subheader("Contract Deployers")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    st.info("Coming next: top deployers, velocity, and retention by cohort.")
    render_footer()
