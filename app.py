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

# Nice number formatters (use yours if you already have them)
def _fmt_int(x):
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "—"

def _fmt_float(x, digits=4):
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return "—"

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
SELECT COUNT(DISTINCT actors[0]) AS total_active_addresses
FROM time_periods tp
cross join flow.core.ez_transaction_actors ft,
    LATERAL FLATTEN(INPUT => ft.actors) a
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
FROM final_data where day<trunc(current_date,'week')
ORDER BY day DESC, type
"""

SQL_ACTIVE_ACCOUNTS_WEEK_TABLE = """
WITH seq AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS num
  FROM TABLE(GENERATOR(ROWCOUNT => 12))
),

-- Monday of *current* week at 00:00, independent of session WEEK_START
base AS (
  SELECT DATEADD('day', 1 - DAYOFWEEKISO(CURRENT_DATE), CURRENT_DATE) AS this_monday
),

weekly_periods AS (
  -- num=0 => last full week: [prev Monday, this Monday)
  SELECT
    DATEADD('week', - (num + 1), b.this_monday) AS start_date,
    DATEADD('week', -  num      , b.this_monday) AS end_date,
    num AS weeks_ago,
    TO_CHAR(DATEADD('week', - (num + 1), b.this_monday), 'YYYY-MM-DD')
      || ' to '
      || TO_CHAR(DATEADD('day', -1, DATEADD('week', -num, b.this_monday)), 'YYYY-MM-DD')
      AS date_range
  FROM seq s
  CROSS JOIN base b
),

cadence_accounts AS (
  SELECT
    wp.weeks_ago,
    wp.date_range,
    COUNT(DISTINCT CAST(first_tx.value AS VARCHAR)) AS new_accounts
  FROM weekly_periods wp
  LEFT JOIN (
    SELECT a.value, MIN(block_timestamp) AS first_tx_time
    FROM flow.core.ez_transaction_actors t,
         LATERAL FLATTEN(INPUT => t.actors) a
    GROUP BY a.value
  ) first_tx
    ON first_tx.first_tx_time >= wp.start_date
   AND first_tx.first_tx_time <  wp.end_date
  GROUP BY wp.weeks_ago, wp.date_range
),

evm_accounts AS (
  SELECT
    wp.weeks_ago,
    wp.date_range,
    COUNT(DISTINCT t.from_address) AS new_accounts
  FROM weekly_periods wp
  LEFT JOIN (
    SELECT from_address, MIN(block_timestamp) AS first_tx_time
    FROM flow.core_evm.fact_transactions
    GROUP BY from_address
  ) t
    ON t.first_tx_time >= wp.start_date
   AND t.first_tx_time <  wp.end_date
  GROUP BY wp.weeks_ago, wp.date_range
),

combined_metrics AS (
  SELECT
    wp.weeks_ago,
    wp.date_range,
    c.new_accounts AS cadence_accounts,
    ROUND(
      (
        c.new_accounts
        - LAG(c.new_accounts) OVER (ORDER BY wp.weeks_ago DESC)
      )
      / NULLIF(LAG(c.new_accounts) OVER (ORDER BY wp.weeks_ago DESC), 0) * 100
    , 2) AS cadence_pct_change,
    e.new_accounts AS evm_accounts,
    ROUND(
      (
        e.new_accounts
        - LAG(e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC)
      )
      / NULLIF(LAG(e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC), 0) * 100
    , 2) AS evm_pct_change,
    (c.new_accounts + e.new_accounts) AS total_accounts,
    ROUND(
      (
        (c.new_accounts + e.new_accounts)
        - LAG(c.new_accounts + e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC)
      )
      / NULLIF(LAG(c.new_accounts + e.new_accounts) OVER (ORDER BY wp.weeks_ago DESC), 0) * 100
    , 2) AS total_pct_change
  FROM weekly_periods wp
  LEFT JOIN cadence_accounts c ON c.weeks_ago = wp.weeks_ago
  LEFT JOIN evm_accounts e     ON e.weeks_ago = wp.weeks_ago
)

SELECT 
  '# transacting wallets on Cadence' AS "New Addresses Created",
  MAX(CASE WHEN weeks_ago = 0 THEN cadence_accounts::STRING END) AS "Last Full Week",
  MAX(CASE WHEN weeks_ago = 1 THEN cadence_accounts::STRING END) AS "1 Week Ago",
  MAX(CASE WHEN weeks_ago = 2 THEN cadence_accounts::STRING END) AS "2 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 3 THEN cadence_accounts::STRING END) AS "3 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 4 THEN cadence_accounts::STRING END) AS "4 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 5 THEN cadence_accounts::STRING END) AS "5 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 6 THEN cadence_accounts::STRING END) AS "6 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 7 THEN cadence_accounts::STRING END) AS "7 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 8 THEN cadence_accounts::STRING END) AS "8 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 9 THEN cadence_accounts::STRING END) AS "9 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 10 THEN cadence_accounts::STRING END) AS "10 Weeks Ago",
  MAX(CASE WHEN weeks_ago = 11 THEN cadence_accounts::STRING END) AS "11 Weeks Ago"
FROM combined_metrics

UNION ALL
SELECT 'Cadence % Change',
  MAX(CASE WHEN weeks_ago = 0 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 1 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 2 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 3 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 4 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 5 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 6 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 7 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 8 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 9 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 10 THEN cadence_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 11 THEN cadence_pct_change::STRING END)
FROM combined_metrics

UNION ALL
SELECT '# transacting wallets on EVM',
  MAX(CASE WHEN weeks_ago = 0 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 1 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 2 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 3 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 4 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 5 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 6 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 7 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 8 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 9 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 10 THEN evm_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 11 THEN evm_accounts::STRING END)
FROM combined_metrics

UNION ALL
SELECT 'EVM % Change',
  MAX(CASE WHEN weeks_ago = 0 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 1 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 2 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 3 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 4 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 5 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 6 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 7 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 8 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 9 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 10 THEN evm_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 11 THEN evm_pct_change::STRING END)
FROM combined_metrics

UNION ALL
SELECT '# total transacting wallets',
  MAX(CASE WHEN weeks_ago = 0 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 1 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 2 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 3 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 4 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 5 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 6 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 7 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 8 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 9 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 10 THEN total_accounts::STRING END),
  MAX(CASE WHEN weeks_ago = 11 THEN total_accounts::STRING END)
FROM combined_metrics

UNION ALL
SELECT 'Total % Change',
  MAX(CASE WHEN weeks_ago = 0 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 1 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 2 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 3 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 4 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 5 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 6 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 7 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 8 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 9 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 10 THEN total_pct_change::STRING END),
  MAX(CASE WHEN weeks_ago = 11 THEN total_pct_change::STRING END)
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

# ---------- FEES ----------
SQL_FEES_SUMMARY = """
WITH time_periods AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            ELSE CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date
),
data AS (
    -- EVM fees
    SELECT 
        SUM(tx_fee) AS fees,
        AVG(tx_fee) AS avg_tx_fee
    FROM flow.core_evm.fact_transactions
    CROSS JOIN time_periods tp
    WHERE block_timestamp >= tp.start_date
    UNION ALL
    -- Cadence fees (FeesDeducted)
    SELECT 
        SUM(y.event_data:amount) AS fees,
        AVG(y.event_data:amount) AS avg_tx_fee
    FROM flow.core.fact_transactions x
    JOIN flow.core.fact_events y ON x.tx_id = y.tx_id
    CROSS JOIN time_periods tp
    WHERE x.block_timestamp >= tp.start_date
      AND y.event_contract = 'A.f919ee77447b7497.FlowFees'
      AND y.event_type = 'FeesDeducted'
)
SELECT 
    SUM(fees) AS total_fees_flow, 
    AVG(avg_tx_fee) AS avg_tx_fee_flow
FROM data
"""

SQL_FEES_TIMESERIES = """
WITH data AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END as date,
        SUM(tx_fee) AS fees,
        AVG(tx_fee) AS avg_tx_fee
    FROM flow.core_evm.fact_transactions
    WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
      AND block_timestamp < CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END
    GROUP BY 1

    UNION

    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', y.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', y.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', y.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', y.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', y.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', y.block_timestamp)
        END as date,
        SUM(y.event_data:amount) AS fees,
        AVG(y.event_data:amount) AS avg_tx_fee
    FROM flow.core.fact_transactions x
    JOIN flow.core.fact_events y ON x.tx_id = y.tx_id
    WHERE y.event_contract = 'A.f919ee77447b7497.FlowFees'
      AND y.event_type = 'FeesDeducted'
      AND y.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
      AND y.block_timestamp < CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END
    GROUP BY 1
)
SELECT 
    date, 
    SUM(fees) AS flow_fees, 
    AVG(avg_tx_fee) AS avg_tx_flow_fee, 
    SUM(SUM(fees)) OVER (ORDER BY date) AS total_flow_fees,
    AVG(SUM(fees)) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_28d_flow_fees
FROM data
GROUP BY date
ORDER BY date DESC
"""

# ---------- SUPPLY ----------
SQL_SUPPLY_LATEST = """
WITH api_data AS (
    SELECT livequery.live.udf_api(
        'https://api.coingecko.com/api/v3/coins/flow/market_chart?vs_currency=usd&days=360&interval=daily'
    ) AS resp
),
prices AS (
    SELECT dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date, value[1] AS price
    FROM api_data, LATERAL FLATTEN(input => resp:data:prices)
),
market_caps AS (
    SELECT dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date, value[1] AS market_cap
    FROM api_data, LATERAL FLATTEN(input => resp:data:market_caps)
),
total_volumes AS (
    SELECT dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date, value[1] AS volume
    FROM api_data, LATERAL FLATTEN(input => resp:data:total_volumes)
),
parsed_data AS (
    SELECT p.date, 'FLOW' AS token, mc.market_cap, p.price, tv.volume
    FROM prices p
    JOIN market_caps mc ON p.date = mc.date
    JOIN total_volumes tv ON p.date = tv.date
    WHERE p.date < CURRENT_DATE
),
total_supply_data AS (
    SELECT date, market_cap / price AS total_supply FROM parsed_data
),
flow_tvl_data AS (
    SELECT date, category, SUM(chain_tvl) AS staked_locked
    FROM external.defillama.fact_protocol_tvl
    WHERE chain ILIKE '%Flow%'
    GROUP BY date, category
),
grouped_tvl_data AS (
    SELECT
        date,
        SUM(CASE WHEN category = 'Staking Pool' THEN staked_locked ELSE 0 END) AS staked_locked,
        SUM(CASE WHEN category = 'Liquid Staking' THEN staked_locked ELSE 0 END) AS liquid_staking,
        SUM(CASE WHEN category IN ('Lending','Services','Derivatives','Launchpad') THEN staked_locked ELSE 0 END) AS non_staked_locked
    FROM flow_tvl_data
    GROUP BY date
),
staking_data AS (
    WITH staking AS (
        SELECT trunc(block_timestamp,'day') AS date, SUM(amount) AS staked_volume
        FROM flow.gov.ez_staking_actions
        WHERE action IN ('DelegatorTokensCommitted','TokensCommitted')
        GROUP BY 1
    ),
    unstaking AS (
        SELECT trunc(block_timestamp,'day') AS date, SUM(amount) AS unstaked_volume
        FROM flow.gov.ez_staking_actions
        WHERE action IN ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
        GROUP BY 1
    )
    SELECT s.date,
           COALESCE(s.staked_volume,0) - COALESCE(u.unstaked_volume,0) AS net_staked_volume,
           SUM(COALESCE(s.staked_volume,0) - COALESCE(u.unstaked_volume,0)) OVER (ORDER BY s.date) + 1.6e8 AS total_staked_volume
    FROM staking s
    LEFT JOIN unstaking u ON s.date = u.date
),
combined_tvl_staking_data AS (
    SELECT gt.date,
           gt.staked_locked + sd.total_staked_volume AS total_staked_locked,
           gt.liquid_staking,
           gt.non_staked_locked
    FROM grouped_tvl_data gt
    JOIN staking_data sd ON gt.date = sd.date
),
supply_breakdown AS (
    SELECT
        p.date,
        'FLOW' AS token,
        MAX(p.market_cap) AS market_cap,
        MAX(p.price) AS price,
        MAX(p.volume) AS volume,
        MAX(ts.total_supply) AS total_supply,
        MAX(ct.total_staked_locked) AS staked_locked,
        MAX(ct.non_staked_locked) AS non_staked_locked,
        MAX(ct.liquid_staking) AS liquid_supply,
        MAX(ts.total_supply) - (MAX(ct.total_staked_locked) + MAX(ct.liquid_staking) + MAX(ct.non_staked_locked)) AS staked_circulating
    FROM parsed_data p
    JOIN total_supply_data ts ON p.date = ts.date
    JOIN combined_tvl_staking_data ct ON p.date = ct.date
    GROUP BY p.date
)
SELECT
    date,
    token,
    total_supply AS total_supply_actual,
    staked_locked,
    non_staked_locked,
    total_supply - (staked_locked + liquid_supply + non_staked_locked) AS unstaked_circulating,
    liquid_supply,
    total_supply_actual - unstaked_circulating AS free_circulating_supply
FROM supply_breakdown
WHERE date < trunc(current_date,'week')
ORDER BY date DESC
LIMIT 1
"""

SQL_SUPPLY_SERIES = """
WITH api_data AS (
    SELECT livequery.live.udf_api(
        'https://api.coingecko.com/api/v3/coins/flow/market_chart?vs_currency=usd&days=360&interval=daily'
    ) AS resp
),
prices AS (
    SELECT dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date, value[1] AS price
    FROM api_data, LATERAL FLATTEN(input => resp:data:prices)
),
market_caps AS (
    SELECT dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date, value[1] AS market_cap
    FROM api_data, LATERAL FLATTEN(input => resp:data:market_caps)
),
total_volumes AS (
    SELECT dateadd(ms, value[0], to_timestamp('1970-01-01')) AS date, value[1] AS volume
    FROM api_data, LATERAL FLATTEN(input => resp:data:total_volumes)
),
parsed_data AS (
    SELECT p.date, 'FLOW' AS token, mc.market_cap, p.price, tv.volume
    FROM prices p
    JOIN market_caps mc ON p.date = mc.date
    JOIN total_volumes tv ON p.date = tv.date
    WHERE p.date < CURRENT_DATE
),
total_supply_data AS (
    SELECT date, market_cap / price AS total_supply FROM parsed_data
),
flow_tvl_data AS (
    SELECT date, category, SUM(chain_tvl) AS staked_locked
    FROM external.defillama.fact_protocol_tvl
    WHERE chain ILIKE '%Flow%'
    GROUP BY date, category
),
grouped_tvl_data AS (
    SELECT
        date,
        SUM(CASE WHEN category = 'Staking Pool' THEN staked_locked ELSE 0 END) AS staked_locked,
        SUM(CASE WHEN category = 'Liquid Staking' THEN staked_locked ELSE 0 END) AS liquid_staking,
        SUM(CASE WHEN category IN ('Lending','Services','Derivatives','Launchpad') THEN staked_locked ELSE 0 END) AS non_staked_locked
    FROM flow_tvl_data
    GROUP BY date
),
staking_data AS (
    WITH staking AS (
        SELECT trunc(block_timestamp,'day') AS date, SUM(amount) AS staked_volume
        FROM flow.gov.ez_staking_actions
        WHERE action IN ('DelegatorTokensCommitted','TokensCommitted')
        GROUP BY 1
    ),
    unstaking AS (
        SELECT trunc(block_timestamp,'day') AS date, SUM(amount) AS unstaked_volume
        FROM flow.gov.ez_staking_actions
        WHERE action IN ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
        GROUP BY 1
    )
    SELECT s.date,
           COALESCE(s.staked_volume,0) - COALESCE(u.unstaked_volume,0) AS net_staked_volume,
           SUM(COALESCE(s.staked_volume,0) - COALESCE(u.unstaked_volume,0)) OVER (ORDER BY s.date) + 1.6e8 AS total_staked_volume
    FROM staking s
    LEFT JOIN unstaking u ON s.date = u.date
),
combined_tvl_staking_data AS (
    SELECT gt.date,
           gt.staked_locked + sd.total_staked_volume AS total_staked_locked,
           gt.liquid_staking,
           gt.non_staked_locked
    FROM grouped_tvl_data gt
    JOIN staking_data sd ON gt.date = sd.date
),
supply_breakdown AS (
    SELECT
        p.date,
        'FLOW' AS token,
        MAX(p.market_cap) AS market_cap,
        MAX(p.price) AS price,
        MAX(p.volume) AS volume,
        MAX(ts.total_supply) AS total_supply,
        MAX(ct.total_staked_locked) AS staked_locked,
        MAX(ct.non_staked_locked) AS non_staked_locked,
        MAX(ct.liquid_staking) AS liquid_supply,
        MAX(ts.total_supply) - (MAX(ct.total_staked_locked) + MAX(ct.liquid_staking) + MAX(ct.non_staked_locked)) AS unstaked_circulating
    FROM parsed_data p
    JOIN total_supply_data ts ON p.date = ts.date
    JOIN combined_tvl_staking_data ct ON p.date = ct.date
    GROUP BY p.date
)
SELECT
    date,
    token,
    total_supply AS total_supply_actual,
    staked_locked,
    non_staked_locked,
    total_supply - (staked_locked + liquid_supply + non_staked_locked) AS unstaked_circulating,
    liquid_supply,
    total_supply_actual - unstaked_circulating AS free_circulating_supply
FROM supply_breakdown
ORDER BY date DESC
"""

# =========================
# CONTRACTS — SQL
# =========================

SQL_CONTRACTS_KPIS = """
WITH time_periods AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            ELSE CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date
),
cadence AS (
  SELECT DISTINCT event_contract AS contract, MIN(block_timestamp) AS debut
  FROM flow.core.fact_events
  JOIN time_periods tp
    ON block_timestamp >= tp.start_date
  GROUP BY 1
),
core_new_contracts AS (
  SELECT COUNT(DISTINCT contract) AS total_new_cadence_contracts FROM cadence
),
evms AS (
  SELECT x.block_timestamp, x.from_address AS creator, y.contract_address AS contract
  FROM flow.core_evm.fact_transactions x
  JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash
  JOIN time_periods tp ON x.block_timestamp >= tp.start_date
  WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
  UNION
  SELECT x.block_timestamp, x.from_address AS creator, x.tx_hash AS contract
  FROM flow.core_evm.fact_transactions x
  JOIN time_periods tp ON x.block_timestamp >= tp.start_date
  WHERE x.origin_function_signature IN ('0x60c06040','0x60806040')
    AND x.tx_hash NOT IN (
      SELECT x.tx_hash
      FROM flow.core_evm.fact_transactions x
      JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash
      WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
    )
),
evm_new_contracts AS (
  SELECT COUNT(DISTINCT contract) AS total_new_evm_contracts FROM evms
)
SELECT
  (SELECT total_new_cadence_contracts FROM core_new_contracts) AS total_new_cadence_contracts,
  (SELECT total_new_evm_contracts     FROM evm_new_contracts) AS total_new_evm_contracts
"""

SQL_CONTRACTS_TIMESERIES = """
WITH 
cadence AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp,
        COUNT(DISTINCT event_contract) AS total_new_cadence_contracts
    FROM flow.core.fact_events
    WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1
),
evms AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END AS time_stamp,
        COUNT(DISTINCT CASE 
            WHEN y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
                 THEN y.contract_address 
            ELSE x.tx_hash 
        END) AS total_new_evm_contracts
    FROM flow.core_evm.fact_transactions x
    LEFT JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash 
    WHERE (y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        OR x.origin_function_signature IN ('0x60c06040','0x60806040'))
      AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    GROUP BY 1
)
SELECT
    COALESCE(c.time_stamp, e.time_stamp) AS day,
    COALESCE(c.total_new_cadence_contracts, 0) AS new_cadence_contracts,
    SUM(COALESCE(c.total_new_cadence_contracts, 0)) OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp)) AS total_cadence_contracts,
    COALESCE(e.total_new_evm_contracts, 0) AS new_evm_contracts,
    SUM(COALESCE(e.total_new_evm_contracts, 0)) OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp)) AS total_evm_contracts,
    COALESCE(c.total_new_cadence_contracts, 0) + COALESCE(e.total_new_evm_contracts, 0) AS full_contracts,
    SUM(COALESCE(c.total_new_cadence_contracts, 0) + COALESCE(e.total_new_evm_contracts, 0)) 
        OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp)) AS total_full_contracts,
    AVG(COALESCE(c.total_new_cadence_contracts, 0) + COALESCE(e.total_new_evm_contracts, 0)) 
        OVER (ORDER BY COALESCE(c.time_stamp, e.time_stamp) ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling_avg_new_contracts
FROM cadence c
FULL JOIN evms e ON c.time_stamp = e.time_stamp
WHERE COALESCE(c.time_stamp, e.time_stamp) < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
ORDER BY 1 DESC
"""

# Cadence vs COA vs EOA distribution (100% stacked area)
SQL_CONTRACTS_DISTRIBUTION_TYPES = """
WITH time_params AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date,
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END AS end_date
),
cadence AS (
    SELECT DISTINCT event_contract AS contract, MIN(block_timestamp) AS debut
    FROM flow.core.fact_events
    WHERE block_timestamp >= (SELECT start_date FROM time_params)
      AND block_timestamp <  (SELECT end_date   FROM time_params)
    GROUP BY 1
),
core_new_contracts AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', debut)
            WHEN 'last_year' THEN DATE_TRUNC('week', debut)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', debut)
            WHEN 'last_month' THEN DATE_TRUNC('day', debut)
            WHEN 'last_week' THEN DATE_TRUNC('day', debut)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', debut)
        END AS day,
        COUNT(DISTINCT contract) AS cadence_new
    FROM cadence
    GROUP BY 1
),
evms AS (
    SELECT x.block_timestamp, x.from_address AS creator, y.contract_address AS contract 
    FROM flow.core_evm.fact_transactions x
    JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash
    WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
      AND x.block_timestamp >= (SELECT start_date FROM time_params)
      AND x.block_timestamp <  (SELECT end_date   FROM time_params)
    UNION
    SELECT x.block_timestamp, x.from_address AS creator, x.tx_hash AS contract 
    FROM flow.core_evm.fact_transactions x
    WHERE x.origin_function_signature IN ('0x60c06040','0x60806040')
      AND x.block_timestamp >= (SELECT start_date FROM time_params)
      AND x.block_timestamp <  (SELECT end_date   FROM time_params)
      AND x.tx_hash NOT IN (
        SELECT x.tx_hash
        FROM flow.core_evm.fact_transactions x
        JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash
        WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
      )
),
evm_new_contracts AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS day,
        COUNT(DISTINCT CASE WHEN creator LIKE '0x0000000000000000000000020000000000000000%' THEN contract END) AS evm_coa_new,
        COUNT(DISTINCT CASE WHEN creator NOT LIKE '0x0000000000000000000000020000000000000000%' THEN contract END) AS evm_eoa_new
    FROM evms
    GROUP BY 1
)
SELECT
    COALESCE(c.day, e.day) AS day,
    COALESCE(c.cadence_new, 0) AS cadence_new,
    COALESCE(e.evm_coa_new, 0) AS evm_coa_new,
    COALESCE(e.evm_eoa_new, 0) AS evm_eoa_new
FROM core_new_contracts c
FULL JOIN evm_new_contracts e ON c.day = e.day
ORDER BY day DESC
"""

# Cadence vs EVM distribution (different style from previous)
SQL_CONTRACTS_CHAIN_DISTRIBUTION = """
WITH 
cadence AS (
    SELECT 
        DISTINCT event_contract AS contract,
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', block_timestamp)
        END AS time_stamp
    FROM flow.core.fact_events
    WHERE block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
),
core_new_contracts AS (
    SELECT time_stamp AS day, 'Cadence' AS type, COUNT(DISTINCT contract) AS new_contracts
    FROM cadence
    GROUP BY 1,2
),
evms AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END AS time_stamp,
        x.tx_hash AS contract
    FROM flow.core_evm.fact_transactions x
    LEFT JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash 
    WHERE (y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
        OR x.origin_function_signature IN ('0x60c06040','0x60806040'))
      AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
),
evm_new_contracts AS (
    SELECT time_stamp AS day, 'EVM' AS type, COUNT(DISTINCT contract) AS new_contracts
    FROM evms
    GROUP BY 1,2
),
info AS (
    SELECT day, type, new_contracts FROM core_new_contracts
    UNION ALL
    SELECT day, type, new_contracts FROM evm_new_contracts
)
SELECT * FROM info
WHERE day < CASE '{{Period}}'
        WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
        WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
        WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
        WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
    END
ORDER BY day DESC
"""

# =========================
# CONTRACT DEPLOYERS — SQL
# =========================

SQL_DEPLOYERS_CADENCE_KPI = """
WITH time_periods AS (
    SELECT
        CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            ELSE CURRENT_DATE - INTERVAL '1 DAY'
        END AS start_date
),
cadence AS (
    SELECT DISTINCT authorizers[0] AS users, MIN(TRUNC(x.block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events AS x
    JOIN flow.core.fact_transactions y ON x.tx_id = y.tx_id
    JOIN time_periods tp ON x.block_timestamp >= tp.start_date
    WHERE x.event_type = 'AccountContractAdded'
    GROUP BY 1
)
SELECT COUNT(DISTINCT users) AS total_cadence_deployers FROM cadence
"""

SQL_DEPLOYERS_TIMESERIES = """
WITH 
cadence AS (
    SELECT DISTINCT authorizers[0] AS users, 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END AS debut
    FROM flow.core.fact_events x
    JOIN flow.core.fact_transactions y ON x.tx_id = y.tx_id
    WHERE x.event_type = 'AccountContractAdded'
      AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
),
core_new_deployers AS (
    SELECT debut AS date, COUNT(DISTINCT users) AS new_cadence_deployers
    FROM cadence
    GROUP BY 1
),
evms AS (
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END AS block_timestamp,
        x.from_address AS creator,
        y.contract_address AS contract 
    FROM flow.core_evm.fact_transactions x
    JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash
    WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
      AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
    UNION
    SELECT 
        CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', x.block_timestamp)
            WHEN 'last_year' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', x.block_timestamp)
            WHEN 'last_month' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_week' THEN DATE_TRUNC('day', x.block_timestamp)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', x.block_timestamp)
        END AS block_timestamp,
        x.from_address AS creator, 
        x.tx_hash AS contract 
    FROM flow.core_evm.fact_transactions x
    WHERE x.origin_function_signature IN ('0x60c06040','0x60806040')
      AND x.block_timestamp >= CASE '{{Period}}'
            WHEN 'all_time' THEN '2020-01-01'::DATE
            WHEN 'last_year' THEN CURRENT_DATE - INTERVAL '1 YEAR'
            WHEN 'last_3_months' THEN CURRENT_DATE - INTERVAL '3 MONTHS'
            WHEN 'last_month' THEN CURRENT_DATE - INTERVAL '1 MONTH'
            WHEN 'last_week' THEN CURRENT_DATE - INTERVAL '1 WEEK'
            WHEN 'last_24h' THEN CURRENT_DATE - INTERVAL '1 DAY'
        END
      AND x.tx_hash NOT IN (
        SELECT x.tx_hash
        FROM flow.core_evm.fact_transactions x
        JOIN flow.core_evm.fact_event_logs y ON x.tx_hash = y.tx_hash
        WHERE y.topics[0] ILIKE '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
      )
),
evms2 AS (
    SELECT DISTINCT creator AS users, block_timestamp AS debut FROM evms
),
evm_new_deployers AS (
    SELECT debut AS date, COUNT(DISTINCT users) AS new_evm_deployers
    FROM evms2
    GROUP BY 1
),
final_results AS (
    SELECT
        COALESCE(x.date, y.date) AS day,
        COALESCE(new_cadence_deployers, 0) AS new_cadence_deployerss,
        SUM(COALESCE(new_cadence_deployers, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_cadence_deployers,
        COALESCE(new_evm_deployers, 0) AS new_evm_deployerss,
        SUM(COALESCE(new_evm_deployers, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_evm_deployers,
        COALESCE(new_cadence_deployers, 0) + COALESCE(new_evm_deployers, 0) AS full_deployers,
        SUM(COALESCE(new_cadence_deployers, 0) + COALESCE(new_evm_deployers, 0)) OVER (ORDER BY COALESCE(x.date, y.date)) AS total_full_deployers,
        AVG(COALESCE(new_cadence_deployers, 0) + COALESCE(new_evm_deployers, 0)) OVER (
            ORDER BY COALESCE(x.date, y.date)
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_full_deployers
    FROM core_new_deployers x 
    FULL JOIN evm_new_deployers y ON x.date = y.date
    WHERE COALESCE(x.date, y.date) < CASE '{{Period}}'
            WHEN 'all_time' THEN DATE_TRUNC('month', current_date)
            WHEN 'last_year' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_3_months' THEN DATE_TRUNC('week', current_date)
            WHEN 'last_month' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_week' THEN DATE_TRUNC('day', current_date)
            WHEN 'last_24h' THEN DATE_TRUNC('hour', current_date)
        END
)
SELECT * FROM final_results
ORDER BY day DESC
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
    LINE_COLOR    = "#111827"   # dark gray (for rolling avg / lines)
    BAR_COLOR     = "#a3a3a3"   # neutral bars

    def render_fees_tab():
        period_key = st.session_state.get("period_key", "last_3_months")
    
        # === KPI ===
        k = qp(render_sql(SQL_FEES_SUMMARY, period_key))
        if k is None or k.empty:
            st.info("No fee data available for this period.")
        else:
            kk = k.copy()
            kk.columns = [c.upper() for c in kk.columns]
            total_fees = pd.to_numeric(kk.iloc[0].get("TOTAL_FEES_FLOW"), errors="coerce")
            avg_fee    = pd.to_numeric(kk.iloc[0].get("AVG_TX_FEE_FLOW"), errors="coerce")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Fee ($FLOW) — [EVM + Cadence]", _fmt_float(total_fees, 2))
            with col2:
                st.metric("Avg Tx Fee ($FLOW)", _fmt_float(avg_fee, 6))
    
        # === Timeseries ===
        df = qp(render_sql(SQL_FEES_TIMESERIES, period_key))
        if df is None or df.empty:
            st.info("No fee timeseries available.")
        else:
            d = df.copy()
            d.columns = [c.upper() for c in d.columns]
            for c in ("FLOW_FEES","AVG_TX_FLOW_FEE","TOTAL_FLOW_FEES","AVG_28D_FLOW_FEES"):
                if c in d.columns:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
            d = d.dropna(subset=["DATE"]).sort_values("DATE")
    
            bars = alt.Chart(d).mark_bar(opacity=0.65, color=BAR_COLOR).encode(
                x=alt.X("DATE:T", title="Date"),
                y=alt.Y("FLOW_FEES:Q", title="Fees ($FLOW)"),
                tooltip=[alt.Tooltip("DATE:T"), alt.Tooltip("FLOW_FEES:Q", format=",.2f")]
            )
            line_ma = alt.Chart(d).mark_line(size=2, color=LINE_COLOR).encode(
                x="DATE:T",
                y=alt.Y("AVG_28D_FLOW_FEES:Q", title="28-period rolling avg"),
                tooltip=[alt.Tooltip("DATE:T"), alt.Tooltip("AVG_28D_FLOW_FEES:Q", format=",.2f")]
            )
            st.altair_chart((bars + line_ma).properties(height=360, title="Fee Over Time"), use_container_width=True)
    
        # === Methodology ===
        with st.expander("Methodology"):
            st.markdown(
                """
    This query aggregates daily transaction fees across two sources, **flow.core_evm.fact_transactions** and **flow.core.fact_transactions** joined with **flow.core.fact_events**, focusing on the `FeesDeducted` event type from the contract `A.f919ee77447b7497.FlowFees`.
    
    - **Daily Transaction Fees:** Captures the total fees generated per day across both Flow EVM and Flow Cadence transactions, supporting daily revenue analysis.  
    - **Average Transaction Fee:** Offers insights into fee trends, helping the ability to gauge average user cost per transaction.  
    - **Cumulative Fees:** The running total (`total_flow_fees`) helps in understanding the accumulation of fees over time, offering a long-term perspective on fee generation.
    
    This query provides a comprehensive view of transaction fees on both Flow Cadence and Flow EVM.
    """
            )
    render_fees_tab()
    render_footer()

with tabs[5]:
    st.subheader("Supply")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    LINE_COLOR    = "#111827"   # dark gray (for rolling avg / lines)
    BAR_COLOR     = "#a3a3a3"   # neutral bars
    
    def render_supply_tab():
        period_key = st.session_state.get("period_key", "last_3_months")
    
        # === Latest snapshot KPIs ===
        snap = qp(SQL_SUPPLY_LATEST)  # no period switch; uses latest available < current week
        if snap is None or snap.empty:
            st.info("No supply snapshot available.")
        else:
            s = snap.copy()
            s.columns = [c.upper() for c in s.columns]
            total_supply = pd.to_numeric(s.iloc[0].get("TOTAL_SUPPLY_ACTUAL"), errors="coerce")
            staked_locked = pd.to_numeric(s.iloc[0].get("STAKED_LOCKED"), errors="coerce")
    
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Supply Breakdown", _fmt_int(total_supply))
            with col2:
                st.metric("Staked Token (Locked)", _fmt_int(staked_locked))
    
        # === Breakdown over time (stacked) ===
        ts = qp(SQL_SUPPLY_SERIES)
        if ts is None or ts.empty:
            st.info("No historical supply breakdown available.")
        else:
            t = ts.copy()
            t.columns = [c.upper() for c in t.columns]
            keep_cols = ["DATE","TOTAL_SUPPLY_ACTUAL","STAKED_LOCKED","NON_STAKED_LOCKED","UNSTAKED_CIRCULATING","LIQUID_SUPPLY"]
            for c in keep_cols:
                if c in t.columns and c != "DATE":
                    t[c] = pd.to_numeric(t[c], errors="coerce")
            t = t.dropna(subset=["DATE"]).sort_values("DATE")
    
            # Melt to long form for stacked area
            plot = t.melt(
                id_vars=["DATE"],
                value_vars=["STAKED_LOCKED","NON_STAKED_LOCKED","LIQUID_SUPPLY","UNSTAKED_CIRCULATING"],
                var_name="Bucket",
                value_name="Amount"
            )
            chart = alt.Chart(plot).mark_area(opacity=0.75).encode(
                x=alt.X("DATE:T", title="Date"),
                y=alt.Y("Amount:Q", title="FLOW"),
                color=alt.Color("Bucket:N", title="Supply bucket"),
                tooltip=[alt.Tooltip("DATE:T"), "Bucket:N", alt.Tooltip("Amount:Q", format=",.0f")]
            ).properties(height=380, title="Total Supply Breakdown Over Time (FLOW)")
            st.altair_chart(chart, use_container_width=True)
    
        # === Methodology ===
        with st.expander("Methodology"):
            st.markdown(
                """
    This query analyzes the daily supply distribution and staking status of the **FLOW** token, categorizing token supply into staked, liquid, and non-staked locked categories. Additionally, it incorporates market data and staking activity trends to provide a comprehensive view of token circulation.
    
    **Market Data (Price, Volume, Market Cap)** comes from CoinGecko’s daily API.  
    **Supply Breakdown** partitions total supply into:
    - **Staked (Locked)** — staking pools  
    - **Non-Staked (Locked)** — lending/services/derivatives/launchpad  
    - **Unstaked Circulating** — available for trading (`Total Supply - (Staked Locked + Liquid Staking + Non-Staked Locked)`)  
    - **Liquid Staking** — tokens in liquid staking protocols  
    
    **Staking Activity** includes net and cumulative staking derived from `flow.gov.ez_staking_actions`.
    
    This provides a comprehensive picture of FLOW token liquidity and the impact of staking/locking over time.
    """
            )
    render_supply_tab()
    render_footer()

with tabs[6]:
    st.subheader("Contracts")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    LINE_COLOR    = "#111827"   # dark gray (for rolling avg / lines)
    BAR_COLOR     = "#a3a3a3"   # neutral bars
    
    def render_contracts_tab():
        period_key = st.session_state.get("period_key", "last_3_months")
    
        # ---- KPIs
        k = qp(render_sql(SQL_CONTRACTS_KPIS, period_key))
        if k is None or k.empty:
            st.info("No contract KPI data for this period.")
        else:
            kk = k.copy(); kk.columns = [c.upper() for c in kk.columns]
            c_cad = pd.to_numeric(kk.iloc[0].get("TOTAL_NEW_CADENCE_CONTRACTS"), errors="coerce")
            c_evm = pd.to_numeric(kk.iloc[0].get("TOTAL_NEW_EVM_CONTRACTS"),     errors="coerce")
            col1, col2 = st.columns(2)
            with col1: st.metric("Verified Contracts (Cadence)", f"{int(c_cad):,}" if pd.notna(c_cad) else "—")
            with col2: st.metric("Verified Contracts (EVM)",     f"{int(c_evm):,}" if pd.notna(c_evm) else "—")
    
        # ---- New deployments (bars) + rolling average (line)
        ts = qp(render_sql(SQL_CONTRACTS_TIMESERIES, period_key))
        if ts is not None and not ts.empty:
            df = ts.copy(); df.columns = [c.upper() for c in df.columns]
            for c in ("FULL_CONTRACTS","ROLLING_AVG_NEW_CONTRACTS"):
                if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["DAY"]).sort_values("DAY")
            bars = alt.Chart(df).mark_bar(opacity=0.7, color=BAR_COLOR).encode(
                x=alt.X("DAY:T", title="Period"),
                y=alt.Y("FULL_CONTRACTS:Q", title="New contracts"),
                tooltip=[alt.Tooltip("DAY:T"), alt.Tooltip("FULL_CONTRACTS:Q", format=",.0f")]
            )
            line = alt.Chart(df).mark_line(size=2, color=LINE_COLOR).encode(
                x="DAY:T",
                y=alt.Y("ROLLING_AVG_NEW_CONTRACTS:Q", title="Rolling avg (28-period)"),
                tooltip=[alt.Tooltip("DAY:T"), alt.Tooltip("ROLLING_AVG_NEW_CONTRACTS:Q", format=",.2f")]
            )
            st.altair_chart((bars + line).properties(height=360, title="Contract (Cadence+EVM) Deployment Over Time"), use_container_width=True)
        else:
            st.info("No contract deployment timeseries.")
    
        # ---- 100% stacked area: Cadence vs EVM-COA vs EVM-EOA
        dtypes = qp(render_sql(SQL_CONTRACTS_DISTRIBUTION_TYPES, period_key))
        if dtypes is not None and not dtypes.empty:
            dd = dtypes.copy(); dd.columns = [c.upper() for c in dd.columns]
            for c in ("CADENCE_NEW","EVM_COA_NEW","EVM_EOA_NEW"):
                if c in dd.columns: dd[c] = pd.to_numeric(dd[c], errors="coerce")
            dd = dd.dropna(subset=["DAY"]).sort_values("DAY")
            long = dd.melt(id_vars=["DAY"], value_vars=["CADENCE_NEW","EVM_COA_NEW","EVM_EOA_NEW"],
                           var_name="Bucket", value_name="Count")
            area = alt.Chart(long).mark_area(opacity=0.85).encode(
                x=alt.X("DAY:T", title="Period"),
                y=alt.Y("Count:Q", stack="normalize", title="Share of new contracts"),
                color=alt.Color("Bucket:N", title="Type", sort=["CADENCE_NEW","EVM_COA_NEW","EVM_EOA_NEW"]),
                tooltip=[alt.Tooltip("DAY:T"), "Bucket:N", alt.Tooltip("Count:Q", format=",.0f")]
            ).properties(height=360, title="Distribution of new contracts by type (Cadence + COA vs EOA)")
            st.altair_chart(area, use_container_width=True)
        else:
            st.info("No type distribution data.")
    
        # ---- Cadence vs EVM distribution (different look): normalized stacked bars + pie
        dist = qp(render_sql(SQL_CONTRACTS_CHAIN_DISTRIBUTION, period_key))
        if dist is not None and not dist.empty:
            d = dist.copy(); d.columns = [c.upper() for c in d.columns]
            d["NEW_CONTRACTS"] = pd.to_numeric(d["NEW_CONTRACTS"], errors="coerce")
            d = d.dropna(subset=["DAY"]).sort_values("DAY")
    
            colA, colB = st.columns(2)
            with colA:
                bar = alt.Chart(d).mark_bar().encode(
                    x=alt.X("DAY:T", title="Period"),
                    y=alt.Y("NEW_CONTRACTS:Q", stack="normalize", title="Share"),
                    color=alt.Color("TYPE:N", scale=alt.Scale(domain=["Cadence","EVM"], range=[CADENCE_COLOR, EVM_COLOR])),
                    tooltip=[alt.Tooltip("DAY:T"), "TYPE:N", alt.Tooltip("NEW_CONTRACTS:Q", format=",.0f")]
                ).properties(height=320, title="Evolution of the distribution of new contracts (Cadence vs EVM)")
                st.altair_chart(bar, use_container_width=True)
    
            with colB:
                pie_df = d.groupby("TYPE", as_index=False)["NEW_CONTRACTS"].sum()
                pie = alt.Chart(pie_df).mark_arc().encode(
                    theta=alt.Theta("NEW_CONTRACTS:Q"),
                    color=alt.Color("TYPE:N", scale=alt.Scale(domain=["Cadence","EVM"], range=[CADENCE_COLOR, EVM_COLOR])),
                    tooltip=[ "TYPE:N", alt.Tooltip("NEW_CONTRACTS:Q", format=",.0f") ]
                ).properties(height=320, title="Distribution of contracts deployed: Cadence vs EVM")
                st.altair_chart(pie, use_container_width=True)
        else:
            st.info("No chain distribution data.")
    
        # ---- Methodology
        with st.expander("Methodology"):
            st.markdown(
                """
    This query calculates the daily and cumulative totals of new contracts deployed on both **Flow Cadence** and **Flow EVM**.  
    It separates Cadence vs EVM (including **COA** vs **EOA** on EVM), shows rolling averages, and compares distributions.
    
    - **Daily New Contracts** (per chain and by EVM account type)  
    - **Cumulative Contract Totals** (per chain)  
    - **Unified Contract Growth** across Flow (Cadence + EVM)  
    - **COA (Cadence Owned Accounts)** vs **EOA (Externally Owned Accounts)** on EVM
    """
            )
    render_contracts_tab()
    render_footer()

with tabs[7]:
    st.subheader("Contract Deployers")
    CADENCE_COLOR = "#4F46E5"  # indigo
    EVM_COLOR     = "#22C55E"  # green
    TOTAL_COLOR   = "#0EA5E9"  # cyan
    NEUTRAL_LINE  = "#64748B"  # slate
    LINE_COLOR    = "#111827"   # dark gray (for rolling avg / lines)
    BAR_COLOR     = "#a3a3a3"   # neutral bars
    def render_contract_deployers_tab():
        period_key = st.session_state.get("period_key", "last_3_months")
    
        # ---- KPI (Cadence)
        k = qp(render_sql(SQL_DEPLOYERS_CADENCE_KPI, period_key))
        if k is None or k.empty:
            st.info("No deployer KPI data.")
        else:
            kk = k.copy(); kk.columns = [c.upper() for c in kk.columns]
            val = pd.to_numeric(kk.iloc[0].get("TOTAL_CADENCE_DEPLOYERS"), errors="coerce")
            st.metric("Deployers (Cadence)", f"{int(val):,}" if pd.notna(val) else "—")
    
        # ---- Timeseries: total new deployers (Cadence+EVM) bars + rolling avg line
        ts = qp(render_sql(SQL_DEPLOYERS_TIMESERIES, period_key))
        if ts is not None and not ts.empty:
            df = ts.copy(); df.columns = [c.upper() for c in df.columns]
            for c in ("FULL_DEPLOYERS","ROLLING_AVG_FULL_DEPLOYERS"):
                if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["DAY"]).sort_values("DAY")
    
            bars = alt.Chart(df).mark_bar(opacity=0.7, color=BAR_COLOR).encode(
                x=alt.X("DAY:T", title="Period"),
                y=alt.Y("FULL_DEPLOYERS:Q", title="New deployers"),
                tooltip=[alt.Tooltip("DAY:T"), alt.Tooltip("FULL_DEPLOYERS:Q", format=",.0f")]
            )
            line = alt.Chart(df).mark_line(size=2, color=LINE_COLOR).encode(
                x="DAY:T",
                y=alt.Y("ROLLING_AVG_FULL_DEPLOYERS:Q", title="Rolling avg (28-period)"),
                tooltip=[alt.Tooltip("DAY:T"), alt.Tooltip("ROLLING_AVG_FULL_DEPLOYERS:Q", format=",.2f")]
            )
            st.altair_chart((bars + line).properties(height=360, title="Contract (Cadence+EVM) Deployment Over Time — Deployers"), use_container_width=True)
        else:
            st.info("No deployer timeseries.")
    
        # ---- Methodology
        with st.expander("Methodology"):
            st.markdown(
                """
    This analysis tracks **unique contract deployers** on both **Flow Cadence** and **Flow EVM**:
    
    - **New Deployers** per day (Cadence and EVM separately)  
    - **Cumulative totals** by chain  
    - **Total Flow Deployers** (Cadence + EVM) with a rolling average
    
    This highlights developer adoption patterns and compares engagement between Cadence and EVM over time.
    """
            )
    render_contract_deployers_tab()
    render_footer()
