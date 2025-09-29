import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector
from datetime import datetime, timezone
import time

st.set_page_config(page_title="Flow Weekly Stats", page_icon="🟩", layout="wide")

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Madrid")

def last_completed_week_start_iso() -> str:
    now = datetime.now(TZ)
    current_week_start = (now - timedelta(days=now.weekday())).date()  # Monday of current week
    last_complete = current_week_start - timedelta(days=7)             # last finished week
    return last_complete.isoformat()

WEEK_KEY = last_completed_week_start_iso()

# Guard: fail clearly if required secrets are missing
REQUIRED = ["SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ROLE"]
missing = [k for k in REQUIRED if k not in st.secrets]
if missing:
    st.error(f"Missing secrets: {', '.join(missing)}. Add them in Settings → Secrets.")
    st.stop()

def now_local() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")

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
def run_query(sql: str, week_key: str) -> pd.DataFrame:
    cur = get_conn().cursor()
    try:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()

def q(sql: str) -> pd.DataFrame:
    return run_query(sql, WEEK_KEY)

with st.sidebar:
    st.title("Flow Weekly Stats")
    st.caption(f"Last completed week key: {WEEK_KEY}")
    if st.button("🔄 Force refresh"):
        run_query.clear()  # clears cache for all SQL
        st.rerun()

def download_btn(df, label, fname):
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label=label, data=csv, file_name=fname, mime="text/csv")

import re

def to_int_safe(x):
    """Return an int from x even if it's '44876 (+3)'; None if not parseable."""
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)) and pd.notna(x):
        return int(x)
    m = re.search(r"-?\d+", str(x))
    return int(m.group()) if m else None

# --- helpers for metrics/charts ---

def to_int_safe(x):
    """Return an int from x even if it's '44876 (+3)'; None if not parseable."""
    import pandas as pd
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return int(x)
    m = re.search(r"-?\d+", str(x))
    return int(m.group()) if m else None

def latest_prev(df, date_col):
    """Return (latest_row_df, prev_row_df) sorted by a date column."""
    if df is None or df.empty:
        return None, None
    dff = df.sort_values(date_col)
    return dff.tail(1), dff.tail(2).head(1)

# ---------- Insights (HTML version with colors & spacing) ----------
import math
from html import escape as _esc

def _to_num(s):
    try:
        return float(str(s).split()[0].replace(",", ""))
    except Exception:
        try:
            return float(s)
        except Exception:
            return None

def _fmt_int(n):
    return f"{int(n):,}" if n is not None and not math.isnan(n) else "—"

def _fmt_money(n, d=2):
    return f"${n:,.{d}f}" if n is not None and not math.isnan(n) else "—"

def _pct_delta(curr, prev):
    if curr is None or prev in (None, 0) or isinstance(curr, float) and math.isnan(curr) or isinstance(prev, float) and math.isnan(prev):
        return None
    return (curr - prev) / prev * 100.0

def _pct_color_html(p):
    if p is None or isinstance(p, float) and math.isnan(p):
        return "<span class='muted'>n/a</span>"
    cls = "up" if p >= 0 else "down"
    return f"<span class='{cls}'>{p:+.2f}%</span>"

def _last_prev(df, date_col):
    if df is None or df.empty or date_col not in df.columns:
        return None, None
    d = df.sort_values(date_col)
    return d.tail(1), d.tail(2).head(1)

def generate_weekly_insights_html(q):
    items = []

    # Transactions
    try:
        tx = q(SQL_TX_OVER_TIME).copy()
        tx.columns = [c.upper() for c in tx.columns]
        cur, prev = _last_prev(tx, "WEEK")
        if cur is not None and prev is not None:
            t_now  = _to_num(cur.iloc[0]["TOTAL_TRANSACTIONS"])
            t_prev = _to_num(prev.iloc[0]["TOTAL_TRANSACTIONS"])
            wow    = _pct_delta(t_now, t_prev)
            # success share
            succ_share = None
            try:
                ttypes = q(SQL_TX_SUCCESS_FAIL).copy()
                ttypes.columns = [c.upper() for c in ttypes.columns]
                lastw = ttypes["WEEK"].max()
                pie   = (ttypes[ttypes["WEEK"] == lastw]
                         .pivot_table(index="WEEK", columns="TYPE", values="TOTAL_TRANSACTIONS", aggfunc="sum"))
                succ_share = float((pie.get("Succeeded", 0) / pie.sum(axis=1)).iloc[0] * 100.0)
            except Exception:
                pass
            line = (
                f"Transactions: <strong>{_fmt_int(t_now)}</strong> "
                f"({_pct_color_html(wow)} WoW)"
            )
            if succ_share is not None:
                line += f"; success rate <strong>{succ_share:.2f}%</strong>"
            line += "."
            items.append(line)
    except Exception:
        pass

    # Fees
    try:
        fee = q(SQL_AVG_TX_FEE_WEEKLY).copy()
        fee.columns = [c.upper() for c in fee.columns]
        cur = fee.sort_values("WEEK").tail(1)
        if not cur.empty:
            flow_fee = _to_num(cur.iloc[0].get("AVG_TX_FEE_FLOW"))
            usd_fee  = _to_num(cur.iloc[0].get("AVG_TX_FEE_USD"))
            items.append(
                f"Average transaction fee: <strong>{flow_fee:.6f}</strong> FLOW "
                f"(<strong>{_fmt_money(usd_fee,6)}</strong>)."
            )
    except Exception:
        pass

    # Users
    try:
        u = q(SQL_USERS_OVER_TIME).copy()
        u.columns = [c.upper() for c in u.columns]
        cur, prev = _last_prev(u, "MONTH")
        if cur is not None and prev is not None:
            act  = _to_num(cur.iloc[0]["ACTIVE_USERS"]); actp = _to_num(prev.iloc[0]["ACTIVE_USERS"])
            new  = _to_num(cur.iloc[0]["NEW_USERS"]);    newp = _to_num(prev.iloc[0]["NEW_USERS"])
            items.append(
                "Users: active "
                f"<strong>{_fmt_int(act)}</strong> ({_pct_color_html(_pct_delta(act,actp))} WoW); "
                f"new <strong>{_fmt_int(new)}</strong> ({_pct_color_html(_pct_delta(new,newp))} WoW)."
            )
    except Exception:
        pass

    # NFTs
    try:
        n = q(SQL_NFT_SALES_OVER_TIME).copy()
        n.columns = [c.upper() for c in n.columns]
        cur, prev = _last_prev(n, "WEEK")
        if cur is not None and prev is not None:
            sales   = _to_num(cur.iloc[0].get("SALES"));            salesp = _to_num(prev.iloc[0].get("SALES"))
            buyers  = _to_num(cur.iloc[0].get("NFT_BUYERS"));       buyersp = _to_num(prev.iloc[0].get("NFT_BUYERS"))
            vol     = _to_num(cur.iloc[0].get("VOLUME"));           volp   = _to_num(prev.iloc[0].get("VOLUME"))
            colls   = _to_num(cur.iloc[0].get("ACTIVE_COLLECTIONS")); collsp = _to_num(prev.iloc[0].get("ACTIVE_COLLECTIONS"))
            items.append(
                "NFTs: sales "
                f"<strong>{_fmt_int(sales)}</strong> ({_pct_color_html(_pct_delta(sales,salesp))}), "
                f"buyers <strong>{_fmt_int(buyers)}</strong> ({_pct_color_html(_pct_delta(buyers,buyersp))}), "
                f"volume <strong>{_fmt_money(vol)}</strong> ({_pct_color_html(_pct_delta(vol,volp))}), "
                f"active collections <strong>{_fmt_int(colls)}</strong> ({_pct_color_html(_pct_delta(colls,collsp))})."
            )
    except Exception:
        pass

    # Contracts
    try:
        cn = q(SQL_CONTRACTS_NUMBERS).copy()
        cn.columns = [c.upper() for c in cn.columns]
        if not cn.empty:
            row = cn.iloc[0]
            active  = _to_num(row.get("ACTIVE_CONTRACTS"))
            pctdiff = _to_num(row.get("PCT_DIFF"))
            newc    = _to_num(row.get("NEW_CONTRACTS"))
            total   = _to_num(row.get("TOTAL_CONTRACTS"))
            items.append(
                "Contracts: active "
                f"<strong>{_fmt_int(active)}</strong> ({_pct_color_html(pctdiff)} WoW); "
                f"new <strong>{_fmt_int(newc)}</strong>; total unique <strong>{_fmt_int(total)}</strong>."
            )
    except Exception:
        pass

    # Staking
    try:
        s = q(SQL_STAKED_OVER_TIME).copy()
        s.columns = [c.upper() for c in s.columns]
        s = s.sort_values("DATE")
        if not s.empty:
            last = s.tail(1)
            net_cum = _to_num(last.iloc[0].get("TOTAL_NET_STAKED_VOLUME"))
            net_week = None
            if "NET_STAKED_VOLUME" in s.columns:
                cur, prev = _last_prev(s, "DATE")
                if cur is not None and prev is not None:
                    net_week = _to_num(cur.iloc[0].get("NET_STAKED_VOLUME"))
            # current stakers
            curr_stakers = None
            try:
                ss = q(SQL_STAKERS_SUMMARY).copy()
                ss.columns = [c.upper() for c in ss.columns]
                curr_stakers = _to_num(ss.iloc[0].get("UNIQUE_STAKERS"))
            except Exception:
                pass
            line = (
                f"Staking: net staked <strong>{_fmt_int(net_cum)}</strong> FLOW (cumulative)"
            )
            if net_week is not None:
                line += f"; weekly net <strong>{_fmt_int(net_week)}</strong> FLOW"
            if curr_stakers is not None:
                line += f"; current stakers <strong>{_fmt_int(curr_stakers)}</strong>"
            line += "."
            items.append(line)
    except Exception:
        pass

    # FLOW price (ASCII minus/plus; no $ in the label to avoid LaTeX)
    try:
        p = q(SQL_FLOW_PRICE_WEEK).copy()
        p.columns = [c.upper() for c in p.columns]
        tcol = next((c for c in p.columns if c in ("RECORDED_HOUR","HOUR","TIMESTAMP","DATE")), None)
        vcol = next((c for c in p.columns if c in ("FLOW_PRICE","PRICE_USD","PRICE")), None)
        if tcol and vcol:
            p[tcol] = pd.to_datetime(p[tcol], errors="coerce")
            p[vcol] = pd.to_numeric(p[vcol], errors="coerce")
            p = p.dropna(subset=[tcol, vcol]).sort_values(tcol)
            if not p.empty:
                last = float(p[vcol].iloc[-1])
                first = float(p[vcol].iloc[0])
                wow  = _pct_delta(last, first)
                hi   = float(p[vcol].max()); lo = float(p[vcol].min())
                items.append(
                    "FLOW price: "
                    f"<strong>{_fmt_money(last,4)}</strong> "
                    f"({_pct_color_html(wow)} vs 7-day open); "
                    f"range <strong>{_fmt_money(lo,4)}</strong> – <strong>{_fmt_money(hi,4)}</strong>."
                )
    except Exception:
        pass

    # Token movers
    try:
        mv = q(SQL_TOKENS_WEEKLY_MOVERS).copy()
        mv.columns = [c.upper() for c in mv.columns]
        tok_col = next((c for c in mv.columns if c in ("TOKEN","ASSET_ID")), None)
        dev_col = next((c for c in mv.columns if "DEV" in c or "DEVIATION" in c), None)
        if tok_col and dev_col and not mv.empty:
            mv[dev_col] = pd.to_numeric(mv[dev_col], errors="coerce") * (100.0 if mv[dev_col].abs().max() <= 2 else 1)
            top = mv.sort_values(dev_col, ascending=False).head(3)[[tok_col,dev_col]]
            bot = mv.sort_values(dev_col, ascending=True).head(3)[[tok_col,dev_col]]
            if not top.empty:
                up = "; ".join([f"{_esc(str(r[tok_col]))} "
                                f"(<span class='up'>{r[dev_col]:+.2f}%</span>)"
                                for _, r in top.iterrows()])
                items.append(f"Top weekly movers: {up}.")
            if not bot.empty:
                dn = "; ".join([f"{_esc(str(r[tok_col]))} "
                                f"(<span class='down'>{r[dev_col]:+.2f}%</span>)"
                                for _, r in bot.iterrows()])
                items.append(f"Bottom weekly movers: {dn}.")
    except Exception:
        pass

    if not items:
        return "<p class='muted'>No insights available for this period.</p>"

    # Styled list with spacing
    css = """
    <style>
      ul.insights { 
        line-height: 2; 
        margin: .8rem 0 0 1.25rem; 
        padding: 0; 
        font-size: 1.02rem;
      }
      ul.insights li { 
        margin: .6rem 0; 
      }
      .up   { color:#137333; font-weight:700; }   /* verd */
      .down { color:#b3261e; font-weight:700; }   /* vermell */
      .muted{ color:#6b7280; }
    </style>
    """

    lis = "\n".join(f"<li>{item}</li>" for item in items)
    return f"<ul class='insights'>{lis}</ul>"


# ---------- “Last data week” ----------
SQL_LAST_DATA_WEEK = """
WITH weeks AS (
  SELECT TRUNC(block_timestamp,'WEEK') AS wk FROM flow.core.fact_transactions WHERE block_timestamp < TRUNC(CURRENT_DATE,'WEEK')
  UNION ALL
  SELECT TRUNC(block_timestamp,'WEEK') AS wk FROM flow.core_evm.fact_transactions WHERE block_timestamp < TRUNC(CURRENT_DATE,'WEEK')
)
SELECT MAX(wk) AS last_week FROM weeks;
"""

# ---------- SQLs ----------
SQL_TX_NUMBERS = """
WITH final AS (
  SELECT
    TRUNC(block_timestamp,'WEEK') AS week,
    CASE WHEN tx_succeeded='true' THEN 'Succeeded' ELSE 'Failed' END AS type,
    COUNT(DISTINCT tx_id) AS total_transactions,
    SUM(COUNT(DISTINCT tx_id)) OVER (
      PARTITION BY CASE WHEN tx_succeeded='true' THEN 'Succeeded' ELSE 'Failed' END
      ORDER BY TRUNC(block_timestamp,'WEEK')
    ) AS cum_transactions
  FROM (
    SELECT DISTINCT tx_id, block_timestamp, tx_succeeded FROM flow.core.fact_transactions
    UNION ALL
    SELECT DISTINCT tx_hash AS tx_id, block_timestamp, tx_succeeded FROM flow.core_evm.fact_transactions
  ) x
  WHERE TRUNC(block_timestamp,'WEEK') < TRUNC(CURRENT_DATE,'WEEK')
  GROUP BY 1,2
)
SELECT * FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY type ORDER BY week DESC) <= 2
ORDER BY week ASC, type ASC;
"""

SQL_TX_OVER_TIME = """WITH previous_week_transactions AS (
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_id) as total_transactions
    FROM
        flow.core.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')-1
    GROUP BY
        1
    UNION ALL
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_hash) as total_transactions
    FROM
        flow.core_evm.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')-1
    GROUP BY
        1
),
aggregated_previous_week AS (
    SELECT
        week,
        sum(total_transactions) as total_transactions
    FROM
        previous_week_transactions
    GROUP BY
        week
),
current_week_transactions AS (
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_id) as total_transactions
    FROM
        flow.core.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')
    GROUP BY
        1
    UNION ALL
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_hash) as total_transactions
    FROM
        flow.core_evm.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')
    GROUP BY
        1
),
aggregated_current_week AS (
    SELECT
        week,
        sum(total_transactions) as total_transactions
    FROM
        current_week_transactions
    GROUP BY
        week
)
SELECT
    current_week.week,
    current_week.total_transactions,
    CONCAT(current_week.total_transactions, ' (', current_week.total_transactions - previous_week.total_transactions, ')') as transactions_diff,
    ((current_week.total_transactions - previous_week.total_transactions) / previous_week.total_transactions) * 100 as pcg_diff,
    SUM(current_week.total_transactions) OVER (ORDER BY current_week.week) as cum_transactions
FROM
    aggregated_current_week current_week
LEFT JOIN
    aggregated_previous_week previous_week
ON
    dateadd(week, -1, current_week.week) = previous_week.week
WHERE
    current_week.week < trunc(current_date,'week')
ORDER BY
    current_week.week desc
"""
SQL_TX_SUCCESS_FAIL = """
 with txs as (
select distinct tx_id, block_timestamp, tx_succeeded
from flow.core.fact_transactions
UNION
select distinct tx_hash as tx_id, block_timestamp, tx_succeeded
from flow.core_evm.fact_transactions
)
SELECT
trunc(block_timestamp,'week') as week,
case when tx_succeeded='true' then 'Succeeded' else 'Failed' end as type,
count(distinct tx_id) as total_transactions
from txs
where week<trunc(current_date,'week') 
group by 1,2
order by 1 asc """
SQL_AVG_TX_FEE_WEEKLY = """WITH evm_data AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS date,
        COUNT(TX_HASH) AS total_transactions,
        SUM(CASE WHEN tx_succeeded = 'TRUE' THEN 1 ELSE 0 END) AS successful_transactions,
        successful_transactions/total_transactions as success_rate,
        SUM(tx_fee) AS fees,
        AVG(tx_fee) AS avg_tx_fee,
        COUNT(DISTINCT FROM_ADDRESS) AS unique_users,
        avg(DATEDIFF(MINUTE, INSERTED_TIMESTAMP, BLOCK_TIMESTAMP)) AS latency_MINUTEs
    FROM 
        flow.core_evm.fact_transactions
    --WHERE 
    --    BLOCK_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE) and block_timestamp<current_date
    GROUP BY 
        1
),
non_evm_data AS (
    SELECT 
        DATE_TRUNC('day', x.BLOCK_TIMESTAMP) AS date,
        COUNT(distinct x.TX_ID) AS total_transactions,
        SUM(CASE WHEN x.TX_SUCCEEDED = 'TRUE' THEN 1 ELSE 0 END) AS successful_transactions,
        successful_transactions/total_transactions as success_rate,
        SUM(y.event_data:amount) AS fees,
        AVG(y.event_data:amount) AS avg_tx_fee,
        COUNT(DISTINCT x.PAYER) AS unique_users,
        avg(DATEDIFF(MINUTE, x.INSERTED_TIMESTAMP, x.BLOCK_TIMESTAMP)) AS latency_MINUTEs
    FROM 
        flow.core.fact_transactions x
join flow.core.fact_events y on x.tx_id=y.tx_id
    WHERE 
    --    x.BLOCK_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE) and 
event_contract='A.f919ee77447b7497.FlowFees'
and event_Type='FeesDeducted' and x.block_timestamp<current_date
    GROUP BY 
        1
),
flow_price as (
SELECT
trunc(hour,'hour') as hour,
avg(price) as price
from flow.price.ez_prices_hourly
where symbol = 'FLOW'
group by 1 
),
final as (
select date, fees, avg_tx_fee from evm_data union select date, fees, avg_tx_fee from non_evm_data
)
 SELECT
trunc(date,'week') as month,
avg(avg_tx_fee) as avg_tx_fee_flow,
avg(avg_tx_fee*price) as avg_tx_fee_usd
from final x join flow_price y on trunc(date,'week')=trunc(hour,'week')
where month<trunc(current_date,'week')
group by 1 order by 1 asc """
SQL_STAKED_OVER_TIME = """WITH
  staking as (
  SELECT
trunc(block_timestamp,'week') as date,
--case when action in ('DelegatorTokensCommitted','TokensCommitted') then 'Staking',
--when action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn') then 'Unstaking'
--  end as actions,
count(distinct tx_id) as transactions,
sum(transactions) over (order by date) as cum_transactions,
count(distinct delegator) as delegators,
sum(delegators) over (order by date) as cum_delegators,
sum(amount) as volume,
sum(volume) over (order by date) as cum_volume,
avg(amount) as avg_volume,
median(amount) as median_volume,
avg(volume) over (order by date rows between 6 preceding and current row) as avg_7d_ma_volume
from flow.gov.ez_staking_actions  where action in ('DelegatorTokensCommitted','TokensCommitted')
  group by 1 order by 1 asc
  ),
unstaking as (
    SELECT
trunc(block_timestamp,'week') as date,
--case when action in ('DelegatorTokensCommitted','TokensCommitted') then 'Staking',
--when action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn') then 'Unstaking'
--  end as actions,
count(distinct tx_id) as transactions,
sum(transactions) over (order by date) as cum_transactions,
count(distinct delegator) as delegators,
sum(delegators) over (order by date) as cum_delegators,
sum(amount) as volume,
sum(volume) over (order by date) as cum_volume,
avg(amount) as avg_volume,
median(amount) as median_volume,
avg(volume) over (order by date rows between 6 preceding and current row) as avg_7d_ma_volume
from flow.gov.ez_staking_actions  where action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
  group by 1 order by 1 asc
)
SELECT
x.date,
x.transactions as staking_transactions,y.transactions as unstaking_transactions,
x.cum_transactions as total_staking_transactions,y.cum_transactions as total_unstaking_transactions,total_staking_transactions-total_unstaking_transactions as net_staking_transactions,
x.delegators as staking_delegators,y.delegators as unstaking_delegators,
x.cum_delegators as total_staking_delegators,y.cum_delegators as total_unstaking_delegators, total_staking_delegators-total_unstaking_delegators as net_staking_delegators,
x.volume as staked_volume, y.volume*(-1) as unstaked_volume, staked_volume+unstaked_volume as net_staked_volume,
x.cum_volume as total_staked_volume, y.cum_volume*(-1) as total_unstaked_volume, total_staked_volume+total_unstaked_volume+2.4e8 as total_net_staked_volume
from staking x
left outer join unstaking y on x.date=y.date 
where x.date<trunc(current_date,'week') and y.date<trunc(current_date,'week')
order by 1 asc 
"""
SQL_STAKERS_SUMMARY = """WITH
  staking as (
  SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('DelegatorTokensCommitted','TokensCommitted')
  group by 1 order by 1 asc
  ),
unstaking as (
    SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
  group by 1 order by 1 asc
),
staking_past_week as (
  SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('DelegatorTokensCommitted','TokensCommitted')
and block_timestamp<current_date-INTERVAL '1 WEEK'
  group by 1 order by 1 asc
  ),
unstaking_past_week as (
    SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
and block_timestamp<current_date-INTERVAL '1 WEEK'
 group by 1 order by 1 asc
),
  final as (
SELECT
ifnull(x.delegator,y.delegator) as delegator,
ifnull(x.volume,0) as total_staked_volume, 
ifnull(y.volume*(-1),0) as total_unstaked_volume, 
total_staked_volume+total_unstaked_volume as total_net_staked_volume
from staking x
left outer join unstaking y on  x.delegator=y.delegator
order by 1 asc 
),
  final2 as (
SELECT
ifnull(x.delegator,y.delegator) as delegator,
ifnull(x.volume,0) as total_staked_volume, 
ifnull(y.volume*(-1),0) as total_unstaked_volume, 
total_staked_volume+total_unstaked_volume as total_net_staked_volume
from staking_past_week x
left outer join unstaking_past_week y on  x.delegator=y.delegator
order by 1 asc 
),
totals as (
SELECT
count(distinct x.delegator) as unique_stakers,
count(distinct y.delegator) as unique_stakers_past_week,
unique_stakers-unique_stakers_past_week as users_diff,
((unique_stakers-unique_stakers_past_week)/unique_stakers_past_week)*100 as pcg_diff
from final x,final2 y --where x.total_net_staked_volume>0 and y.total_net_staked_volume>0
)
select 
unique_stakers,unique_stakers_past_week,
case when users_diff>=0 then CONCAT(unique_stakers, ' (+', users_diff, ')')
when users_diff<0 then CONCAT(unique_stakers, ' (-', users_diff, ')') end as users_diff,
pcg_diff from totals
 """
SQL_FLOW_PRICE_WEEK = """with
revv as (
SELECT 
  hour,
  AVG(price) AS price_usd,
  MAX(price) AS high,
  STDDEV_POP(price) AS std
FROM flow.price.ez_prices_hourly
WHERE symbol ILIKE '%flow%'
  AND hour >= CURRENT_DATE - INTERVAL '1 WEEK' and price<0.5
GROUP BY hour
ORDER BY hour ASC 
),
flow as (
 select 
hour,
price_usd,
high,
LAG(price_usd, 1) OVER (ORDER BY hour) AS open,
std
from revv
) 
select
y.hour as recorded_hour,
y.price_usd as flow_price,
(y.high - y.open) / y.open * 100 as flow_price_change_percentage,
y.std as flow_price_volatility
from flow y
order by 1 desc """
SQL_TOKENS_WEEKLY_MOVERS = """SELECT asset_id as token,
  AVG(close) AS avg_price,
  (AVG(close) - AVG(CASE
                      WHEN hour BETWEEN DATEADD(day, -1, GETDATE()) AND GETDATE() THEN close
                      ELSE NULL
                    END)) / AVG(close) AS avg_deviation_weekly_pct
FROM flow.price.fact_prices_ohlc_hourly
WHERE hour BETWEEN DATEADD(day, -7, GETDATE()) AND GETDATE()
GROUP BY 1 having token is not null order by avg_deviation_weekly_pct desc 
"""

# Accounts
SQL_USERS_BY_REGION = """WITH
-- Calculate the debut (first activity) of each user
news AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        MIN(TRUNC(block_timestamp, 'hour')) AS debut
    FROM
        flow.core.fact_transactions
    GROUP BY
        1
),
-- Generate a list of all hours (0 to 23)
all_hours AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS hour_of_day
    FROM
        TABLE(GENERATOR(ROWCOUNT => 24))
),
-- Count the number of transactions for each user per hour
actives AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        EXTRACT(HOUR FROM block_timestamp) AS active_hour,        
        COUNT(DISTINCT tx_id) AS transactions
    FROM
        flow.core.fact_transactions x
    GROUP BY
        1,
        2
),
-- Join the debut and hourly transaction counts for each user
user_activity AS (
    SELECT
        n.user,
        n.debut,
        h.hour_of_day,
        COALESCE(a.transactions, 0) AS transactions,
        RANK() OVER (PARTITION BY n.user ORDER BY COALESCE(a.transactions, 0) DESC) AS hourly_rank
    FROM
        news n
    CROSS JOIN
        all_hours h
    LEFT JOIN
        actives a ON n.user = a.user AND h.hour_of_day = active_hour
),
-- Determine the range of most active and least active hours for each user
user_hourly_ranges AS (
    SELECT
        user,
        debut,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions DESC) AS most_active_hours,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions ASC) AS least_active_hours
    FROM (
        SELECT
            user,
            debut,
            hour_of_day,
            transactions,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions DESC) AS active_rank,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions ASC) AS inactive_rank
        FROM
            user_activity
    )
    WHERE
        active_rank <= 6-- OR inactive_rank <= 3
    GROUP BY
        user, debut
),
user_regions AS (
    SELECT
    user,
    debut,
    SUM(CASE WHEN POSITION('08' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Europe_Central_count,
    
    SUM(CASE WHEN POSITION('02' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS US_East_Coast_count,
    
    SUM(CASE WHEN POSITION('08' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Asia_count,
    
    SUM(CASE WHEN POSITION('23' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('00' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('07' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('08' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Europe_Western_count,
    
    SUM(CASE WHEN POSITION('17' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS US_Central_count,
    
    SUM(CASE WHEN POSITION('11' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('20' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS US_West_Coast_count,
    
    SUM(CASE WHEN POSITION('20' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('22' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('06' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS South_America_count,

    SUM(CASE WHEN POSITION('00' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Africa_count,

    SUM(CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('11' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Oceania_count
FROM
    user_hourly_ranges
GROUP BY
    user, debut
) --select * from user_regions where user='0xecb50f2955d00a5c'
,
regions as (
SELECT
    user,
    debut,
    CASE
        WHEN Europe_Central_count > US_East_Coast_count AND Europe_Central_count > Asia_count AND Europe_Central_count > Europe_Western_count AND Europe_Central_count > US_Central_count AND Europe_Central_count > US_West_Coast_count AND Europe_Central_count > South_America_count THEN 'Europe Central'
        WHEN US_East_Coast_count > Europe_Central_count AND US_East_Coast_count > Asia_count AND US_East_Coast_count > Europe_Western_count AND US_East_Coast_count > US_Central_count AND US_East_Coast_count > US_West_Coast_count AND US_East_Coast_count > South_America_count THEN 'United States (East Coast)'
        WHEN Asia_count > Europe_Central_count AND Asia_count > US_East_Coast_count AND Asia_count > Europe_Western_count AND Asia_count > US_Central_count AND Asia_count > US_West_Coast_count AND Asia_count > South_America_count THEN 'Asia'
        WHEN Europe_Western_count > Europe_Central_count AND Europe_Western_count > US_East_Coast_count AND Europe_Western_count > Asia_count AND Europe_Western_count > US_Central_count AND Europe_Western_count > US_West_Coast_count AND Europe_Western_count > South_America_count THEN 'Europe Western'
        WHEN US_Central_count > Europe_Central_count AND US_Central_count > US_East_Coast_count AND US_Central_count > Asia_count AND US_Central_count > Europe_Western_count AND US_Central_count > US_West_Coast_count AND US_Central_count > South_America_count THEN 'United States (Central)'
        WHEN US_West_Coast_count > Europe_Central_count AND US_West_Coast_count > US_East_Coast_count AND US_West_Coast_count > Asia_count AND US_West_Coast_count > Europe_Western_count AND US_West_Coast_count > US_Central_count AND US_West_Coast_count > South_America_count THEN 'United States (West Coast)'
        WHEN South_America_count > Europe_Central_count AND South_America_count > US_East_Coast_count AND South_America_count > Asia_count AND South_America_count > Europe_Western_count AND South_America_count > US_Central_count AND South_America_count > US_West_Coast_count then 'South America'
        WHEN Africa_count > Europe_Central_count AND Africa_count > US_East_Coast_count AND Africa_count > Asia_count AND Africa_count > Europe_Western_count AND Africa_count > US_Central_count AND Africa_count > US_West_Coast_count AND Africa_count > South_America_count AND Africa_count > Oceania_count THEN 'Africa'
        WHEN Oceania_count > Europe_Central_count AND Oceania_count > US_East_Coast_count AND Oceania_count > Asia_count AND Oceania_count > Europe_Western_count AND Oceania_count > US_Central_count AND Oceania_count > US_West_Coast_count AND Oceania_count > South_America_count AND Oceania_count > Africa_count THEN 'Oceania'
        
else 'Unknown Region' end as geographical_region
from user_regions
)
SELECT
trunc(block_timestamp,'week') as "Week",
case when geographical_region in ('United States (East Coast)','United States (Central)','United States (West Coast)') then 'US' 
when geographical_region in ('Europe Central','Europe Western') then 'Europe'
else geographical_region end as "Geographical Region",
count(distinct authorizers[0]) as "Active Accounts"
from flow.core.fact_transactions x join regions y on x.authorizers[0]=user
--where ""Week""<trunc(current_date,'week')
group by 1,2 order by 1 asc, 3 desc  
"""
SQL_USERS_BY_REGION_OVER_TIME = """WITH
-- Calculate the debut (first activity) of each user
news AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        MIN(TRUNC(block_timestamp, 'hour')) AS debut
    FROM
        flow.core.fact_transactions
    GROUP BY
        1
),
-- Generate a list of all hours (0 to 23)
all_hours AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS hour_of_day
    FROM
        TABLE(GENERATOR(ROWCOUNT => 24))
),
-- Count the number of transactions for each user per hour
actives AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        EXTRACT(HOUR FROM block_timestamp) AS active_hour,        
        COUNT(DISTINCT tx_id) AS transactions
    FROM
        flow.core.fact_transactions x
    GROUP BY
        1,
        2
),
-- Join the debut and hourly transaction counts for each user
user_activity AS (
    SELECT
        n.user,
        n.debut,
        h.hour_of_day,
        COALESCE(a.transactions, 0) AS transactions,
        RANK() OVER (PARTITION BY n.user ORDER BY COALESCE(a.transactions, 0) DESC) AS hourly_rank
    FROM
        news n
    CROSS JOIN
        all_hours h
    LEFT JOIN
        actives a ON n.user = a.user AND h.hour_of_day = active_hour
),
-- Determine the range of most active and least active hours for each user
user_hourly_ranges AS (
    SELECT
        user,
        debut,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions DESC) AS most_count,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions ASC) AS least_count
    FROM (
        SELECT
            user,
            debut,
            hour_of_day,
            transactions,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions DESC) AS active_rank,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions ASC) AS inactive_rank
        FROM
            user_activity
    )
    WHERE
        active_rank <= 6-- OR inactive_rank <= 3
    GROUP BY
        user, debut
),
user_regions AS (
    SELECT
    user,
    debut,
    SUM(CASE WHEN POSITION('08' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_count) > 0 THEN 1 ELSE 0 END) AS Europe_Central_count,
    
    SUM(CASE WHEN POSITION('02' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_count) > 0 THEN 1 ELSE 0 END) AS US_East_Coast_count,
    
    SUM(CASE WHEN POSITION('08' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_count) > 0 THEN 1 ELSE 0 END) AS Asia_count,
    
    SUM(CASE WHEN POSITION('23' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('00' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('07' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('08' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_count) > 0 THEN 1 ELSE 0 END) AS Europe_Western_count,
    
    SUM(CASE WHEN POSITION('17' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_count) > 0 THEN 1 ELSE 0 END) AS US_Central_count,
    
    SUM(CASE WHEN POSITION('11' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('20' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_count) > 0 THEN 1 ELSE 0 END) AS US_West_Coast_count,
    
    SUM(CASE WHEN POSITION('20' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('22' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('06' IN most_count) > 0 THEN 1 ELSE 0 END) AS South_America_count,

    SUM(CASE WHEN POSITION('00' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_count) > 0 THEN 1 ELSE 0 END) AS Africa_count,

    SUM(CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('11' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_count) > 0 THEN 1 ELSE 0 END) AS Oceania_count
FROM
    user_hourly_ranges
GROUP BY
    user, debut
) --select * from user_regions where user='0xecb50f2955d00a5c'
,
regions as (
SELECT
    user,
    debut,
    CASE
        WHEN Europe_Central_count > US_East_Coast_count AND Europe_Central_count > Asia_count AND Europe_Central_count > Europe_Western_count AND Europe_Central_count > US_Central_count AND Europe_Central_count > US_West_Coast_count AND Europe_Central_count > South_America_count THEN 'Europe Central'
        WHEN US_East_Coast_count > Europe_Central_count AND US_East_Coast_count > Asia_count AND US_East_Coast_count > Europe_Western_count AND US_East_Coast_count > US_Central_count AND US_East_Coast_count > US_West_Coast_count AND US_East_Coast_count > South_America_count THEN 'United States (East Coast)'
        WHEN Asia_count > Europe_Central_count AND Asia_count > US_East_Coast_count AND Asia_count > Europe_Western_count AND Asia_count > US_Central_count AND Asia_count > US_West_Coast_count AND Asia_count > South_America_count THEN 'Asia'
        WHEN Europe_Western_count > Europe_Central_count AND Europe_Western_count > US_East_Coast_count AND Europe_Western_count > Asia_count AND Europe_Western_count > US_Central_count AND Europe_Western_count > US_West_Coast_count AND Europe_Western_count > South_America_count THEN 'Europe Western'
        WHEN US_Central_count > Europe_Central_count AND US_Central_count > US_East_Coast_count AND US_Central_count > Asia_count AND US_Central_count > Europe_Western_count AND US_Central_count > US_West_Coast_count AND US_Central_count > South_America_count THEN 'United States (Central)'
        WHEN US_West_Coast_count > Europe_Central_count AND US_West_Coast_count > US_East_Coast_count AND US_West_Coast_count > Asia_count AND US_West_Coast_count > Europe_Western_count AND US_West_Coast_count > US_Central_count AND US_West_Coast_count > South_America_count THEN 'United States (West Coast)'
        WHEN South_America_count > Europe_Central_count AND South_America_count > US_East_Coast_count AND South_America_count > Asia_count AND South_America_count > Europe_Western_count AND South_America_count > US_Central_count AND South_America_count > US_West_Coast_count then 'South America'
        WHEN Africa_count > Europe_Central_count AND Africa_count > US_East_Coast_count AND Africa_count > Asia_count AND Africa_count > Europe_Western_count AND Africa_count > US_Central_count AND Africa_count > US_West_Coast_count AND Africa_count > South_America_count AND Africa_count > Oceania_count THEN 'Africa'
        WHEN Oceania_count > Europe_Central_count AND Oceania_count > US_East_Coast_count AND Oceania_count > Asia_count AND Oceania_count > Europe_Western_count AND Oceania_count > US_Central_count AND Oceania_count > US_West_Coast_count AND Oceania_count > South_America_count AND Oceania_count > Africa_count THEN 'Oceania'
else 'Unknown Region' end as geographical_region
from user_regions
),
--regions as (
--SELECT
--    user,
--    debut,
--    most_count,
--    CASE
--        WHEN POSITION('11' IN most_count) > 0 OR POSITION('12' IN most_count) > 0 OR POSITION('13' IN most_count) > 0 THEN 'Europe Central'
--        WHEN POSITION('05' IN most_count) > 0 OR POSITION('06' IN most_count) > 0 OR POSITION('07' IN most_count) > 0 THEN 'United States (East Coast)'
--        WHEN POSITION('17' IN most_count) > 0 OR POSITION('18' IN most_count) > 0 OR POSITION('19' IN most_count) > 0 THEN 'Asia'
--        WHEN POSITION('02' IN most_count) > 0 OR POSITION('03' IN most_count) > 0 OR POSITION('04' IN most_count) > 0 THEN 'Europe Western'
--        WHEN POSITION('08' IN most_count) > 0 OR POSITION('09' IN most_count) > 0 OR POSITION('10' IN most_count) > 0 THEN 'United States (Central)'
--        WHEN POSITION('14' IN most_count) > 0 OR POSITION('15' IN most_count) > 0 OR POSITION('16' IN most_count) > 0 THEN 'United States (West Coast)'
--        WHEN POSITION('00' IN most_count) > 0 OR POSITION('01' IN most_count) > 0 OR POSITION('02' IN most_count) > 0 THEN 'South America'
--        ELSE 'Unknown Region'
--   END AS geographical_region
--FROM
--    user_hourly_ranges
--)
region_counts AS (
    SELECT
        trunc(debut,'week') as debut,
        case when geographical_region in ('United States (East Coast)','United States (Central)','United States (West Coast)') then 'US'
        when geographical_region in ('Europe Central','Europe Western') then 'Europe'
 else geographical_region end AS region,
        COUNT(DISTINCT user) AS total_accounts
    FROM
        regions
    GROUP BY
        1,2
),
total_accounts AS (
    SELECT
        trunc(debut,'week') as debut,
        COUNT(DISTINCT user) AS total
    FROM
        regions
group by 1
),
known_accounts as (
SELECT
trunc(debut,'week') as debut,
COUNT(DISTINCT user) AS total3
from regions 
WHERE
        geographical_region <> 'Unknown Region'
group by 1
),
unknown_accounts AS (
    SELECT
        trunc(debut,'week') as debut,
        COUNT(DISTINCT user) AS total2
    FROM
        regions
    WHERE
        geographical_region = 'Unknown Region'
group by 1
),
pcgs as (
SELECT
distinct x.debut, x.region,
total_accounts/total3 as pcg,
total_accounts+total2*pcg as total_def
from region_counts x
left join known_accounts y on x.debut=y.debut 
left join unknown_accounts z on x.debut=z.debut
where x.region <> 'Unknown Region'
)

SELECT
trunc(debut,'week') as "Week",
region as "Geographical Region",
total_def as "Accounts Created"
from pcgs --group by 1,2 
--where ""Week""<trunc(current_date,'week')
order by 1 asc, 3 desc  
"""
SQL_USERS_NUMBERS = """WITH news AS (
    SELECT
        DISTINCT CAST(value AS VARCHAR) AS users,  -- Explicitly casting to VARCHAR
        MIN(trunc(b.block_timestamp, 'week')) AS debut
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        DISTINCT from_address AS users,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
news2 AS (
    SELECT
        debut,
        COUNT(DISTINCT users) AS new_users
    FROM
        news
    GROUP BY debut
),
actives AS (
    SELECT
        trunc(b.block_timestamp, 'week') AS week,
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS active_users  -- Explicitly casting to VARCHAR
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        trunc(block_timestamp, 'week') AS week,
        COUNT(DISTINCT from_address) AS active_users
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
aggregated_actives AS (
    SELECT
        week,
        SUM(active_users) AS active_users
    FROM
        actives
    GROUP BY week
),
final AS (
    SELECT
        a.week,
        a.active_users,
        n.new_users,
        SUM(n.new_users) OVER (ORDER BY a.week) AS unique_users
    FROM
        aggregated_actives a
    LEFT JOIN
        news2 n ON a.week = n.debut
    WHERE
        a.week < trunc(current_date, 'week')
    ORDER BY 1 ASC
),
final2 AS (
    SELECT
        a.week,
        a.active_users,
        n.new_users,
        SUM(n.new_users) OVER (ORDER BY a.week) AS unique_users
    FROM
        aggregated_actives a
    LEFT JOIN
        news2 n ON a.week = n.debut
    WHERE
        a.week < current_date - INTERVAL '2 WEEKS'
    ORDER BY 1 ASC
),
final_week AS (
    SELECT * FROM final ORDER BY week DESC LIMIT 1
),
final_past_week AS (
    SELECT * FROM final2 ORDER BY week DESC LIMIT 1
)
SELECT
    final.*,
    CONCAT(final.unique_users, ' (', final.unique_users - final2.unique_users, ')') AS new_accounts,
    ((final.active_users - final2.active_users) / final2.active_users) * 100 AS pcg_diff
FROM
    final_week AS final
JOIN
    final_past_week AS final2
WHERE
    final.week < trunc(current_date, 'week')"""
SQL_USERS_OVER_TIME = """WITH news AS (
    SELECT
        DISTINCT CAST(value AS VARCHAR) AS users,  -- Explicitly casting to VARCHAR
        MIN(trunc(b.block_timestamp, 'week')) AS debut
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        DISTINCT from_address AS users,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
news2 AS (
    SELECT
        debut,
        COUNT(DISTINCT users) AS new_users
    FROM
        news
    GROUP BY debut
),
actives AS (
    SELECT
        trunc(b.block_timestamp, 'week') AS week,
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS active_users  -- Explicitly casting to VARCHAR
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        trunc(block_timestamp, 'week') AS week,
        COUNT(DISTINCT from_address) AS active_users
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
aggregated_actives AS (
    SELECT
        week,
        SUM(active_users) AS active_users
    FROM
        actives
    GROUP BY week
),
final AS (
    SELECT
        a.week,
        a.active_users,
        n.new_users,
        SUM(n.new_users) OVER (ORDER BY a.week) AS unique_users
    FROM
        aggregated_actives a
    LEFT JOIN
        news2 n ON a.week = n.debut
    WHERE
        a.week < trunc(current_date, 'week')
    ORDER BY 1 ASC
)
 SELECT
week as month,
active_users, new_users
from final
order by 1 desc 
 """

# NFTs
SQL_NFT_SALES_NUMBERS = """WITH prices AS (
    SELECT 
        trunc(hour, 'week') AS hour,
        token_address AS token_contract,
        AVG(price) AS price_usd
    FROM flow.price.ez_prices_hourly
    WHERE symbol = 'FLOW'
    GROUP BY 1, 2
),
finalis AS (
    SELECT
        trunc(block_timestamp, 'week') AS week,
        tx_id,
        buyer,
        currency,
        price,
        nft_collection
    FROM flow.nft.ez_nft_sales x
    WHERE tx_succeeded = 'true'

    UNION

    SELECT
        trunc(x.block_timestamp, 'week') AS week,
        x.tx_id,
        z.authorizers[0] AS buyer,
        x.event_data:salePaymentVaultType AS currency,
        x.event_data:salePrice AS price,
        x.event_data:nftType AS nft_collection
    FROM flow.core.fact_events x
    JOIN flow.core.fact_transactions z ON x.tx_id = z.tx_id
    WHERE x.event_contract = 'A.4eb8a10cb9f87357.NFTStorefrontV2'
    AND x.event_type = 'ListingCompleted'
    AND x.event_data:purchased = 'true'
),
final AS (
    SELECT
        week,
        COUNT(DISTINCT tx_id) AS sales,
        COUNT(DISTINCT buyer) AS nft_buyers, --currency,
       -- CASE 
       --     WHEN currency ILIKE '%flow%' THEN SUM(price) * COALESCE(AVG(price_usd), 1)
       --     ELSE SUM(price)
       -- END AS volume,
        sum(price) as volume,
        sum(volume) over (order by week) as total_volume,
        COUNT(DISTINCT nft_collection) AS active_collections
    FROM finalis x
--    LEFT JOIN prices y ON week = hour
    WHERE week < trunc(CURRENT_DATE, 'week')
    GROUP BY week --,currency
),

finalis2 as (
SELECT
trunc(block_timestamp,'week') as week,
tx_id,
buyer,
currency,
price,
--price*avg(price_usd) as volume,
nft_collection
from flow.nft.ez_nft_sales x
where tx_succeeded='true'

union 
 
SELECT
trunc(x.block_timestamp,'week') as week,
x.tx_id,
z.authorizers[0] as buyer,
x.event_data:salePaymentVaultType as currency,
x.event_data:salePrice as price,
x.event_data:nftType as nft_collection
from flow.core.fact_events x
join flow.core.fact_transactions z on x.tx_id=z.tx_id
where x.event_contract='A.4eb8a10cb9f87357.NFTStorefrontV2'
and x.event_type='ListingCompleted' --and event_data:customID='flowverse-nft-marketplace'
and x.event_data:purchased='true'


 ),

final2 AS (
    SELECT
        week,
        COUNT(DISTINCT tx_id) AS sales,
        COUNT(DISTINCT buyer) AS nft_buyers, --currency,
       -- CASE 
       --     WHEN currency ILIKE '%flow%' THEN SUM(price) * COALESCE(AVG(price_usd), 1)
       --     ELSE SUM(price)
       -- END AS volume,
        sum(price) as volume,
        sum(volume) over (order by week) as total_volume,
        COUNT(DISTINCT nft_collection) AS active_collections
    FROM finalis2 x
    --LEFT JOIN prices y ON week = hour
    WHERE week < trunc(CURRENT_DATE, 'week') -1
    GROUP BY week --,currency
),


final_week as (select * from final order by 1 desc limit 1),
final_past_week as (select * from final2 order by 1 desc limit 1)
select 
final.*,concat(final.sales,' (',final.sales-final2.sales,')') as sales_diff, ((final.sales-final2.sales)/final2.sales)*100 as pcg_diff_sales,
 concat(final.volume,' (',final.volume-final2.volume,')') as vol_diff, ((final.volume-final2.volume)/final2.volume)*100 as pcg_diff_vol

from final_week as final join final_past_week as final2
where final.week<trunc(current_date,'week')

"""
SQL_NFT_SALES_OVER_TIME = """with
prices as (
select
trunc(hour,'week') as hour,
token_address as token_contract,
avg(price) as price_usd
from flow.price.ez_prices_hourly
group by 1,2
),
opensea_data AS (
    SELECT 
        tx_hash, 
        from_address, 
        block_timestamp, 
        value AS price
    FROM flow.core_evm.fact_transactions 
    WHERE to_address='0x0000000000000068f116a894984e2db1123eb395' 
        AND tx_succeeded='TRUE'
),
opensea_event_data AS (
    SELECT 
        tx_hash, 
        CASE
      WHEN LTRIM(SUBSTR(data,3), '0') = '' THEN NULL
      ELSE
        /* use TRY_TO_DECIMAL so bad formats become NULL rather than bomb out */
        TRY_TO_DECIMAL(
          LTRIM(SUBSTR(data, 3), '0'),
          REPEAT(
            'X',
            GREATEST(LENGTH(LTRIM(SUBSTR(data,3), '0')), 1)
          )
        ) / POW(10,18)
    END AS event_price
    FROM flow.core_evm.fact_event_logs 
    WHERE topic_0='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
        AND contract_address='0xd3bf53dac106a0290b0483ecbc89d40fcc961f3e'
),
finalis as (
-- major cadence 
SELECT
trunc(block_timestamp,'week') as week,
tx_id,
buyer,
currency,
price,
--price*avg(price_usd) as volume,
nft_collection
from flow.nft.ez_nft_sales x
where tx_succeeded='true' 

union 

-- other cadence
SELECT
trunc(x.block_timestamp,'week') as week,
x.tx_id,
z.authorizers[0] as buyer,
x.event_data:salePaymentVaultType as currency,
x.event_data:salePrice as price,
x.event_data:nftType as nft_collection
from flow.core.fact_events x
join flow.core.fact_transactions z on x.tx_id=z.tx_id
where x.event_contract='A.4eb8a10cb9f87357.NFTStorefrontV2'
and x.event_type='ListingCompleted' --and event_data:customID='flowverse-nft-marketplace'
and x.event_data:purchased='true'

union

-- beezie
select trunc(x.block_timestamp,'week') as week,
x.tx_hash,
x.topics[2] as buyer,
'a.b19436aae4d94622.fiattoken' as currency,
CASE
      WHEN LTRIM(SUBSTR(y.data,3), '0') = '' THEN NULL
      ELSE
        /* use TRY_TO_DECIMAL so bad formats become NULL rather than bomb out */
        TRY_TO_DECIMAL(
          LTRIM(SUBSTR(y.data, 3), '0'),
          REPEAT(
            'X',
            GREATEST(LENGTH(LTRIM(SUBSTR(y.data,3), '0')), 1)
          )
        ) / POW(10,6)
    END as price,
x.origin_from_address as collection
from flow.core_evm.fact_event_logs x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash
where x.origin_function_signature in ('0x052eb226','0x09c56431') and x.contract_address='0xd112634f06902a977db1d596c77715d72f8da8a9' and x.tx_succeeded='TRUE'
and x.data='0x' and x.event_index=12 and y.topics[0]='0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'

union
--mintify
select trunc(x.block_timestamp,'week') as week,
y.tx_hash,
y.origin_from_address as buyer,
'A.1654653399040a61.FlowToken' as currency,
x.value as price,
y.contract_address as collection
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash
where x.origin_function_signature in ('0xe7acab24','0x87201b41') and lower(x.to_address)=lower('0x00000003cf2c206e1fdA7fd032b2f9bdE12Ec6Cc') and x.tx_succeeded=TRUE
and y.origin_function_signature in ('0xe7acab24','0x87201b41') and lower(y.origin_to_address)=lower('0x00000003cf2c206e1fdA7fd032b2f9bdE12Ec6Cc') and y.tx_succeeded='TRUE' and data='0x'

union
-- opensea

SELECT 
    trunc(opensea_data.block_timestamp,'week') as week, 
    opensea_data.tx_hash,
    opensea_data.from_address as buyer,
    'A.1654653399040a61.FlowToken' as currency, 
    COALESCE(NULLIF(opensea_data.price, 0), opensea_event_data.event_price) AS price,
    NULL as collection
FROM opensea_data
LEFT JOIN opensea_event_data 
ON opensea_data.tx_hash = opensea_event_data.tx_hash
having price is not null

 )
SELECT
distinct week,
count(distinct tx_id) as sales,
sum(sales) over (order by week) as total_sales,
count(distinct buyer) as nft_buyers,
sum(price)*avg(price_usd) as volume,
sum(volume) over (order by week) as total_volume,
count(distinct nft_collection) as active_collections
from finalis x
left join prices y on week=hour
and x.currency ilike y.token_contract
where week<trunc(current_date,'week')
group by 1 order by 1 desc  
"""

# Contracts
SQL_CONTRACTS_NUMBERS = """WITH core_news AS (
    SELECT DISTINCT event_contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events
    GROUP BY 1
),
evm_news AS (
    SELECT DISTINCT contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM (
select x.block_timestamp, x.from_address as creator,y.contract_address as contract 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
UNION
select x.block_timestamp, x.from_address as creator, x.tx_hash as contract 
from flow.core_evm.fact_transactions x
where (x.origin_function_signature='0x60c06040' or x.origin_function_signature='0x60806040') and tx_hash not in (select x.tx_hash 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%')
)
    GROUP BY 1
),
combined_news AS (
    SELECT new_contract, debut FROM core_news
    UNION ALL
    SELECT new_contract, debut FROM evm_news
),
tots as (
select count(distinct new_contract) as total_contracts from combined_news
),
active_contracts AS (
    SELECT
        trunc(x.block_timestamp, 'week') AS date,
        COUNT(DISTINCT event_contract) AS active_contracts
    FROM flow.core.fact_events x
    WHERE x.tx_succeeded = 'true'
    GROUP BY 1
    UNION ALL
    SELECT
        trunc(y.block_timestamp, 'week') AS date,
        COUNT(DISTINCT contract_address) AS active_contracts
    FROM flow.core_evm.fact_event_logs y
    GROUP BY 1
),
current_week_active AS (
    SELECT date, sum(active_contracts) as active_contracts
    FROM active_contracts
    WHERE date = trunc(current_date, 'week') - INTERVAL '1 WEEK' group by 1
),
previous_week_active AS (
    SELECT date, sum(active_contracts) as active_contracts
    FROM active_contracts
    WHERE date = trunc(current_date, 'week') - INTERVAL '2 WEEKS' group by 1
),
current_week_stats AS (
    SELECT 
        trunc(current_date, 'week') - INTERVAL '1 WEEK' AS date,
        COUNT(DISTINCT new_contract) AS new_contractss
    FROM combined_news
    WHERE debut >= trunc(current_date, 'week') - INTERVAL '1 WEEK'
        AND debut < trunc(current_date, 'week') group by 1
),
previous_week_stats AS (
    SELECT 
        trunc(current_date, 'week') - INTERVAL '2 WEEKS' AS date,
        COUNT(DISTINCT new_contract) AS new_contractss
    FROM combined_news
    WHERE debut >= trunc(current_date, 'week') - INTERVAL '2 WEEKS'
        AND debut < trunc(current_date, 'week') - INTERVAL '1 WEEK' group by 1
)

SELECT 
    cw.date AS current_week,
    COALESCE(cw.active_contracts, 0) AS active_contracts,
    COALESCE(pw.active_contracts, 0) AS previous_week_active_contracts,
    COALESCE(pwa.new_contractss, 0) AS new_contracts,
    total_contracts,
    COALESCE(pws.new_contractss, 0) AS previous_week_new_contracts,
    COALESCE((cw.active_contracts - pw.active_contracts) / NULLIF(pw.active_contracts, 0) * 100, 0) AS pct_diff
FROM current_week_active cw
FULL OUTER JOIN previous_week_active pw ON cw.date-INTERVAL '1 WEEK' = pw.date
FULL OUTER JOIN current_week_stats pwa ON cw.date = pwa.date
FULL OUTER JOIN previous_week_stats pws ON cw.date-INTERVAL '1 WEEK' = pws.date
join tots
WHERE cw.date >= '2024-01-01'
ORDER BY current_week DESC"""
SQL_CONTRACTS_ACTIVE_OVER_TIME = """WITH news AS (
    SELECT DISTINCT event_contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events
    GROUP BY 1
),
evm_news AS (
    SELECT DISTINCT contract_address AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core_evm.fact_event_logs
    GROUP BY 1
),
combined_news AS (
    SELECT new_contract, debut FROM news
    UNION ALL
    SELECT new_contract, debut FROM evm_news
),
final as (
SELECT
    trunc(x.block_timestamp, 'week') AS date,
    COUNT(DISTINCT x.event_contract) AS active_contracts
FROM flow.core.fact_events x
WHERE x.tx_succeeded = 'true' and date<trunc(current_date,'week')
GROUP BY 1

UNION ALL

SELECT
    trunc(y.block_timestamp, 'week') AS date,
    COUNT(DISTINCT y.contract_address) AS active_contracts
FROM flow.core_evm.fact_event_logs y where date>'2020-01-01' and date<trunc(current_date,'week')
GROUP BY 1
ORDER BY date ASC
)
select date, sum(active_contracts) as active_contracts from final group by 1 order by 1 desc """
SQL_CONTRACTS_NEW_OVER_TIME = """WITH core_news AS (
    SELECT DISTINCT event_contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events
    GROUP BY 1
),
evm_news AS (
    SELECT DISTINCT contract AS new_contract, creator,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM (
select x.block_timestamp, x.from_address as creator,y.contract_address as contract 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
UNION
select x.block_timestamp, x.from_address as creator, x.tx_hash as contract 
from flow.core_evm.fact_transactions x
where (x.origin_function_signature='0x60c06040' or x.origin_function_signature='0x60806040') and tx_hash not in (select x.tx_hash 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%')
)
    GROUP BY 1,2
),
combined_news AS (
    SELECT new_contract, debut, 'Cadence' as source FROM core_news
    UNION ALL
    SELECT new_contract, debut, CASE WHEN creator LIKE '0x0000000000000000000000020000000000000000%' THEN 'COA EVM Contract' else 'Non-COA EVM Contract' end as source FROM evm_news
)
SELECT
    debut AS date, source,
    COUNT(DISTINCT new_contract) AS new_contracts,
    SUM(COUNT(DISTINCT new_contract)) OVER (partition by source ORDER BY debut) AS unique_contracts
FROM combined_news where date>'2020-01-01' and date<trunc(current_date,'week')
GROUP BY debut, source
ORDER BY debut ASC"""


# ---------- UI ----------
#st.sidebar.title("Flow Weekly Stats")
#ttl = st.sidebar.slider("Cache TTL (s)", 60, 900, 300, step=30)
#st.sidebar.caption(f"Now: {now_local()}")
# allow canviar TTL en calent

st.title("🟩 Flow Weekly Stats — Dashboard")

# Last data week
try:
    lastwk = q(SQL_LAST_DATA_WEEK)
    last_week = pd.to_datetime(lastwk.iloc[0,0]).date() if not lastwk.empty else None
    st.info(f"Last data week available: **{last_week}** (weeks are ISO-truncated).")
except Exception as e:
    st.warning(f"No 'last data week' info: {e}")

tabs = st.tabs([
    "Overview", "Transactions", "Staking",
    "Accounts", "NFTs", "Contracts",
    "Prices & Tokens", "Conclusions"
])

# ---------- Overview ----------
with tabs[0]:
    st.markdown("### General Flow Blockchain Weekly Stats")
    st.markdown(
        "> This page summarizes the **last completed week** across Flow: transactions, fees, and users. "
        "All weekly charts exclude the current in-progress week."
    )

    # Pull once
    tx_over = q(SQL_TX_OVER_TIME)          # has WEEK, TOTAL_TRANSACTIONS, PCG_DIFF, CUM_TRANSACTIONS
    fee     = q(SQL_AVG_TX_FEE_WEEKLY)     # has WEEK, AVG_TX_FEE_FLOW, AVG_TX_FEE_USD
    usersOT = q(SQL_USERS_OVER_TIME)       # has MONTH (week), ACTIVE_USERS, NEW_USERS
    priceWK = q(SQL_FLOW_PRICE_WEEK)       # has RECORDED_HOUR, FLOW_PRICE

    # Latest/prev rows
    tx_latest, tx_prev = latest_prev(tx_over, "WEEK")
    users_latest, _    = latest_prev(usersOT, "MONTH")

    # ----- KPI row 1 -----
    c1, c2, c3 = st.columns(3)
    if tx_latest is not None and tx_prev is not None:
        weekly = int(tx_latest["TOTAL_TRANSACTIONS"].iloc[0])
        weekly_delta = weekly - int(tx_prev["TOTAL_TRANSACTIONS"].iloc[0])
        total_tx = int(tx_latest["CUM_TRANSACTIONS"].iloc[0])

        c1.metric("Weekly Flow transactions", f"{weekly:,}")
        c2.metric("Total Flow transactions", f"{total_tx:,}")
    else:
        c1.metric("Weekly Flow transactions", "—")
        c2.metric("Total Flow transactions", "—")

    if not fee.empty:
        c3.metric("Average tx fee (USD)", f"${fee.tail(1)['AVG_TX_FEE_USD'].iloc[0]:.6f}")

    # ----- KPI row 2 -----
    c4, c5, c6 = st.columns(3)
    if users_latest is not None:
        c4.metric("Active accounts", f"{int(users_latest['ACTIVE_USERS'].iloc[0]):,}")
        c5.metric("New accounts", f"{int(users_latest['NEW_USERS'].iloc[0]):,}")
    else:
        c4.metric("Active accounts", "—")
        c5.metric("New accounts", "—")

    if not priceWK.empty:
        curr_price = float(priceWK.sort_values("RECORDED_HOUR").tail(1)["FLOW_PRICE"].iloc[0])
        c6.metric("Current FLOW price", f"${curr_price:.2f}")

    st.markdown("### Weekly Transactions (trend)")
    if tx_over is not None and not tx_over.empty:
        chart = alt.Chart(tx_over.sort_values("WEEK")).mark_line().encode(
            x="WEEK:T", y="TOTAL_TRANSACTIONS:Q",
            tooltip=["WEEK","TOTAL_TRANSACTIONS","PCG_DIFF"]
        )
        st.altair_chart(chart, use_container_width=True)
        download_btn(tx_over, "⬇️ Download transactions (CSV)", "transactions_weekly.csv")


# ---------- Transactions ----------
with tabs[1]:
    st.subheader("Transactions Numbers")

    tx_over = q(SQL_TX_OVER_TIME)
    df_types = q(SQL_TX_SUCCESS_FAIL)

    # KPI tiles
    if tx_over is not None and not tx_over.empty:
        latest, prev = latest_prev(tx_over, "WEEK")
        a, b, c = st.columns(3)
        if latest is not None and prev is not None:
            a.metric("Total Flow transactions", f"{int(latest['CUM_TRANSACTIONS'].iloc[0]):,}")
            b.metric("Difference vs past week", f"{float(latest['PCG_DIFF'].iloc[0]):.2f}%")
            c.metric("Weekly Flow transactions", f"{int(latest['TOTAL_TRANSACTIONS'].iloc[0]):,}")

    # Color palette for Succeeded/Failed
    tx_color = alt.Color(
        "TYPE:N",
        scale=alt.Scale(domain=["Succeeded","Failed"], range=["#7BC96F","#FF9DA7"]),
        legend=alt.Legend(orient="bottom")
    )

    # Put pie + stacked bars on the same row
    colA, colB = st.columns(2)

    if df_types is not None and not df_types.empty:
        lastw = df_types["WEEK"].max()
        pie_df = (df_types[df_types["WEEK"] == lastw]
                  .groupby("TYPE", as_index=False)["TOTAL_TRANSACTIONS"].sum())
        donut = alt.Chart(pie_df).mark_arc(innerRadius=60).encode(
            theta="TOTAL_TRANSACTIONS:Q", color=tx_color,
            tooltip=["TYPE","TOTAL_TRANSACTIONS"]
        )
        colA.markdown("**Weekly transactions by type (latest week)**")
        colA.altair_chart(donut, use_container_width=True)

        colB.markdown("**WoW Flow transactions (stacked)**")
        chart_bar = alt.Chart(df_types).mark_bar().encode(
            x="WEEK:T", y="TOTAL_TRANSACTIONS:Q", color=tx_color,
            tooltip=["WEEK","TYPE","TOTAL_TRANSACTIONS"]
        )
        colB.altair_chart(chart_bar, use_container_width=True)

        # % share area (recolored to match)
        piv = df_types.pivot_table(index="WEEK", columns="TYPE",
                                   values="TOTAL_TRANSACTIONS", aggfunc="sum").fillna(0)
        piv["TOTAL"] = piv.sum(axis=1)
        share = (
            piv[["Succeeded","Failed"]].div(piv["TOTAL"], axis=0).reset_index()
               .melt("WEEK", var_name="TYPE", value_name="SHARE")
        )
        st.markdown("**WoW % share (Succeeded vs Failed)**")
        area = alt.Chart(share).mark_area(opacity=0.6).encode(
            x="WEEK:T",
            y=alt.Y("SHARE:Q", axis=alt.Axis(format='%')),
            color=tx_color,
            tooltip=["WEEK","TYPE", alt.Tooltip("SHARE:Q", format=".1%")]
        )
        st.altair_chart(area, use_container_width=True)

        download_btn(df_types, "⬇️ Download data", "tx_success_failed.csv")


# ---------- Staking ----------
with tabs[2]:

    st.subheader("Staking Numbers")

    dfa  = q(SQL_STAKED_OVER_TIME)
    dfa2 = q(SQL_STAKERS_SUMMARY)
    
    if dfa is not None and not dfa.empty:
        dfa = dfa.sort_values("DATE").copy()
    
        # ensure numeric
        for c in [
            "TOTAL_STAKED_VOLUME","TOTAL_UNSTAKED_VOLUME","TOTAL_NET_STAKED_VOLUME",
            "TOTAL_STAKING_DELEGATORS","TOTAL_UNSTAKING_DELEGATORS","NET_STAKING_DELEGATORS"
        ]:
            if c in dfa.columns:
                dfa[c] = pd.to_numeric(dfa[c], errors="coerce")
    
        last = dfa.tail(1)
    
        # ---- KPI 1: current net staked FLOW
        if "TOTAL_NET_STAKED_VOLUME" in dfa.columns and pd.notna(last["TOTAL_NET_STAKED_VOLUME"].iloc[0]):
            curr_net_flow = float(last["TOTAL_NET_STAKED_VOLUME"].iloc[0])
        else:
            staked   = float(last.get("TOTAL_STAKED_VOLUME",   pd.Series([0])).iloc[0] or 0)
            unstaked = float(last.get("TOTAL_UNSTAKED_VOLUME", pd.Series([0])).iloc[0] or 0)  # often negative
            curr_net_flow = staked + unstaked
    
        # ---- KPI 2: current stakers (delegators) from dfa2, with robust fallbacks
        curr_deleg = None
        if dfa2 is not None and not dfa2.empty:
            # handle casing differences
            upmap = {c.upper(): c for c in dfa2.columns}
    
            # take the latest row if a week/date column exists, else first row
            if "WEEK" in upmap:
                dfa2_use = dfa2.sort_values(upmap["WEEK"]).tail(1)
            elif "DATE" in upmap:
                dfa2_use = dfa2.sort_values(upmap["DATE"]).tail(1)
            else:
                dfa2_use = dfa2.head(1)
    
            # prefer UNIQUE_STAKERS (string-safe via to_int_safe)
            if "UNIQUE_STAKERS" in upmap:
                curr_deleg = to_int_safe(dfa2_use.iloc[0][upmap["UNIQUE_STAKERS"]])
            elif "CURRENT_STAKERS" in upmap:
                curr_deleg = to_int_safe(dfa2_use.iloc[0][upmap["CURRENT_STAKERS"]])
    
        # final fallback: compute from cumulative totals in dfa
        if curr_deleg is None and {"TOTAL_STAKING_DELEGATORS","TOTAL_UNSTAKING_DELEGATORS"}.issubset(dfa.columns):
            curr_deleg = int(
                (last["TOTAL_STAKING_DELEGATORS"].iloc[0] or 0)
                - (last["TOTAL_UNSTAKING_DELEGATORS"].iloc[0] or 0)
            )
    
        k1, k2 = st.columns(2)
        k1.metric("Current net staked FLOW", f"{curr_net_flow:,.0f} FLOW")
        k2.metric("Current stakers (delegators)", f"{curr_deleg:,}" if curr_deleg is not None else "—")
    else:
        st.info("No staking data available.")

    st.subheader("WoW FLOW staked — cumulative vs net")

    dfa = q(SQL_STAKED_OVER_TIME)
    if not dfa.empty:
        dfa = dfa.sort_values("DATE").copy()
        # ensure types
        for c in [
            "TOTAL_STAKED_VOLUME","TOTAL_UNSTAKED_VOLUME","TOTAL_NET_STAKED_VOLUME",
            "STAKING_DELEGATORS","UNSTAKING_DELEGATORS","NET_STAKING_DELEGATORS",
            "STAKED_VOLUME","UNSTAKED_VOLUME","NET_STAKED_VOLUME"
        ]:
            if c in dfa.columns:
                dfa[c] = pd.to_numeric(dfa[c], errors="coerce")
        dfa["DATE"] = pd.to_datetime(dfa["DATE"], errors="coerce")
    
        # Bars dataframe with friendly labels
        bars_df = dfa[["DATE","TOTAL_STAKED_VOLUME","TOTAL_UNSTAKED_VOLUME"]].melt(
            "DATE", var_name="Series", value_name="Volume"
        )
        label_map = {
            "TOTAL_STAKED_VOLUME": "Staked",
            "TOTAL_UNSTAKED_VOLUME": "Unstaked",
        }
        bars_df["Series"] = bars_df["Series"].map(label_map)
    
        bars = (
            alt.Chart(bars_df)
            .mark_bar(opacity=0.75)
            .encode(
                x=alt.X("DATE:T", title="Week"),
                y=alt.Y("Volume:Q", title="Volume (cumulative)"),
                color=alt.Color(
                    "Series:N",
                    scale=alt.Scale(domain=["Staked","Unstaked"], range=["#7BC96F","#FF9DA7"]),
                    legend=alt.Legend(title=None, orient="top"),
                ),
                tooltip=["DATE:T","Series", alt.Tooltip("Volume:Q", format=",.0f")],
            )
        )
    
        # Net line (right axis)
        if "TOTAL_NET_STAKED_VOLUME" in dfa.columns:
            line = (
                alt.Chart(dfa)
                .mark_line(strokeWidth=3, color="black")
                .encode(
                    x="DATE:T",
                    y=alt.Y(
                        "TOTAL_NET_STAKED_VOLUME:Q",
                        axis=alt.Axis(title="Net staked (cumulative)", orient="right"),
                    ),
                    tooltip=["DATE:T", alt.Tooltip("TOTAL_NET_STAKED_VOLUME:Q", format=",.0f")],
                )
            )
            chart = alt.layer(bars, line).resolve_scale(y="independent")
        else:
            chart = bars
    
        st.altair_chart(chart, use_container_width=True)
        download_btn(dfa, "⬇️ Download data", "staking_over_time.csv")
    else:
        st.info("No staking data available.")

    



# ---------- Accounts ----------
with tabs[3]:
    st.subheader("Users — summary")

    # 1) Summary KPIs (unique, active this week, new this week)
    u1 = q(SQL_USERS_NUMBERS)  # columns: WEEK, ACTIVE_USERS, NEW_USERS, UNIQUE_USERS, ...
    if u1 is not None and not u1.empty:
        uniq = to_int_safe(u1.iloc[0].get("UNIQUE_USERS"))
        active = to_int_safe(u1.iloc[0].get("ACTIVE_USERS"))
        new = to_int_safe(u1.iloc[0].get("NEW_USERS"))

        # Big centered number for unique users
        st.markdown(
            f"""
            <div style="text-align:center; margin: 6px 0 20px 0;">
              <div style="font-size:16px; color:#6b7280;">Flow unique users</div>
              <div style="font-size:44px; font-weight:700;">{uniq:,}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        c1, c2 = st.columns(2)
        c1.metric("Flow # active users this week", f"{active:,}" if active is not None else "—")
        c2.metric("Flow # new users this week", f"{new:,}" if new is not None else "—")
    else:
        st.info("No user summary available.")

    st.markdown("### Flow weekly new and active users")

    u2 = q(SQL_USERS_OVER_TIME)
    if u2 is not None and not u2.empty:
        # ensure numeric
        for c in ["ACTIVE_USERS", "NEW_USERS"]:
            if c in u2.columns:
                u2[c] = pd.to_numeric(u2[c], errors="coerce")
    
        # melt with pandas instead of Altair transform_fold (avoids type issues)
        u2_long = u2.melt(
            id_vars=["MONTH"],           # your SQL aliases week as MONTH
            value_vars=["ACTIVE_USERS", "NEW_USERS"],
            var_name="metric",
            value_name="value",
        ).sort_values("MONTH")
    
        line_users = (
            alt.Chart(u2_long)
            .mark_line()
            .encode(
                x=alt.X("MONTH:T", title="Month"),
                y=alt.Y("value:Q", title="Active and new users"),
                color=alt.Color("metric:N", legend=alt.Legend(orient="top")),
                tooltip=[
                    alt.Tooltip("MONTH:T", title="Week"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("value:Q", title="Value", format=",.0f"),
                ],
            )
        )
        st.altair_chart(line_users, use_container_width=True)
        download_btn(u2, "⬇️ Download", "users_over_time.csv")
    else:
        st.info("No weekly users time series available.")


    # 2) Region snapshot (latest week)
    st.markdown("### Users by region — latest complete week")
    u3 = q(SQL_USERS_BY_REGION)  # columns: Week, Geographical Region, Active Accounts
    if u3 is not None and not u3.empty:
        latest_wk = u3["Week"].max()
        snap = u3[u3["Week"] == latest_wk].copy()
        snap["Active Accounts"] = pd.to_numeric(snap["Active Accounts"], errors="coerce")
        bar = (
            alt.Chart(snap.sort_values("Active Accounts", ascending=False))
            .mark_bar()
            .encode(
                y=alt.Y("Geographical Region:N", sort="-x", title="Region"),
                x=alt.X("Active Accounts:Q", title="Active accounts"),
                tooltip=["Geographical Region", alt.Tooltip("Active Accounts:Q", format=",.0f")],
            )
        )
        st.altair_chart(bar, use_container_width=True)
        download_btn(snap, "⬇️ Download snapshot (CSV)", "users_by_region_latest.csv")
    else:
        st.info("No region snapshot available.")

    # 3) Region evolution (stacked area)
    st.markdown("### Users by region — weekly evolution")
    u4 = q(SQL_USERS_BY_REGION_OVER_TIME)  # columns: Week, Geographical Region, Accounts Created
    if u4 is not None and not u4.empty:
        u4["Accounts Created"] = pd.to_numeric(u4["Accounts Created"], errors="coerce")
        area = (
            alt.Chart(u4.sort_values("Week"))
            .mark_area(opacity=0.8)
            .encode(
                x=alt.X("Week:T", title="Week"),
                y=alt.Y("Accounts Created:Q", title="Accounts created"),
                color=alt.Color("Geographical Region:N", legend=alt.Legend(orient="top")),
                tooltip=["Week", "Geographical Region", alt.Tooltip("Accounts Created:Q", format=",.0f")],
            )
        )
        st.altair_chart(area, use_container_width=True)
        download_btn(u4, "⬇️ Download evolution (CSV)", "users_by_region_over_time.csv")
    else:
        st.info("No regional evolution series available.")


# ---------- NFTs ----------
with tabs[4]:
    st.subheader("NFT sales — numbers & trends")

    # Over-time dataset already contains weekly aggregated numbers
    n2 = q(SQL_NFT_SALES_OVER_TIME)   # columns: WEEK, SALES, NFT_BUYERS, VOLUME, ACTIVE_COLLECTIONS, ...
    if n2 is None or n2.empty:
        st.info("No NFT data available.")
        st.stop()

    n2 = n2.sort_values("WEEK")
    latest, prev = latest_prev(n2, "WEEK")

    # KPI tiles (compute deltas from over-time to keep numeric types)
    c1, c2, c3, c4 = st.columns(4)
    if latest is not None and prev is not None:
        sales      = int(latest["SALES"].iloc[0])
        sales_prev = int(prev["SALES"].iloc[0])
        buyers      = int(latest["NFT_BUYERS"].iloc[0])
        buyers_prev = int(prev["NFT_BUYERS"].iloc[0])
        vol       = float(latest["VOLUME"].iloc[0])
        vol_prev  = float(prev["VOLUME"].iloc[0])
        colls      = int(latest["ACTIVE_COLLECTIONS"].iloc[0])
        colls_prev = int(prev["ACTIVE_COLLECTIONS"].iloc[0])

        c1.metric("Weekly NFT sales", f"{sales:,}", delta=f"{sales-sales_prev:+,}")
        c2.metric("NFT buyers", f"{buyers:,}", delta=f"{buyers-buyers_prev:+,}")
        c3.metric("Sales volume (USD)", f"${vol:,.0f}", delta=f"${(vol-vol_prev):+,.0f}")
        c4.metric("Active collections", f"{colls:,}", delta=f"{colls-colls_prev:+,}")

    st.markdown("### Weekly trend — sales & buyers")
    df_sb = n2[["WEEK","SALES","NFT_BUYERS"]].rename(columns={"NFT_BUYERS":"BUYERS"})
    line_sb = alt.Chart(df_sb.melt("WEEK", var_name="metric", value_name="value")).mark_line().encode(
        x="WEEK:T", y="value:Q", color=alt.Color("metric:N", legend=alt.Legend(orient="bottom")),
        tooltip=["WEEK","metric","value"]
    )
    st.altair_chart(line_sb, use_container_width=True)

    st.markdown("### Weekly trend — volume (USD)")
    bar_vol = alt.Chart(n2).mark_bar().encode(
        x="WEEK:T", y=alt.Y("VOLUME:Q", title="USD"),
        tooltip=["WEEK", alt.Tooltip("VOLUME:Q", format=",.0f")]
    )
    st.altair_chart(bar_vol, use_container_width=True)

    st.markdown("### Weekly trend — active collections")
    line_coll = alt.Chart(n2).mark_line().encode(
        x="WEEK:T", y="ACTIVE_COLLECTIONS:Q",
        tooltip=["WEEK","ACTIVE_COLLECTIONS"]
    )
    st.altair_chart(line_coll, use_container_width=True)

    download_btn(n2, "⬇️ Download data", "nft_sales_over_time.csv")


# ---------- Contracts ----------
with tabs[5]:
    st.subheader("Weekly Contract Numbers")

    # --- KPIs ---
    cnums = q(SQL_CONTRACTS_NUMBERS)  # cols: CURRENT_WEEK, ACTIVE_CONTRACTS, PREVIOUS_WEEK_ACTIVE_CONTRACTS,
                                      #       NEW_CONTRACTS, TOTAL_CONTRACTS, PREVIOUS_WEEK_NEW_CONTRACTS, PCT_DIFF
    if cnums is not None and not cnums.empty:
        row = cnums.iloc[0]
        active      = to_int_safe(row.get("ACTIVE_CONTRACTS"))
        prev_active = to_int_safe(row.get("PREVIOUS_WEEK_ACTIVE_CONTRACTS"))
        new_ctrs    = to_int_safe(row.get("NEW_CONTRACTS"))
        total_ctrs  = to_int_safe(row.get("TOTAL_CONTRACTS"))
        pct_diff    = float(row.get("PCT_DIFF")) if pd.notna(row.get("PCT_DIFF")) else None
        delta_active = (active - prev_active) if (active is not None and prev_active is not None) else None

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Active contracts", f"{active:,}" if active is not None else "—")
        k2.metric("Difference vs past week", f"{pct_diff:.2f}%" if pct_diff is not None else "—")
        k3.metric("New contracts", f"{new_ctrs:,}" if new_ctrs is not None else "—")
        k4.metric("Total unique contracts", f"{total_ctrs:,}" if total_ctrs is not None else "—")

        # Short auto-summary like your card text
        bullets = []
        if pct_diff is not None:
            bullets.append(
                f"**Weekly difference**: {pct_diff:+.2f}% vs last week"
                + (f" (Δ {delta_active:+,} active)" if delta_active is not None else "")
                + "."
            )
        if new_ctrs is not None and total_ctrs is not None:
            bullets.append(f"**Unique contracts** increased to **{total_ctrs:,}**.")
        if active is not None:
            bullets.append(f"**Current active** contracts this week: **{active:,}**.")
        if bullets:
            st.markdown(
                "1. " + "\n2. ".join(bullets) + "\n\n_In summary: Flow contracts activity and deployment momentum shown below._"
            )

        download_btn(cnums, "⬇️ Download KPIs (CSV)", "contracts_numbers.csv")

    # --- WoW active contracts (bars) ---
    st.markdown("### WoW active contracts")
    c2 = q(SQL_CONTRACTS_ACTIVE_OVER_TIME)  # cols: DATE, ACTIVE_CONTRACTS
    if c2 is not None and not c2.empty:
        c2 = c2.sort_values("DATE").copy()
        c2["ACTIVE_CONTRACTS"] = pd.to_numeric(c2["ACTIVE_CONTRACTS"], errors="coerce")
        bars_active = (
            alt.Chart(c2)
            .mark_bar()
            .encode(
                x=alt.X("DATE:T", title="DATE"),
                y=alt.Y("ACTIVE_CONTRACTS:Q", title="Active contracts"),
                tooltip=["DATE", alt.Tooltip("ACTIVE_CONTRACTS:Q", format=",.0f")],
            )
        )
        st.altair_chart(bars_active, use_container_width=True)
        download_btn(c2, "⬇️ Download (CSV)", "contracts_active_over_time.csv")
    else:
        st.info("No active-contracts series available.")

    # --- WoW new contracts by source (stacked) + cumulative unique ---
    st.markdown("### WoW new contracts (by source) & cumulative unique")
    
    c3 = q(SQL_CONTRACTS_NEW_OVER_TIME)
    if c3 is not None and not c3.empty:
        # normalize column access (Snowflake usually uppercases, but be safe)
        up = {c.upper(): c for c in c3.columns}
        dcol = up.get("DATE")
        scol = up.get("SOURCE")
        ncol = up.get("NEW_CONTRACTS")
        ucol = up.get("UNIQUE_CONTRACTS")  # per source (may not be used)
    
        if not all([dcol, scol, ncol]):
            st.warning(f"Unexpected schema for contracts_new_over_time: {list(c3.columns)}")
        else:
            # coerce types & clean
            c3 = c3.copy()
            c3[dcol] = pd.to_datetime(c3[dcol], errors="coerce")
            c3[ncol] = pd.to_numeric(c3[ncol], errors="coerce")
            if ucol in c3:
                c3[ucol] = pd.to_numeric(c3[ucol], errors="coerce")
            c3 = c3.dropna(subset=[dcol, ncol])
    
            if c3.empty:
                st.info("No new-contracts rows after cleaning.")
            else:
                c3 = c3.sort_values(dcol)

                # palette for the "SOURCE" field
                source_color = alt.Color(
                    f"{scol}:N",
                    scale=alt.Scale(
                        # make sure these labels match your SOURCE values
                        domain=["COA EVM Contract", "Non-COA EVM Contract", "Cadence"],
                        range =[ "#7BC96F",           "#9CA3AF",              "#60A5FA" ]  # light green, grey, blue
                    ),
                    legend=alt.Legend(orient="top", title="SOURCE"),
                )
                
                bars_new = (
                    alt.Chart(c3)
                      .mark_bar()
                      .encode(
                          x=alt.X(f"{dcol}:T", title="DATE"),
                          y=alt.Y(f"{ncol}:Q", title="# of new contracts", scale=alt.Scale(zero=True)),
                          color=source_color,   # <— use the custom palette
                          tooltip=[
                              alt.Tooltip(f"{dcol}:T", title="Date"),
                              alt.Tooltip(f"{scol}:N", title="Source"),
                              alt.Tooltip(f"{ncol}:Q", title="New", format=",.0f"),
                          ],
                      )
                )
    
                # cumulative unique across all sources (client-side)
                total_unique = (
                    c3.groupby(dcol, as_index=False)[ncol].sum()
                      .assign(UNIQUE_TOTAL=lambda d: d[ncol].cumsum())
                )
                line_unique = (
                    alt.Chart(total_unique)
                      .mark_line(strokeWidth=2)
                      .encode(
                          x=alt.X(f"{dcol}:T"),
                          y=alt.Y("UNIQUE_TOTAL:Q",
                                  axis=alt.Axis(title="Cumulative unique contracts", orient="right"),
                                  scale=alt.Scale(zero=True)),
                          tooltip=[alt.Tooltip(f"{dcol}:T", title="Date"),
                                   alt.Tooltip("UNIQUE_TOTAL:Q", title="Unique (cum.)", format=",.0f")]
                      )
                )
    
                st.altair_chart(alt.layer(bars_new, line_unique).resolve_scale(y="independent"),
                                use_container_width=True)
                download_btn(c3, "⬇️ Download (CSV)", "contracts_new_over_time.csv")
    else:
        st.info("No new-contracts series available.")



# ---------- Prices & Tokens ----------
with tabs[6]:
    st.subheader("$FLOW — last week (hourly)")

    p1 = q(SQL_FLOW_PRICE_WEEK).copy()
    if p1 is None or p1.empty:
        st.info("No price data returned.")
    else:
        # 1) normalize columns
        p1.columns = [str(c).upper() for c in p1.columns]
        ts_col = next((c for c in p1.columns if c in ("RECORDED_HOUR","HOUR","TIMESTAMP","DATE")), None)
        price_col = next((c for c in p1.columns if c in ("FLOW_PRICE","PRICE_USD","PRICE")), None)

        if ts_col is None or price_col is None:
            st.error(f"Could not find required columns. Have: {list(p1.columns)}")
        else:
            # 2) coerce types & clean
            p1[ts_col] = pd.to_datetime(p1[ts_col], errors="coerce")
            p1[price_col] = pd.to_numeric(p1[price_col], errors="coerce")
            p1 = p1.dropna(subset=[ts_col, price_col]).sort_values(ts_col)
            if p1.empty:
                st.info("Price series is empty after cleaning.")
            else:
                # 3) moving average + dynamic domain (no checkbox)
                p1["MA_6H"] = p1[price_col].rolling(6, min_periods=1).mean()
                ymax = float(p1[price_col].max())
                domain = [0.3, 0.5] if ymax <= 1.0 else [0, round(ymax * 1.1, 4)]

                base = alt.Chart(p1).properties(height=340)
                line_price = base.mark_line().encode(
                    x=alt.X(f"{ts_col}:T", title="Hour"),
                    y=alt.Y(f"{price_col}:Q", title="Price (USD)",
                            scale=alt.Scale(domain=domain, clamp=True)),
                    tooltip=[alt.Tooltip(f"{ts_col}:T", title="Hour"),
                             alt.Tooltip(f"{price_col}:Q", title="Price", format="$.4f")],
                )
                # add faint points so something is always visible even if the line is super flat
                pts = base.mark_point(opacity=0.4, size=18).encode(
                    x=f"{ts_col}:T", y=f"{price_col}:Q"
                )
                line_ma = base.mark_line(strokeDash=[4, 2]).encode(
                    x=f"{ts_col}:T",
                    y="MA_6H:Q",
                    tooltip=[alt.Tooltip("MA_6H:Q", title="MA 6h", format="$.4f")],
                )

                st.altair_chart(alt.layer(line_price, pts, line_ma), use_container_width=True)

                # 4) KPIs
                last = float(p1[price_col].iloc[-1])
                first = float(p1[price_col].iloc[0])
                pct_7d = ((last - first) / first * 100.0) if first else None
                m1, m2, m3 = st.columns(3)
                m1.metric("Current $FLOW", f"${last:.4f}", f"{pct_7d:+.2f}% vs 7d open" if pct_7d is not None else None)
                m2.metric("Week high", f"${p1[price_col].max():.4f}")
                m3.metric("Week low", f"${p1[price_col].min():.4f}")

                # 5) daily summary + downloads
                day = (
                    p1.assign(DAY=p1[ts_col].dt.date)
                      .groupby("DAY", as_index=False)
                      .agg(open=(price_col, "first"), close=(price_col, "last"),
                           high=(price_col, "max"), low=(price_col, "min"),
                           avg=(price_col, "mean"), std=(price_col, "std"),
                           ticks=(price_col, "count"))
                )
                st.dataframe(
                    day,
                    use_container_width=True,
                    column_config={
                        "DAY": st.column_config.DateColumn("Day"),
                        "open": st.column_config.NumberColumn("Open", format="$%.4f"),
                        "close": st.column_config.NumberColumn("Close", format="$%.4f"),
                        "high": st.column_config.NumberColumn("High", format="$%.4f"),
                        "low": st.column_config.NumberColumn("Low", format="$%.4f"),
                        "avg": st.column_config.NumberColumn("Avg", format="$%.4f"),
                        "std": st.column_config.NumberColumn("Volatility (σ)", format="%.4f"),
                        "ticks": st.column_config.NumberColumn("# points", format="%d"),
                    }
                )
                download_btn(day, "⬇️ Download daily summary", "flow_price_daily.csv")
                download_btn(p1.rename(columns={ts_col:"RECORDED_HOUR", price_col:"FLOW_PRICE"}),
                             "⬇️ Download hourly series", "flow_price_week.csv")


    st.subheader("Top tokens weekly movers")
    p2 = q(SQL_TOKENS_WEEKLY_MOVERS).copy()
    if not p2.empty:
        # normalizar nombres/formatos
        # (tu SQL devuelve avg_deviation_weekly_pct como proporción; convierto a %)
        pct_col = [c for c in p2.columns if "DEVIATION" in c.upper()][0]
        p2.rename(columns={pct_col: "WEEKLY_DEV_PCT"}, inplace=True)
        p2["WEEKLY_DEV_PCT"] = pd.to_numeric(p2["WEEKLY_DEV_PCT"], errors="coerce") * 100.0
        if "AVG_PRICE" in p2.columns:
            p2.rename(columns={"AVG_PRICE": "AVG_PRICE_USD"}, inplace=True)

        st.dataframe(
            p2.sort_values("WEEKLY_DEV_PCT", ascending=False).head(50),
            use_container_width=True,
            column_config={
                "TOKEN": st.column_config.TextColumn("Token"),
                "AVG_PRICE": st.column_config.NumberColumn("Avg price", format="$%.4f")
                              if "AVG_PRICE" in p2.columns else None,
                "AVG_PRICE_USD": st.column_config.NumberColumn("Avg price (USD)", format="$%.4f")
                                 if "AVG_PRICE_USD" in p2.columns else None,
                "WEEKLY_DEV_PCT": st.column_config.NumberColumn("Deviation (7d)", format="%.2f%%"),
            }
        )
        download_btn(p2, "⬇️ Download movers (CSV)", "tokens_weekly_movers.csv")
    else:
        st.info("No token movers data.")


# ---------- Conclusions ----------
with tabs[7]:
    st.subheader("Conclusions")
    html = generate_weekly_insights_html(q)   # <- la funció HTML
    st.markdown(html, unsafe_allow_html=True) # <- només aquesta línia per renderitzar



st.caption(f"Last updated: {now_local()}")
