# State of Flow — Growth Dashboard (Streamlit)

A polished Streamlit dashboard to track Flow’s growth across **Transactions, Active Accounts, Accounts Created, New vs Active vs Total Accounts, Fees, Supply, Contracts, and Contract Deployers**—with a global **period selector** (All time, 1y, 3m, 1m, 1w, 24h).

🟢 **Cadence** and 🔵 **EVM** views are consistently color-coded across all charts for instant readability.

---

## Features

* **8 tabs** with curated KPIs + pro charts:

  * **Transactions:** totals, time series, stacked bars w/ rolling average, and Cadence vs EVM split.
  * **Active Accounts:** totals + time series (Cadence, EVM, combined).
  * **Accounts Created:** totals + time series with cumulative and rolling avg.
  * **New / Active / Total Accounts:** aligned daily trends (stacked bars + overlay lines).
  * **Fees:** total + time series (sum & avg fee) with rolling average.
  * **Supply:** latest supply split + supply breakdown over time.
  * **Contracts:** new contracts (Cadence, EVM COA, EVM EOA), distribution (100% stacked), + weekly by type.
  * **Contract Deployers:** new + cumulative deployers over time.
* **Global Period Selector** (default: `last_3_months`):

  * `all_time`, `last_year`, `last_3_months`, `last_month`, `last_week`, `last_24h`
* **Clean UX**: stacked bars (metric) + line (rolling avg), smart tooltips, side-by-side key cards.
* **Footer** (shared across tabs): “State of Flow Blockchain” + official links.

---

## Data Sources

* **Snowflake (Flipside data, volked to Snowflake)**

  * `flow.core.fact_transactions`
  * `flow.core_evm.fact_transactions`
  * `flow.core.fact_events`
  * `flow.core.ez_transaction_actors`
  * `flow.gov.ez_staking_actions`
* **CoinGecko API** (via `livequery.live.udf_api`) for historical **price/market cap/volume** (used in supply module).
* **Note on DeFiLlama**: Not used directly (no `external` or `livequery.defillama` schema available). Supply split leverages Flow staking actions + CoinGecko price instead.

---

## Quickstart

### 1) Requirements

* Python ≥ 3.10
* Snowflake account + read access to the listed schemas/tables

### 2) Install

```bash
pip install -r requirements.txt
```

### 3) Configure Snowflake

Create `.streamlit/secrets.toml` (Streamlit Cloud) **or** local env vars.

**Example `secrets.toml`:**

```toml
[snowflake]
ACCOUNT   = "xxxx-xy123"
USER      = "YOUR_USER"
PASSWORD  = "YOUR_PASSWORD"
ROLE      = "YOUR_ROLE"
WAREHOUSE = "COMPUTE_WH"
DATABASE  = "FLOW"
SCHEMA    = "CORE"
```

### 4) Run

```bash
streamlit run app.py
```

---

## Architecture & Helpers

* **SQL templates** include `{{Period}}`. We render them with:

  ```python
  def render_sql(sql: str, period_key: str) -> str:
      return sql.replace("{{Period}}", period_key)
  ```
* **Query helper** (independent of other defs, simple pass-through):

  ```python
  def qp(sql: str) -> pd.DataFrame:
      return run_query(sql)
  ```
* **Period mapping** (UI → token used in SQL):

  * All time → `all_time`
  * 1 year → `last_year`
  * **3 months (default)** → `last_3_months`
  * 1 month → `last_month`
  * 1 week → `last_week`
  * 24h → `last_24h`

---

## Design System

* **Colors**

  * Cadence: `#22c55e` (green)
  * EVM: `#3b82f6` (blue)
  * Bars default: soft slate `#94a3b8` with overlay line accents.
* **Chart patterns**

  * **Metric bars stacked** (e.g., counts) with **overlay line** for rolling averages.
  * **100% stacked area** for distribution shares (e.g., Cadence vs EVM).
  * **Side-by-side KPI cards** for quick comparison.

---

## Tab Details & Methodology (Short)

### Transactions

* **Totals**: EVM, Cadence, Combined.
* **Over Time**: bars = transactions per period; line = 4-point rolling avg; cumulative total included where useful.
* **Split**: 100% stacked area (Cadence vs EVM) + pie share.

**Methodology**
“Calculates daily transaction counts for Flow Cadence (`flow.core.fact_transactions`) and Flow EVM (`flow.core_evm.fact_transactions`), compares with previous periods, and derives cumulative totals and rolling averages.”

### Active Accounts

* **KPI**: Active Accounts \[EVM + Cadence] (unique addresses/actors in period).
* **Time series**: Cadence, EVM, and combined with rolling avg (combined).

**Methodology**
“Counts distinct active users per day from `flow.core.ez_transaction_actors` (Cadence) and `flow.core_evm.fact_transactions` (EVM). Aggregates and rolls up for trend analysis.”

### Accounts Created

* **KPI**: Accounts Created \[EVM + Cadence] (first-seen logic).
* **Time series**: daily new accounts, cumulative total, and rolling average.

**Methodology**
“First appearance per actor/address defines ‘new account’. Outputs daily totals, cumulative growth, and 4-step rolling average.”

### New / Active / Total Accounts

* **Unified view**: Daily new, active (combined), cumulative totals; rolling averages overlayed.

**Methodology**
“Combines first-seen (new) and per-day active counts, aggregated across Cadence + EVM to compare acquisition vs engagement.”

### Fees

* **KPI**: Fee (\$FLOW) \[EVM + Cadence] (sum + avg).
* **Over Time**: daily fee totals and average fee; with rolling average.

**Methodology**
“Sums fees from EVM (`tx_fee`) and Cadence (`FeesDeducted` events from `A.f919ee77447b7497.FlowFees`) and computes average fees and rolling averages.”

### Supply

* **KPIs**: Total Supply Breakdown (latest), Staked Token (Locked).
* **Over Time**: supply split (staked/locked, liquid staking proxy, non-staked locked, unstaked circulating, free circulating).

**Methodology**
“Uses CoinGecko price/market cap (via `livequery.live.udf_api`) to derive total supply; integrates Flow staking/unstaking (`flow.gov.ez_staking_actions`) to build cumulative staked volume. Liquidity/locking buckets exclude DeFiLlama (schema not available) and rely on staking + price-derived supply.”

### Contracts

* **KPIs**: Verified Contracts (Cadence), Verified Contracts (EVM).
* **Over Time**: Contract (Cadence+EVM) deployments with rolling avg.
* **Distributions**:

  * **New contracts by type** (Cadence, EVM-COA, EVM-EOA) — **weekly** bars (left).
  * **Distribution over time** (100% stacked area) (right).

**Methodology**
“Cadence from `flow.core.fact_events`. EVM contracts via constructor signatures + `OwnershipTransferred` event hash; splits **COA** (Cadence Owned) vs **EOA** by deployer address pattern.”

### Contract Deployers

* **KPI**: Deployers (Cadence)
* **Over Time**: New & cumulative deployers (Cadence + EVM) with rolling avg.

**Methodology**
“Identifies unique deployers per day for Cadence (`AccountContractAdded`) and EVM (deployer `from_address` on constructor/creation), then aggregates.”

---

## Footer (Shown on all tabs)

**The State of Flow Blockchain**
“Flow is a layer one blockchain with a next-gen smart contract language (Cadence) and full EVM equivalence… (full descriptive text used in the app).”

**Flow Links**

* Website: [https://www.flow.com](https://www.flow.com)
* Twitter: [https://twitter.com/flow\_blockchain](https://twitter.com/flow_blockchain)
* Discord: [http://chat.onflow.org/](http://chat.onflow.org/)
* GitHub: [https://github.com/onflow](https://github.com/onflow)
* Status: [https://status.onflow.org/](https://status.onflow.org/)
* Roadmap: [https://flow.com/flow-roadmap](https://flow.com/flow-roadmap)
* Forum: [https://forum.flow.com/](https://forum.flow.com/)
* Primer: [https://flow.com/primer](https://flow.com/primer)
* Primer (PDF)

---

## Troubleshooting

* **`UnboundLocalError: alt`**
  You’ve shadowed the `altair` import with a local variable. Rename any local `alt` or import as `_alt` inside the function.

* **`TypeError: Decimal * float`**
  Cast before math:

  ```python
  df["PCT_CHANGE_14D"] = (df["MA_28D"].astype(float) - df["MA_28D_LAG"].astype(float)) / df["MA_28D_LAG"].astype(float) * 100.0
  ```

* **Segmented control missing**
  Use `st.radio` or `st.selectbox` instead of `st.segmented_control`.

* **“No data” KPIs**
  Usually a filter/window issue. Ensure:

  * `render_sql(sql, period_key)` is applied before `qp(sql)`.
  * Time windows use `< current period bucket` (e.g., `day < DATE_TRUNC('day', current_date)`).
  * Combine Cadence/EVM with **UNION ALL** where appropriate.

* **Weekly tables starting previous week (Mon updates)**
  Generate ranges with:

  ```sql
  DATE_TRUNC('week', CURRENT_DATE) - 7  as start_of_prev_week
  DATE_TRUNC('week', CURRENT_DATE)      as start_of_this_week (exclusive)
  ```

  Then iterate backwards for N weeks using `DATEADD('week', -n, ...)`.

* **Snowflake auth**
  Verify role/warehouse/database/schema and that the tables listed under **Data Sources** are accessible.

---

## Contributing

* Keep the **Cadence/EVM color system** consistent.
* Add new metrics behind the global period selector (`{{Period}}`).
* Favor **stacked bars + overlay line** for primary trends.

---

## License & Attribution

* Data courtesy of **Flipside** datasets hosted on **Snowflake**; price/market data from **CoinGecko** via API.
