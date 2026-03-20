"""
Pharma Patent Cliff — Streamlit Dashboard
Reads from Snowflake MART schema via st.secrets["snowflake"].

Pages:
  1. Revenue Cliff       — market cap decline by drug/company
  2. Drawdown Analysis   — price peak-to-trough + recovery timeline
  3. Pipeline Readiness  — Phase III coverage score for all 8 companies

Deployment: Streamlit Community Cloud
  - Connects to GitHub repo automatically
  - Credentials stored in .streamlit/secrets.toml (gitignored)
  - Cold start: ~30s after inactivity (documented in README)
"""

import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Pharma Patent Cliff",
    page_icon="💊",
    layout="wide",
)

# ── Snowflake Connection ───────────────────────────────────────────────────────

@st.cache_resource(ttl=3600)
def get_snowflake_connection():
    """Cached Snowflake connection — refreshes every hour."""
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        warehouse=cfg.get("warehouse", "COMPUTE_WH"),
        database=cfg.get("database", "PHARMA_CLIFF"),
        schema="MART",
        role=cfg.get("role", "SYSADMIN"),
    )


@st.cache_data(ttl=3600)
def query_snowflake(sql: str) -> pd.DataFrame:
    """Run a query and return a DataFrame. Results cached for 1 hour."""
    conn = get_snowflake_connection()
    return pd.read_sql(sql, conn)


# ── Data Loaders ───────────────────────────────────────────────────────────────

def load_revenue_decline() -> pd.DataFrame:
    return query_snowflake("SELECT * FROM MART.MART_REVENUE_DECLINE ORDER BY CLIFF_YEAR, TICKER")


def load_drawdown_timeline() -> pd.DataFrame:
    return query_snowflake("SELECT * FROM MART.MART_DRAWDOWN_TIMELINE ORDER BY CLIFF_DATE, TICKER")


def load_pipeline_readiness() -> pd.DataFrame:
    return query_snowflake("SELECT * FROM MART.MART_PIPELINE_READINESS ORDER BY READINESS_SCORE DESC")


@st.cache_data(ttl=3600)
def load_data_freshness() -> dict:
    """Query MAX(_loaded_at) from each RAW table for sidebar freshness badge."""
    sql = """
        SELECT
            'orange_book'     AS source, MAX(_loaded_at) AS last_loaded FROM PHARMA_CLIFF.RAW.ORANGE_BOOK
        UNION ALL SELECT 'yfinance',        MAX(_loaded_at) FROM PHARMA_CLIFF.RAW.YFINANCE
        UNION ALL SELECT 'clinical_trials', MAX(_loaded_at) FROM PHARMA_CLIFF.RAW.CLINICAL_TRIALS
        UNION ALL SELECT 'edgar',           MAX(_loaded_at) FROM PHARMA_CLIFF.RAW.EDGAR
    """
    try:
        df = query_snowflake(sql)
        df.columns = [c.lower() for c in df.columns]
        return dict(zip(df["source"], df["last_loaded"]))
    except Exception:
        return {}


# ── Page 1: Revenue Cliff ──────────────────────────────────────────────────────

def page_revenue_cliff():
    st.header("Revenue Cliff — Market Cap Impact by Drug")
    st.caption(
        "Market cap used as revenue proxy. "
        "Actual revenue from 10-K EDGAR filings deferred to v2."
    )

    df = load_revenue_decline()
    if df.empty:
        st.warning("No revenue decline data available. Run the pipeline first.")
        return

    # Normalize column names to lowercase for reliability
    df.columns = [c.lower() for c in df.columns]

    tickers = sorted(df["ticker"].unique())
    selected = st.selectbox("Select company", tickers, index=0)
    company_df = df[df["ticker"] == selected].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric("Cliff drugs (total)", len(company_df))
    imminent = company_df[company_df["cliff_category"].isin(["imminent", "past"])]
    col2.metric("Imminent cliffs (≤2yr)", len(imminent))
    avg_decline = company_df["pct_decline"].mean()
    if pd.notna(avg_decline):
        col3.metric("Avg market cap decline", f"{avg_decline:.1f}%")

    # Market cap timeline — filter to rows with pct_decline for chart only; table keeps all
    chart_df = company_df.dropna(subset=["pct_decline"])
    if chart_df.empty:
        st.info("Decline data not yet available — cliff dates are in the future.")
    st.subheader(f"{selected} — Drug Cliff Timeline")
    fig = px.scatter(
        chart_df,
        x="cliff_year",
        y="pct_decline",
        size="market_cap_pre",
        color="cliff_category",
        hover_data=["trade_name", "ingredient", "nce_expiry_date"],
        color_discrete_map={
            "past": "#6c757d",
            "imminent": "#dc3545",
            "near_term": "#fd7e14",
            "long_term": "#28a745",
        },
        title=f"{selected} Patent Cliff Events — % Market Cap Decline",
        labels={
            "cliff_year": "Cliff Year",
            "pct_decline": "Market Cap Decline (%)",
        },
    )
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

    # Detail table
    st.subheader("Drug Detail")
    display_cols = [
        "trade_name", "ingredient", "cliff_year",
        "cliff_category", "pct_decline", "market_cap_pre", "market_cap_post",
    ]
    display_cols = [c for c in display_cols if c in company_df.columns]
    st.dataframe(
        company_df[display_cols].rename(columns={
            "trade_name": "Drug",
            "ingredient": "Active Ingredient",
            "cliff_year": "Cliff Year",
            "cliff_category": "Timing",
            "pct_decline": "Mkt Cap Decline %",
            "market_cap_pre": "Mkt Cap Pre ($)",
            "market_cap_post": "Mkt Cap Post ($)",
        }),
        use_container_width=True,
    )


# ── Page 2: Drawdown Analysis ──────────────────────────────────────────────────

def page_drawdown_analysis():
    st.header("Stock Drawdown Analysis — Price Timeline Around Cliff")

    df = load_drawdown_timeline()
    if df.empty:
        st.warning("No drawdown data available. Run the pipeline first.")
        return

    df.columns = [c.lower() for c in df.columns]

    tickers = sorted(df["ticker"].unique())
    selected = st.selectbox("Select company", tickers, key="drawdown_ticker")
    company_df = df[df["ticker"] == selected].copy()

    if company_df.empty:
        st.info(f"No drawdown events for {selected}.")
        return

    # Filter to events with measured drawdown (past cliffs only)
    chart_df = company_df.dropna(subset=["drawdown_pct"])

    col1, col2, col3, col4 = st.columns(4)
    if not chart_df.empty:
        worst_idx = chart_df["drawdown_pct"].idxmin()
        worst = chart_df.loc[worst_idx]
        col1.metric("Worst drawdown", f"{worst['drawdown_pct']:.1f}%", delta_color="inverse")
        col2.metric("Drug", worst["trade_name"])
        col3.metric("Cliff date", str(worst["cliff_date"]))
        col4.metric(
            "Days to recovery",
            f"{worst['recovery_days']:.0f}d" if pd.notna(worst["recovery_days"]) else "Not recovered",
        )
    else:
        st.info(f"No post-cliff drawdown data for {selected} — all cliff dates are in the future.")

    # Drawdown depth chart
    st.subheader("Drawdown Depth by Drug")
    fig = go.Figure()
    for _, row in chart_df.iterrows():
        cliff_date = pd.to_datetime(row["cliff_date"])
        # Simplified 3-point timeline: peak → cliff → trough → recovery
        x_dates = [cliff_date, cliff_date, cliff_date + pd.Timedelta(days=int(row["drawdown_duration_days"] or 90))]
        y_pcts = [0, 0, float(row["drawdown_pct"] or 0)]
        fig.add_trace(go.Scatter(
            x=x_dates,
            y=y_pcts,
            mode="lines+markers",
            name=row["trade_name"],
            hovertemplate=(
                f"<b>{row['trade_name']}</b><br>"
                f"Cliff: {row['cliff_date']}<br>"
                f"Drawdown: {row['drawdown_pct']:.1f}%<br>"
                f"Duration: {row['drawdown_duration_days']:.0f} days<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=f"{selected} — Price Drawdown from Cliff Date",
        xaxis_title="Date",
        yaxis_title="Price Change from Peak (%)",
        height=450,
        yaxis=dict(tickformat=".1f", ticksuffix="%"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Detail table
    st.subheader("Drawdown Events")
    display_cols = [
        "trade_name", "cliff_date", "drawdown_pct",
        "drawdown_duration_days", "recovery_date", "recovery_days",
        "market_cap_at_risk",
    ]
    display_cols = [c for c in display_cols if c in company_df.columns]
    st.dataframe(
        company_df[display_cols].rename(columns={
            "trade_name": "Drug",
            "cliff_date": "Cliff Date",
            "drawdown_pct": "Drawdown %",
            "drawdown_duration_days": "Days to Trough",
            "recovery_date": "Recovery Date",
            "recovery_days": "Days to Recovery",
            "market_cap_at_risk": "Mkt Cap at Risk ($)",
        }),
        use_container_width=True,
    )


# ── Page 3: Pipeline Readiness ─────────────────────────────────────────────────

def page_pipeline_readiness():
    st.header("Pipeline Readiness — Phase III Coverage vs Patent Cliffs")
    st.caption(
        "Readiness score = Phase III drug count / cliff drug count (bounded 0-1). "
        "Score > 1.0 means pipeline exceeds cliff exposure — bounded at 1.0 in display."
    )

    df = load_pipeline_readiness()
    if df.empty:
        st.warning("No pipeline readiness data available. Run the pipeline first.")
        return

    df.columns = [c.lower() for c in df.columns]

    # Color map
    color_map = {"green": "#28a745", "yellow": "#ffc107", "red": "#dc3545"}
    df["color"] = df["readiness_flag"].map(color_map)

    # Summary bar chart
    fig = px.bar(
        df,
        x="ticker",
        y="readiness_score",
        color="readiness_flag",
        color_discrete_map={"green": "#28a745", "yellow": "#ffc107", "red": "#dc3545"},
        hover_data=["company_name", "cliff_drug_count", "phase3_drug_count"],
        title="Pipeline Readiness Score by Company",
        labels={"ticker": "Ticker", "readiness_score": "Readiness Score (0-1)"},
    )
    fig.add_hline(y=0.5, line_dash="dash", line_color="orange", annotation_text="0.5 threshold")
    fig.add_hline(y=1.0, line_dash="dash", line_color="green", annotation_text="1.0 threshold")
    fig.update_layout(height=400, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)

    # Comparison table with color coding
    st.subheader("Full Comparison Table")

    def color_score(val):
        if val >= 1.0:
            return "background-color: #d4edda"
        elif val >= 0.5:
            return "background-color: #fff3cd"
        else:
            return "background-color: #f8d7da"

    display_df = df[["ticker", "company_name", "cliff_drug_count", "phase3_drug_count", "readiness_score", "readiness_flag"]].copy()
    display_df = display_df.rename(columns={
        "ticker": "Ticker",
        "company_name": "Company",
        "cliff_drug_count": "Cliff Drugs (≤5yr)",
        "phase3_drug_count": "Phase III Pipeline",
        "readiness_score": "Readiness Score",
        "readiness_flag": "Status",
    })

    styled = display_df.style.applymap(color_score, subset=["Readiness Score"])
    st.dataframe(styled, use_container_width=True)

    # Methodology note
    with st.expander("Methodology"):
        st.markdown("""
        **Readiness Score** = Phase III drug count / cliff drug count (bounded 0–1)

        - **Green** (≥ 1.0): Pipeline depth matches or exceeds near-term cliff exposure
        - **Yellow** (0.5–1.0): Partial pipeline coverage — some cliff risk
        - **Red** (< 0.5): Pipeline significantly under-covers upcoming cliffs

        **Cliff drugs**: NCE exclusivity expiry within 5 years (imminent + near_term + past categories)

        **Phase III pipeline**: Active, recruiting, or enrolling Phase III trials from ClinicalTrials.gov

        **Limitation**: Score uses drug count as a proxy. Revenue-weighted readiness (deferred to v2)
        would weight Keytruda differently than a smaller-revenue drug.
        """)


# ── Main Navigation ────────────────────────────────────────────────────────────

def main():
    st.title("Pharma Patent Cliff Dashboard")
    st.caption(
        "Data: FDA Orange Book (NCE exclusivity) · yFinance (stock prices) · "
        "ClinicalTrials.gov (Phase III pipeline) · Pipeline: Airflow → S3 → Snowflake → dbt"
    )

    page = st.sidebar.radio(
        "Navigate",
        ["Revenue Cliff", "Drawdown Analysis", "Pipeline Readiness"],
    )

    st.sidebar.markdown("---")

    # Data freshness badge
    freshness = load_data_freshness()
    if freshness:
        st.sidebar.markdown("**Data freshness**")
        source_labels = {
            "orange_book": "Orange Book",
            "yfinance": "yFinance",
            "clinical_trials": "Clinical Trials",
            "edgar": "EDGAR",
        }
        for source, label in source_labels.items():
            ts = freshness.get(source)
            if ts:
                ts_str = pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(ts) else "—"
            else:
                ts_str = "—"
            st.sidebar.caption(f"{label}: {ts_str}")
        st.sidebar.markdown("---")

    st.sidebar.markdown(
        "**Note:** App may take ~30s to load after inactivity "
        "(Streamlit Community Cloud cold start)."
    )
    st.sidebar.markdown(
        "[GitHub](https://github.com/ericg1212/data-engineering-portfolio) | "
        "[Project 1: AI Sharpe Premium](https://github.com/ericg1212/data-engineering-portfolio)"
    )

    if page == "Revenue Cliff":
        page_revenue_cliff()
    elif page == "Drawdown Analysis":
        page_drawdown_analysis()
    elif page == "Pipeline Readiness":
        page_pipeline_readiness()


if __name__ == "__main__":
    main()
