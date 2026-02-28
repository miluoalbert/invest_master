"""
投资组合分析仪表盘 (Streamlit App)

启动：
    streamlit run app.py

入口页面展示：
  • 资产配置总览（汇总卡片）
  • 大类资产分布（股票/债券/商品/现金...）→ 饼图 + 明细表
  • 货币分布（CNY / USD / HKD ...）→ 饼图 + 明细表
  • 完整持仓明细表
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date

from src.database import Database
from src.services import PortfolioService, AnalysisService

# ── 页面配置 ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="投资组合分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 全局样式 ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="metric-container"] { background-color: #1e2530; border-radius: 8px; padding: 12px; }
    .warn-badge { color: #f39c12; font-size: 12px; }
    div.stDataFrame { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# 辅助函数
# ============================================================

@st.cache_data(ttl=300, show_spinner="正在加载投资组合数据...")
def load_portfolio(as_of_date_str: str | None) -> pd.DataFrame:
    """
    从数据库加载完整投资组合 DataFrame（TTL 5 分钟缓存）。
    as_of_date_str 用 str 是因为 cache_data 要求可哈希参数。
    """
    as_of_date = (
        date.fromisoformat(as_of_date_str) if as_of_date_str else None
    )
    db = Database()
    with db.session() as conn:
        svc = PortfolioService(conn, base_currency='CNY')
        df  = svc.get_full_portfolio(as_of_date=as_of_date)
    return df


def fmt_cny(value: float) -> str:
    """格式化为人民币字符串，带万/亿单位"""
    if abs(value) >= 1e8:
        return f"¥ {value/1e8:,.2f} 亿"
    elif abs(value) >= 1e4:
        return f"¥ {value/1e4:,.2f} 万"
    else:
        return f"¥ {value:,.2f}"


def make_pie_chart(
    df: pd.DataFrame,
    values_col: str,
    names_col: str,
    title: str,
    color_map: dict | None = None,
) -> go.Figure:
    """通用饼图生成器（使用 plotly express donut 样式）"""
    fig = px.pie(
        df,
        values=values_col,
        names=names_col,
        title=title,
        hole=0.42,
        color=names_col,
        color_discrete_map=color_map,
    )
    fig.update_traces(
        textposition='outside',
        textinfo='percent+label',
        hovertemplate='%{label}<br>¥ %{value:,.0f}<br>%{percent}',
    )
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation='v', x=1.02),
        margin=dict(t=50, b=20, l=20, r=20),
        height=380,
    )
    return fig


# ── 大类资产颜色预设 ──────────────────────────────────────────────────────────
ASSET_CLASS_COLORS = {
    '股票'  : '#4e88e5',
    '债券'  : '#4ec98c',
    '商品'  : '#f4b942',
    'REITs' : '#9b59b6',
    '现金'  : '#95a5a6',
    '另类'  : '#e74c3c',
    '混合'  : '#1abc9c',
}

CURRENCY_COLORS = {
    '人民币 (CNY)': '#e74c3c',
    '美元 (USD)'  : '#3498db',
    '港币 (HKD)'  : '#f39c12',
    '欧元 (EUR)'  : '#2ecc71',
    '英镑 (GBP)'  : '#9b59b6',
    '日元 (JPY)'  : '#e67e22',
}


# ============================================================
# 页面主体
# ============================================================

def render_sidebar() -> str | None:
    """渲染侧边栏，返回 as_of_date_str（None = 当前）"""
    with st.sidebar:
        st.title("⚙️ 控制面板")
        st.divider()

        use_custom_date = st.toggle("使用历史日期快照", value=False)
        as_of_date_str  = None

        if use_custom_date:
            selected = st.date_input("截止日期", value=date.today())
            as_of_date_str = str(selected)
            st.caption(f"📅 展示截至 {selected} 的持仓状态")
        else:
            st.caption("📅 展示当前最新持仓状态")

        st.divider()
        st.markdown("**折算基础货币**")
        st.markdown("🇨🇳 人民币 (CNY)")
        st.caption("汇率来源：数据库 → fallback 手动汇率")

        if st.button("🔄 刷新数据", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    return as_of_date_str


def render_summary_cards(summary: dict):
    """渲染顶部汇总卡片"""
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💼 投资组合总市值", fmt_cny(summary['total_value']))
    c2.metric("📈 证券持仓市值",    fmt_cny(summary['security_value']))
    c3.metric("💵 现金余额",        fmt_cny(summary['cash_value']))
    c4.metric("🗂️ 持仓标的数",      f"{summary['position_count']} 个")

    if summary['price_warn_count'] > 0:
        st.warning(
            f"⚠️ 有 **{summary['price_warn_count']}** 个持仓未获取到市场行情，"
            "已使用**均摊成本价**代替市值估算（偏差仅体现在无行情品种）。"
            "请在 `tb_market_data` 中补充最新行情以获得精准市值。",
            icon="📋",
        )


def render_asset_class_section(analysis: AnalysisService):
    """渲染大类资产分布板块"""
    df = analysis.get_asset_class_distribution()
    if df.empty:
        st.info("暂无持仓数据")
        return

    st.subheader("📊 大类资产分布")
    col_chart, col_table = st.columns([1, 1], gap="large")

    with col_chart:
        fig = make_pie_chart(
            df,
            values_col='value_cny',
            names_col='label',
            title='大类资产（折算 CNY）',
            color_map=ASSET_CLASS_COLORS,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.markdown("&nbsp;", unsafe_allow_html=True)  # 垂直对齐辅助
        display_df = df[['label', 'value_cny', 'weight_pct', 'count']].copy()
        display_df.columns = ['大类', 'CNY 市值', '占比 %', '持仓数']
        display_df['CNY 市值'] = display_df['CNY 市值'].apply(fmt_cny)
        display_df['占比 %']   = display_df['占比 %'].apply(lambda x: f"{x:.2f}%")
        st.dataframe(
            display_df, use_container_width=True, hide_index=True, height=300
        )

        # 价格来源提示
        if 'has_cost_only' in df.columns and df['has_cost_only'].any():
            st.caption("🔸 部分分类含用成本价估值的持仓")


def render_currency_section(analysis: AnalysisService):
    """渲染货币分布板块"""
    df = analysis.get_currency_distribution()
    if df.empty:
        st.info("暂无持仓数据")
        return

    st.subheader("💱 货币资产分布")
    col_chart, col_table = st.columns([1, 1], gap="large")

    with col_chart:
        fig = make_pie_chart(
            df,
            values_col='value_cny',
            names_col='label',
            title='货币分布（折算 CNY）',
            color_map=CURRENCY_COLORS,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.markdown("&nbsp;", unsafe_allow_html=True)
        display_df = df[['label', 'value_cny', 'weight_pct', 'count']].copy()
        display_df.columns = ['货币', 'CNY 市值', '占比 %', '资产数']
        display_df['CNY 市值'] = display_df['CNY 市值'].apply(fmt_cny)
        display_df['占比 %']   = display_df['占比 %'].apply(lambda x: f"{x:.2f}%")
        st.dataframe(
            display_df, use_container_width=True, hide_index=True, height=300
        )


def render_holdings_detail(portfolio_df: pd.DataFrame):
    """渲染持仓明细可展开表格"""
    with st.expander("📋 持仓明细（展开查看）", expanded=False):
        # 筛选控件
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        types       = ['全部'] + portfolio_df['type'].unique().tolist()
        classes     = ['全部'] + portfolio_df['asset_class'].unique().tolist()
        currencies  = ['全部'] + portfolio_df['currency'].unique().tolist()

        sel_type    = filter_col1.selectbox("类型", types, key='det_type')
        sel_class   = filter_col2.selectbox("大类", classes, key='det_class')
        sel_curr    = filter_col3.selectbox("货币", currencies, key='det_curr')

        df = portfolio_df.copy()
        if sel_type  != '全部': df = df[df['type']        == sel_type]
        if sel_class != '全部': df = df[df['asset_class'] == sel_class]
        if sel_curr  != '全部': df = df[df['currency']    == sel_curr]

        # 展示列整理
        val_col = [c for c in df.columns if c.startswith('value_cny')][0]
        show_df = df[[
            'ticker', 'name', 'asset_class', 'currency', 'qty',
            'avg_cost', 'latest_price', 'price_source', val_col
        ]].copy()
        show_df.columns = [
            '代码', '名称', '大类', '货币', '持仓量',
            '均摊成本', '最新价格', '价格来源', 'CNY 市值'
        ]
        show_df['CNY 市值'] = show_df['CNY 市值'].apply(lambda x: round(x, 2))
        show_df['均摊成本'] = show_df['均摊成本'].apply(lambda x: round(x, 4) if pd.notna(x) else '-')
        show_df['最新价格'] = show_df['最新价格'].apply(lambda x: round(x, 4) if pd.notna(x) else '-')

        st.dataframe(
            show_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'CNY 市值': st.column_config.NumberColumn(format="¥%,.0f"),
            }
        )
        st.caption(f"共 {len(show_df)} 行 | 🔸 价格来源=cost 表示使用成本价估值")


# ============================================================
# 主函数
# ============================================================

def main():
    st.title("📊 投资组合分析仪表盘")
    st.caption("资产配置流派 | 折算基础货币：人民币 (CNY)")
    st.divider()

    as_of_date_str = render_sidebar()

    # ── 加载数据 ─────────────────────────────────────────────────
    try:
        portfolio_df = load_portfolio(as_of_date_str)
    except Exception as e:
        st.error(f"❌ 数据库连接失败：{e}\n\n请检查 `conf/database.ini` 配置。")
        st.stop()

    if portfolio_df.empty:
        st.warning("📭 当前无持仓数据，请先通过 `main_imports.py` 导入交易记录。")
        st.stop()

    analysis = AnalysisService(portfolio_df, base_currency='CNY')

    # ── 汇总卡片 ─────────────────────────────────────────────────
    render_summary_cards(analysis.get_summary())
    st.divider()

    # ── 双列布局：大类分布 + 货币分布 ────────────────────────────
    render_asset_class_section(analysis)
    st.divider()
    render_currency_section(analysis)
    st.divider()

    # ── 持仓明细 ─────────────────────────────────────────────────
    render_holdings_detail(portfolio_df)


if __name__ == "__main__":
    main()
