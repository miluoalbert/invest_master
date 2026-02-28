"""
投资组合分析服务 (AnalysisService)

职责：
  接收 PortfolioService.get_full_portfolio() 的 DataFrame，
  提供不同维度的聚合分析。

当前实现：
  • get_asset_class_distribution() ← 大类资产分布（股票/债券/商品/...）
  • get_currency_distribution()    ← 货币资产分布（CNY / USD / HKD / ...）
  • get_account_distribution()     ← 账户分布（各账户净值占比）

扩展点：
  • 加入目标权重对比 → 再平衡偏差分析
  • 加入历史市值 → 收益率时序分析
"""

import pandas as pd

# 大类资产的中文展示名
ASSET_CLASS_LABELS: dict[str, str] = {
    'EQUITY'     : '股票',
    'BOND'       : '债券',
    'COMMODITY'  : '商品',
    'REITS'      : 'REITs',
    'CASH'       : '现金',
    'ALTERNATIVE': '另类',
    'MULTI'      : '混合',
    'BENCHMARK'  : '基准指数',
}

# 货币展示名
CURRENCY_LABELS: dict[str, str] = {
    'CNY': '人民币 (CNY)',
    'USD': '美元 (USD)',
    'HKD': '港币 (HKD)',
    'EUR': '欧元 (EUR)',
    'GBP': '英镑 (GBP)',
    'JPY': '日元 (JPY)',
}


class AnalysisService:

    def __init__(self, portfolio_df: pd.DataFrame, base_currency: str = 'CNY'):
        """
        Args:
            portfolio_df : PortfolioService.get_full_portfolio() 的返回值
            base_currency: 折算基础货币（列名 value_{base_currency.lower()} 须存在）
        """
        self._df   = portfolio_df.copy()
        self._base = base_currency
        self._val_col = f'value_{base_currency.lower()}'

        if self._val_col not in self._df.columns:
            raise ValueError(f"portfolio_df 中缺少列 '{self._val_col}'，请检查 PortfolioService 配置")

    # ──────────────────────────────────────────────────────────────
    # 大类资产分布
    # ──────────────────────────────────────────────────────────────

    def get_asset_class_distribution(self) -> pd.DataFrame:
        """
        按大类资产（asset_class）聚合 CNY 市值与占比。

        Returns DataFrame columns:
            asset_class, label, value_cny, weight_pct,
            count（该分类下资产数），price_source_note
        """
        total = self._df[self._val_col].sum()
        if total == 0:
            return pd.DataFrame()

        grp = (
            self._df
            .groupby('asset_class', sort=False)
            .agg(
                value          =(self._val_col, 'sum'),
                count          =('ticker', 'count'),
                has_cost_only  =('price_source', lambda s: (s == 'cost').any()),
            )
            .reset_index()
        )

        grp['label']       = grp['asset_class'].map(ASSET_CLASS_LABELS).fillna(grp['asset_class'])
        grp['weight_pct']  = (grp['value'] / total * 100).round(2)
        grp['value']       = grp['value'].round(2)
        grp = grp.sort_values('value', ascending=False).reset_index(drop=True)

        # 重命名 value 列以包含货币
        grp = grp.rename(columns={'value': self._val_col})
        return grp

    # ──────────────────────────────────────────────────────────────
    # 货币分布
    # ──────────────────────────────────────────────────────────────

    def get_currency_distribution(self) -> pd.DataFrame:
        """
        按原始计价货币（currency）聚合 CNY 市值与占比。

        区分「持仓货币」（资产在该货币的价值）而非汇兑货币。

        Returns DataFrame columns:
            currency, label, value_cny, weight_pct, count
        """
        total = self._df[self._val_col].sum()
        if total == 0:
            return pd.DataFrame()

        grp = (
            self._df
            .groupby('currency', sort=False)
            .agg(
                value=(self._val_col, 'sum'),
                count=('ticker', 'count'),
            )
            .reset_index()
        )

        grp['label']      = grp['currency'].map(CURRENCY_LABELS).fillna(grp['currency'])
        grp['weight_pct'] = (grp['value'] / total * 100).round(2)
        grp['value']      = grp['value'].round(2)
        grp = grp.sort_values('value', ascending=False).reset_index(drop=True)
        grp = grp.rename(columns={'value': self._val_col})
        return grp

    # ──────────────────────────────────────────────────────────────
    # 账户分布
    # ──────────────────────────────────────────────────────────────

    def get_account_distribution(self) -> pd.DataFrame:
        """
        仅对 CASH 行按账户聚合，返回各账户现金分布。
        证券持仓无账户归属，暂不统计（后续可通过持仓关联账户补充）。

        Returns DataFrame columns:
            ticker（账户_货币标签）, name, currency, balance, value_cny, weight_pct
        """
        total = self._df[self._val_col].sum()
        if total == 0:
            return pd.DataFrame()

        cash_df = self._df[self._df['type'] == 'CASH'].copy()
        cash_df['weight_pct'] = (cash_df[self._val_col] / total * 100).round(2)
        cash_df[self._val_col] = cash_df[self._val_col].round(2)
        return cash_df[['name', 'currency', 'qty', self._val_col, 'weight_pct']].reset_index(drop=True)

    # ──────────────────────────────────────────────────────────────
    # 汇总快照
    # ──────────────────────────────────────────────────────────────

    def get_summary(self) -> dict:
        """
        返回投资组合关键指标快照（用于卡片展示）。
        """
        df = self._df
        total = float(df[self._val_col].sum())
        security_val = float(df[df['type'] == 'SECURITY'][self._val_col].sum())
        cash_val     = float(df[df['type'] == 'CASH'][self._val_col].sum())

        price_warn_count = int((df['price_source'] == 'cost').sum())

        return {
            'total_value'       : round(total, 2),
            'security_value'    : round(security_val, 2),
            'cash_value'        : round(cash_val, 2),
            'base_currency'     : self._base,
            'position_count'    : int(df[df['type'] == 'SECURITY'].shape[0]),
            'price_warn_count'  : price_warn_count,  # 使用成本价代替市价的持仓数量
        }
