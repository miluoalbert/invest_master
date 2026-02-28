"""
持仓服务 (PortfolioService)

职责：
  • 整合 PortfolioRepository 的原始查询结果
  • 为每笔持仓计算市值（优先使用市场价，无则用均摊成本价）
  • 将所有持仓及现金折算为统一基础货币（默认 CNY）
  • 对外暴露标准化的 pandas DataFrame

输出 DataFrame 结构（get_full_portfolio）：
  type        : 'SECURITY' | 'CASH'
  ticker      : 资产代码（现金行为账户名_货币）
  name        : 资产名称
  asset_class : 资产大类（EQUITY/BOND/COMMODITY/...；现金为 CASH）
  sub_class   : 资产子类
  currency    : 原始计价币种
  qty         : 持有数量（现金行 qty = balance）
  avg_cost    : 均摊成本价
  latest_price: 最新市场价（无行情时 = avg_cost）
  price_source: 'market' | 'cost'（价格来源标记）
  value_local : 本币市值 = qty × latest_price
  value_cny   : 折算 CNY 市值
"""

import pandas as pd

from src.repositories.portfolio_repo import PortfolioRepository
from src.services.fx_service import FxService


class PortfolioService:

    def __init__(self, conn, base_currency: str = 'CNY'):
        self._repo = PortfolioRepository(conn)
        self._fx   = FxService(conn, target_currency=base_currency)
        self._base = base_currency

    # ──────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────

    def get_full_portfolio(self, as_of_date=None) -> pd.DataFrame:
        """
        返回当前完整投资组合（证券持仓 + 现金余额），含 CNY 折算市值。

        Args:
            as_of_date: 截止日期（datetime / date / str），None 表示当前最新状态

        Returns:
            pd.DataFrame，详见模块文档字符串中的列说明。
        """
        security_df = self._build_security_df(as_of_date)
        cash_df     = self._build_cash_df(as_of_date)
        df = pd.concat([security_df, cash_df], ignore_index=True)
        df = df.sort_values(['asset_class', 'currency', 'ticker']).reset_index(drop=True)
        return df

    # ──────────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────────

    def _build_security_df(self, as_of_date) -> pd.DataFrame:
        """处理证券持仓：查询持仓 → 匹配市价 → 计算市值 → 折算 CNY"""
        positions = self._repo.get_positions(as_of_date)
        market_prices = self._repo.get_latest_market_prices()

        rows = []
        for pos in positions:
            ticker      = pos['ticker']
            qty         = float(pos['total_qty'])
            avg_cost    = float(pos['avg_cost']) if pos['avg_cost'] else 0.0
            currency    = pos['currency']

            # ── 市价匹配 ──────────────────────────────────────────
            if ticker in market_prices:
                latest_price = market_prices[ticker]['price']
                price_source = 'market'
            else:
                latest_price = avg_cost   # fallback：用成本价代替市价
                price_source = 'cost'

            value_local = qty * latest_price
            value_base  = self._fx.convert(value_local, currency)

            rows.append({
                'type'        : 'SECURITY',
                'ticker'      : ticker,
                'name'        : pos['name'],
                'asset_class' : pos['asset_class'],
                'sub_class'   : pos['sub_class'] or '',
                'currency'    : currency,
                'qty'         : qty,
                'avg_cost'    : avg_cost,
                'latest_price': latest_price,
                'price_source': price_source,
                'value_local' : value_local,
                f'value_{self._base.lower()}': value_base,
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=self._schema_columns())

    def _build_cash_df(self, as_of_date) -> pd.DataFrame:
        """处理现金余额：每个账户每种货币当作一行 CASH 持仓"""
        cash_balances = self._repo.get_cash_balances(as_of_date)

        rows = []
        for cb in cash_balances:
            balance  = float(cb['balance'])
            currency = cb['currency']
            if balance <= 0:          # 负余额不纳入（理论上不应出现）
                continue

            value_base = self._fx.convert(balance, currency)
            label      = f"{cb['account_name']}_{currency}"

            rows.append({
                'type'        : 'CASH',
                'ticker'      : label,
                'name'        : f"{cb['account_name']} 现金",
                'asset_class' : 'CASH',
                'sub_class'   : currency,
                'currency'    : currency,
                'qty'         : balance,
                'avg_cost'    : 1.0,
                'latest_price': 1.0,
                'price_source': 'N/A',
                'value_local' : balance,
                f'value_{self._base.lower()}': value_base,
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=self._schema_columns())

    def _schema_columns(self) -> list[str]:
        return [
            'type', 'ticker', 'name', 'asset_class', 'sub_class',
            'currency', 'qty', 'avg_cost', 'latest_price', 'price_source',
            'value_local', f'value_{self._base.lower()}'
        ]
