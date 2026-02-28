"""
持仓查询仓库 (PortfolioRepository)

职责：纯粹的 SQL 查询层，不含业务逻辑。
提供：
  - get_positions()         ← 当前证券持仓（含均摊成本）
  - get_cash_balances()     ← 各账户各币种现金余额
  - get_latest_market_prices() ← 最新市场行情（用于市值估算）
"""
from .base_repo import BaseRepository


class PortfolioRepository(BaseRepository):

    def get_positions(self, as_of_date=None):
        """
        计算当前证券持仓（按资产聚合）。

        逻辑：
          • 仅统计 BUY / SELL 类型交易（含 qty 字段）
          • total_qty > 0.0001 视为持有（过滤已清仓）
          • avg_cost = Σ(买入qty × 买入price) / Σ(买入qty)（FIFO前先用加权均价）

        Returns: list of RealDict, columns:
            asset_id, ticker, name, asset_class, sub_class,
            currency, total_qty, avg_cost
        """
        date_filter = "AND t.date <= %(as_of_date)s" if as_of_date else ""
        params = {"as_of_date": as_of_date} if as_of_date else {}

        sql = f"""
            SELECT
                a.id            AS asset_id,
                a.ticker,
                a.name,
                a.asset_class::TEXT,
                a.sub_class,
                a.currency,
                SUM(t.qty)      AS total_qty,
                -- 加权平均买入成本（仅统计 qty > 0 的买入方向）
                SUM(CASE WHEN t.qty > 0 THEN t.qty * t.price ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN t.qty > 0 THEN t.qty ELSE 0 END), 0)
                    AS avg_cost
            FROM tb_transactions t
            JOIN tb_assets a ON t.asset_id = a.id
            WHERE t.type IN ('BUY', 'SELL')
              AND t.qty IS NOT NULL
              {date_filter}
            GROUP BY a.id, a.ticker, a.name, a.asset_class, a.sub_class, a.currency
            HAVING SUM(t.qty) > 0.0001
            ORDER BY a.asset_class, a.currency, a.ticker;
        """
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def get_cash_balances(self, as_of_date=None):
        """
        计算各账户各币种的净现金余额。

        逻辑：
          • cash_flow 的设计：入金/股息/利息 为正，出金/买入/税费 为负
          • 对所有交易类型的 cash_flow 求和，即得账户现金净值
          • |balance| > 0.01 才纳入，过滤数值误差

        Returns: list of RealDict, columns:
            account_id, account_name, currency, balance
        """
        date_filter = "AND t.date <= %(as_of_date)s" if as_of_date else ""
        params = {"as_of_date": as_of_date} if as_of_date else {}

        sql = f"""
            SELECT
                acc.id          AS account_id,
                acc.name        AS account_name,
                t.currency,
                SUM(t.cash_flow) AS balance
            FROM tb_transactions t
            JOIN tb_accounts acc ON t.account_id = acc.id
            WHERE 1=1 {date_filter}
            GROUP BY acc.id, acc.name, t.currency
            HAVING ABS(SUM(t.cash_flow)) > 0.01
            ORDER BY acc.name, t.currency;
        """
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def get_latest_market_prices(self):
        """
        从市场行情表获取每个资产最新一天的收盘价。

        Returns: dict { ticker: {'price': float, 'date': date} }
        """
        sql = """
            SELECT DISTINCT ON (md.asset_id)
                a.ticker,
                md.close_price  AS latest_price,
                md.date         AS price_date
            FROM tb_market_data md
            JOIN tb_assets a ON md.asset_id = a.id
            ORDER BY md.asset_id, md.date DESC;
        """
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        return {
            row['ticker']: {
                'price': float(row['latest_price']),
                'date':  row['price_date'],
            }
            for row in rows
        }

    def get_latest_exchange_rates(self, to_currency='CNY'):
        """
        从汇率表获取最新一天各币种到目标货币的汇率。

        Returns: dict { from_currency: rate }
        """
        sql = """
            SELECT DISTINCT ON (from_currency)
                from_currency,
                rate
            FROM tb_exchange_rates
            WHERE to_currency = %(to_currency)s
            ORDER BY from_currency, date DESC;
        """
        self.cursor.execute(sql, {'to_currency': to_currency})
        rows = self.cursor.fetchall()
        return {row['from_currency']: float(row['rate']) for row in rows}
