"""
汇率服务 (FxService)

查询顺序：
  1. 数据库 tb_exchange_rates（最新数据）
  2. 内置 fallback 手动汇率（适合无网络或初始化阶段）

用法：
    fx = FxService(conn)
    cny_value = fx.convert(1000, 'USD', 'CNY')
    rate = fx.get_rate('HKD', 'CNY')
"""

# ── 手动维护的 fallback 汇率（均为折算目标 CNY） ────────────────────────────
# 格式：(from_currency, to_currency) -> rate
# 在 tb_exchange_rates 表有数据后，以数据库为准
FALLBACK_RATES: dict[tuple[str, str], float] = {
    ('USD', 'CNY'): 7.25,
    ('HKD', 'CNY'): 0.93,
    ('CNY', 'CNY'): 1.0,
    ('EUR', 'CNY'): 7.85,
    ('GBP', 'CNY'): 9.10,
    ('JPY', 'CNY'): 0.048,
}


class FxService:
    """
    汇率服务：将各币种金额折算为目标货币（默认 CNY）。

    Attributes:
        _db_rates: 从数据库加载的最新汇率缓存 { from_currency: rate }
        _target: 目标折算货币
    """

    def __init__(self, conn, target_currency: str = 'CNY'):
        self._target = target_currency
        self._db_rates: dict[str, float] = {}
        self._load_from_db(conn)

    def _load_from_db(self, conn):
        """从 tb_exchange_rates 加载到 target_currency 的最新汇率"""
        try:
            from src.repositories.portfolio_repo import PortfolioRepository
            repo = PortfolioRepository(conn)
            self._db_rates = repo.get_latest_exchange_rates(self._target)
        except Exception as e:
            print(f"⚠️ FxService: 无法从数据库加载汇率，将使用 fallback 汇率。原因: {e}")

    def get_rate(self, from_currency: str, to_currency: str | None = None) -> float:
        """
        获取 from_currency → to_currency 的汇率。
        to_currency 默认使用构造时指定的 target_currency。

        Returns:
            float 汇率，如果找不到则返回 1.0 并打印警告
        """
        to_currency = to_currency or self._target

        # 相同货币直接返回 1.0
        if from_currency == to_currency:
            return 1.0

        # 1. 优先使用数据库汇率（已限定 to_currency = self._target）
        if to_currency == self._target and from_currency in self._db_rates:
            return self._db_rates[from_currency]

        # 2. fallback 到手动汇率表
        key = (from_currency, to_currency)
        if key in FALLBACK_RATES:
            return FALLBACK_RATES[key]

        print(f"⚠️ FxService: 未找到汇率 {from_currency}/{to_currency}，默认使用 1.0，请手动补充")
        return 1.0

    def convert(self, amount: float, from_currency: str, to_currency: str | None = None) -> float:
        """
        将金额从 from_currency 折算到 to_currency（默认 target_currency）。
        """
        if amount is None:
            return 0.0
        rate = self.get_rate(from_currency, to_currency)
        return float(amount) * rate

    def get_supported_currencies(self) -> list[str]:
        """返回所有支持的 from_currency 列表"""
        currencies = set(self._db_rates.keys())
        for (frm, _) in FALLBACK_RATES:
            currencies.add(frm)
        return sorted(currencies)
