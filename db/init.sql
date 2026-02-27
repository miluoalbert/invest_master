/*
 * 投资分析数据库初始化脚本 (PostgreSQL)
 * 版本: v1.0
 * 适用风格: 资产配置流派 (多币种、底层穿透、定期再平衡)
 */

-- 1. 清理旧表 (如果存在，开发阶段方便重置)
DROP TABLE IF EXISTS tb_lookthrough_components CASCADE;
DROP TABLE IF EXISTS tb_portfolio_targets CASCADE;
DROP TABLE IF EXISTS tb_market_data CASCADE;
DROP TABLE IF EXISTS tb_transactions CASCADE;
DROP TABLE IF EXISTS tb_exchange_rates CASCADE;
DROP TABLE IF EXISTS tb_accounts CASCADE;
DROP TABLE IF EXISTS tb_assets CASCADE;

-- 2. 创建枚举类型 (规范数据输入)
-- 交易类型：买、卖、分红、利息、入金、出金、换汇、税费
DROP TYPE IF EXISTS trx_type;
CREATE TYPE trx_type AS ENUM ('BUY', 'SELL', 'DIVIDEND', 'INTEREST', 'DEPOSIT', 'WITHDRAW', 'FX_CONVERT', 'TAX', 'FEE');

-- 资产大类：股票、债券、商品、房地产(REITs)、现金、另类、混合、基准指数
DROP TYPE IF EXISTS asset_class_type;
CREATE TYPE asset_class_type AS ENUM ('EQUITY', 'BOND', 'COMMODITY', 'REITS', 'CASH', 'ALTERNATIVE', 'MULTI', 'BENCHMARK');

-- --------------------------------------------------------
-- 3. 基础信息表构建
-- --------------------------------------------------------

-- [资产主表] 所有投资标的的身份证 (含ETF、股票、指数)
CREATE TABLE tb_assets (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,      -- 代码 (e.g., 'AAPL', 'VOO', 'SPX')
    name VARCHAR(100) NOT NULL,              -- 名称
    asset_class asset_class_type NOT NULL,   -- 资产大类 (用于宏观配置)
    sub_class VARCHAR(50),                   -- 子类 (e.g., 'US Large Cap', 'China Gov Bond')
    currency VARCHAR(5) NOT NULL DEFAULT 'USD', -- 交易币种 (USD, CNY, HKD)
    exchange VARCHAR(20),                    -- 交易所 (NYSE, HKEX, SSE)
    isin VARCHAR(20),                        -- 国际证券识别码 (可选，用于跨市场精确匹配)
    tracked_index_code VARCHAR(20),          -- 追踪指数代码 (ETF用：如VOO/SPY→'SPX'，指数自身为NULL)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- [账户表] 记录钱在哪里 (券商/银行)
CREATE TABLE tb_accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,               -- 账户名 (e.g., '富途牛牛', '招商银行')
    broker VARCHAR(50),                      -- 券商机构
    base_currency VARCHAR(5) DEFAULT 'USD',  -- 账户基础币种
    is_active BOOLEAN DEFAULT TRUE           -- 是否启用
);

-- [汇率表] 用于将所有资产折算回本币 (如 CNY) 进行汇总
CREATE TABLE tb_exchange_rates (
    date DATE NOT NULL,
    from_currency VARCHAR(5) NOT NULL,       -- e.g., 'USD'
    to_currency VARCHAR(5) NOT NULL,         -- e.g., 'CNY'
    rate DECIMAL(10, 6) NOT NULL,            -- 汇率
    PRIMARY KEY (date, from_currency, to_currency)
);

-- --------------------------------------------------------
-- 4. 核心业务表构建
-- --------------------------------------------------------

-- [交易流水表] 记录每一笔操作 (系统核心事实表)
CREATE TABLE tb_transactions (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,                 -- 交易时间
    type trx_type NOT NULL,                  -- 交易类型
    account_id INTEGER REFERENCES tb_accounts(id), -- 关联账户
    asset_id INTEGER REFERENCES tb_assets(id),     -- 关联资产 (可为空，如纯现金转账)
    
    -- 量价数据
    qty DECIMAL(15, 4),                      -- 数量 (买入为正，卖出为负)
    price DECIMAL(15, 4),                    -- 成交单价 (原币种)
    fee DECIMAL(10, 2) DEFAULT 0,            -- 手续费 (原币种)
    tax DECIMAL(10, 2) DEFAULT 0,            -- 税费 (原币种)
    
    -- 金额汇总
    cash_flow DECIMAL(15, 2) NOT NULL,    -- 变动总金额 (含费税，负数为现金流出(如买入)，正数为现金流入(如分红、入金))
    currency VARCHAR(5) NOT NULL,            -- 发生金额的币种
    
    -- 用于 FX_CONVERT 场景
    fx_rate_to_base DECIMAL(10, 6) DEFAULT NULL,  -- 仅在真实换汇(FX_CONVERT)时记录实际成交汇率，日常外币交易留空
    
    note TEXT                                -- 备注
);

-- [市场行情表] 用于每日市值更新
CREATE TABLE tb_market_data (
    asset_id INTEGER REFERENCES tb_assets(id),
    date DATE NOT NULL,
    close_price DECIMAL(15, 4),              -- 收盘价
    nav DECIMAL(15, 4),                      -- 基金净值 (可选)
    volume BIGINT,                           -- 成交量 (可选)
    PRIMARY KEY (asset_id, date)
);

-- --------------------------------------------------------
-- 5. 高级分析表 (穿透与策略)
-- --------------------------------------------------------

-- [底层资产穿透表] (你特别要求的)
-- 记录ETF/基金的持仓结构，用于分析底层暴露
CREATE TABLE tb_lookthrough_components (
    id SERIAL PRIMARY KEY,
    
    -- 关联母资产 (注意：这里关联 ticker 字符串，方便爬虫脚本直接写入)
    parent_ticker VARCHAR(20) NOT NULL REFERENCES tb_assets(ticker),
    report_date DATE NOT NULL,               -- 持仓报告日期
    
    -- 底层资产信息
    underlying_ticker VARCHAR(20),           -- 底层代码 (e.g., 'NVDA')
    underlying_name VARCHAR(100),            -- 底层名称
    underlying_asset_class VARCHAR(20),      -- 底层类型 (Stock, Bond)
    
    -- 核心权重数据
    weight DECIMAL(10, 6) NOT NULL,          -- 权重 (0.0521 = 5.21%)
    market_value DECIMAL(18, 2),             -- 持仓市值 (可选)
    
    -- 分析维度 (便于 Group By 分析)
    sector VARCHAR(50),                      -- 行业 (GICS Sector)
    region VARCHAR(30),                      -- 区域 (North America, EM)
    country VARCHAR(30),                     -- 国家
    currency VARCHAR(5),                     -- 底层资产计价币种 (CNY, USD, JPY)
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- [配置目标表] 用于再平衡计算
CREATE TABLE tb_portfolio_targets (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(50) DEFAULT 'Main',
    
    -- 目标可以是具体资产，也可以是资产大类
    target_type VARCHAR(10) CHECK (target_type IN ('ASSET', 'CLASS')), 
    target_key VARCHAR(20) NOT NULL,         -- 对应的 Ticker 或 Asset_Class
    
    target_weight DECIMAL(5, 4) NOT NULL,    -- 目标权重 (0.20 = 20%)
    tolerance DECIMAL(5, 4) DEFAULT 0.05,    -- 容忍阈值 (超过5%提示再平衡)
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------------
-- 6. 索引优化 (提升查询性能)
-- --------------------------------------------------------

-- 交易表：按时间查询最常用
CREATE INDEX idx_trx_date ON tb_transactions(date);
CREATE INDEX idx_trx_asset ON tb_transactions(asset_id);

-- 穿透表：确保唯一性，防止重复插入同一天的持仓
CREATE UNIQUE INDEX idx_lookthrough_unique 
ON tb_lookthrough_components (parent_ticker, report_date, underlying_ticker);

-- 穿透表：加速 "查底层" (例如：查我间接持有了多少 NVDA)
CREATE INDEX idx_lookthrough_underlying ON tb_lookthrough_components(underlying_ticker);

-- 资产表：按追踪指数分组查询 (用于聚合相同指数的ETF)
CREATE INDEX idx_tracked_index ON tb_assets(tracked_index_code);

-- 市场数据表：按时间排序
CREATE INDEX idx_market_date ON tb_market_data(date);