"""
IBKR Transaction History CSV → 系统标准 [transactions.csv] 转换脚本

使用方式:
    python scripts/ibkr_to_transactions.py \
        --input [IBKR.TRANSACTIONS.1Y.csv] \
        --output data/transactions_ibkr.csv \
        --account "IBKR-U63890"
"""

import argparse
import csv
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ── 1. IBKR Transaction Type → 系统 trx_type 映射 ──────────────────────────
IBKR_TYPE_MAP = {
    "Buy":                      "BUY",
    "Sell":                     "SELL",
    "Dividend":                 "DIVIDEND",
    "Payment in Lieu":          "DIVIDEND",
    "Credit Interest":          "INTEREST",
    "Deposit":                  "DEPOSIT",
    "Withdrawal":               "WITHDRAW",
    "Foreign Tax Withholding":  "TAX",
    "Other Fee":                "FEE",
}

# ── 2. 需要跳过/合并处理的类型 ──────────────────────────────────────────────
SKIP_TYPES = {
    "Cancellation",   # 取消操作，通常成对出现，净效果为0，忽略
}

# ── 3. 工具函数 ──────────────────────────────────────────────────────────────

def parse_ibkr_csv(filepath: str) -> list[dict]:
    """读取IBKR CSV，只提取 'Transaction History,Data,...' 行"""
    rows = []
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = None
        for row in reader:
            if len(row) < 2:
                continue
            # 找到表头行
            if row[0] == "Transaction History" and row[1] == "Header":
                headers = row[2:]  # 从第3列起是字段名
                continue
            # 找到数据行
            if row[0] == "Transaction History" and row[1] == "Data":
                if headers is None:
                    raise ValueError("未找到 Transaction History Header 行")
                record = dict(zip(headers, row[2:]))
                rows.append(record)
    return rows


def clean_number(val: str) -> float:
    """清理数字字符串，处理空值/'-'"""
    if not val or val.strip() in ("-", ""):
        return 0.0
    # 去除逗号(千分符)
    val = val.replace(",", "").strip()
    try:
        return float(val)
    except ValueError:
        return 0.0


def format_date(date_str: str) -> str:
    """统一日期格式为 YYYY-M-D (与transactions.csv一致)"""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str.strip()


def extract_ticker_from_desc(description: str) -> str:
    """从描述中提取ticker，例: 'SGOV(US46436E7186) Cash Dividend...' → 'SGOV'"""
    match = re.match(r"^([A-Z0-9.]+)\(", description)
    if match:
        return match.group(1)
    return ""


# ── 4. 核心转换逻辑 ──────────────────────────────────────────────────────────

def merge_tax_into_dividends(rows: list[dict]) -> list[dict]:
    """
    将 Foreign Tax Withholding 行合并到对应的 Dividend 行的 tax 字段，
    并从输出中移除独立的税费行。

    IBKR的税费有时会先扣后退（如先 -11.21 再 +11.21），取净值。
    按 (date, symbol) 分组计算净税额。
    """
    # 按 (date, symbol) 统计净税额
    tax_net: dict[tuple, float] = defaultdict(float)
    for row in rows:
        if row.get("Transaction Type") == "Foreign Tax Withholding":
            key = (row["Date"], row["Symbol"])
            tax_net[key] += clean_number(row.get("Net Amount", "0"))

    # 标记已处理的税行
    processed = []
    seen_tax_keys: set[tuple] = set()

    for row in rows:
        t_type = row.get("Transaction Type", "")

        if t_type == "Foreign Tax Withholding":
            key = (row["Date"], row["Symbol"])
            if key not in seen_tax_keys:
                seen_tax_keys.add(key)
                net = tax_net[key]
                if abs(net) > 1e-6:  # 净税额不为0，保留一行
                    row = row.copy()
                    row["_net_tax"] = net  # 存入净税额供后用
                    row["_keep_tax_row"] = True
                    processed.append(row)
                # 净税额为0则完全忽略
            continue  # 无论如何都跳过重复税行

        # 为 Dividend/Payment in Lieu 注入 tax 字段
        if t_type in ("Dividend", "Payment in Lieu"):
            key = (row["Date"], row["Symbol"])
            row = row.copy()
            row["_injected_tax"] = abs(tax_net.get(key, 0.0))

        processed.append(row)

    return processed


def convert_row(row: dict, account_name: str) -> dict | None:
    """将单条IBKR记录转换为系统格式，返回None表示跳过"""
    t_type_raw = row.get("Transaction Type", "").strip()

    # 跳过取消类型
    if t_type_raw in SKIP_TYPES:
        return None

    # 独立税费行（净额非0）→ TAX类型
    if row.get("_keep_tax_row"):
        return {
            "date":             format_date(row["Date"]),
            "type":             "TAX",
            "account_name":     account_name,
            "ticker":           row.get("Symbol", "").strip() or "",
            "qty":              "",
            "price":            "",
            "fee":              "",
            "tax":              abs(row["_net_tax"]),
            "cash_flow":        row["_net_tax"],   # 负数=扣税，正数=退税
            "currency":         row.get("Price Currency", "USD").strip() or "USD",
            "fx_rate_to_base":  1.00,
            "note":             row.get("Description", "").strip(),
        }

    # 系统类型映射
    sys_type = IBKR_TYPE_MAP.get(t_type_raw)
    if sys_type is None:
        print(f"  [WARN] 未识别的交易类型: '{t_type_raw}'，行已跳过: {row}")
        return None

    symbol  = row.get("Symbol", "").strip()
    qty_raw = clean_number(row.get("Quantity", ""))
    price   = clean_number(row.get("Price", ""))
    comm    = abs(clean_number(row.get("Commission", "0")))
    net_amt = clean_number(row.get("Net Amount", "0"))
    curr    = row.get("Price Currency", "USD").strip() or "USD"
    desc    = row.get("Description", "").strip()

    # 无symbol时尝试从描述中提取(分红、利息等)
    if not symbol or symbol == "-":
        symbol = extract_ticker_from_desc(desc)

    # 分红注入税费
    injected_tax = abs(row.get("_injected_tax", 0.0))

    # 买卖时 qty：BUY为正，SELL为负
    if sys_type == "BUY":
        qty = abs(qty_raw)
    elif sys_type == "SELL":
        qty = -abs(qty_raw)
    else:
        qty = qty_raw if qty_raw else ""

    return {
        "date":             format_date(row["Date"]),
        "type":             sys_type,
        "account_name":     account_name,
        "ticker":           symbol,
        "qty":              qty if qty != "" else "",
        "price":            price if price else "",
        "fee":              comm if comm else "",
        "tax":              injected_tax if injected_tax else "",
        "cash_flow":        net_amt,
        "currency":         curr,
        "fx_rate_to_base":  1.00,
        "note":             desc,
    }


# ── 5. 主流程 ────────────────────────────────────────────────────────────────

OUTPUT_FIELDS = [
    "date", "type", "account_name", "ticker",
    "qty", "price", "fee", "tax",
    "cash_flow", "currency", "fx_rate_to_base", "note",
]


def main():
    parser = argparse.ArgumentParser(description="IBKR CSV → 系统 [transactions.csv] 转换工具")
    parser.add_argument("--input",   required=True, help="IBKR原始CSV路径")
    parser.add_argument("--output",  required=True, help="输出CSV路径")
    parser.add_argument("--account", default="IBKR",
                        help="账户名称，对应系统 account_name 字段 (默认: IBKR)")
    args = parser.parse_args()

    input_path  = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"📂 读取: {input_path}")
    raw_rows = parse_ibkr_csv(str(input_path))
    print(f"   共读取 {len(raw_rows)} 条原始记录")

    print("🔄 合并税费行...")
    merged_rows = merge_tax_into_dividends(raw_rows)

    print("✏️  转换格式...")
    output_rows = []
    skip_count  = 0
    for row in merged_rows:
        result = convert_row(row, args.account)
        if result is None:
            skip_count += 1
            continue
        output_rows.append(result)

    # 按日期升序排列（与transactions.csv风格一致）
    output_rows.sort(key=lambda r: r["date"])

    print(f"💾 写入: {output_path}")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\n✅ 完成！转换 {len(output_rows)} 条，跳过 {skip_count} 条（Cancellation等）")
    print(f"   输出文件: {output_path.resolve()}")


if __name__ == "__main__":
    main()