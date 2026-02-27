import os
from src.database import Database
from src.repositories import AssetRepository, AccountRepository, LookthroughRepository, TransactionRepository

def main():
    # 初始化数据库连接管理器
    db = Database()
    
    # 获取数据文件的绝对路径
    base_dir = os.path.dirname(os.path.abspath(__file__))
    assets_csv = os.path.join(base_dir, 'data', 'assets.csv')
    accounts_csv = os.path.join(base_dir, 'data', 'accounts.csv')
    transactions_csv = os.path.join(base_dir, 'data', 'transactions.csv')

    print("🚀 === 投资系统数据初始化 === \n")

    # 使用 session 上下文，确保出现任何错误都回滚
    with db.session() as conn:
        
        # 1. 处理账户
        account_repo = AccountRepository(conn)
        if os.path.exists(accounts_csv):
            acc_count = account_repo.upsert_from_csv(accounts_csv)
            print(f"✅ 账户处理完成: 新增 {acc_count} 条")
        
        # 2. 处理资产
        asset_repo = AssetRepository(conn)
        if os.path.exists(assets_csv):
            asset_count = asset_repo.upsert_from_csv(assets_csv)
            print(f"✅ 资产处理完成: 更新/新增 {asset_count} 条")

        # 3. 处理交易记录
        trans_repo = TransactionRepository(conn)
        if os.path.exists(transactions_csv):
            trans_count = trans_repo.upsert_from_csv(transactions_csv, account_repo, asset_repo)
            print(f"✅ 交易记录处理完成: 新增 {trans_count} 条")

        # # 4. 模拟功能测试：更新ETF穿透数据
        # print("\n🧪 正在进行穿透数据测试...")
        # look_repo = LookthroughRepository(conn)
        # # 模拟 VT 的两个成分股
        # mock_holdings = [
        #     {'ticker': 'MSFT', 'name': 'Microsoft', 'weight': 0.04, 'country': 'USA'},
        #     {'ticker': 'TCEHY', 'name': 'Tencent', 'weight': 0.01, 'country': 'China'}
        # ]
        # # 假设 VT 已经在 asset_repo 中导入了
        # if asset_repo.get_id_by_ticker("VT"):
        #     look_repo.update_etf_holdings("VT", "2023-12-31", mock_holdings)
        # else:
        #     print("⚠️ 跳过穿透测试: 资产表中未找到 'VT'")

    print("\n🎉 所有任务执行完毕。")

if __name__ == "__main__":
    main()