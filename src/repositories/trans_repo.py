from .base_repo import BaseRepository

class TransactionRepository(BaseRepository):
    
    def add_transaction(self, date, trans_type, account_id, asset_id, 
                        qty, price, fee, cash_flow, currency, fx_rate, tax=0, note=None):
        """插入一条交易记录"""
        sql = """
            INSERT INTO tb_transactions 
            (date, type, account_id, asset_id, qty, price, fee, tax, cash_flow, currency, fx_rate_to_base, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        self.cursor.execute(sql, (
            date, trans_type, account_id, asset_id, 
            qty, price, fee, tax, cash_flow, currency, fx_rate, note
        ))
        new_id = self.cursor.fetchone()['id']
        return new_id
    
    def upsert_from_csv(self, csv_path, account_repo, asset_repo):
        """
        从CSV批量导入交易记录
        参数:
            csv_path: CSV文件路径
            account_repo: AccountRepository实例，用于查找账户ID
            asset_repo: AssetRepository实例，用于查找资产ID
        """
        df = self._read_csv(csv_path)
        if df is None: return 0
        
        count = 0
        print(f"📂 正在导入交易 ({len(df)} 条)...")
        
        sql = """
            INSERT INTO tb_transactions 
            (date, type, account_id, asset_id, qty, price, fee, tax, cash_flow, currency, fx_rate_to_base, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        
        for idx, row in df.iterrows():
            # 通过名称查找账户ID
            account_id = account_repo.get_id_by_name(row['account_name'])
            if not account_id:
                print(f"  ⚠️ 第{idx+2}行: 未找到账户 '{row['account_name']}'，跳过")
                continue
            
            # 通过ticker查找资产ID（对于DEPOSIT/WITHDRAW等，ticker可为空）
            asset_id = None
            if row.get('ticker') and row['ticker']:
                asset_id = asset_repo.get_id_by_ticker(row['ticker'])
                if not asset_id:
                    print(f"  ⚠️ 第{idx+2}行: 未找到资产 '{row['ticker']}'，跳过")
                    continue
            
            try:
                self.cursor.execute(sql, (
                    row['date'],
                    row['type'],
                    account_id,
                    asset_id,
                    row.get('qty'),
                    row.get('price'),
                    row.get('fee', 0),
                    row.get('tax', 0),
                    row['cash_flow'],
                    row['currency'],
                    row.get('fx_rate_to_base'),
                    row.get('note')
                ))
                count += 1
            except Exception as e:
                print(f"  ❌ 第{idx+2}行导入失败: {e}")
                continue
        
        return count

    def get_recent_transactions(self, limit=10):
        """查询最近交易 (关联了 Asset 和 Account 表)"""
        sql = """
            SELECT t.date, t.type, a.ticker, ac.name as account, t.qty, t.price, t.cash_flow
            FROM tb_transactions t
            LEFT JOIN tb_assets a ON t.asset_id = a.id
            LEFT JOIN tb_accounts ac ON t.account_id = ac.id
            ORDER BY t.date DESC
            LIMIT %s
        """
        self.cursor.execute(sql, (limit,))
        return self.cursor.fetchall()