from .base_repo import BaseRepository

class AccountRepository(BaseRepository):
    
    def get_id_by_name(self, name):
        sql = "SELECT id FROM tb_accounts WHERE name = %s"
        self.cursor.execute(sql, (name,))
        res = self.cursor.fetchone()
        return res['id'] if res else None

    def upsert_from_csv(self, csv_path):
        df = self._read_csv(csv_path)
        if df is None: return 0
        
        count = 0
        print(f"📂 正在导入账户 ({len(df)} 条)...")
        
        for _, row in df.iterrows():
            # 简单查重逻辑
            if self.get_id_by_name(row['name']):
                print(f"  - 跳过(已存在): {row['name']}")
                continue
                
            sql = """
                INSERT INTO tb_accounts (name, broker, base_currency)
                VALUES (%s, %s, %s)
            """
            self.cursor.execute(sql, (row['name'], row['broker'], row['base_currency']))
            count += 1
            
        return count