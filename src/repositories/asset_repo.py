from .base_repo import BaseRepository

class AssetRepository(BaseRepository):
    
    def get_id_by_ticker(self, ticker):
        sql = "SELECT id FROM tb_assets WHERE ticker = %s"
        self.cursor.execute(sql, (ticker,))
        res = self.cursor.fetchone()
        return res['id'] if res else None

    def upsert_from_csv(self, csv_path):
        df = self._read_csv(csv_path)
        if df is None: return 0
        
        count = 0
        print(f"📂 正在导入资产 ({len(df)} 条)...")
        
        sql = """
            INSERT INTO tb_assets (ticker, name, asset_class, sub_class, currency, exchange, isin, tracked_index_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                name = EXCLUDED.name,
                asset_class = EXCLUDED.asset_class,
                sub_class = EXCLUDED.sub_class,
                currency = EXCLUDED.currency,
                exchange = EXCLUDED.exchange,
                isin = EXCLUDED.isin,
                tracked_index_code = EXCLUDED.tracked_index_code
            RETURNING id;
        """
        
        for _, row in df.iterrows():
            self.cursor.execute(sql, (
                row['ticker'], row['name'], row['asset_class'], 
                row['sub_class'], row['currency'], row['exchange'], row['isin'],
                row.get('tracked_index_code', None)  # 可选字段
            ))
            count += 1
            
        return count