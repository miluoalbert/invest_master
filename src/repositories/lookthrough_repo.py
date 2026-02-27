from .base_repo import BaseRepository

class LookthroughRepository(BaseRepository):
    
    def update_etf_holdings(self, parent_ticker, report_date, holdings_data):
        """
        更新ETF持仓
        :param holdings_data: List of dicts. 
         e.g. [{'ticker': 'AAPL', 'name': 'Apple', 'weight': 0.07}, ...]
        """
        print(f"🔄 更新 {parent_ticker} 的穿透数据...")
        
        sql = """
            INSERT INTO tb_lookthrough_components 
            (parent_ticker, report_date, underlying_ticker, underlying_name, weight, sector, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (parent_ticker, report_date, underlying_ticker) 
            DO UPDATE SET weight = EXCLUDED.weight;
        """
        
        count = 0
        for item in holdings_data:
            self.cursor.execute(sql, (
                parent_ticker,
                report_date,
                item.get('ticker'),
                item.get('name'),
                item.get('weight'),
                item.get('sector', None),
                item.get('country', None)
            ))
            count += 1
            
        print(f"✅ {parent_ticker}: 更新了 {count} 条持仓成分。")