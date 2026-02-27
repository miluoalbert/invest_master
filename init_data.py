import pandas as pd
import psycopg2
from psycopg2 import sql
import configparser
import os
import sys

# ==========================================
# 配置加载 (复用之前的逻辑)
# ==========================================
def get_db_connection(config_file='conf/database.ini'):
    config_path = os.path.join(config_file)
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    db_config = dict(config.items('postgresql'))
    conn = psycopg2.connect(**db_config)
    return conn

# ==========================================
# 核心导入逻辑
# ==========================================

def import_accounts(conn):
    """导入账户数据"""
    file_path = os.path.join('data', 'accounts.csv')
    if not os.path.exists(file_path):
        print(f"⚠️ 跳过: 找不到 {file_path}")
        return

    df = pd.read_csv(file_path)
    # 将 NaN 替换为 None (SQL NULL)
    df = df.where(pd.notnull(df), None)
    
    cursor = conn.cursor()
    count = 0
    
    print(f"\n📂 正在处理账户数据 ({len(df)} 条)...")
    
    for _, row in df.iterrows():
        try:
            # 逻辑：检查是否存在，不存在则插入
            # (由于 tb_accounts 目前没有唯一索引约束 name，我们手动检查以防重复)
            check_query = "SELECT id FROM tb_accounts WHERE name = %s"
            cursor.execute(check_query, (row['name'],))
            
            if cursor.fetchone():
                print(f"  - 跳过 (已存在): {row['name']}")
            else:
                insert_query = """
                    INSERT INTO tb_accounts (name, broker, base_currency)
                    VALUES (%s, %s, %s);
                """
                cursor.execute(insert_query, (row['name'], row['broker'], row['base_currency']))
                count += 1
                print(f"  + 插入: {row['name']}")
                
        except Exception as e:
            print(f"  ❌ 错误 {row['name']}: {e}")
            conn.rollback() 
            return

    conn.commit()
    print(f"✅ 账户导入完成，新增 {count} 条。")


def import_assets(conn):
    """导入资产数据 (支持 Upsert 更新)"""
    file_path = os.path.join('data', 'assets.csv')
    if not os.path.exists(file_path):
        print(f"⚠️ 跳过: 找不到 {file_path}")
        return

    df = pd.read_csv(file_path)
    df = df.where(pd.notnull(df), None) # 处理空值

    cursor = conn.cursor()
    inserted = 0
    updated = 0
    
    print(f"\n📂 正在处理资产数据 ({len(df)} 条)...")
    
    for _, row in df.iterrows():
        try:
            # 使用 Upsert 逻辑 (ON CONFLICT DO UPDATE)
            # 只要 ticker 相同，就会更新其他字段，方便你修改 CSV 修正数据
            query = """
                INSERT INTO tb_assets (ticker, name, asset_class, sub_class, currency, exchange, isin)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker) 
                DO UPDATE SET
                    name = EXCLUDED.name,
                    asset_class = EXCLUDED.asset_class,
                    sub_class = EXCLUDED.sub_class,
                    currency = EXCLUDED.currency,
                    exchange = EXCLUDED.exchange,
                    isin = EXCLUDED.isin;
            """
            
            cursor.execute(query, (
                row['ticker'], 
                row['name'], 
                row['asset_class'], 
                row['sub_class'], 
                row['currency'], 
                row['exchange'], 
                row['isin']
            ))
            
            # 判断是插入还是更新 (通过 rowcount 并不是特别准，这里简化处理)
            # 在 Postgres 中，Insert 返回 1，Update 也可能返回 1
            # 我们可以简单打印正在处理谁
            print(f"  > 处理成功: {row['ticker']}")
            
        except Exception as e:
            print(f"  ❌ 错误 {row['ticker']}: {e}")
            print("     提示: 请检查 CSV 中的 asset_class 是否属于定义的 ENUM 类型 (EQUITY, BOND...)")
            conn.rollback()
            return

    conn.commit()
    print(f"✅ 资产导入完成。")

# ==========================================
# 主程序
# ==========================================
if __name__ == "__main__":
    try:
        conn = get_db_connection()
        print("🔗 数据库连接成功")
        
        import_accounts(conn)
        import_assets(conn)
        
        conn.close()
        print("\n🎉 所有任务执行完毕")
        
    except Exception as e:
        print(f"\n❌ 发生致命错误: {e}")