import pandas as pd
import os

class BaseRepository:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

    def _read_csv(self, file_path):
        """读取CSV并将NaN转换为None (SQL NULL)"""
        if not os.path.exists(file_path):
            print(f"⚠️ 文件不存在: {file_path}")
            return None
        
        try:
            # 显式指定 UTF-8 编码读取 CSV，支持中文等多字节字符
            df = pd.read_csv(file_path, encoding='utf-8')
            # 将 pandas 的 NaN 替换为 None，否则数据库会报错
            return df.where(pd.notnull(df), None)
        except Exception as e:
            print(f"❌ 读取CSV失败: {e}")
            return None