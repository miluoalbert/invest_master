import psycopg2
from psycopg2.extras import RealDictCursor
import configparser
import os
from contextlib import contextmanager

class Database:
    def __init__(self, conf_path=None):
        # 自动寻找配置文件的绝对路径
        if not conf_path:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            conf_path = os.path.join(base_dir, 'conf', 'database.ini')
        
        self.config = self._load_config(conf_path)
        
    def _load_config(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"配置文件未找到: {path}")
        config = configparser.ConfigParser()
        config.read(path)
        return dict(config.items('postgresql'))

    def get_connection(self):
        # 使用 RealDictCursor，查询结果直接是字典，方便取值
        conn = psycopg2.connect(**self.config, cursor_factory=RealDictCursor)
        # 设置客户端编码为 UTF-8，确保中文等多字节字符正常传输
        conn.set_client_encoding('UTF8')
        return conn

    @contextmanager
    def session(self):
        """
        事务上下文管理器
        用法:
        with db.session() as conn:
            repo = AssetRepo(conn)
            repo.do_something()
        # 退出缩进时自动 commit，出错自动 rollback
        """
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"❌ 数据库事务回滚: {e}")
            raise e
        finally:
            conn.close()