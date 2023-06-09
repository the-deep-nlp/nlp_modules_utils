import logging
import psycopg2
from typing import Dict, Any

logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)

class Database:
    """
    Class to handle database connections
    """
    def __init__(
        self,
        endpoint: str,
        database: str,
        username: str,
        password: str,
        port: int=5432,
    ):
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.password = password
        self.port = port
    
    def status_update_db(
        self,
        db_config: Dict[str, Any],
        sql_statement: str
    ) -> None:
        """
        Updates the status in the database
        """
        db = Database(**db_config)
        db_conn, db_cursor = db()
        if db_cursor:
            try:
                db_cursor.execute(sql_statement)
                logger.info("Db updated. Number of rows affected: %s", db_cursor.rowcount)
                db_conn.commit()
                db_cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(error)
            finally:
                if db_conn is not None:
                    db_conn.close()

    def __call__(self):
        try:
            conn = psycopg2.connect(
                host=self.endpoint,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            cur = conn.cursor()
            return conn, cur
        except Exception as exc:
            logger.error("Database connection failed %s", exc)
            return None, None