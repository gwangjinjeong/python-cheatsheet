import cx_Oracle
import os
import pandas as pd

class OracleDBQueryExecutor:
    """
    [OracleDBQueryExecutor 클래스 사용 예시]
    from oracle_db_query_executor import OracleDBQueryExecutor
    # ex) username = "admin", password = "123", database = "127.0.0.1:1521/table_name", location = r"C:\Users\Administrator\Documents\Workspace\instantclient_21_6"")
    db = OracleDBQueryExecutor("username", "password", "database", "location")

    ## Select
    query = "SELECT * FROM table"
    data = db.get_data("SELECT * FROM table")

    ## Insert
    query = "INSERT INTO te_reuters(ZREUTERS,ZDATE,HIGH,LOW,OPEN,CLOSE,VOLUME) VALUES (:1,:2,:3,:4,:5,:6,:7)"
    rows = [('AAPL.O', '20220101', 100.0, 80.0, 90.0, 95.0, 100000),
            ('AMZN.O', '20220101', 2000.0, 1800.0, 1900.0, 1950.0, 50000),
            ('GOOGL.O', '20220101', 3000.0, 2800.0, 2900.0, 2950.0, 20000),
            ('MSFT.O', '20220101', 150.0, 130.0, 140.0, 145.0, 80000),
            ('TSLA.O', '20220101', 500.0, 400.0, 450.0, 475.0, 30000)]

    oracle_db.insert_data(query, rows)
            
    """
    def __init__(self,
                 username: str, password: str, database: str,
                 instantclient_location: str,
                 debug: bool = False) -> None:
        self.username = username
        self.password = password
        self.database = database
        self.instantclient_location = instantclient_location
        if debug:
            self.logger = self._logging

    def _logging(self):
        lgr = logging.getLogger('oracle_db_logger')
        lgr.setLevel(logging.ERROR)
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        fh = logging.FileHandler(dt.today().strftime('%y-%m-%d') + '-error.log')
        fh.setLevel(logging.ERROR)  # ensure all messages are logged to file
        frmt = logging.Formatter("%(asctime)s,%(levelname)s,%(message)s", "%Y-%m-%d %H:%M:%S") # 포맷설정
        fh.setFormatter(frmt)
        lgr.addHandler(fh)
        
    def connect(self):
        try:
            os.environ["PATH"] = self.instantclient_location + ";" + os.environ["PATH"]
            OracleConnect = cx_Oracle.connect(self.username, self.password, self.database)
        except cx_Oracle.DatabaseError as e:
            self.logger.error('Failed to connect to Oracle database: %s', e)
            raise e
        return OracleConnect

    def get_cursor(self) -> (cx_Oracle.Connection, cx_Oracle.Cursor):
        conn = self.connect()
        cursor = conn.cursor()
        return conn, cursor

    def get_column_names(self, cursor: cx_Oracle.Cursor) -> list[str]:
        col_names = [col[0] for col in cursor.description]
        return col_names

    def fetch_data(self, cursor: cx_Oracle.Cursor) -> pd.DataFrame:
        rows = cursor.fetchall()
        col_names = self.get_column_names(cursor)
        print(col_names)
        df = pd.DataFrame(rows, columns=col_names)
        return df

    def get_data(self, query: str) -> pd.DataFrame:
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                data = self.fetch_data(cursor)
        return data

    def insert_data(self, query: str, rows: list[tuple]) -> None:
        conn, cursor = self.get_cursor()
        try:
            cursor.executemany(query, rows)
        except (cx_Oracle.DatabaseError, TypeError) as e:
            self.logger.error('Failed to execute query: %s. Error: %s', query, e)
            raise e
        conn.commit()
        cursor.close()
        conn.close()
        
    def update_data(self, query: str) -> None:
        conn, cursor = self.get_cursor()
        try:
            cursor.execute(query)
        except cx_Oracle.DatabaseError as e:
            self.logger.error('Failed to execute query: %s. Error: %s', query, e)
            raise e
        conn.commit()
        cursor.close()
        conn.close()


