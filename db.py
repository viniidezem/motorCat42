import time
import mysql.connector


class Database:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def connect(self, retries=5, retry_delay=5):
        """Tenta se conectar ao banco de dados e retorna a conexão."""
        for i in range(retries):
            try:
                return mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
            except mysql.connector.Error as e:
                print(f"Erro ao conectar ao banco. Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)

        raise Exception("Não foi possível se conectar ao banco de dados após várias tentativas.")

    def execute_query(self, query, fetchall=False, commit=False, retries=5, retry_delay=5):
        """Executa uma query no banco de dados e retorna os resultados se fetchall=True."""
        conn = self.connect(retries, retry_delay)
        cursor = conn.cursor()

        for i in range(retries):
            try:
                cursor.execute(query)
                result = None
                if fetchall:
                    result = cursor.fetchall()
                if commit:
                    conn.commit()
                return result
            except mysql.connector.Error as e:
                print(f"Erro ao executar query: {query}. Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)

        raise Exception(f"Não foi possível executar a query: {query} após várias tentativas.")

    def insert_data(self, sql, data, retries=5, retry_delay=5):
        """Insere dados no banco de dados."""
        conn = self.connect(retries, retry_delay)
        cursor = conn.cursor()

        for i in range(retries):
            try:
                if isinstance(data, tuple):
                    data = [data]

                cursor.executemany(sql, data)
                conn.commit()
                break
            except mysql.connector.Error as e:
                print(f"Erro ao inserir dados: {data}. Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)

        conn.close()
