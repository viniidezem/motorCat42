import time

import mysql.connector


def connect():
    while True:
        try:
            return mysql.connector.connect(
                host="localhost",
                port="3308",
                user="root",
                password="citel13347",
                database="AUTCOM_BAELETRICA")

            break

        except mysql.connector.Error as e:

            # se não conseguir se conectar ao banco, espera um tempo e tenta novamente
            print("Erro ao conectar ao banco. Tentando novamente em 5 segundos...")

            time.sleep(5)


def exec_sql(command, retorno, commit=False):
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(command)
    result = []
    if retorno:
        column_names = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
        for row in rows:
            values = {column_names[i]: row[i] for i in range(len(row))}
            result.extend([values])
        if commit:
            conn.commit()
            conn.close()
        else:
            conn.close()
        return result
    else:
        if commit:
            conn.commit()
            conn.close()
        else:
            conn.close()


def insert_data(sql, tupla):
    conn = connect()
    cursor = conn.cursor()
    values = (tupla)
    if len(values) > 0:
        if type(values[0]) is tuple:
            cursor.executemany(sql, values)
        else:
            cursor.execute(sql, values)
        conn.commit()
    conn.close()
