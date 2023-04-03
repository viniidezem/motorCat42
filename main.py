from threads_functions import ProcessaNotasThread
from processaNotas import ProcessaNotas
from db import Database

# Crie uma instância da classe Database
db = Database("localhost", "3308", "root", "citel13347", "AUTCOM_BAELETRICA")

# Crie uma instância da classe ProcessaNotas passando a instância do Database como parâmetro
processa_notas = ProcessaNotas(db)

# Crie uma instância da classe ThreadsFunctions passando a instância do ProcessaNotas como parâmetro
threads_functions = ProcessaNotasThread(db, processa_notas,)

# Chame o método run da classe ThreadsFunctions
threads_functions.run()