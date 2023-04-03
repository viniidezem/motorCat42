import multiprocessing
import queue
import threading
import time

from preparaTabelas import itensProcessar
from processaNotas import processaItems


# Função que executa as threads
def worker(q, semaphore):
    while True:
        try:
            item = q.get(timeout=1)
        except queue.Empty:
            break

        # Adquire o semáforo antes de processar o item
        semaphore.acquire()

        # Processa o item
        processaItems(item)

        # Libera o semáforo após processar o item
        semaphore.release()
        q.task_done()

if __name__ == "__main__":

    num_cores = multiprocessing.cpu_count()
    num_threads = min(10, num_cores)

    # tempo inicial
    tempo_inicial = time.time()

    # Conecta ao banco de dados e recupera todos os itens da tabela
    items = itensProcessar()

    # Cria uma fila de tarefas e adiciona todos os itens da tabela
    q = queue.Queue()
    for item in items:
        q.put(item)

    # Cria um semáforo com o número máximo de threads que podem executar ao mesmo tempo
    max_threads = 1 #num_threads
    semaphore = threading.Semaphore(max_threads)

    # Cria as threads e as inicia
    threads = []
    for i in range(max_threads):
        t = threading.Thread(target=worker, args=(q, semaphore))
        threads.append(t)
        t.start()

    # Aguarda todas as threads terminarem
    q.join()
    for t in threads:
        t.join()

    # tempo final
    tempo_final = time.time()

    # tempo de execução
    tempo_execucao = tempo_final - tempo_inicial

    print("Tempo de execução:", tempo_execucao, "segundos")