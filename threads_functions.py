import queue
import threading
from preparaTabelas import itensProcessar
from processaNotas import processaItems


def init_task_queue():
    """Inicializa a fila de tarefas com os itens da tabela."""
    items = itensProcessar()
    q = queue.Queue()
    for item in items:
        q.put(item)
    return q


def create_threads(q, max_threads):
    """Cria as threads e as inicia."""
    threads = []
    semaphore = threading.Semaphore(max_threads)
    for i in range(max_threads):
        t = threading.Thread(target=worker, args=(q, semaphore))
        threads.append(t)
        t.start()
    return threads


def wait_for_threads_to_finish(q, threads):
    """Aguarda todas as threads terminarem."""
    q.join()
    for t in threads:
        t.join()


def worker(q, semaphore):
    """Função que executa as threads"""
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
