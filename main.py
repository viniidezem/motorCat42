import multiprocessing
import time
from threads_functions import init_task_queue, create_threads, wait_for_threads_to_finish


def run_threads(max_threads):
    """Executa as threads."""
    q = init_task_queue()
    threads = create_threads(q, max_threads)
    wait_for_threads_to_finish(q, threads)


def main():
    # Define o número máximo de threads que podem executar ao mesmo tempo
    MAX_THREADS = 1

    # Define o número de threads com base no número de núcleos da CPU
    num_cores = multiprocessing.cpu_count()
    num_threads = min(10, num_cores)

    # Executa as threads
    tempo_inicial = time.time()
    run_threads(MAX_THREADS)
    tempo_final = time.time()

    # Calcula o tempo de execução
    tempo_execucao = tempo_final - tempo_inicial

    # Imprime o tempo de execução
    print("Tempo de execução:", tempo_execucao, "segundos")


if __name__ == "__main__":
    main()
