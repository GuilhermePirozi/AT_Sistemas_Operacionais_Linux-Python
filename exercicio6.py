import asyncio
import multiprocessing
import threading
import time

N = 6

def tarefa(numero):
    time.sleep(0.5)
    return numero * 2

async def tarefa_async(numero):
    await asyncio.sleep(0.5)
    return numero * 2

def versao_original():
    inicio = time.perf_counter()
    for numero in range(N):
        tarefa(numero)
    return time.perf_counter() - inicio

async def versao_asyncio():
    inicio = time.perf_counter()
    tarefas = [tarefa_async(numero) for numero in range(N)]
    await asyncio.gather(*tarefas)
    return time.perf_counter() - inicio

def versao_multithreading():
    resultados = [None] * N

    def worker(numero):
        resultados[numero] = tarefa(numero)

    inicio = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(numero,)) for numero in range(N)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return time.perf_counter() - inicio

def versao_processamento_paralelo():
    inicio = time.perf_counter()
    with multiprocessing.Pool(processes=N) as pool:
        pool.map(tarefa, range(N))
    return time.perf_counter() - inicio

def main():
    tempo_original = versao_original()
    tempo_asyncio = asyncio.run(versao_asyncio())
    tempo_threads = versao_multithreading()
    tempo_processos = versao_processamento_paralelo()

    tempos = [
        ("Original", tempo_original),
        ("Asyncio", tempo_asyncio),
        ("Multithreading", tempo_threads),
        ("Processamento paralelo", tempo_processos),
    ]

    print(f"Tarefas executadas: {N}")
    print(f"{'Versao':<25} {'Tempo (s)':>12} {'Ganho':>12}")
    print("-" * 51)
    for nome, tempo in tempos:
        ganho = tempo_original / tempo
        print(f"{nome:<25} {tempo:>12.3f} {ganho:>11.1f}x")

if __name__ == "__main__":
    main()