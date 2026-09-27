import argparse
import asyncio
import multiprocessing
import os
import random
import time
from datetime import datetime


def simular_bloco(numero_do_bloco, samples_partial):
    gerador = random.Random(os.getpid() + time.time_ns() + numero_do_bloco)
    hits = 0

    for _ in range(samples_partial):
        x = gerador.uniform(-1, 1)
        y = gerador.uniform(-1, 1)

        if x**2 + y**2 <= 1:
            hits += 1

    return hits, os.getpid()


def executar_serial(total_samples):
    inicio = time.perf_counter()
    hits, _ = simular_bloco(0, total_samples)
    pi_estimado = 4 * hits / total_samples
    tempo = time.perf_counter() - inicio
    return pi_estimado, tempo


def escrever_log(caminho, blocos_concluidos, n_tasks):
    instante = datetime.now().isoformat(timespec="seconds")
    progresso = 100 * blocos_concluidos / n_tasks

    with open(caminho, "a", encoding="utf-8") as arquivo:
        arquivo.write(
            f"{instante} | progresso={progresso:.2f}% "
            f"({blocos_concluidos}/{n_tasks}) | pid={os.getpid()}\n"
        )


async def tarefa_de_monitoramento(evento_fim):
    while not evento_fim.is_set():
        await asyncio.sleep(0.1)


async def executar_paralelo(n_tasks, chunk_size):
    inicio = time.perf_counter()
    evento_fim = asyncio.Event()
    tarefas_async = [
        asyncio.create_task(tarefa_de_monitoramento(evento_fim))
        for _ in range(20)
    ]

    total_hits = 0
    blocos_concluidos = 0
    pids_workers = set()
    escrever_log("monitoramento.log", blocos_concluidos, n_tasks)

    try:
        with multiprocessing.Pool() as pool:
            resultados = [
                pool.apply_async(simular_bloco, (numero, chunk_size))
                for numero in range(n_tasks)
            ]

            pendentes = list(resultados)

            while pendentes:
                concluidos = [resultado for resultado in pendentes if resultado.ready()]

                for resultado in concluidos:
                    hits, pid_worker = resultado.get()
                    total_hits += hits
                    pids_workers.add(pid_worker)
                    blocos_concluidos += 1
                    pendentes.remove(resultado)
                    escrever_log("monitoramento.log", blocos_concluidos, n_tasks)

                await asyncio.sleep(0.05)
    finally:
        evento_fim.set()
        await asyncio.gather(*tarefas_async)

    total_samples = n_tasks * chunk_size
    pi_estimado = 4 * total_hits / total_samples
    tempo = time.perf_counter() - inicio
    return pi_estimado, tempo, pids_workers


def ler_argumentos():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-tasks", type=int, default=100)
    parser.add_argument("--chunk-size", type=int, default=100_000)
    argumentos = parser.parse_args()

    if argumentos.n_tasks <= 0 or argumentos.chunk_size <= 0:
        parser.error("n-tasks e chunk-size devem ser maiores que zero")

    return argumentos


async def main():
    argumentos = ler_argumentos()
    total_samples = argumentos.n_tasks * argumentos.chunk_size

    pi_serial, tempo_serial = executar_serial(total_samples)
    pi_paralelo, tempo_paralelo, pids_workers = await executar_paralelo(
        argumentos.n_tasks, argumentos.chunk_size
    )

    print(f"Total de amostras: {total_samples}")
    print(f"Serial   -> pi = {pi_serial:.8f} | tempo = {tempo_serial:.4f} s")
    print(f"Paralela -> pi = {pi_paralelo:.8f} | tempo = {tempo_paralelo:.4f} s")
    print(f"PIDs dos workers: {sorted(pids_workers)}")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    asyncio.run(main())
