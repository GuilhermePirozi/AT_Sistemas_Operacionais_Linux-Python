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
    parser.add_argument("--n-tasks", type=int)
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument(
        "--comparar",
        action="store_true",
        help="executa testes com 1, 5 e 10 milhoes de amostras",
    )
    argumentos = parser.parse_args()

    if argumentos.n_tasks is not None and argumentos.n_tasks <= 0:
        parser.error("n-tasks deve ser maior que zero")

    if argumentos.chunk_size <= 0:
        parser.error("n-tasks e chunk-size devem ser maiores que zero")

    return argumentos

async def executar_teste(n_tasks, chunk_size):
    total_samples = n_tasks * chunk_size
    pi_serial, tempo_serial = executar_serial(total_samples)
    pi_paralelo, tempo_paralelo, pids_workers = await executar_paralelo(
        n_tasks, chunk_size
    )

    return {
        "n_tasks": n_tasks,
        "chunk_size": chunk_size,
        "total_samples": total_samples,
        "pi_serial": pi_serial,
        "tempo_serial": tempo_serial,
        "pi_paralelo": pi_paralelo,
        "tempo_paralelo": tempo_paralelo,
        "pids_workers": pids_workers,
    }


def formatar_amostras(total_samples):
    return f"{total_samples:,}".replace(",", ".")

async def executar_comparacao(chunk_size):
    resultados = []

    for n_tasks in (10, 50, 100):
        total_samples = n_tasks * chunk_size
        print(f"Executando teste com {formatar_amostras(total_samples)} amostras...")
        resultados.append(await executar_teste(n_tasks, chunk_size))

    print("\n" + "=" * 72)
    print("COMPARACAO FINAL")
    print("=" * 72)

    todos_os_tempos = []
    for numero_teste, resultado in enumerate(resultados, start=1):
        mais_rapida = (
            "Versao serial"
            if resultado["tempo_serial"] < resultado["tempo_paralelo"]
            else "Versao paralela"
        )
        print(f"\nTESTE {numero_teste}")
        print(f"Numero de tarefas (n_tasks): {resultado['n_tasks']}")
        print(
            "Tamanho de cada bloco (chunk_size): "
            f"{formatar_amostras(resultado['chunk_size'])}"
        )
        print(
            "Quantidade total de amostras: "
            f"{formatar_amostras(resultado['total_samples'])}"
        )
        print(
            f"Versao serial:   pi estimado = {resultado['pi_serial']:.8f} "
            f"| tempo de execucao = {resultado['tempo_serial']:.4f} s"
        )
        print(
            f"Versao paralela: pi estimado = {resultado['pi_paralelo']:.8f} "
            f"| tempo de execucao = {resultado['tempo_paralelo']:.4f} s"
        )
        print(f"Versao mais rapida: {mais_rapida}")
        print("-" * 72)
        todos_os_tempos.extend(
            [
                (
                    resultado["tempo_serial"],
                    "Versao serial",
                    resultado["total_samples"],
                ),
                (
                    resultado["tempo_paralelo"],
                    "Versao paralela",
                    resultado["total_samples"],
                ),
            ]
        )

    menor = min(todos_os_tempos)
    maior = max(todos_os_tempos)
    print("\nRESUMO DOS TEMPOS")
    print("-" * 72)
    print(
        f"Menor tempo: {menor[0]:.4f} s - {menor[1]} "
        f"com {formatar_amostras(menor[2])} amostras"
    )
    print(
        f"Maior tempo: {maior[0]:.4f} s - {maior[1]} "
        f"com {formatar_amostras(maior[2])} amostras"
    )

async def main():
    argumentos = ler_argumentos()

    if argumentos.comparar or argumentos.n_tasks is None:
        await executar_comparacao(argumentos.chunk_size)
        return

    resultado = await executar_teste(argumentos.n_tasks, argumentos.chunk_size)

    total_samples = resultado["total_samples"]
    print(f"Total de amostras: {total_samples}")
    print(
        f"Serial   -> pi = {resultado['pi_serial']:.8f} "
        f"| tempo = {resultado['tempo_serial']:.4f} s"
    )
    print(
        f"Paralela -> pi = {resultado['pi_paralelo']:.8f} "
        f"| tempo = {resultado['tempo_paralelo']:.4f} s"
    )
    print(f"PIDs dos workers: {sorted(resultado['pids_workers'])}")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    asyncio.run(main())