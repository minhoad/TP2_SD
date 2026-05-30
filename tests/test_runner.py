"""
test_runner.py — Executor de testes para o sistema P2P TP2_SD

Uso:
    # Um caso específico
    python3 tests/test_runner.py --file fileA.dat --peers 2 --piece-size 1024

    # Suite completa definida na Tabela 1 do enunciado
    python3 tests/test_runner.py --run-all
"""
import subprocess
import time
import sys
import hashlib
import os
import json
import argparse

# ---------------------------------------------------------------------------
# Topologias estáticas (sem Tracker)
# ---------------------------------------------------------------------------
TOPOLOGIES = {
    2: {
        "peers": [
            {"host": "127.0.0.1", "port": 8000, "file": True},   # seeder
            {"host": "127.0.0.1", "port": 8001},                   # leecher
        ],
        "neighbors": {
            "127.0.0.1:8000": ["127.0.0.1:8001"],
            "127.0.0.1:8001": ["127.0.0.1:8000"],
        },
    },
    4: {
        "peers": [
            {"host": "127.0.0.1", "port": 8000, "file": True},
            {"host": "127.0.0.1", "port": 8001},
            {"host": "127.0.0.1", "port": 8002},
            {"host": "127.0.0.1", "port": 8003},
        ],
        "neighbors": {
            "127.0.0.1:8000": ["127.0.0.1:8001", "127.0.0.1:8002"],
            "127.0.0.1:8001": ["127.0.0.1:8000", "127.0.0.1:8003"],
            "127.0.0.1:8002": ["127.0.0.1:8000"],
            "127.0.0.1:8003": ["127.0.0.1:8001"],
        },
    },
}

# ---------------------------------------------------------------------------
# Tabela 1 do enunciado — casos de teste obrigatórios
# ---------------------------------------------------------------------------
#  (label, file_name, num_peers, piece_size, max_wait_s)
TEST_SUITE = [
    # ── Padrão: 2 peers, bloco 1 KB ─────────────────────────────────────────
    ("TC-01  2 peers · 1 KB · fileA  10 KB",  "fileA.dat",      2, 1024,  30),
    ("TC-02  2 peers · 1 KB · fileB   1 MB",  "fileB.dat",      2, 1024,  60),
    ("TC-03  2 peers · 1 KB · fileC  10 MB",  "fileC.dat",      2, 1024, 120),
    # ── Variação: 4 peers ────────────────────────────────────────────────────
    ("TC-04  4 peers · 1 KB · fileA  10 KB",  "fileA.dat",      4, 1024,  30),
    ("TC-05  4 peers · 1 KB · fileB   1 MB",  "fileB.dat",      4, 1024,  60),
    ("TC-06  4 peers · 1 KB · fileC  10 MB",  "fileC.dat",      4, 1024, 120),
    # ── Variação: bloco 4 KB ─────────────────────────────────────────────────
    ("TC-07  4 peers · 4 KB · fileA  10 KB",  "fileA.dat",      4, 4096,  30),
    ("TC-08  4 peers · 4 KB · fileB   1 MB",  "fileB.dat",      4, 4096,  30),
    ("TC-09  4 peers · 4 KB · fileC  10 MB",  "fileC.dat",      4, 4096,  60),
    # ── Variação: arquivos maiores ────────────────────────────────────────────
    ("TC-10  2 peers · 1 KB · fileA  20 KB",  "fileA_20kb.dat", 2, 1024,  30),
    ("TC-11  2 peers · 1 KB · fileB   5 MB",  "fileB_5mb.dat",  2, 1024,  90),
    ("TC-12  2 peers · 1 KB · fileC  20 MB",  "fileC_20mb.dat", 2, 1024, 180),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def _kill_lingering_peers(num_peers: int) -> None:
    """Encerra processos peer.py ainda em execução."""
    # pkill é mais confiável que fuser -k no ambiente de sandbox
    try:
        subprocess.run(['pkill', '-9', '-f', 'peer.py'],
                       capture_output=True, timeout=3)
    except Exception:
        pass
    time.sleep(0.5)  # aguarda OS liberar as portas


def _ensure_file(file_name: str) -> None:
    """Gera o arquivo de teste se ainda não existir."""
    if os.path.exists(file_name):
        return
    size_map = {
        'fileA.dat':       10 * 1024,
        'fileA_20kb.dat':  20 * 1024,
        'fileB.dat':        1 * 1024 * 1024,
        'fileB_5mb.dat':    5 * 1024 * 1024,
        'fileC.dat':       10 * 1024 * 1024,
        'fileC_20mb.dat':  20 * 1024 * 1024,
    }
    size = size_map.get(file_name)
    if size is None:
        raise ValueError(f"Tamanho desconhecido para '{file_name}'")
    with open(file_name, 'wb') as f:
        f.write(os.urandom(size))
    print(f"  Gerado {file_name}  ({size:,} bytes)")


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def run_test(file_name: str,
             piece_size: int = 1024,
             num_peers: int = 4,
             max_wait: int = 120) -> dict:
    """
    Executa um teste P2P e retorna um dicionário de resultado.

    Retorno:
        {
            "ok": bool,
            "elapsed": float,           # segundos até todos completarem (ou timeout)
            "leechers": {peer_id: {"ok": bool, "size_ok": bool, "hash_ok": bool}},
            "pieces": int,              # total de blocos
        }
    """
    _ensure_file(file_name)
    topo = TOPOLOGIES[num_peers]

    original_hash = _sha256(file_name)
    original_size = os.path.getsize(file_name)
    pieces = (original_size + piece_size - 1) // piece_size

    # Escreve config.json em src/
    config = {"piece_size": piece_size, "neighbors": topo["neighbors"]}
    os.makedirs('src', exist_ok=True)
    with open(os.path.join('src', 'config.json'), 'w') as f:
        json.dump(config, f)

    os.makedirs('logs', exist_ok=True)
    _kill_lingering_peers(num_peers)

    processes, log_files = [], []
    leechers = [p for p in topo["peers"] if not p.get("file")]

    try:
        for peer_cfg in topo["peers"]:
            host, port = peer_cfg["host"], peer_cfg["port"]
            cmd = [sys.executable, 'src/peer.py',
                   '--host', host, '--port', str(port),
                   '--config', 'src/config.json']
            if peer_cfg.get("file"):
                cmd += ['--file', file_name]

            log_path = os.path.join('logs', f'peer_{port}.log')
            fh = open(log_path, 'w')
            log_files.append(fh)

            processes.append(subprocess.Popen(cmd, stdout=fh, stderr=fh))

            # Delay entre peers: garante que cada um esteja ouvindo
            # antes que o próximo tente conectar.
            time.sleep(0.8)

        # Aguarda leechers
        start = time.time()
        completed: set = set()

        while time.time() - start < max_wait:
            time.sleep(1)
            for lp in leechers:
                pid = f"{lp['host']}:{lp['port']}"
                if pid in completed:
                    continue
                dl = os.path.join('downloads', pid, file_name)
                if (os.path.exists(dl)
                        and os.path.getsize(dl) == original_size
                        and _sha256(dl) == original_hash):
                    completed.add(pid)
            if len(completed) == len(leechers):
                break

        elapsed = round(time.time() - start, 1)

        # Verifica resultado
        leecher_results = {}
        for lp in leechers:
            pid = f"{lp['host']}:{lp['port']}"
            dl = os.path.join('downloads', pid, file_name)
            if not os.path.exists(dl):
                leecher_results[pid] = {"ok": False,
                                        "size_ok": False, "hash_ok": False}
                continue
            size_ok = os.path.getsize(dl) == original_size
            hash_ok = _sha256(dl) == original_hash
            leecher_results[pid] = {"ok": size_ok and hash_ok,
                                    "size_ok": size_ok, "hash_ok": hash_ok}

        all_ok = all(v["ok"] for v in leecher_results.values())
        return {"ok": all_ok, "elapsed": elapsed,
                "leechers": leecher_results, "pieces": pieces}

    finally:
        for proc in processes:
            proc.terminate()
        deadline = time.time() + 5
        for proc in processes:
            try:
                proc.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for fh in log_files:
            fh.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_result(label, file_name, num_peers, piece_size, result):
    status = "PASS" if result["ok"] else "FAIL"
    size_mb = os.path.getsize(file_name) / (1024 * 1024)
    print(f"  [{status}]  {label}")
    print(f"         arquivo={file_name} ({size_mb:.2f} MB)  "
          f"peers={num_peers}  bloco={piece_size}B  "
          f"blocos={result['pieces']}  "
          f"tempo={result['elapsed']}s")
    for pid, r in result["leechers"].items():
        ok_str = "OK" if r["ok"] else "FALHA"
        print(f"         {pid}: {ok_str}  "
              f"tamanho={'OK' if r['size_ok'] else 'ERR'}  "
              f"hash={'OK' if r['hash_ok'] else 'ERR'}")


def cmd_run_all():
    results = []
    print(f"\n{'='*65}")
    print(" SUITE COMPLETA — Tabela 1")
    print(f"{'='*65}\n")

    for label, file_name, num_peers, piece_size, max_wait in TEST_SUITE:
        print(f"→ {label}")
        os.makedirs('downloads', exist_ok=True)
        # Limpa downloads anteriores para não poluir o resultado
        import shutil
        shutil.rmtree('downloads', ignore_errors=True)
        shutil.rmtree('logs', ignore_errors=True)

        result = run_test(file_name, piece_size=piece_size,
                          num_peers=num_peers, max_wait=max_wait)
        _print_result(label, file_name, num_peers, piece_size, result)
        results.append((label, result))
        print()

    # Resumo final
    print(f"\n{'='*65}")
    print(" RESUMO")
    print(f"{'='*65}")
    passed = sum(1 for _, r in results if r["ok"])
    print(f"  {passed}/{len(results)} testes passaram\n")
    print(f"  {'Caso de Teste':<42} {'Resultado':>8}  {'Tempo':>7}  Blocos")
    print(f"  {'-'*42} {'-'*8}  {'-'*7}  ------")
    for label, r in results:
        status = "PASS" if r["ok"] else "FAIL"
        print(f"  {label:<42} {status:>8}  {r['elapsed']:>6.1f}s  {r['pieces']}")


def cmd_single(file_name, num_peers, piece_size, max_wait):
    import shutil
    shutil.rmtree('downloads', ignore_errors=True)
    shutil.rmtree('logs', ignore_errors=True)
    label = f"{num_peers} peers · {piece_size}B · {file_name}"
    print(f"\n→ {label}")
    result = run_test(file_name, piece_size=piece_size,
                      num_peers=num_peers, max_wait=max_wait)
    _print_result(label, file_name, num_peers, piece_size, result)
    return result["ok"]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runner de testes P2P — TP2 Sistemas Distribuídos')
    parser.add_argument('--file', default='fileA.dat',
                        help='Arquivo a transferir (ex: fileA.dat)')
    parser.add_argument('--peers', type=int, default=4, choices=[2, 4],
                        help='Número de peers (2 ou 4)')
    parser.add_argument('--piece-size', type=int, default=1024,
                        help='Tamanho do bloco em bytes (ex: 1024, 4096)')
    parser.add_argument('--timeout', type=int, default=120,
                        help='Timeout máximo em segundos')
    parser.add_argument('--run-all', action='store_true',
                        help='Executa a suite completa (Tabela 1 do enunciado)')
    args = parser.parse_args()

    if args.run_all:
        cmd_run_all()
    else:
        ok = cmd_single(args.file, args.peers, args.piece_size, args.timeout)
        sys.exit(0 if ok else 1)
