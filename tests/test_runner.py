import subprocess
import time
import sys
import hashlib
import os
import signal

TEST_CONFIG = {
    "piece_size": 1024,
    "peers": [
        {"host": "127.0.0.1", "port": 8000, "file": "fileA.dat"},
        {"host": "127.0.0.1", "port": 8001},
        {"host": "127.0.0.1", "port": 8002},
        {"host": "127.0.0.1", "port": 8003}
    ],
    "neighbors": {
        "127.0.0.1:8000": ["127.0.0.1:8001", "127.0.0.1:8002"],
        "127.0.0.1:8001": ["127.0.0.1:8000", "127.0.0.1:8003"],
        "127.0.0.1:8002": ["127.0.0.1:8000"],
        "127.0.0.1:8003": ["127.0.0.1:8001"]
    }
}

def run_test(file_name, max_wait=60):
    # Gera arquivo se não existir
    if not os.path.exists(file_name):
        size_map = {
            'fileA.dat': 10*1024,
            'fileB.dat': 1*1024*1024,
            'fileC.dat': 10*1024*1024
        }
        with open(file_name, 'wb') as f:
            f.write(os.urandom(size_map[file_name]))
    original_hash = hashlib.sha256(open(file_name, 'rb').read()).hexdigest()

    # Cria arquivo config.json temporário
    with open('config.json', 'w') as f:
        json.dump(TEST_CONFIG, f)

    processes = []
    try:
        for peer_cfg in TEST_CONFIG['peers']:
            host, port = peer_cfg['host'], peer_cfg['port']
            cmd = [sys.executable, 'peer.py', '--host', host, '--port', str(port)]
            if peer_cfg.get('file') == file_name:
                cmd += ['--file', file_name]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(proc)
            print(f"Iniciado peer {host}:{port}")

        # Aguarda até todos os leechers terem o arquivo ou timeout
        start = time.time()
        completed = set()
        while time.time() - start < max_wait:
            time.sleep(2)
            all_done = True
            for peer_cfg in TEST_CONFIG['peers']:
                if peer_cfg.get('file'):  # seeder
                    continue
                peer_id = f"{peer_cfg['host']}:{peer_cfg['port']}"
                download_path = f"downloads/{file_name}"
                if os.path.exists(download_path):
                    with open(download_path, 'rb') as f:
                        if hashlib.sha256(f.read()).hexdigest() == original_hash:
                            completed.add(peer_id)
                else:
                    all_done = False
            if len(completed) == len([p for p in TEST_CONFIG['peers'] if not p.get('file')]):
                print("Todos os leechers concluíram o download com sucesso.")
                break
            else:
                print(f"Aguardando... {len(completed)}/{len([p for p in TEST_CONFIG['peers'] if not p.get('file')])} completos")
        else:
            print("Timeout: alguns peers não completaram o download.")

        # Verificação final
        for peer_cfg in TEST_CONFIG['peers']:
            if peer_cfg.get('file'):
                continue
            download_path = f"downloads/{file_name}"
            if not os.path.exists(download_path):
                print(f"FALHA: Peer {peer_cfg['host']}:{peer_cfg['port']} não baixou o arquivo.")
                continue
            with open(download_path, 'rb') as f:
                downloaded_hash = hashlib.sha256(f.read()).hexdigest()
            if downloaded_hash == original_hash:
                print(f"OK: Peer {peer_cfg['host']}:{peer_cfg['port']} arquivo íntegro.")
            else:
                print(f"FALHA: Peer {peer_cfg['host']}:{peer_cfg['port']} hash incorreto.")

    finally:
        for proc in processes:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

if __name__ == '__main__':
    import json
    # Executar para cada arquivo de teste
    run_test('fileA.dat')
    # run_test('fileB.dat')  
    # run_test('fileC.dat')