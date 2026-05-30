import os

# Tabela 1 do enunciado: valores padrão + variações de teste
FILES = {
    'fileA.dat':       10 * 1024,          # 10 KB  (File A — padrão)
    'fileA_20kb.dat':  20 * 1024,          # 20 KB  (File A — variação)
    'fileB.dat':        1 * 1024 * 1024,   #  1 MB  (File B — padrão)
    'fileB_5mb.dat':    5 * 1024 * 1024,   #  5 MB  (File B — variação)
    'fileC.dat':       10 * 1024 * 1024,   # 10 MB  (File C — padrão)
    'fileC_20mb.dat':  20 * 1024 * 1024,   # 20 MB  (File C — variação)
}

for name, size in FILES.items():
    with open(name, 'wb') as f:
        f.write(os.urandom(size))
    print(f"Gerado {name:20s}  ({size:>12,} bytes)")
