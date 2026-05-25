import os
size_map = {
    'fileA.dat': 10 * 1024,    # 10 KB
    'fileB.dat': 1 * 1024 * 1024,  # 1 MB
    'fileC.dat': 10 * 1024 * 1024  # 10 MB
}
for name, size in size_map.items():
    with open(name, 'wb') as f:
        f.write(os.urandom(size))
    print(f"Criado {name} ({size} bytes)")