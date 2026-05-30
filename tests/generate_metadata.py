# tests/generate_metadata.py
import json
import hashlib
import os

def generate_metadata(filepath, piece_size=1024):
    with open(filepath, 'rb') as f:
        data = f.read()
    pieces_count = (len(data) + piece_size - 1) // piece_size
    info_hash = hashlib.sha256(data).hexdigest()
    metadata = {
        'filename': os.path.basename(filepath),
        'piece_size': piece_size,
        'pieces_count': pieces_count,
        'total_size': len(data),
        'info_hash': info_hash
    }
    return metadata

if __name__ == '__main__':
    import sys
    filepath = sys.argv[1]
    piece_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1024
    meta = generate_metadata(filepath, piece_size)
    with open(filepath + '.meta', 'w') as f:
        json.dump(meta, f)
    print(f"Metadados salvos em {filepath}.meta")
