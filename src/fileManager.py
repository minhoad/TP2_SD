import hashlib
import os
import json

class FileManager:
    def __init__(self, piece_size=1024, metadata=None):
        self.piece_size = piece_size
        self.blocks = {}          # índice -> bytes
        self.total_size = 0
        self.pieces_count = 0
        self.bitfield = bytearray()
        self.filename = None
        self.info_hash = None

        if metadata:
            self.load_metadata(metadata)

    def create_from_file(self, filepath):
        self.filename = os.path.basename(filepath)
        with open(filepath, 'rb') as f:
            data = f.read()
        self.total_size = len(data)
        self.pieces_count = (self.total_size + self.piece_size - 1) // self.piece_size
        self.bitfield = bytearray((self.pieces_count + 7) // 8)
        self.info_hash = hashlib.sha256(data).hexdigest()
        for i in range(self.pieces_count):
            start = i * self.piece_size
            end = min(start + self.piece_size, self.total_size)
            self.blocks[i] = data[start:end]
            self._set_bit(i)

    def load_metadata(self, metadata):
        """Carrega metadados (info_hash, pieces_count, filename) sem os blocos."""
        self.info_hash = metadata['info_hash']
        self.pieces_count = metadata['pieces_count']
        self.piece_size = metadata['piece_size']
        self.filename = metadata['filename']
        self.total_size = metadata.get('total_size', self.pieces_count * self.piece_size)
        self.bitfield = bytearray((self.pieces_count + 7) // 8)
        # blocks dicionário vazio

    def save_metadata(self):
        return {
            'info_hash': self.info_hash,
            'pieces_count': self.pieces_count,
            'piece_size': self.piece_size,
            'filename': self.filename,
            'total_size': self.total_size
        }

    def _set_bit(self, index):
        byte_idx = index // 8
        bit_idx = index % 8
        self.bitfield[byte_idx] |= (1 << (7 - bit_idx))

    def has_block(self, index):
        byte_idx = index // 8
        bit_idx = index % 8
        return bool(self.bitfield[byte_idx] & (1 << (7 - bit_idx)))

    def set_block(self, index, data):
        self.blocks[index] = data
        self._set_bit(index)

    def missing_blocks(self):
        return [i for i in range(self.pieces_count) if not self.has_block(i)]

    def is_complete(self):
        return len(self.blocks) == self.pieces_count and all(self.has_block(i) for i in range(self.pieces_count))

    def save_file(self, output_path):
        os.makedirs(output_path, exist_ok=True)
        full_path = os.path.join(output_path, self.filename)
        with open(full_path, 'wb') as f:
            for i in range(self.pieces_count):
                f.write(self.blocks[i])
        # Verificação de integridade
        with open(full_path, 'rb') as f:
            if hashlib.sha256(f.read()).hexdigest() != self.info_hash:
                raise Exception("Checksum do arquivo final inválido!")
        return full_path