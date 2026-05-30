import asyncio
import argparse
import json
import logging
import os
from fileManager import FileManager
from peer_connection import PeerConnection
from protocol import *

logger = logging.getLogger("Peer")

class Peer:
    def __init__(self, host, port, neighbors, file_path=None, piece_size=1024):
        self.host = host
        self.port = port
        self.peer_id = f"{host}:{port}"
        self.neighbors = neighbors  # lista de strings "host:port"
        self.piece_size = piece_size
        self.file_manager = FileManager(piece_size)
        if file_path:
            self.file_manager.create_from_file(file_path)
            logger.info(f"Seeder inicial: {self.file_manager.filename} ({self.file_manager.pieces_count} blocos)")
        else:
            # Leecher: ainda não tem metadados. Serão obtidos via handshake com o primeiro vizinho.
            # Inicialmente, info_hash vazio; será atualizado quando receber o handshake.
            self.file_manager.info_hash = ""  # placeholder
        self.connections = {}  # peer_id -> PeerConnection
        self.server = None

    async def broadcast_have(self, index):
        """Anuncia a todos os vizinhos que agora possui um bloco.

        Apenas envia para conexões onde o handshake já foi concluído
        (remote_bitfield definido). Erros de escrita são ignorados
        individualmente para não propagar exceções para o chamador.
        """
        for conn in self.connections.values():
            if conn.remote_bitfield is None:
                continue  # handshake ainda não completou
            try:
                await conn.send_have(index)
            except Exception:
                pass  # conexão já pode estar encerrada

    async def start(self):
        # Inicia servidor
        self.server = await asyncio.start_server(
            self._handle_new_connection, self.host, self.port
        )
        logger.info(f"Servidor ouvindo em {self.host}:{self.port}")
        # Conecta aos vizinhos (apenas se não for o seeder? sempre conecta)
        await self.connect_to_neighbors()
        # Mantém rodando
        async with self.server:
            await self.server.serve_forever()

    async def _handle_new_connection(self, reader, writer):
        conn = PeerConnection(self, reader, writer, is_outgoing=False)
        peer_addr = f"{writer.get_extra_info('peername')[0]}:{writer.get_extra_info('peername')[1]}"
        self.connections[peer_addr] = conn
        asyncio.create_task(conn.handle_connection())
        asyncio.create_task(conn.request_pieces())

    async def connect_to_neighbors(self):
        for nb in self.neighbors:
            if nb in self.connections:
                continue
            try:
                host, port = nb.split(':')
                port = int(port)
                reader, writer = await asyncio.open_connection(host, port)
                conn = PeerConnection(self, reader, writer, is_outgoing=True)
                self.connections[nb] = conn
                asyncio.create_task(conn.handle_connection())
                asyncio.create_task(conn.request_pieces())
                logger.info(f"Conectado a {nb}")
            except Exception as e:
                logger.error(f"Falha ao conectar a {nb}: {e}")

    def save_file(self):
        output_dir = os.path.join("downloads", self.peer_id)
        saved_path = self.file_manager.save_file(output_dir)
        logger.info(f"Arquivo salvo em {saved_path}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--file', help='Arquivo para ser seeder inicial')
    parser.add_argument('--config', default='config.json')
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    piece_size = config.get('piece_size', 1024)
    neighbors_list = config['neighbors'].get(f"{args.host}:{args.port}", [])

    peer = Peer(args.host, args.port, neighbors_list, args.file, piece_size)
    await peer.start()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    asyncio.run(main())