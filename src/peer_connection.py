import asyncio
import struct
import logging
from protocol import *
from fileManager import FileManager

logger = logging.getLogger("PeerConnection")

class PeerConnection:
    def __init__(self, peer, reader=None, writer=None, is_outgoing=False):
        self.peer = peer
        self.reader = reader
        self.writer = writer
        self.remote_bitfield = None
        self.am_interested = False
        self.remote_interested = False
        self.remote_peer_id = None
        self.is_outgoing = is_outgoing
        self.pending_requests = 0
        self.max_pending = 5
        self.remote_info_hash = None

    async def send_handshake(self):
        info_hash = self.peer.file_manager.info_hash or b'\x00'*64   # se vazio, envia 64 zeros
        if isinstance(info_hash, str):
            info_hash = info_hash.encode()
        elif len(info_hash) < 64:
            info_hash = info_hash.ljust(64, b'\x00')
        peer_bytes = self.peer.peer_id.encode()
        payload = info_hash + peer_bytes
        self.writer.write(pack_message(MSG_HANDSHAKE, payload))
        await self.writer.drain()

    async def send_metadata(self):
        """Envia metadados do arquivo (seeder -> leecher)."""
        meta = self.peer.file_manager.save_metadata()
        payload = json.dumps(meta).encode()
        self.writer.write(pack_message(MSG_METADATA, payload))
        await self.writer.drain()

    async def send_bitfield(self):
        self.writer.write(pack_message(MSG_BITFIELD, bytes(self.peer.file_manager.bitfield)))
        await self.writer.drain()

    async def send_interested(self):
        self.writer.write(pack_message(MSG_INTERESTED))
        await self.writer.drain()
        self.am_interested = True

    async def send_not_interested(self):
        self.writer.write(pack_message(MSG_NOT_INTERESTED))
        await self.writer.drain()
        self.am_interested = False

    async def send_request(self, index):
        payload = struct.pack('!I', index)
        self.writer.write(pack_message(MSG_REQUEST, payload))
        await self.writer.drain()
        self.pending_requests += 1

    async def send_piece(self, index, data):
        payload = struct.pack('!I', index) + data
        self.writer.write(pack_message(MSG_PIECE, payload))
        await self.writer.drain()

    async def send_have(self, index):
        payload = struct.pack('!I', index)
        self.writer.write(pack_message(MSG_HAVE, payload))
        await self.writer.drain()

    def _remote_has_block(self, index):
        if self.remote_bitfield is None:
            return False
        byte_idx = index // 8
        bit_idx = index % 8
        if byte_idx >= len(self.remote_bitfield):
            return False
        return bool(self.remote_bitfield[byte_idx] & (1 << (7 - bit_idx)))

    async def handle_connection(self):
        try:
            # Handshake
            await self.send_handshake()
            msg_type, payload = await recv_message(self.reader)
            if msg_type != MSG_HANDSHAKE:
                logger.error("Handshake esperado, recebido %d", msg_type)
                return
            if len(payload) < 64:
                logger.error("Payload handshake muito curto")
                return
            self.remote_info_hash = payload[:64].decode().strip('\x00')
            self.remote_peer_id = payload[64:].decode()


            if self.remote_info_hash != self.peer.file_manager.info_hash:
                logger.warning("Info hash diferente, desconectando.")
                return

            # Envia bitfield e recebe bitfield do remoto
            await self.send_bitfield()
            msg_type, payload = await recv_message(self.reader)
            if msg_type != MSG_BITFIELD:
                logger.error("Bitfield esperado, recebido %d", msg_type)
                return
            self.remote_bitfield = bytearray(payload)

            # Se eu estiver faltando blocos e o remoto tiver algum, envio INTERESTED
            if self.peer.file_manager.missing_blocks():
                if any(self._remote_has_block(i) for i in self.peer.file_manager.missing_blocks()):
                    await self.send_interested()

            # Loop de recebimento de mensagens
            while True:
                msg_type, payload = await recv_message(self.reader)
                if msg_type == MSG_INTERESTED:
                    self.remote_interested = True
                elif msg_type == MSG_NOT_INTERESTED:
                    self.remote_interested = False
                elif msg_type == MSG_REQUEST:
                    index = struct.unpack('!I', payload)[0]
                    if self.peer.file_manager.has_block(index):
                        data = self.peer.file_manager.blocks[index]
                        await self.send_piece(index, data)
                elif msg_type == MSG_PIECE:
                    index = struct.unpack('!I', payload[:4])[0]
                    data = payload[4:]
                    if not self.peer.file_manager.has_block(index):
                        self.peer.file_manager.set_block(index, data)
                        logger.info(f"Bloco {index} recebido de {self.remote_peer_id}")
                        self.pending_requests -= 1
                        # Anuncia HAVE para todos os vizinhos
                        await self.peer.broadcast_have(index)
                        # Verifica se completou
                        if self.peer.file_manager.is_complete():
                            logger.info("Download completo!")
                            self.peer.save_file()
                elif msg_type == MSG_HAVE:
                    index = struct.unpack('!I', payload)[0]
                    # Atualiza bitfield remoto
                    byte_idx = index // 8
                    bit_idx = index % 8
                    if byte_idx < len(self.remote_bitfield):
                        self.remote_bitfield[byte_idx] |= (1 << (7 - bit_idx))
                    # Se ainda não estou interessado e o remoto tem algum bloco que me falta, envio INTERESTED
                    if not self.am_interested and self.peer.file_manager.missing_blocks():
                        if any(self._remote_has_block(i) for i in self.peer.file_manager.missing_blocks()):
                            await self.send_interested()
        except Exception as e:
            logger.error(f"Erro na conexão com {self.remote_peer_id}: {e}")
        finally:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()

    async def request_pieces(self):
        """Loop de requisição de blocos faltantes."""
        while self.am_interested and not self.peer.file_manager.is_complete():
            if self.pending_requests >= self.max_pending:
                await asyncio.sleep(0.1)
                continue
            missing = self.peer.file_manager.missing_blocks()
            possible = [i for i in missing if self._remote_has_block(i)]
            if not possible:
                await self.send_not_interested()
                break
            # Escolhe um bloco (simples: o primeiro)
            block_index = possible[0]
            await self.send_request(block_index)
            await asyncio.sleep(0.01)