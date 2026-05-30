import asyncio
import struct
import logging
import json
from protocol import *

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
        self.remote_info_hash = None
        self.metadata_received = False

        # Controle de fluxo: rastreia blocos já requisitados mas ainda não recebidos.
        # Evita pedir o mesmo bloco múltiplas vezes (requisições duplicadas).
        self.in_flight = set()
        self.max_pending = 10

        # Sinaliza que a conexão está ativa; request_pieces usa para encerrar
        # ao detectar queda da conexão.
        self._active = True

    # ------------------------------------------------------------------ sends

    async def send_handshake(self):
        info_hash = self.peer.file_manager.info_hash or ""
        if isinstance(info_hash, str):
            info_hash = info_hash.encode()
        if len(info_hash) < 64:
            info_hash = info_hash.ljust(64, b'\x00')
        peer_bytes = self.peer.peer_id.encode()
        self.writer.write(pack_message(MSG_HANDSHAKE, info_hash + peer_bytes))
        await self.writer.drain()

    async def send_metadata(self):
        meta = self.peer.file_manager.save_metadata()
        self.writer.write(pack_message(MSG_METADATA, json.dumps(meta).encode()))
        await self.writer.drain()

    async def send_bitfield(self):
        self.writer.write(pack_message(MSG_BITFIELD,
                                       bytes(self.peer.file_manager.bitfield)))
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
        self.writer.write(pack_message(MSG_REQUEST, struct.pack('!I', index)))
        await self.writer.drain()

    async def send_piece(self, index, data):
        self.writer.write(pack_message(MSG_PIECE, struct.pack('!I', index) + data))
        await self.writer.drain()

    async def send_have(self, index):
        self.writer.write(pack_message(MSG_HAVE, struct.pack('!I', index)))
        await self.writer.drain()

    # ------------------------------------------------------------------ utils

    def _remote_has_block(self, index):
        if self.remote_bitfield is None:
            return False
        byte_idx, bit_idx = index // 8, index % 8
        if byte_idx >= len(self.remote_bitfield):
            return False
        return bool(self.remote_bitfield[byte_idx] & (1 << (7 - bit_idx)))

    # ------------------------------------------------------------------ main loop

    async def handle_connection(self):
        try:
            # ----- 1. HANDSHAKE -----
            if self.is_outgoing:
                await self.send_handshake()
                msg_type, payload = await recv_message(self.reader)
                if msg_type != MSG_HANDSHAKE:
                    logger.error("Esperado HANDSHAKE, recebido %d", msg_type)
                    return
            else:
                msg_type, payload = await recv_message(self.reader)
                if msg_type != MSG_HANDSHAKE:
                    logger.error("Esperado HANDSHAKE, recebido %d", msg_type)
                    return
                await self.send_handshake()

            if len(payload) < 64:
                logger.error("Payload handshake muito curto")
                return
            self.remote_info_hash = payload[:64].decode().strip('\x00')
            self.remote_peer_id = payload[64:].decode()

            # ----- 2. METADADOS -----
            local_has_metadata = bool(self.peer.file_manager.info_hash)
            remote_has_metadata = bool(self.remote_info_hash)

            if local_has_metadata and not remote_has_metadata:
                await self.send_metadata()
                logger.info("Metadados enviados para %s", self.remote_peer_id)
            elif not local_has_metadata and remote_has_metadata:
                msg_type, payload = await recv_message(self.reader)
                if msg_type != MSG_METADATA:
                    logger.error("Esperado METADATA, recebido %d", msg_type)
                    return
                meta = json.loads(payload.decode())
                self.peer.file_manager.load_metadata(meta)
                logger.info("Metadados recebidos: %s (%d blocos)",
                            self.peer.file_manager.filename,
                            self.peer.file_manager.pieces_count)
                self.metadata_received = True

            # ----- 3. BITFIELD -----
            await self.send_bitfield()
            msg_type, payload = await recv_message(self.reader)
            if msg_type != MSG_BITFIELD:
                logger.error("Esperado BITFIELD, recebido %d", msg_type)
                return
            self.remote_bitfield = bytearray(payload)

            missing = self.peer.file_manager.missing_blocks()
            if missing and any(self._remote_has_block(i) for i in missing):
                await self.send_interested()

            # ----- 4. LOOP PRINCIPAL -----
            while True:
                msg_type, payload = await recv_message(self.reader)

                if msg_type == MSG_INTERESTED:
                    self.remote_interested = True

                elif msg_type == MSG_NOT_INTERESTED:
                    self.remote_interested = False

                elif msg_type == MSG_REQUEST:
                    index = struct.unpack('!I', payload)[0]
                    if self.peer.file_manager.has_block(index):
                        await self.send_piece(index,
                                              self.peer.file_manager.blocks[index])

                elif msg_type == MSG_PIECE:
                    index = struct.unpack('!I', payload[:4])[0]
                    data = payload[4:]
                    # Remove do in_flight independente de ter o bloco ou não
                    self.in_flight.discard(index)
                    if not self.peer.file_manager.has_block(index):
                        self.peer.file_manager.set_block(index, data)
                        logger.info("Bloco %d recebido de %s", index,
                                    self.remote_peer_id)
                        await self.peer.broadcast_have(index)
                        if self.peer.file_manager.is_complete():
                            logger.info("Download completo!")
                            self.peer.save_file()

                elif msg_type == MSG_HAVE:
                    index = struct.unpack('!I', payload)[0]
                    byte_idx, bit_idx = index // 8, index % 8
                    if byte_idx < len(self.remote_bitfield):
                        self.remote_bitfield[byte_idx] |= (1 << (7 - bit_idx))
                    if not self.am_interested:
                        missing = self.peer.file_manager.missing_blocks()
                        if missing and any(self._remote_has_block(i)
                                          for i in missing):
                            await self.send_interested()

        except Exception as e:
            logger.error("Erro na conexão com %s: %s", self.remote_peer_id, e)
        finally:
            self._active = False
            self.in_flight.clear()
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()

    # ------------------------------------------------------------------ request loop

    async def request_pieces(self):
        """Solicita blocos faltantes ao peer remoto de forma pipelined."""
        # Aguarda handshake completar
        while self._active and self.remote_bitfield is None:
            await asyncio.sleep(0.1)

        while self._active and not self.peer.file_manager.is_complete():
            if not self.am_interested:
                await asyncio.sleep(0.3)
                continue

            if len(self.in_flight) >= self.max_pending:
                await asyncio.sleep(0.005)
                continue

            missing = self.peer.file_manager.missing_blocks()
            # Exclui blocos já em voo para evitar requisições duplicadas
            possible = [i for i in missing
                        if self._remote_has_block(i) and i not in self.in_flight]
            if not possible:
                await asyncio.sleep(0.3)
                continue

            block_index = possible[0]
            self.in_flight.add(block_index)
            await self.send_request(block_index)
            # Yield ao event loop para handle_connection processar respostas
            await asyncio.sleep(0)
