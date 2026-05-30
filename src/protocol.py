import struct

MSG_HANDSHAKE      = 0x01
MSG_BITFIELD       = 0x02
MSG_INTERESTED     = 0x03
MSG_NOT_INTERESTED = 0x04
MSG_REQUEST        = 0x05
MSG_PIECE          = 0x06
MSG_HAVE           = 0x07
MSG_METADATA       = 0x08

def pack_message(msg_type, payload=b''):
    """Empacota mensagem: tamanho (4 bytes) + tipo (1 byte) + payload."""
    size = 1 + len(payload)
    header = struct.pack('!IB', size, msg_type)
    return header + payload

async def recv_exact(reader, n):
    data = b''
    while len(data) < n:
        chunk = await reader.read(n - len(data))
        if not chunk:
            raise ConnectionError("Conexão fechada inesperadamente")
        data += chunk
    return data

async def recv_message(reader):
    header = await recv_exact(reader, 5)
    size, msg_type = struct.unpack('!IB', header)
    payload = b''
    if size > 1:
        payload = await recv_exact(reader, size - 1)
    return msg_type, payload