# Relatório TP2 — Sistema P2P de Transferência de Arquivos

**Aluno(s):** Darmes Dias, Gabriel Neri  
**Data:** 29/05/2026  
**Repositório:** [https://github.com/minhoad/TP2_SD](https://github.com/minhoad/TP2_SD)

---

## 1. Introdução

Este relatório descreve a implementação e avaliação de um sistema elementar de transferência de arquivos utilizando o modelo **Peer-to-Peer (P2P)**. O sistema permite que cada nó (Peer) opere simultaneamente como cliente e servidor, fragmentando arquivos em blocos e transferindo-os diretamente entre pares conectados.

Os objetivos principais são:
- Implementar a simetria P2P (cada peer é cliente e servidor simultaneamente)
- Fragmentar e remontar arquivos grandes em blocos de tamanho configurável
- Gerenciar a comunicação bidirecional e concorrente em cada nó
- Validar a integridade dos dados transferidos via checksum SHA-256

---

## 2. Decisões de Projeto

### 2.1 Linguagem e Biblioteca de Sockets

**Python 3.10+ com asyncio**

Optamos por Python com a biblioteca `asyncio` por:
- **I/O não-bloqueante nativo**: `asyncio.start_server()` e `asyncio.open_connection()` permitem múltiplas conexões simultâneas sem threads manuais
- **Event loop único**: simplifica a coordenação entre conexões de entrada (servidor) e saída (cliente)
- **Biblioteca padrão**: nenhuma dependência externa é necessária

```python
# Servidor assíncrono escutando em uma porta
self.server = await asyncio.start_server(
    self._handle_new_connection, self.host, self.port
)

# Cliente conectando a um vizinho
reader, writer = await asyncio.open_connection(host, port)
```

### 2.2 Protocolo de Mensagens

Desenvolvemos um protocolo binário inspirado no BitTorrent, com 8 tipos de mensagem:

| Código | Mensagem        | Payload                          | Descrição                              |
|--------|-----------------|----------------------------------|----------------------------------------|
| 0x01   | HANDSHAKE       | info_hash (64B) + peer_id        | Identificação inicial entre peers      |
| 0x02   | BITFIELD        | bytearray de bits                | Mapa de blocos que o peer possui       |
| 0x03   | INTERESTED      | (vazio)                          | Indica interesse em baixar blocos      |
| 0x04   | NOT_INTERESTED  | (vazio)                          | Indica falta de interesse              |
| 0x05   | REQUEST         | índice do bloco (4B)             | Solicita um bloco específico           |
| 0x06   | PIECE           | índice (4B) + dados              | Envia um bloco de dados                |
| 0x07   | HAVE            | índice do bloco (4B)             | Anuncia posse de novo bloco            |
| 0x08   | METADATA        | JSON com metadados do arquivo    | Transfere info_hash, nome, tamanho     |

**Formato do Frame:**

```
┌─────────────────┬──────────────┬─────────────────┐
│  Tamanho (4B)   │  Tipo (1B)   │    Payload      │
│    uint32 BE    │    uint8     │   (variável)    │
└─────────────────┴──────────────┴─────────────────┘
```

A serialização utiliza `struct.pack('!IB', size, msg_type)` com network byte order (big-endian).

### 2.3 Fluxo de Conexão

```
  Peer A (Outgoing)                    Peer B (Incoming)
       │                                     │
       │──── HANDSHAKE (info_hash, id) ─────>│
       │<─── HANDSHAKE (info_hash, id) ──────│
       │                                     │
       │  [Se A tem metadados e B não]       │
       │──── METADATA (JSON) ───────────────>│
       │                                     │
       │──── BITFIELD ──────────────────────>│
       │<─── BITFIELD ───────────────────────│
       │                                     │
       │──── INTERESTED ────────────────────>│
       │──── REQUEST (índice) ──────────────>│
       │<─── PIECE (índice, dados) ──────────│
       │──── HAVE (índice) ─────────────────>│
```

### 2.4 Gerenciamento de Blocos (Bitfield)

O rastreamento de blocos utiliza um `bytearray` onde cada bit representa a posse de um bloco:

```python
def _set_bit(self, index):
    byte_idx = index // 8
    bit_idx = index % 8
    self.bitfield[byte_idx] |= (1 << (7 - bit_idx))

def has_block(self, index):
    byte_idx = index // 8
    bit_idx = index % 8
    return bool(self.bitfield[byte_idx] & (1 << (7 - bit_idx)))
```

### 2.5 Pipeline de Requisições (in_flight)

Para maximizar a utilização da rede, implementamos requisições em pipeline:

```python
self.in_flight = set()   # blocos requisitados, aguardando resposta
self.max_pending = 10    # limite de requisições simultâneas
```

### 2.6 Integridade (SHA-256)

Verificação em dois momentos:
1. **Criação:** hash SHA-256 do arquivo completo armazenado como `info_hash`
2. **Salvamento:** hash recalculado e comparado após remontagem

### 2.7 Topologia de Vizinhos

Configuração estática definida em `config.json`:

```
                           ┌───────────┐
                           │   8000    │  (Seeder)
                           └─────┬─────┘
                    ┌────────────┴────────────┐
                    │                         │
               ┌────▼────┐               ┌────▼────┐
               │  8001   │               │  8002   │
               └────┬────┘               └─────────┘
                    │
               ┌────▼────┐
               │  8003   │
               └─────────┘

  Conexões: 8000→[8001,8002], 8001→[8000,8003], 8002→[8000], 8003→[8001]
```

**Importante:** O peer 8003 **não** conecta ao seeder 8000. Obtém blocos exclusivamente via 8001.

---

## 3. Estrutura do Código

| Arquivo             | Responsabilidade                                      |
|---------------------|-------------------------------------------------------|
| `peer.py`           | Classe principal `Peer`: servidor, conexões, orquestração |
| `peer_connection.py`| Gerencia uma conexão individual com outro peer        |
| `protocol.py`       | Constantes de mensagem e funções de serialização      |
| `fileManager.py`    | Fragmentação, bitfield, remontagem e validação SHA-256|
| `config.json`       | Configuração de vizinhos e tamanho de bloco           |
| `test_runner.py`    | Executor automatizado dos casos de teste              |

---

## 4. Resultados dos Testes

Os testes seguem a **Tabela 1 do enunciado**, variando número de peers, tamanho de bloco e tamanho de arquivo.

### 4.1 Tabela de Resultados

| Caso   | Peers | Bloco | Arquivo       | Blocos | Resultado | Tempo    |
|--------|-------|-------|---------------|--------|-----------|----------|
| TC-01  | 2     | 1 KB  | fileA (10 KB) | 10     | **PASS**  | 1.0s     |
| TC-02  | 2     | 1 KB  | fileB (1 MB)  | 1024   | **PASS**  | 1.0s     |
| TC-03  | 2     | 1 KB  | fileC (10 MB) | 10240  | **PASS**  | 23.3s    |
| TC-04  | 4     | 1 KB  | fileA (10 KB) | 10     | **PASS**  | 1.0s     |
| TC-05  | 4     | 1 KB  | fileB (1 MB)  | 1024   | **PASS**  | 1.0s     |
| TC-06  | 4     | 1 KB  | fileC (10 MB) | 10240  | **FAIL**  | 120.9s*  |
| TC-07  | 4     | 4 KB  | fileA (10 KB) | 3      | **PASS**  | 1.0s     |
| TC-08  | 4     | 4 KB  | fileB (1 MB)  | 256    | **PASS**  | 1.0s     |
| TC-09  | 4     | 4 KB  | fileC (10 MB) | 2560   | **PASS**  | 1.0s     |
| TC-10  | 2     | 1 KB  | fileA (20 KB) | 20     | **PASS**  | 1.0s     |
| TC-11  | 2     | 1 KB  | fileB (5 MB)  | 5120   | **PASS**  | 5.0s     |
| TC-12  | 2     | 1 KB  | fileC (20 MB) | 20480  | **PASS**  | 84.9s    |

**Resumo: 11/12 testes passaram (91.7%)**

*TC-06 falhou por **timeout** (120s): peers 8001 e 8002 completaram, mas 8003 não finalizou a tempo.

### 4.2 Verificações Realizadas

Para cada teste, o `test_runner.py` verifica:
- **Integridade:** SHA-256 do arquivo baixado = SHA-256 do original
- **Tamanho:** arquivo remontado possui o tamanho correto em bytes
- **Logs:** confirmação de que blocos foram recebidos das fontes esperadas

### 4.3 Exemplo de Log — Propagação em Cadeia (4 Peers)

Os logs abaixo demonstram a transferência do arquivo `fileA.dat` (10 KB, 10 blocos) entre 4 peers:

**peer_8000.log (Seeder inicial):**
```
2026-05-29 22:22:44,846 INFO Seeder inicial: fileA.dat (10 blocos)
2026-05-29 22:22:44,846 INFO Servidor ouvindo em 127.0.0.1:8000
2026-05-29 22:22:45,649 INFO Metadados enviados para 127.0.0.1:8001
2026-05-29 22:22:46,451 INFO Metadados enviados para 127.0.0.1:8002
```

**peer_8001.log (Leecher → torna-se Seeder para 8003):**
```
2026-05-29 22:22:45,648 INFO Servidor ouvindo em 127.0.0.1:8001
2026-05-29 22:22:45,649 INFO Conectado a 127.0.0.1:8000
2026-05-29 22:22:45,649 INFO Metadados recebidos: fileA.dat (10 blocos)
2026-05-29 22:22:45,750 INFO Bloco 0 recebido de 127.0.0.1:8000
2026-05-29 22:22:45,750 INFO Bloco 1 recebido de 127.0.0.1:8000
...
2026-05-29 22:22:45,751 INFO Bloco 9 recebido de 127.0.0.1:8000
2026-05-29 22:22:45,751 INFO Download completo!
2026-05-29 22:22:45,752 INFO Arquivo salvo em downloads/127.0.0.1:8001/fileA.dat
2026-05-29 22:22:47,252 INFO Metadados enviados para 127.0.0.1:8003
```

**peer_8003.log (Recebe exclusivamente de 8001, não do seeder):**
```
2026-05-29 22:22:47,251 INFO Servidor ouvindo em 127.0.0.1:8003
2026-05-29 22:22:47,251 INFO Conectado a 127.0.0.1:8001
2026-05-29 22:22:47,252 INFO Metadados recebidos: fileA.dat (10 blocos)
2026-05-29 22:22:47,352 INFO Bloco 0 recebido de 127.0.0.1:8001
2026-05-29 22:22:47,353 INFO Bloco 1 recebido de 127.0.0.1:8001
...
2026-05-29 22:22:47,353 INFO Bloco 9 recebido de 127.0.0.1:8001
2026-05-29 22:22:47,353 INFO Download completo!
2026-05-29 22:22:47,354 INFO Arquivo salvo em downloads/127.0.0.1:8003/fileA.dat
```

**Observação crítica:** O peer 8003 recebeu **todos os blocos de 127.0.0.1:8001**, demonstrando a propagação P2P em cadeia: `8000 → 8001 → 8003`.

---

## 5. Análise

### 5.1 Impacto do Tamanho do Bloco (1 KB vs 4 KB)

| Arquivo | Blocos (1 KB) | Blocos (4 KB) | Tempo 1KB (4 peers) | Tempo 4KB (4 peers) |
|---------|---------------|---------------|---------------------|---------------------|
| 10 KB   | 10            | 3             | 1.0s                | 1.0s                |
| 1 MB    | 1024          | 256           | 1.0s                | 1.0s                |
| 10 MB   | 10240         | 2560          | **FAIL** (120.9s)*  | **1.0s**            |

*TC-06 (1 KB) falhou por bloco perdido; TC-09 (4 KB) passou normalmente.

**Conclusão:** Blocos maiores reduzem drasticamente o tempo de transferência para arquivos grandes devido a:
- **75% menos mensagens** REQUEST/PIECE (2.560 vs 10.240)
- Menor overhead de serialização e context switches
- Menor carga no event loop do asyncio

### 5.2 Análise da Falha TC-06

O TC-06 (4 peers, 1 KB, 10 MB) falhou porque o peer 8003 **não recebeu todos os blocos**.

**Investigação dos Logs:**

Analisando os logs em `test_results/tc06_logs/`, descobrimos:

```bash
# Contagem de blocos recebidos
$ grep -c "recebido de" logs/peer_8001.log
10240  # ← 8001 recebeu TODOS os blocos

$ grep -c "recebido de" logs/peer_8003.log  
10239  # ← 8003 recebeu 10.239 de 10.240 (falta 1!)
```

**Identificação do bloco faltante:**

```bash
$ diff test_results/expected.txt test_results/received_8003.txt
536d535
< 535   # ← Bloco 535 está faltando!
```

**Evidência no log do peer_8003.log:**

```
2026-05-29 22:46:27,855 INFO Bloco 534 recebido de 127.0.0.1:8001
2026-05-29 22:46:27,855 INFO Bloco 536 recebido de 127.0.0.1:8001
                             ↑ Bloco 535 foi PULADO
```

**Timeline real:**
- 8001 recebeu bloco 535 de 8000 às **22:46:25,863**
- 8003 iniciou conexão com 8001 às **22:46:25,838** (25ms ANTES!)
- 8003 provavelmente requisitou bloco 535 antes de 8001 tê-lo
- A requisição ficou sem resposta e o mecanismo não re-requisita

**Por que o download não completa:**
- `is_complete()` verifica se `len(self.blocks) == self.pieces_count`
- Com 10.239 blocos de 10.240, retorna `False`
- Arquivo nunca é salvo → verificação do hash falha

**Arquivos de evidência preservados em `test_results/`:**
- `tc06_logs/peer_800*.log` — logs completos da execução
- `expected.txt` — lista de blocos esperados (0-10239)
- `received_8003.txt` — lista de blocos que 8003 recebeu
- `tc06_output.txt` — saída do teste

### 5.3 Impacto do Número de Peers

| Cenário               | 2 Peers | 4 Peers  | Observação                    |
|-----------------------|---------|----------|-------------------------------|
| 10 KB, 1 KB/bloco     | 1.0s    | 1.0s     | Overhead mínimo               |
| 1 MB, 1 KB/bloco      | 1.0s    | 1.0s     | Overhead mínimo               |
| 10 MB, 1 KB/bloco     | 23.3s   | **FAIL** | Bloco perdido na cadeia 8001→8003 |
| 10 MB, 4 KB/bloco     | -       | 1.0s     | Menos blocos = menos chance de perda |

**Análise:** Com 4 peers e blocos pequenos (1 KB), a probabilidade de perder um bloco aumenta:
- São 10.240 requisições individuais
- 8003 depende exclusivamente de 8001
- Se 8003 requisita antes de 8001 ter o bloco, a requisição é perdida

### 5.4 Throughput Observado

| Teste  | Dados      | Tempo  | Throughput     |
|--------|------------|--------|----------------|
| TC-03  | 10 MB      | 23.3s  | 0.43 MB/s      |
| TC-11  | 5 MB       | 5.0s   | 1.0 MB/s       |
| TC-12  | 20 MB      | 84.9s  | 0.24 MB/s      |

O throughput diminui com arquivos maiores devido ao overhead de mais mensagens REQUEST/PIECE.

### 5.5 Limitações Identificadas

1. **Sem re-requisição de blocos perdidos:** Conforme evidenciado no TC-06, se uma requisição fica em `in_flight` sem resposta (ex: bloco 535 requisitado antes de 8001 tê-lo), o sistema não detecta timeout e não re-solicita. O bloco fica permanentemente faltando.

2. **Race condition temporal:** O peer 8003 pode começar a requisitar blocos de 8001 antes de 8001 completar seu download. Se 8003 requisita um bloco que 8001 ainda não tem, a requisição é ignorada silenciosamente.

3. **Seleção sequencial de blocos:** Sempre requisita o primeiro bloco faltante, não implementa "rarest first"

4. **Única fonte por bloco:** Cada bloco é requisitado de apenas um peer. Se esse peer não responde, o bloco nunca é obtido.

5. **Topologia fixa:** A cadeia 8000→8001→8003 cria dependência. Se 8003 conectasse também a 8002, poderia baixar blocos faltantes de outra fonte.

---

## 6. Conclusões

O sistema P2P implementado **atende aos requisitos do enunciado**:

1. **Simetria P2P:** ✓ Cada peer opera simultaneamente como servidor e cliente (demonstrado nos logs)

2. **Fragmentação e Remontagem:** ✓ Arquivos divididos em blocos configuráveis (1 KB e 4 KB testados), transferidos e remontados corretamente

3. **Comunicação Não-Bloqueante:** ✓ `asyncio` permite múltiplas conexões simultâneas em thread único

4. **Validação de Integridade:** ✓ SHA-256 garante arquivo remontado idêntico ao original (verificado em todos os 11 testes que passaram)

5. **Propagação em Cadeia:** ✓ Peer 8003 obteve arquivo completo via intermediário 8001, sem conexão direta ao seeder

**Resultado Final:** 11/12 testes passaram (91.7%)

**Principal Aprendizado:** O tamanho do bloco impacta criticamente a escalabilidade. Blocos de 4 KB permitem transferir 10 MB entre 4 peers em 1s, enquanto blocos de 1 KB causam timeout na mesma configuração.

---

## Referências

1. Cohen, B. (2003). The BitTorrent Protocol Specification. BEP 3.
2. Python Documentation. asyncio — Asynchronous I/O. https://docs.python.org/3/library/asyncio.html
