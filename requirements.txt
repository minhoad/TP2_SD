# TP2 — Sistemas Distribuídos: Sistema P2P de Transferência de Arquivos
#
# Implementação utiliza exclusivamente a biblioteca padrão do Python.
# Nenhuma dependência externa é necessária.
#
# Versão mínima requerida: Python 3.10
#   - asyncio.start_server / asyncio.open_connection (disponível desde 3.7)
#   - Sintaxe match/case não utilizada; compatível com 3.10+
#
# Para criar um ambiente virtual isolado:
#   python3 -m venv env
#   source env/bin/activate
#
# Executar os testes:
#   python3 tests/generate_test_files.py   # gera os arquivos de teste
#   python3 tests/test_runner.py --run-all # suite completa (Tabela 1)
