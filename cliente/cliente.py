"""
TRABALHO 1 - MICROSSERVIÇOS, MENSAGERIA E CRIPTOGRAFIA ASSIMÉTRICA
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

Cliente: recebe e exibe as notificações das categorias de interesse.
"""

import pika
import json
import re
import os
from datetime import datetime

# --- Configurações ---
EXCHANGE = 'Promocoes'
NOME_CLIENTE = os.environ.get('CLIENTE_NOME', 'Cliente_A')     # Também define o nome da fila

# Cada item vira "promocao.<item>". Também aceita um padrão completo,
# como 'promocao.*' (recebe todas as routing keys, inclusive as internas)
CATEGORIAS_INTERESSE = ['livro', 'jogo', 'destaque']
# CATEGORIAS_INTERESSE = ['eletronico', 'jogo']
# CATEGORIAS_INTERESSE = ['promocao.*']

# --- Funções utilitárias ---

def montar_routing_key(interesse):
    """Converte 'livro' em 'promocao.livro'. Padrões completos são mantidos."""
    if '.' in interesse or '#' in interesse:
        return interesse
    return f'promocao.{interesse}'

def formatar_preco(valor):
    try:
        return f'R$ {float(valor):.2f}'
    except (TypeError, ValueError):
        return str(valor)

# --- Exibição das notificações ---

def exibir_notificacao(ch, method, properties, body):
    try:
        evento = json.loads(body)
    except Exception:
        return

    # As notificações não são assinadas, então ignora mensagens fora do formato
    if not isinstance(evento, dict) or not isinstance(evento.get('payload'), dict):
        return

    routing_key = method.routing_key
    payload = evento['payload']
    agora = datetime.now().strftime('%H:%M:%S')
    hot_deal = payload.get('hot_deal') or routing_key == 'promocao.destaque'

    if hot_deal:
        print(f'\n[{agora}] ════════════════════════════════════')
        print(f'  🔥  HOT DEAL  🔥  [{routing_key}]')
    else:
        print(f'\n[{agora}] ── Nova promoção [{routing_key}] ──')

    print(f'  Promoção : {payload.get("titulo", "?")}')
    print(f'  Categoria: {payload.get("categoria", "?")}')
    if hot_deal and 'score' in payload:
        print(f'  Score    : {payload["score"]}')
    if payload.get('preco') is not None:
        print(f'  Preço    : {formatar_preco(payload["preco"])}')
    if payload.get('loja'):
        print(f'  Loja     : {payload["loja"]}')
    if payload.get('descricao'):
        print(f'  Detalhes : {payload["descricao"]}')

    if hot_deal:
        print('  ════════════════════════════════════')
    else:
        print()

# --- Função principal ---

def main():
    conexao = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    canal = conexao.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

    # Fila exclusiva do cliente. Se o nome já estiver em uso, cria uma anônima
    nome_fila = 'Fila_' + re.sub(r'[^A-Za-z0-9_]+', '_', NOME_CLIENTE)
    try:
        canal.queue_declare(queue=nome_fila, exclusive=True)
    except pika.exceptions.ChannelClosedByBroker:
        canal = conexao.channel()
        canal.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
        nome_fila = canal.queue_declare(queue='', exclusive=True).method.queue

    routing_keys = [montar_routing_key(interesse) for interesse in CATEGORIAS_INTERESSE]
    for routing_key in routing_keys:
        canal.queue_bind(exchange=EXCHANGE, queue=nome_fila, routing_key=routing_key)

    print('╔══════════════════════════════════════╗')
    print('║   CLIENTE DE PROMOÇÕES               ║')
    print('╚══════════════════════════════════════╝')
    print(f'  Perfil    : {NOME_CLIENTE}')
    print(f'  Fila      : {nome_fila}')
    print(f'  Interesses: {", ".join(routing_keys)}')
    print('  Aguardando notificações... (Ctrl+C para sair)\n')

    canal.basic_consume(queue=nome_fila, on_message_callback=exibir_notificacao, auto_ack=True)
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        print('\nCliente encerrado.')
        conexao.close()

if __name__ == '__main__':
    main()
