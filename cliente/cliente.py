import pika
import json
import re
import sys
import os
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

EXCHANGE = 'Promocoes'

# Interesses hard-coded (conforme permitido pelo enunciado).
# Cada item vira uma routing key "promocao.<item>". Também é aceito um
# padrão de binding completo (topic exchange), por exemplo 'promocao.*',
# que casa com TODAS as routing keys de um nível — inclusive as internas
# (promocao.recebida / publicada / voto), já que a exchange é a mesma.
CATEGORIAS_INTERESSE = ['livro', 'jogo', 'destaque']
# CATEGORIAS_INTERESSE = ['eletronico', 'jogo']
# CATEGORIAS_INTERESSE = ['promocao.*']   # "espião": vê todo o barramento

# Nome do perfil (usado também para nomear a fila do cliente).
# Ex.: CLIENTE_NOME=Cliente_B python cliente/cliente.py
NOME_CLIENTE = os.environ.get('CLIENTE_NOME', 'Cliente_A')


def _routing_key(interesse):
    """Aceita tanto 'livro' quanto um padrão completo 'promocao.*'."""
    return interesse if ('.' in interesse or '#' in interesse) else f'promocao.{interesse}'


def _formatar_preco(valor):
    try:
        return f'R$ {float(valor):.2f}'
    except (TypeError, ValueError):
        return str(valor)


def callback(ch, method, properties, body):
    try:
        evento = json.loads(body)
    except Exception:
        return

    rk      = method.routing_key
    payload = evento.get('payload', {})
    agora   = datetime.now().strftime('%H:%M:%S')
    is_hot  = payload.get('hot_deal') or rk == 'promocao.destaque'

    if is_hot:
        print(f'\n[{agora}] ════════════════════════════════════')
        print(f'  🔥  HOT DEAL  🔥  [{rk}]')
    else:
        print(f'\n[{agora}] ── Nova promoção [{rk}] ──')

    print(f'  Promoção : {payload.get("titulo", "?")}')
    print(f'  Categoria: {payload.get("categoria", "?")}')
    if is_hot and 'score' in payload:
        print(f'  Score    : {payload["score"]}')
    if payload.get('preco') is not None:
        print(f'  Preço    : {_formatar_preco(payload["preco"])}')
    if payload.get('loja'):
        print(f'  Loja     : {payload["loja"]}')
    if payload.get('descricao'):
        print(f'  Detalhes : {payload["descricao"]}')

    if is_hot:
        print('  ════════════════════════════════════')
    else:
        print()


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

    # Cada cliente cria a sua própria fila (Fila_Cliente_A, Fila_Cliente_B, ...).
    # A fila é exclusiva: some quando o processo termina. Se o nome já estiver
    # em uso por outra instância, usa uma fila anônima.
    nome_fila = 'Fila_' + re.sub(r'[^A-Za-z0-9_]+', '_', NOME_CLIENTE)
    try:
        channel.queue_declare(queue=nome_fila, exclusive=True)
    except pika.exceptions.ChannelClosedByBroker:
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
        nome_fila = channel.queue_declare(queue='', exclusive=True).method.queue

    bindings = [_routing_key(c) for c in CATEGORIAS_INTERESSE]
    for rk in bindings:
        channel.queue_bind(exchange=EXCHANGE, queue=nome_fila, routing_key=rk)

    print('╔══════════════════════════════════════╗')
    print('║   CLIENTE DE PROMOÇÕES               ║')
    print('╚══════════════════════════════════════╝')
    print(f'  Perfil    : {NOME_CLIENTE}')
    print(f'  Fila      : {nome_fila}')
    print(f'  Interesses: {", ".join(bindings)}')
    print('  Aguardando notificações... (Ctrl+C para sair)\n')

    channel.basic_consume(queue=nome_fila, on_message_callback=callback, auto_ack=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print('\nCliente encerrado.')
        connection.close()


if __name__ == '__main__':
    main()
