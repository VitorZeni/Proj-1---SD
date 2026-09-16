import pika
import json
import uuid
import sys
import os
import threading

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

from crypto_utils import (carregar_chave_privada, carregar_chave_publica,
                          criar_evento, validar_evento, normalizar_categoria,
                          ROUTING_KEYS_RESERVADAS)

EXCHANGE = 'Promocoes'
FILA_GATEWAY = 'Fila_Gateway'

chave_privada_gateway  = carregar_chave_privada('keys/gateway/private.pem')
chave_publica_promocao = carregar_chave_publica('keys/promocao/public.pem')

# Lista local de promoções validadas pelo MS Promoção: id -> dict
promocoes_validadas = {}
# Ordem de chegada, para permitir votar pelo número exibido na listagem
ordem = []
lock = threading.Lock()

# ── Consumidor de promocao.publicada (thread em background) ──────────────────

def _on_publicada(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('\n[Gateway] Mensagem malformada (JSON inválido) — descartada.')
            return

        # Valida a assinatura digital do MS Promoção antes de confiar no evento.
        promo, erro = validar_evento(evento, chave_publica_promocao)
        if erro:
            print(f'\n[Gateway] Evento descartado — {erro}.')
            print('> ', end='', flush=True)
            return

        with lock:
            if promo['id'] not in promocoes_validadas:
                ordem.append(promo['id'])
            promocoes_validadas[promo['id']] = promo
        print(f'\n[Gateway] Promoção validada: "{promo["titulo"]}" [{promo["categoria"]}]')
        print('> ', end='', flush=True)
    except Exception as e:
        print(f'\n[Gateway] Erro ao processar evento: {e}')


def _consumidor_thread():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

    # Fila nomeada (conforme o diagrama do enunciado). Se já existir outra
    # instância do Gateway usando "Fila_Gateway", cai para uma fila anônima
    # exclusiva, de modo que os dois terminais recebam TODAS as promoções
    # (e não metade cada um).
    try:
        ch.queue_declare(queue=FILA_GATEWAY, exclusive=True)
        fila = FILA_GATEWAY
    except pika.exceptions.ChannelClosedByBroker:
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
        fila = ch.queue_declare(queue='', exclusive=True).method.queue

    ch.queue_bind(exchange=EXCHANGE, queue=fila, routing_key='promocao.publicada')
    ch.basic_consume(queue=fila, on_message_callback=_on_publicada, auto_ack=True)
    ch.start_consuming()


# ── Publicação ────────────────────────────────────────────────────────────────

def _publicar(channel, routing_key, payload):
    """Assina o payload com a chave privada do Gateway e publica o envelope."""
    evento = criar_evento(routing_key, payload, chave_privada_gateway)
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(evento),
    )


# ── Ações do menu ─────────────────────────────────────────────────────────────

def _cadastrar_promocao(channel):
    print('\n╔══════════════════════════════╗')
    print('║   CADASTRAR NOVA PROMOÇÃO    ║')
    print('╚══════════════════════════════╝')
    titulo    = input('  Título     : ').strip()
    if not titulo:
        print('  [!] O título é obrigatório.')
        return
    descricao = input('  Descrição  : ').strip()
    print('  Categorias sugeridas: livro, jogo, eletronico, roupa, esporte')
    categoria = normalizar_categoria(input('  Categoria  : '))
    if not categoria:
        reservadas = ', '.join(sorted(ROUTING_KEYS_RESERVADAS))
        print('  [!] Categoria inválida: não pode ser vazia nem uma das palavras')
        print(f'      reservadas do protocolo ({reservadas}).')
        return
    preco_str = input('  Preço (R$) : ').strip()
    loja      = input('  Loja       : ').strip()

    try:
        preco = float(preco_str.replace(',', '.'))
    except ValueError:
        print('  [!] Preço inválido.')
        return

    payload = {
        'id':        str(uuid.uuid4()),
        'titulo':    titulo,
        'descricao': descricao,
        'categoria': categoria,
        'preco':     preco,
        'loja':      loja,
    }
    _publicar(channel, 'promocao.recebida', payload)
    print(f'  [✓] Promoção enviada para validação! ID: {payload["id"]}')
    print(f'      (será notificada na routing key "promocao.{categoria}")')


def _snapshot():
    """Cópia consistente da lista local, na ordem de chegada."""
    with lock:
        return [promocoes_validadas[i] for i in ordem if i in promocoes_validadas]


def _listar_promocoes():
    print('\n╔══════════════════════════════╗')
    print('║   PROMOÇÕES DISPONÍVEIS      ║')
    print('╚══════════════════════════════╝')
    snap = _snapshot()
    if not snap:
        print('  Nenhuma promoção validada ainda.')
        return snap
    for i, p in enumerate(snap, 1):
        preco = p.get('preco')
        preco_txt = f'R$ {preco:.2f}' if isinstance(preco, (int, float)) else 'preço n/d'
        print(f'  {i}. [{p["categoria"].upper()}] {p["titulo"]}')
        print(f'     {preco_txt}  |  {p.get("loja", "")}')
        if p.get('descricao'):
            print(f'     {p["descricao"]}')
        print(f'     ID: {p["id"]}')
        print()
    return snap


def _votar_promocao(channel):
    snap = _listar_promocoes()
    if not snap:
        return

    print('╔══════════════════════════════╗')
    print('║      VOTAR EM PROMOÇÃO       ║')
    print('╚══════════════════════════════╝')
    escolha = input('  Número da promoção (ou ID) : ').strip()

    if escolha.isdigit() and 1 <= int(escolha) <= len(snap):
        promo = snap[int(escolha) - 1]
    else:
        with lock:
            promo = promocoes_validadas.get(escolha)
    if promo is None:
        print('  [!] Promoção não encontrada.')
        return

    voto_input = input('  Voto (p = positivo  /  n = negativo) : ').strip().lower()
    if voto_input == 'p':
        voto = 'positivo'
    elif voto_input == 'n':
        voto = 'negativo'
    else:
        print('  [!] Entrada inválida.')
        return

    payload = {
        'promocao_id': promo['id'],
        'titulo':      promo['titulo'],
        'descricao':   promo.get('descricao', ''),
        'categoria':   promo['categoria'],
        'preco':       promo.get('preco'),
        'loja':        promo.get('loja', ''),
        'voto':        voto,
    }
    _publicar(channel, 'promocao.voto', payload)
    print(f'  [✓] Voto "{voto}" registrado para "{promo["titulo"]}"!')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Conexão de publicação (o consumo roda em thread/conexão separada,
    # pois pika.BlockingConnection não é thread-safe)
    conn_pub = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch_pub = conn_pub.channel()
    ch_pub.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

    # Thread consumidora em background
    t = threading.Thread(target=_consumidor_thread, daemon=True)
    t.start()

    print('╔══════════════════════════════════════╗')
    print('║   SISTEMA DE PROMOÇÕES — GATEWAY     ║')
    print('╚══════════════════════════════════════╝')

    while True:
        print('\n  1. Cadastrar promoção')
        print('  2. Listar promoções validadas')
        print('  3. Votar em promoção')
        print('  0. Sair')
        try:
            opcao = input('> ').strip()
        except (EOFError, KeyboardInterrupt):
            opcao = '0'

        if opcao == '1':
            _cadastrar_promocao(ch_pub)
        elif opcao == '2':
            _listar_promocoes()
        elif opcao == '3':
            _votar_promocao(ch_pub)
        elif opcao == '0':
            print('Encerrando gateway...')
            conn_pub.close()
            break
        else:
            print('  [!] Opção inválida.')


if __name__ == '__main__':
    main()
