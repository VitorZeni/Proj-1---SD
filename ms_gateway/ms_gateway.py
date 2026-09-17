"""
TRABALHO 1 - MICROSSERVIÇOS, MENSAGERIA E CRIPTOGRAFIA ASSIMÉTRICA
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

MS Gateway: menu do terminal para cadastrar, listar e votar em promoções.
"""

import pika
import json
import uuid
import sys
import os
import threading

# Permite importar o crypto_utils da raiz do projeto
raiz_projeto = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, raiz_projeto)
os.chdir(raiz_projeto)

from crypto_utils import (carregar_chave_privada, carregar_chave_publica,
                          criar_evento, validar_evento, normalizar_categoria,
                          ROUTING_KEYS_RESERVADAS)

# --- Configurações ---
EXCHANGE = 'Promocoes'
FILA = 'Fila_Gateway'

chave_privada_gateway = carregar_chave_privada('keys/gateway/private.pem')
chave_publica_promocao = carregar_chave_publica('keys/promocao/public.pem')

# --- Recursos compartilhados entre as threads ---
promocoes_validadas = {}    # id -> promoção aprovada pelo MS Promoção
ordem_chegada = []          # ids na ordem de chegada, para votar pelo número
lock = threading.Lock()

# --- Consumo de promocao.publicada (thread separada) ---

def receber_publicada(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('\n[Gateway] Mensagem com JSON inválido descartada.')
            return

        promo, erro = validar_evento(evento, chave_publica_promocao)
        if erro:
            print(f'\n[Gateway] Evento descartado: {erro}.')
            print('> ', end='', flush=True)
            return

        with lock:
            if promo['id'] not in promocoes_validadas:
                ordem_chegada.append(promo['id'])
            promocoes_validadas[promo['id']] = promo

        print(f'\n[Gateway] Promoção validada: "{promo["titulo"]}" [{promo["categoria"]}]')
        print('> ', end='', flush=True)

    except Exception as e:
        print(f'\n[Gateway] Erro ao processar evento: {e}')

def consumir_publicadas():
    conexao = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    canal = conexao.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

    # Se outro Gateway já usa a Fila_Gateway, cria uma fila anônima
    # para que os dois recebam todas as promoções
    try:
        canal.queue_declare(queue=FILA, exclusive=True)
        fila = FILA
    except pika.exceptions.ChannelClosedByBroker:
        canal = conexao.channel()
        canal.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
        fila = canal.queue_declare(queue='', exclusive=True).method.queue

    canal.queue_bind(exchange=EXCHANGE, queue=fila, routing_key='promocao.publicada')
    canal.basic_consume(queue=fila, on_message_callback=receber_publicada, auto_ack=True)
    canal.start_consuming()

# --- Publicação ---

def publicar(routing_key, payload):
    """Assina e publica um evento. Retorna True se foi enviado ao broker."""
    evento = criar_evento(routing_key, payload, chave_privada_gateway)

    # Abre uma conexão por publicação: uma conexão longa cairia por falta de
    # heartbeat enquanto o menu fica parado no input()
    try:
        conexao = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        try:
            conexao.channel().basic_publish(exchange=EXCHANGE, routing_key=routing_key,
                                            body=json.dumps(evento))
        finally:
            conexao.close()
        return True
    except pika.exceptions.AMQPError as e:
        print(f'  [!] Falha ao publicar no RabbitMQ ({type(e).__name__}). Verifique o broker e tente novamente.')
        return False

# --- Ações do menu ---

def cadastrar_promocao():
    print('\n╔══════════════════════════════╗')
    print('║   CADASTRAR NOVA PROMOÇÃO    ║')
    print('╚══════════════════════════════╝')

    titulo = input('  Título     : ').strip()
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
    loja = input('  Loja       : ').strip()

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
    if not publicar('promocao.recebida', payload):
        return

    print(f'  [✓] Promoção enviada para validação! ID: {payload["id"]}')
    print(f'      (será notificada na routing key "promocao.{categoria}")')

def copiar_promocoes():
    """Retorna uma cópia da lista de promoções validadas, na ordem de chegada."""
    with lock:
        return [promocoes_validadas[i] for i in ordem_chegada if i in promocoes_validadas]

def listar_promocoes():
    print('\n╔══════════════════════════════╗')
    print('║   PROMOÇÕES DISPONÍVEIS      ║')
    print('╚══════════════════════════════╝')

    promocoes = copiar_promocoes()
    if not promocoes:
        print('  Nenhuma promoção validada ainda.')
        return promocoes

    for i, promo in enumerate(promocoes, 1):
        preco = promo.get('preco')
        preco_txt = f'R$ {preco:.2f}' if isinstance(preco, (int, float)) else 'preço n/d'
        print(f'  {i}. [{promo["categoria"].upper()}] {promo["titulo"]}')
        print(f'     {preco_txt}  |  {promo.get("loja", "")}')
        if promo.get('descricao'):
            print(f'     {promo["descricao"]}')
        print(f'     ID: {promo["id"]}')
        print()

    return promocoes

def votar_promocao():
    promocoes = listar_promocoes()
    if not promocoes:
        return

    print('╔══════════════════════════════╗')
    print('║      VOTAR EM PROMOÇÃO       ║')
    print('╚══════════════════════════════╝')

    # Aceita o número da listagem ou o ID completo
    escolha = input('  Número da promoção (ou ID) : ').strip()
    if escolha.isdigit() and 1 <= int(escolha) <= len(promocoes):
        promo = promocoes[int(escolha) - 1]
    else:
        with lock:
            promo = promocoes_validadas.get(escolha)

    if promo is None:
        print('  [!] Promoção não encontrada.')
        return

    opcao_voto = input('  Voto (p = positivo  /  n = negativo) : ').strip().lower()
    if opcao_voto == 'p':
        voto = 'positivo'
    elif opcao_voto == 'n':
        voto = 'negativo'
    else:
        print('  [!] Entrada inválida.')
        return

    # O voto leva os dados da promoção para o Ranking montar o destaque
    payload = {
        'promocao_id': promo['id'],
        'titulo':      promo['titulo'],
        'descricao':   promo.get('descricao', ''),
        'categoria':   promo['categoria'],
        'preco':       promo.get('preco'),
        'loja':        promo.get('loja', ''),
        'voto':        voto,
    }
    if not publicar('promocao.voto', payload):
        return

    print(f'  [✓] Voto "{voto}" registrado para "{promo["titulo"]}"!')

# --- Função principal ---

def main():
    # Declara a exchange e já falha aqui se o broker estiver fora
    conexao = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    conexao.channel().exchange_declare(exchange=EXCHANGE, exchange_type='topic')
    conexao.close()

    thread_consumo = threading.Thread(target=consumir_publicadas, daemon=True)
    thread_consumo.start()

    print('╔══════════════════════════════════════╗')
    print('║   SISTEMA DE PROMOÇÕES - GATEWAY     ║')
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
            cadastrar_promocao()
        elif opcao == '2':
            listar_promocoes()
        elif opcao == '3':
            votar_promocao()
        elif opcao == '0':
            print('Encerrando gateway...')
            break
        else:
            print('  [!] Opção inválida.')

if __name__ == '__main__':
    main()
