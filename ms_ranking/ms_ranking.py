import pika
import json
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

from crypto_utils import (carregar_chave_privada, carregar_chave_publica,
                          criar_evento, validar_evento)

EXCHANGE = 'Promocoes'
HOT_DEAL_THRESHOLD = 3  # score mínimo (votos positivos - negativos) para virar destaque

chave_publica_gateway = carregar_chave_publica('keys/gateway/public.pem')
chave_privada_ranking = carregar_chave_privada('keys/ranking/private.pem')

# Estado local: promocao_id -> {positivos, negativos, titulo, descricao, categoria, preco, loja, hot_deal}
votos = {}


def callback(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('[Ranking] Mensagem malformada (JSON inválido) — descartada.')
            return

        # 1. Verifica assinatura digital com a chave pública do Gateway.
        payload, erro = validar_evento(evento, chave_publica_gateway)
        if erro:
            print(f'[Ranking] Evento descartado — {erro}.')
            return

        pid  = payload.get('promocao_id')
        voto = payload.get('voto')
        if not pid or voto not in ('positivo', 'negativo'):
            print('[Ranking] Evento descartado — voto ou promocao_id inválido.')
            return

        # 2. Atualiza o contador de votos da promoção.
        if pid not in votos:
            votos[pid] = {
                'positivos': 0,
                'negativos': 0,
                'titulo':    payload.get('titulo', 'Sem título'),
                'descricao': payload.get('descricao', ''),
                'categoria': payload.get('categoria', 'geral'),
                'preco':     payload.get('preco'),
                'loja':      payload.get('loja', ''),
                'hot_deal':  False,
            }

        if voto == 'positivo':
            votos[pid]['positivos'] += 1
        else:
            votos[pid]['negativos'] += 1

        # 3. Recalcula o score de popularidade.
        p = votos[pid]
        score = p['positivos'] - p['negativos']
        print(f'[Ranking] "{p["titulo"]}" — +{p["positivos"]} / -{p["negativos"]} (score: {score})')

        # 4. Publica destaque apenas na primeira vez que atinge o limiar.
        if score >= HOT_DEAL_THRESHOLD and not p['hot_deal']:
            p['hot_deal'] = True
            hot_payload = {
                'promocao_id': pid,
                'titulo':      p['titulo'],
                'descricao':   p['descricao'],
                'categoria':   p['categoria'],
                'preco':       p['preco'],
                'loja':        p['loja'],
                'score':       score,
            }
            hot_evento = criar_evento('promocao.destaque', hot_payload, chave_privada_ranking)
            ch.basic_publish(
                exchange=EXCHANGE,
                routing_key='promocao.destaque',
                body=json.dumps(hot_evento),
            )
            print(f'[Ranking] 🔥 HOT DEAL publicado: "{p["titulo"]}" (score {score})')
    except Exception as e:
        # Nenhuma mensagem inesperada pode derrubar o microsserviço.
        print(f'[Ranking] Erro ao processar evento — descartado: {e}')


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
channel.queue_declare(queue='Fila_Ranking', durable=True)
channel.queue_bind(exchange=EXCHANGE, queue='Fila_Ranking', routing_key='promocao.voto')

channel.basic_consume(queue='Fila_Ranking', on_message_callback=callback, auto_ack=True)

print(f'[Ranking] Aguardando votos em "promocao.voto" (hot deal threshold = {HOT_DEAL_THRESHOLD})...')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('\n[Ranking] Encerrando...')
    connection.close()
