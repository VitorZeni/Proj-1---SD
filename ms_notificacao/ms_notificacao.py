import pika
import json
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

from crypto_utils import (carregar_chave_publica, validar_evento,
                          normalizar_categoria)

EXCHANGE = 'Promocoes'

chave_publica_promocao = carregar_chave_publica('keys/promocao/public.pem')
chave_publica_ranking  = carregar_chave_publica('keys/ranking/public.pem')


def _publicar_categoria(ch, categoria, payload):
    """Publica a notificação em promocao.<categoria>.

    A categoria é normalizada: além de padronizar o texto, isso impede
    que uma categoria como "destaque" ou "recebida" gere uma routing key
    reservada — o que faria uma notificação (não assinada) voltar para a
    fila de um microsserviço que só aceita eventos assinados.
    """
    segura = normalizar_categoria(categoria)
    if not segura:
        print(f'[Notificação] Categoria inválida/reservada "{categoria}" — notificação descartada.')
        return None
    routing_key = f'promocao.{segura}'
    ch.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps({'event': routing_key, 'payload': payload}),
    )
    return routing_key


def callback(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('[Notificação] Mensagem malformada (JSON inválido) — descartada.')
            return

        rk = method.routing_key

        if rk == 'promocao.publicada':
            # Assinada pelo MS Promoção.
            p, erro = validar_evento(evento, chave_publica_promocao)
            if erro:
                print(f'[Notificação] Evento "promocao.publicada" descartado — {erro}.')
                return

            notif = {
                'id':        p.get('id'),
                'titulo':    p.get('titulo'),
                'descricao': p.get('descricao', ''),
                'categoria': p.get('categoria'),
                'preco':     p.get('preco'),
                'loja':      p.get('loja', ''),
                'hot_deal':  False,
                'mensagem':  f'Nova promoção: {p.get("titulo")}',
            }
            destino = _publicar_categoria(ch, p.get('categoria'), notif)
            if destino:
                print(f'[Notificação] Publicado: {destino} — "{p.get("titulo")}"')

        elif rk == 'promocao.destaque':
            # Assinada pelo MS Ranking.
            p, erro = validar_evento(evento, chave_publica_ranking)
            if erro:
                print(f'[Notificação] Evento "promocao.destaque" descartado — {erro}.')
                return

            # O payload já vem completo do Ranking (que o recebeu do Gateway).
            notif = {
                'promocao_id': p.get('promocao_id'),
                'titulo':      p.get('titulo'),
                'descricao':   p.get('descricao', ''),
                'categoria':   p.get('categoria'),
                'preco':       p.get('preco'),
                'loja':        p.get('loja', ''),
                'score':       p.get('score'),
                'hot_deal':    True,
                # A palavra "hot deal" exigida pelo enunciado vai explícita
                # no corpo da notificação de destaque.
                'destaque':    'hot deal',
                'mensagem':    f'hot deal: {p.get("titulo")} (score {p.get("score")})',
            }
            destino = _publicar_categoria(ch, p.get('categoria'), notif)
            if destino:
                print(f'[Notificação] 🔥 HOT DEAL publicado: {destino} — '
                      f'"{p.get("titulo")}" (score {p.get("score")})')
    except Exception as e:
        # Nenhuma mensagem inesperada pode derrubar o microsserviço.
        print(f'[Notificação] Erro ao processar evento — descartado: {e}')


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
channel.queue_declare(queue='Fila_Notificacao', durable=True)
channel.queue_bind(exchange=EXCHANGE, queue='Fila_Notificacao',
                   routing_key='promocao.publicada')
channel.queue_bind(exchange=EXCHANGE, queue='Fila_Notificacao',
                   routing_key='promocao.destaque')

channel.basic_consume(queue='Fila_Notificacao',
                      on_message_callback=callback, auto_ack=True)

print('[Notificação] Aguardando eventos (promocao.publicada / promocao.destaque)...')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('\n[Notificação] Encerrando...')
    connection.close()
