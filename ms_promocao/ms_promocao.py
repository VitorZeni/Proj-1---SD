import pika
import json
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

from crypto_utils import (carregar_chave_privada, carregar_chave_publica,
                          criar_evento, validar_evento, normalizar_categoria)

EXCHANGE = 'Promocoes'

chave_publica_gateway  = carregar_chave_publica('keys/gateway/public.pem')
chave_privada_promocao = carregar_chave_privada('keys/promocao/private.pem')

# Registro local das promoções aprovadas por este serviço: id -> promoção
promocoes_registradas = {}

CAMPOS_OBRIGATORIOS = ('id', 'titulo', 'categoria')


def callback(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('[Promoção] Mensagem malformada (JSON inválido) — descartada.')
            return

        # 1. Verifica assinatura digital com a chave pública do Gateway.
        promo, erro = validar_evento(evento, chave_publica_gateway)
        if erro:
            print(f'[Promoção] Evento descartado — {erro}.')
            return

        # 2. Verifica se a promoção tem os campos mínimos esperados.
        faltando = [c for c in CAMPOS_OBRIGATORIOS if not promo.get(c)]
        if faltando:
            print(f'[Promoção] Evento descartado — campos ausentes: {", ".join(faltando)}.')
            return

        # 3. A categoria não pode colidir com as routing keys internas.
        if normalizar_categoria(promo['categoria']) != promo['categoria']:
            print(f'[Promoção] Evento descartado — categoria inválida/reservada '
                  f'"{promo["categoria"]}".')
            return

        # 4. Registra a promoção no serviço.
        novo = promo['id'] not in promocoes_registradas
        promocoes_registradas[promo['id']] = promo

        preco = promo.get('preco')
        preco_txt = f'R$ {preco:.2f}' if isinstance(preco, (int, float)) else 'preço n/d'
        status = 'Validada' if novo else 'Revalidada (reentrega)'
        print(f'[Promoção] {status}: "{promo["titulo"]}" [{promo["categoria"]}] {preco_txt} '
              f'— total registradas: {len(promocoes_registradas)}')

        # 5. Assina com a própria chave privada e publica promocao.publicada.
        novo_evento = criar_evento('promocao.publicada', promo, chave_privada_promocao)
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key='promocao.publicada',
            body=json.dumps(novo_evento),
        )
        print(f'[Promoção] Publicado evento "promocao.publicada" para "{promo["titulo"]}"')
    except Exception as e:
        # Nenhuma mensagem inesperada pode derrubar o microsserviço.
        print(f'[Promoção] Erro ao processar evento — descartado: {e}')


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
channel.queue_declare(queue='Fila_Promocao', durable=True)
channel.queue_bind(exchange=EXCHANGE, queue='Fila_Promocao', routing_key='promocao.recebida')

channel.basic_consume(queue='Fila_Promocao', on_message_callback=callback, auto_ack=True)

print('[Promoção] Aguardando eventos em "promocao.recebida"...')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('\n[Promoção] Encerrando...')
    connection.close()
