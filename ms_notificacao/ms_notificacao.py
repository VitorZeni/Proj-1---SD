"""
TRABALHO 1 - MICROSSERVIÇOS, MENSAGERIA E CRIPTOGRAFIA ASSIMÉTRICA
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

MS Notificação: publica as promoções e os hot deals na routing key da categoria.
"""

import pika
import json
import sys
import os

# Permite importar o crypto_utils da raiz do projeto
raiz_projeto = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, raiz_projeto)
os.chdir(raiz_projeto)

from crypto_utils import carregar_chave_publica, validar_evento, normalizar_categoria

# --- Configurações ---
EXCHANGE = 'Promocoes'
FILA = 'Fila_Notificacao'

chave_publica_promocao = carregar_chave_publica('keys/promocao/public.pem')
chave_publica_ranking = carregar_chave_publica('keys/ranking/public.pem')

# --- Notificações ---

def publicar_notificacao(ch, categoria, payload):
    """Publica em promocao.<categoria>, sem assinatura. Retorna a routing key usada."""
    # Evita que uma categoria como "destaque" caia nas filas internas
    categoria_segura = normalizar_categoria(categoria)
    if not categoria_segura:
        print(f'[Notificação] Categoria inválida "{categoria}": notificação descartada.')
        return None

    routing_key = f'promocao.{categoria_segura}'
    ch.basic_publish(exchange=EXCHANGE, routing_key=routing_key,
                     body=json.dumps({'event': routing_key, 'payload': payload}))
    return routing_key

def notificar_publicada(ch, evento):
    promo, erro = validar_evento(evento, chave_publica_promocao)
    if erro:
        print(f'[Notificação] Evento "promocao.publicada" descartado: {erro}.')
        return

    notificacao = {
        'id':        promo.get('id'),
        'titulo':    promo.get('titulo'),
        'descricao': promo.get('descricao', ''),
        'categoria': promo.get('categoria'),
        'preco':     promo.get('preco'),
        'loja':      promo.get('loja', ''),
        'hot_deal':  False,
        'mensagem':  f'Nova promoção: {promo.get("titulo")}',
    }
    routing_key = publicar_notificacao(ch, promo.get('categoria'), notificacao)
    if routing_key:
        print(f'[Notificação] Publicado em {routing_key}: "{promo.get("titulo")}"')

def notificar_destaque(ch, evento):
    promo, erro = validar_evento(evento, chave_publica_ranking)
    if erro:
        print(f'[Notificação] Evento "promocao.destaque" descartado: {erro}.')
        return

    notificacao = {
        'promocao_id': promo.get('promocao_id'),
        'titulo':      promo.get('titulo'),
        'descricao':   promo.get('descricao', ''),
        'categoria':   promo.get('categoria'),
        'preco':       promo.get('preco'),
        'loja':        promo.get('loja', ''),
        'score':       promo.get('score'),
        'hot_deal':    True,
        'destaque':    'hot deal',      # Palavra exigida pelo enunciado
        'mensagem':    f'hot deal: {promo.get("titulo")} (score {promo.get("score")})',
    }
    routing_key = publicar_notificacao(ch, promo.get('categoria'), notificacao)
    if routing_key:
        print(f'[Notificação] 🔥 HOT DEAL publicado em {routing_key}: '
              f'"{promo.get("titulo")}" (score {promo.get("score")})')

def processar_evento(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('[Notificação] Mensagem com JSON inválido descartada.')
            return

        if method.routing_key == 'promocao.publicada':
            notificar_publicada(ch, evento)
        elif method.routing_key == 'promocao.destaque':
            notificar_destaque(ch, evento)

    except Exception as e:
        print(f'[Notificação] Erro ao processar evento: {e}')

# --- Função principal ---

def main():
    conexao = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    canal = conexao.channel()

    canal.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
    canal.queue_declare(queue=FILA, durable=True)
    canal.queue_bind(exchange=EXCHANGE, queue=FILA, routing_key='promocao.publicada')
    canal.queue_bind(exchange=EXCHANGE, queue=FILA, routing_key='promocao.destaque')
    canal.basic_consume(queue=FILA, on_message_callback=processar_evento, auto_ack=True)

    print('[Notificação] Aguardando eventos (promocao.publicada / promocao.destaque)...')
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        print('\n[Notificação] Encerrando...')
        conexao.close()

if __name__ == '__main__':
    main()
