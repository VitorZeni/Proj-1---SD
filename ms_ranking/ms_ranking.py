"""
TRABALHO 1 - MICROSSERVIÇOS, MENSAGERIA E CRIPTOGRAFIA ASSIMÉTRICA
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

MS Ranking: contabiliza os votos e publica as promoções em destaque (hot deal).
"""

import pika
import json
import sys
import os

# Permite importar o crypto_utils da raiz do projeto
raiz_projeto = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, raiz_projeto)
os.chdir(raiz_projeto)

from crypto_utils import (carregar_chave_privada, carregar_chave_publica,
                          criar_evento, validar_evento)

# --- Configurações ---
EXCHANGE = 'Promocoes'
FILA = 'Fila_Ranking'
HOT_DEAL_THRESHOLD = 3      # Score mínimo (positivos - negativos) para virar destaque

chave_publica_gateway = carregar_chave_publica('keys/gateway/public.pem')
chave_privada_ranking = carregar_chave_privada('keys/ranking/private.pem')

# Votos por promoção: promocao_id -> {positivos, negativos, dados da promoção, hot_deal}
votos = {}

# --- Processamento dos votos ---

def publicar_destaque(ch, promocao_id, dados, score):
    """Assina e publica o evento promocao.destaque."""
    payload = {
        'promocao_id': promocao_id,
        'titulo':      dados['titulo'],
        'descricao':   dados['descricao'],
        'categoria':   dados['categoria'],
        'preco':       dados['preco'],
        'loja':        dados['loja'],
        'score':       score,
    }
    evento = criar_evento('promocao.destaque', payload, chave_privada_ranking)
    ch.basic_publish(exchange=EXCHANGE, routing_key='promocao.destaque', body=json.dumps(evento))
    print(f'[Ranking] 🔥 HOT DEAL publicado: "{dados["titulo"]}" (score {score})')

def processar_voto(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('[Ranking] Mensagem com JSON inválido descartada.')
            return

        # 1. Valida a assinatura do Gateway
        payload, erro = validar_evento(evento, chave_publica_gateway)
        if erro:
            print(f'[Ranking] Evento descartado: {erro}.')
            return

        promocao_id = payload.get('promocao_id')
        voto = payload.get('voto')
        if not promocao_id or voto not in ('positivo', 'negativo'):
            print('[Ranking] Evento descartado: voto ou promocao_id inválido.')
            return

        # 2. Atualiza o contador de votos
        if promocao_id not in votos:
            votos[promocao_id] = {
                'positivos': 0,
                'negativos': 0,
                'titulo':    payload.get('titulo', 'Sem título'),
                'descricao': payload.get('descricao', ''),
                'categoria': payload.get('categoria', 'geral'),
                'preco':     payload.get('preco'),
                'loja':      payload.get('loja', ''),
                'hot_deal':  False,
            }

        dados = votos[promocao_id]
        if voto == 'positivo':
            dados['positivos'] += 1
        else:
            dados['negativos'] += 1

        # 3. Recalcula o score
        score = dados['positivos'] - dados['negativos']
        print(f'[Ranking] "{dados["titulo"]}": +{dados["positivos"]} / -{dados["negativos"]} (score: {score})')

        # 4. Publica o destaque só na primeira vez que atinge o limite
        if score >= HOT_DEAL_THRESHOLD and not dados['hot_deal']:
            dados['hot_deal'] = True
            publicar_destaque(ch, promocao_id, dados, score)

    except Exception as e:
        print(f'[Ranking] Erro ao processar evento: {e}')

# --- Função principal ---

def main():
    conexao = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    canal = conexao.channel()

    canal.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
    canal.queue_declare(queue=FILA, durable=True)
    canal.queue_bind(exchange=EXCHANGE, queue=FILA, routing_key='promocao.voto')
    canal.basic_consume(queue=FILA, on_message_callback=processar_voto, auto_ack=True)

    print(f'[Ranking] Aguardando votos em "promocao.voto" (limite hot deal = {HOT_DEAL_THRESHOLD})...')
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        print('\n[Ranking] Encerrando...')
        conexao.close()

if __name__ == '__main__':
    main()
