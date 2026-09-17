"""
TRABALHO 1 - MICROSSERVIÇOS, MENSAGERIA E CRIPTOGRAFIA ASSIMÉTRICA
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

MS Promoção: valida as promoções recebidas e publica as aprovadas.
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
                          criar_evento, validar_evento, normalizar_categoria)

# --- Configurações ---
EXCHANGE = 'Promocoes'
FILA = 'Fila_Promocao'
CAMPOS_OBRIGATORIOS = ('id', 'titulo', 'categoria')

chave_publica_gateway = carregar_chave_publica('keys/gateway/public.pem')
chave_privada_promocao = carregar_chave_privada('keys/promocao/private.pem')

# Promoções aprovadas: id -> promoção
promocoes_registradas = {}

# --- Processamento dos eventos ---

def processar_promocao(ch, method, properties, body):
    try:
        try:
            evento = json.loads(body)
        except Exception:
            print('[Promoção] Mensagem com JSON inválido descartada.')
            return

        # 1. Valida a assinatura do Gateway
        promo, erro = validar_evento(evento, chave_publica_gateway)
        if erro:
            print(f'[Promoção] Evento descartado: {erro}.')
            return

        faltando = [campo for campo in CAMPOS_OBRIGATORIOS if not promo.get(campo)]
        if faltando:
            print(f'[Promoção] Evento descartado: campos ausentes ({", ".join(faltando)}).')
            return

        if normalizar_categoria(promo['categoria']) != promo['categoria']:
            print(f'[Promoção] Evento descartado: categoria inválida "{promo["categoria"]}".')
            return

        # 2. Registra a promoção
        nova = promo['id'] not in promocoes_registradas
        promocoes_registradas[promo['id']] = promo

        preco = promo.get('preco')
        preco_txt = f'R$ {preco:.2f}' if isinstance(preco, (int, float)) else 'preço n/d'
        status = 'Validada' if nova else 'Validada novamente'
        print(f'[Promoção] {status}: "{promo["titulo"]}" [{promo["categoria"]}] {preco_txt} '
              f'(total: {len(promocoes_registradas)})')

        # 3. Assina com a chave do MS Promoção e publica
        novo_evento = criar_evento('promocao.publicada', promo, chave_privada_promocao)
        ch.basic_publish(exchange=EXCHANGE, routing_key='promocao.publicada', body=json.dumps(novo_evento))
        print(f'[Promoção] Publicado "promocao.publicada" para "{promo["titulo"]}"')

    except Exception as e:
        print(f'[Promoção] Erro ao processar evento: {e}')

# --- Função principal ---

def main():
    conexao = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    canal = conexao.channel()

    canal.exchange_declare(exchange=EXCHANGE, exchange_type='topic')
    canal.queue_declare(queue=FILA, durable=True)
    canal.queue_bind(exchange=EXCHANGE, queue=FILA, routing_key='promocao.recebida')
    canal.basic_consume(queue=FILA, on_message_callback=processar_promocao, auto_ack=True)

    print('[Promoção] Aguardando eventos em "promocao.recebida"...')
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        print('\n[Promoção] Encerrando...')
        conexao.close()

if __name__ == '__main__':
    main()
