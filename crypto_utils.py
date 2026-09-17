"""
TRABALHO 1 - MICROSSERVIÇOS, MENSAGERIA E CRIPTOGRAFIA ASSIMÉTRICA
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

Funções compartilhadas de chaves, assinatura digital e envelope dos eventos.
"""

import json
import base64
import re
import unicodedata
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding

# --- Constantes ---
# Nomes usados nas routing keys internas, não podem ser usados como categoria
ROUTING_KEYS_RESERVADAS = {'recebida', 'publicada', 'voto', 'destaque'}

# --- Chaves ---

def gerar_chaves(nome_servico, diretorio='keys'):
    """Gera um par de chaves RSA e salva em arquivos .pem."""
    chave_privada = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    chave_publica = chave_privada.public_key()

    with open(f'{diretorio}/{nome_servico}/private.pem', 'wb') as f:
        f.write(chave_privada.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))

    with open(f'{diretorio}/{nome_servico}/public.pem', 'wb') as f:
        f.write(chave_publica.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))

    print(f'Chaves geradas para {nome_servico}')

def carregar_chave_privada(caminho):
    """Carrega uma chave privada de um arquivo .pem."""
    with open(caminho, 'rb') as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def carregar_chave_publica(caminho):
    """Carrega uma chave pública de um arquivo .pem."""
    with open(caminho, 'rb') as f:
        return serialization.load_pem_public_key(f.read())

# --- Assinatura digital ---

def assinar(payload, chave_privada):
    """Assina o payload (SHA-256 + RSA) e retorna a assinatura em base64."""
    # sort_keys garante o mesmo JSON no produtor e no consumidor
    conteudo = json.dumps(payload, sort_keys=True).encode()
    assinatura = chave_privada.sign(conteudo, padding.PKCS1v15(), hashes.SHA256())
    return base64.b64encode(assinatura).decode()

def verificar(payload, assinatura_b64, chave_publica):
    """Retorna True se a assinatura do payload for válida."""
    try:
        conteudo = json.dumps(payload, sort_keys=True).encode()
        assinatura = base64.b64decode(assinatura_b64)
        chave_publica.verify(assinatura, conteudo, padding.PKCS1v15(), hashes.SHA256())
        return True
    except Exception:
        return False

# --- Envelope dos eventos ---

def criar_evento(routing_key, payload, chave_privada):
    """Monta o evento {event, payload, signature} já assinado."""
    return {
        'event':     routing_key,
        'payload':   payload,
        'signature': assinar(payload, chave_privada),
    }

def validar_evento(evento, chave_publica):
    """
    Valida a estrutura e a assinatura de um evento.
    Retorna (payload, None) se for válido ou (None, motivo) se não for.
    """
    if not isinstance(evento, dict):
        return None, 'evento não é um objeto JSON'

    payload = evento.get('payload')
    if not isinstance(payload, dict):
        return None, 'campo "payload" ausente ou inválido'

    assinatura = evento.get('signature')
    if not isinstance(assinatura, str) or not assinatura:
        return None, 'campo "signature" ausente'

    if not verificar(payload, assinatura, chave_publica):
        return None, 'assinatura digital inválida'

    return payload, None

def normalizar_categoria(categoria):
    """Padroniza a categoria (sem acento, espaço ou ponto). Retorna '' se for inválida."""
    texto = unicodedata.normalize('NFKD', (categoria or '').strip().lower())
    texto = texto.encode('ascii', 'ignore').decode()
    texto = re.sub(r'[^a-z0-9]+', '_', texto).strip('_')

    if not texto or texto in ROUTING_KEYS_RESERVADAS:
        return ''
    return texto
