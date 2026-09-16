import json
import base64
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding

# ── 1. Geração de chaves ──────────────────────────────────────────

def gerar_chaves(nome_servico: str, diretorio: str = 'keys'):
    """Gera par de chaves RSA e salva em arquivos .pem"""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    public_key = private_key.public_key()

    # Salva chave privada
    with open(f'{diretorio}/{nome_servico}/private.pem', 'wb') as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))

    # Salva chave pública
    with open(f'{diretorio}/{nome_servico}/public.pem', 'wb') as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))

    print(f'Chaves geradas para {nome_servico}')

# ── 2. Carregar chaves ────────────────────────────────────────────

def carregar_chave_privada(caminho: str):
    """Carrega chave privada de arquivo .pem"""
    with open(caminho, 'rb') as f:
        return serialization.load_pem_private_key(
            f.read(),
            password=None,
        )

def carregar_chave_publica(caminho: str):
    """Carrega chave pública de arquivo .pem"""
    with open(caminho, 'rb') as f:
        return serialization.load_pem_public_key(f.read())

# ── 3. Assinar ────────────────────────────────────────────────────

def assinar(payload: dict, chave_privada) -> str:
    """Assina o payload e retorna assinatura em base64"""
    conteudo = json.dumps(payload, sort_keys=True).encode()
    assinatura = chave_privada.sign(
        conteudo,
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    return base64.b64encode(assinatura).decode()

# ── 4. Verificar ──────────────────────────────────────────────────

def verificar(payload: dict, assinatura_b64: str, chave_publica) -> bool:
    """Verifica assinatura do payload → True se válida, False se inválida"""
    try:
        conteudo = json.dumps(payload, sort_keys=True).encode()
        assinatura = base64.b64decode(assinatura_b64)
        chave_publica.verify(
            assinatura,
            conteudo,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        return True
    except Exception:
        return False

# ── 5. Envelope de evento (criação / validação estrutural) ────────

# Routing keys reservadas ao protocolo interno do sistema. Nenhuma
# categoria de promoção pode usar esses nomes, sob pena de um evento de
# notificação (não assinado) ser reentregue a um microsserviço que espera
# um evento assinado.
ROUTING_KEYS_RESERVADAS = {'recebida', 'publicada', 'voto', 'destaque'}


def criar_evento(routing_key: str, payload: dict, chave_privada) -> dict:
    """Monta o envelope { event, payload, signature } já assinado."""
    return {
        'event':     routing_key,
        'payload':   payload,
        'signature': assinar(payload, chave_privada),
    }


def validar_evento(evento, chave_publica):
    """Valida estrutura + assinatura de um envelope.

    Retorna (payload, None) se o evento for íntegro e autêntico, ou
    (None, motivo) caso contrário. Nunca lança exceção: qualquer
    mensagem malformada é rejeitada como um evento inválido.
    """
    if not isinstance(evento, dict):
        return None, 'envelope não é um objeto JSON'
    payload = evento.get('payload')
    if not isinstance(payload, dict):
        return None, 'campo "payload" ausente ou inválido'
    assinatura = evento.get('signature')
    if not isinstance(assinatura, str) or not assinatura:
        return None, 'campo "signature" ausente'
    if not verificar(payload, assinatura, chave_publica):
        return None, 'assinatura digital inválida'
    return payload, None


def normalizar_categoria(categoria: str) -> str:
    """Converte o texto digitado pelo usuário em uma categoria segura.

    Remove acentuação/espaços/pontos (que quebrariam a hierarquia da
    routing key) e rejeita as palavras reservadas do protocolo.
    Retorna '' se a categoria for inválida.
    """
    import unicodedata
    import re

    texto = unicodedata.normalize('NFKD', (categoria or '').strip().lower())
    texto = texto.encode('ascii', 'ignore').decode()
    texto = re.sub(r'[^a-z0-9]+', '_', texto).strip('_')
    if not texto or texto in ROUTING_KEYS_RESERVADAS:
        return ''
    return texto
