"""
TRABALHO 1 - MICROSSERVIÇOS, MENSAGERIA E CRIPTOGRAFIA ASSIMÉTRICA
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

Gera os pares de chaves RSA dos serviços que assinam eventos.
"""

import sys
import os

# Permite importar o crypto_utils da raiz do projeto
raiz_projeto = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, raiz_projeto)
os.chdir(raiz_projeto)

from crypto_utils import gerar_chaves

SERVICOS = ['gateway', 'promocao', 'ranking']

for servico in SERVICOS:
    os.makedirs(f'keys/{servico}', exist_ok=True)
    gerar_chaves(servico, 'keys')

print('Todas as chaves foram geradas com sucesso.')
