import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from crypto_utils import gerar_chaves

os.chdir(project_root)

for servico in ['gateway', 'promocao', 'ranking']:
    os.makedirs(f'keys/{servico}', exist_ok=True)

gerar_chaves('gateway', 'keys')
gerar_chaves('promocao', 'keys')
gerar_chaves('ranking', 'keys')

print('Todas as chaves foram geradas com sucesso.')