import configparser
import os


def ler_arquivo_ini(caminho_arquivo=None):
    # Define o diretório padrão para a leitura do arquivo de configuração
    diretorio_padrao = os.path.dirname(os.path.abspath(__file__))
    if caminho_arquivo is None:
        # Caso o caminho não seja informado, usa o diretório padrão
        caminho_arquivo = os.path.join(diretorio_padrao, 'opcoes.ini')
    else:
        # Caso contrário, verifica se o arquivo existe no caminho informado
        if not os.path.exists(caminho_arquivo):
            # Se não existir, tenta o diretório padrão
            caminho_arquivo = os.path.join(diretorio_padrao, 'opcoes.ini')

    config = configparser.ConfigParser()
    config.read(caminho_arquivo)
    dados = {}
    for secao in config.sections():
        dados[secao] = {}
        for chave, valor in config.items(secao):
            if valor.isdigit():
                dados[secao][chave] = int(valor)
            else:
                dados[secao][chave] = valor
    return dados
