"""
Este script processa arquivos JSON contendo dados de notas fiscais e consolida
os registros válidos em um único arquivo .parquet.

Pré-requisitos:
- 

Notas:
- 
"""
import polars as pl
import logging
import time

from helpers.limpador_basico import (
    LimpadorDeDados
)

import json, re, os
from tqdm import tqdm


'''
    Configurações
'''

PATH_DATABASE = 'data/banco_analise_1.parquet'
PATH_OUTPUT = 'data/banco_analise_2.parquet'

# Dados da NF
NF_COLUNAS = [
    'ano',
    'uf',
    'municipio',
    'fornecedor',
    'tipo',
    'n_doc',
    'emissao',
    'valor_nota_fiscal'
]

# Informações dos itens da nota fiscal
ITENS_COLUNAS = [
    'item',
    'un',
    'qt',
    'valor_unidade',
    'valor_unidade_total'
]

UNKNOWN_TEXT = 'Desconhecido'

# Dados para visualização
VIS_COLS = [
    'id_nota_fiscal',
    'valor_nota_fiscal',
    'item',
    'qt',
    'valor_unidade',
    'valor_unidade_total',
]
