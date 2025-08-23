"""
Este script processa arquivos JSON contendo dados de notas fiscais e consolida
os registros válidos em um único arquivo .parquet.

Pré-requisitos:
- Todos os arquivos .JSON devem estar previamente baixados.
- O caminho para esses arquivos deve ser definido na variável `PATH_DATABASE`.

Notas:
- Registros inválidos (ex: itens vazios ou com erros de estrutura) são ignorados. Ex:
--- Nota sem itens e/ou strings vazias
"""
import polars as pl
import logging
import time
from helpers.limpador_basico import (
    LimpadorDeDados
)

import json, re, os
from tqdm import tqdm

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
load_dotenv()

'''
    Configurações
'''

# how many rows to process via AI model
DAILY_LIMIT = 100_000

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

CACHE_PATH = "cache_itens_classificados.json"

os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")

'''
    Funcoes e Classes Auxiliares
'''

class FluxoDeLimpezaMerendaEscolar():

    def __init__(self, database: pl.DataFrame) -> pl.DataFrame:
        self.data = database
        self.regex_unidade = (
            r"(?i)\b("
            r"KG|KGS?|KGI|KILOGRAMA|KILOS?|KGM|KF|KU|KGR|KGB|KG2\.5|KG2|KG,|KG'|KGL|KG.*|"
            r"GRAMA|GRAMAS|G|GM|GMS|G,|"
            r"500G|400G|125G|"
            r"TON|TONELADA|TNS?|TONS?|"
            r"LITRO|LTR|LTS?|LITROS?|LT|LT/84G|LT420G|LT125|LATA1|"
            r"ML|MILILITRO|MILILITROS|"
            r"CX|CAIXA|CAIXAS|CXS?|CXA?|CX18|CXA1|CXA3|CTE|CXKG"
            r"CX12|CX20|CX30|CX50|CX6UND|CX60UND|CX60|"
            r"UN|UND|UNIT|UNID|UNIDADES?|UNS?|UID|UND1|30UND|6UND|UNITAR|UN0030|UND\.12|6\.0UND|UN1|U|1000UN|UN.*|30UN\.|30UN|"
            r"DZ|DÚZIA|DÚZIAS|DOZ|DZS|DZN|DZ12|DZ/1|DZ_|DZA|DZ?|DÚZ|DÃ?ZIA|DZ.*|DÃ?ZIA|DÃ{1}ZIA|DUZIAS|"
            r"BDJ.*|BDJ|BJD|BANDEJ|BANDEJA|BANDEJAS|BAN|BAND|BNDJ|BJ?|BD30|BJA|BAD|BADJ|BANDJA|BNDJA|BDJAS|BJ1|BANJ|BANDE|BANEJ|BD30U|"
            r"PACOTE|PCT|PKG|PCTS?|PACOTES|PACTE|PCT100|PCT\.5|PCT50|PC0001|PCT5|PKT|"
            r"CART|CAR|CARTEL|CARTELA|CARTELAS|CA|CARTS|CT|CARTONADO|CT|CRT|CARTON?|CART\.|CT30|CT/30|CTLA|TL|CRTELA|CATELA|CLT|"
            r"FD|FARDO|FARDOS|FD20|"
            r"PL|PLACAS?|PLACA|"
            r"ALIM|ALIMENTAÇÃO|ALIMENTACOES|ALIMENTACAO|"
            r"OUTROS|OUT|OTHER|"
            r"LATA|LATAS|LAT125|LATA1|LT0001|"
            r"FRD|FORMA|FORMAS|FRM|"
            r"MACO|MAÇOS?|MAC|"
            r"KIT|JOGO|"
            r"PENTE|PENTES|PN|PNT|PEN|PET|PENT|"
            r"FAVO|FAVOS|"
            r"TABELA|TBL|TB|"
            r"RL|ROL|ROLOS|"
            r"CUBA|CUBAS|"
            r"EMBAL|EMB|EMBL|"
            r"QT|QUILATOS?|QL|QUILAT|"
            r"OVO|OVOS|"
            r"CONJUNTO|CJ|"
            r"DISP|DISPOSITIVO|"
            r"CRTL|CONTROLE|CR|CR\.|"
            r"BANDEI|BANDEJA|BANDJA|"
            r"PT|POTE|"
            r"SC|SACO|SACOS|"
            r"DP|DISPENSADOR|"
            r"KM|KILOMETRO|KILOMETROS"
            r")\b"
        )

    def ai_cleaner(self, df):
        # Modelo Gemini
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash-lite",
            temperature=0.2,
            max_tokens=256,
        )

        # Parser de saída JSON
        parser = JsonOutputParser()

        # Corrigindo o prompt com chaves duplas
        prompt = ChatPromptTemplate.from_template("""
        Extraia as seguintes informações da descrição do produto abaixo:

        Descrição: {item}

        Retorne um JSON com os campos:
        {{
        "unit": "<unidade extraída, ex: KG, G, CX, PCT>",
        "quantity": <quantidade total do produto, como número>,
        "confidence": <confiança da extração, de 0 a 1>
        }}
        Atenção:
        1. A descrição pode conter erros de digitacao, mesmo assim, tente extrair a unidade quando estiver definida, caso contrário retorne 'Desconhecido'
        2. Identifique quando a unidade é composta, exemplo 3x4PCT -> 12 PACOTES, 5CX -> 5 CAIXAS, 4 QUILOGRAMAS -> 5 KG
        """)


        # Dicionário de cache
        cache = {}
        if os.path.exists("item_cache.json"):
            with open("item_cache.json", "r") as f: cache = json.load(f)
        
        # Pipeline
        chain = prompt | llm | parser

        # Lista única de itens
        unique_items = df["item"].unique().to_list()

        print('Quantidade de itens únicos: ', len(unique_items))

        # minute_limit = 15
        items_to_process = [item for item in unique_items if item not in cache]
        today_batch = items_to_process[:DAILY_LIMIT]

        # private function to make multiple calls
        def _process_item(item):
            try:
                return item, chain.invoke({"item": item})
            except Exception as e:
                print(f"Erro em '{item}': {e}")
                return item, {"unit": None, "quantity": None, "confidence": 0.0}
            
        # Paralelizar chamadas
        with ThreadPoolExecutor(max_workers=10) as executor:  # ajuste max_workers conforme limite de API
            futures = {executor.submit(_process_item, item): item for item in today_batch}
            for future in tqdm(as_completed(futures), total=len(today_batch), desc="Processando itens únicos"):
                item, result = future.result()
                cache[item] = result

        with open("item_cache.json", "w") as f: json.dump(cache, f, indent=2)

        # Funções de mapeamento
        def get_unit(item): return cache.get(item, {}).get("unit", "Desconhecido")

        def get_quantity(item):
            val = cache.get(item, {}).get("quantity", None)
            try: return float(val) if val is not None else None
            except (ValueError, TypeError): return None

        def get_confidence(item):
            val = cache.get(item, {}).get("confidence", 0.0)
            try: return float(val)
            except (ValueError, TypeError): return 0.0
            
        # Aplicar ao dataframe
        df = df.with_columns([
            pl.col("item").map_elements(get_unit, return_dtype=pl.Utf8).alias("unidade_item"),
            pl.col("item").map_elements(get_quantity, return_dtype=pl.Float64).alias("quantidade_item"),
            pl.col("item").map_elements(get_confidence, return_dtype=pl.Float64).alias("confident"),
        ])

        return df


    def executar(self):
        etapas = [
            "Conversão de campos numéricos",
            "Limpeza de texto",
            "Renomear colunas",
            "Remover unidades dos itens",
            "Extraindo unidades e quantidades do campo descricao",
            "Gerar ID único",
            "Adicionar ID da nota fiscal"
        ]
        
        pbar = tqdm(etapas, desc="Executando pipeline de limpeza", ncols=100)

        # Etapa 1
        pbar.set_description("Convertendo campos numéricos")
        database = self.data.with_columns([
            pl.col('valor_unidade').cast(pl.Float32, strict=False).fill_null(0),
            pl.col('valor_nota_fiscal').cast(pl.Float32, strict=False).fill_null(0),
            pl.col('valor_unidade_total').cast(pl.Float32, strict=False).fill_null(0),
            pl.col('qt').cast(pl.Float32, strict=False).fill_null(0),
        ])
        pbar.update(1)

        # Etapa 2
        pbar.set_description("Limpando strings")
        limpador_de_texto = LimpadorDeDados(UNKNOWN_TEXT)
        database = limpador_de_texto.limpeza_basica(database)
        pbar.update(1)

        # Etapa 3
        pbar.set_description("Renomeando colunas")
        database = database.rename({col: col.lower() for col in database.columns})
        pbar.update(1)

        # Etapa 4
        pbar.set_description("Removendo unidades dos itens")
        database = database.with_columns(
            pl.col("item")
                .str.replace_all(r"[^A-Z\s]", "")
                .str.replace_all(self.regex_unidade, "")
                .str.replace_all(r"\d+", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .alias("item_deduplicado")
        )
        pbar.update(1)

        # Etapa 5
        pbar.set_description("Extraindo unidades e quantidades do campo descricao")
        database = self.ai_cleaner(database)
        pbar.update(1)

        # Etapa 6
        pbar.set_description("Gerando ID único por nota")
        id_nf = database.select(NF_COLUNAS).unique().with_row_index(name="id_nf")
        database = database.join(id_nf, on=NF_COLUNAS, how="left")
        pbar.update(1)

        # Etapa 7
        pbar.set_description("Adicionando ID da nota fiscal")
        database = database.with_columns((
            pl.col('n_doc').cast(pl.Utf8) + 
            '_' +
            pl.col('emissao').str.replace_all('/', '') + 
            '_' +
            pl.col('valor_nota_fiscal').fill_null(0).cast(pl.Int32).cast(pl.Utf8) + 
            '_' +
            pl.col('id_nf').cast(pl.Utf8)
        ).alias('id_nota_fiscal')).drop('id_nf')
        pbar.update(1)

        pbar.close()

        return database



'''
    Iniciar Script
'''
def main():
    start = time.time()

    database = pl.read_parquet(PATH_DATABASE)

    fluxo_de_limpeza = FluxoDeLimpezaMerendaEscolar(database)
    
    database_processado = fluxo_de_limpeza.executar()

    database_processado.write_parquet(PATH_OUTPUT)

    end = time.time()

    logging.info(f"Tempo total de execução: {end - start:.2f} segundos")



if __name__ == "__main__":
    main()