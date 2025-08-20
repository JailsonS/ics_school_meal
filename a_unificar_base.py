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
from pathlib import Path

'''
    Configurações
'''

PATH_DATABASE = 'trase/data/brazil/school_meal_pnae/notebooks/data/SPENDING'
PATH_OUTPUT = 'trase/data/brazil/school_meal_pnae/notebooks/data/banco_analise_1.parquet'

'''
    Funções auxiliares
'''

def load_json_files(path: Path) -> list[pl.DataFrame]:
    """
    Carrega todos os arquivos JSON recursivamente de um diretório e retorna uma lista de DataFrames Polars.
    """
    path = Path(path)
    dataframes = []

    for file_path in path.rglob("*.json"):
        try:
            df = pl.read_json(file_path, infer_schema_length=None)
            dataframes.append(df)
        except Exception as e:
            print(f"[Erro] Falha ao ler {file_path}: {e}")
    
    return dataframes

def process_dataframe(df: pl.DataFrame, index: int) -> pl.DataFrame | None:
    """
    Processa um DataFrame individual e retorna o resultado final.
    Retorna None se o DataFrame for inválido.
    """
    if "dados" not in df.columns:
        print(f"[{index}] DataFrame não tem coluna 'dados'")
        return None

    dtype = df.schema["dados"]
    if not str(dtype).lower().startswith("list(struct({"):
        print(f"[{index}] Coluna 'dados' não é list[struct], é {dtype}")
        print(df.row(0))
        return None

    df_notas = df.explode("dados").unnest("dados")
    df_notas = df_notas.rename({"valor": "valor_nota_fiscal"})

    if "itens" not in df_notas.columns:
        # Garante que 'itens' esteja presente como lista vazia
        df_notas = df_notas.with_columns([
            pl.Series("itens", [[]] * df_notas.height)
        ])

    df_items = df_notas.explode("itens").unnest("itens")
    return df_items

'''
    Execução
'''

if __name__ == "__main__":
    # ~5 min running
    list_dataframes = load_json_files(PATH_DATABASE)
    list_df_all = []

    for i, df in enumerate(list_dataframes):
        processed = process_dataframe(df, i)
        
        if processed is None: continue
        
        list_df_all.append(processed)

    df_all = pl.concat(list_df_all, how='vertical_relaxed', rechunk=True)
    df_all = df_all.rename({'valor_un': 'valor_unidade', 'valor': 'valor_unidade_total'})
    
    df_all.write_parquet(PATH_OUTPUT)
