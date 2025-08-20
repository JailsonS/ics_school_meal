
import polars as pl
import numpy as np
from collections import defaultdict, Counter
from langchain_community.embeddings import OllamaEmbeddings
from tqdm import tqdm  # para barra de progresso (opcional)


class LimpadorDeDados:
    """
    Classe para limpeza básica de strings:
    - Remove espaços em branco nas bordas
    - Remove asteriscos
    - Remove acentos e símbolos especiais
    - Deduplica nomes semelhantes
    - Converte para maiúsculas
    """

    def __init__(self, UNKNOWN_TEXT):
        self.UNKNOWN_TEXT = UNKNOWN_TEXT


    def limpeza_basica(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns([
            pl.col(col)
            .str.replace_all(r"\*", "")                    # remove '*'
            .str.strip_chars()                             # remove espaços nas bordas
            .str.replace_all(r"\.", "")                    # remove '.'
            .str.replace_all(r"\s+", " ")                  # remove múltiplos espaços internos
            .str.normalize("NFKD")                         # decompõe acentos
            .str.replace_all(r"\p{M}", "")                 # remove marcas de acento (Unicode combining marks)
            .str.to_uppercase()                            # caixa alta
            .alias(col)
            for col, dtype in df.schema.items() if dtype == pl.Utf8
        ])
