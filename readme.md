## Processamento da Base de Dados PNAE
Este documento detalha os procedimentos aplicados à base de dados de compra de alimentos para merenda escolar, no âmbito do Programa Nacional de Alimentação Escolar (PNAE), com o objetivo de limpar e padronizar os nomes dos produtos e converter as unidades. A base de dados foi fornecida pelo grupo de pesquisa da universidade XXX e acessada no ambiente Amazon S3, em formato JSON.

## 1. Dados de Entrada
Os dados brutos das compras foram baixados do ambiente S3 para a máquina local de processamento. O diretório no S3 é:
- `Cdata/PNAE/CORRECTED_SPENDING/`

A base de dados completa, baixada, ocupa aproximadamente 5 GB de espaço e contém 1000 arquivos JSON.

## 2. Processamento

O método de processamento dos dados emprega uma abordagem híbrida, combinando técnicas heurísticas e um modelo de inteligência artificial. Além disso, foram utilizadas bibliotecas otimizadas para o processamento de grandes volumes de dados, como Polars, em vez de Pandas. A Figura 1 ilustra o fluxo geral do processamento.

[![Figura 1 - Fluxograma de tratamento dos dados](https://drive.google.com/file/d/1aWyDPSdlr5umu-8MtzXpo5YcVI9Xo_vm/view?usp=drive_link "Figura 1 - Fluxograma de tratamento dos dados")](https://drive.google.com/file/d/1aWyDPSdlr5umu-8MtzXpo5YcVI9Xo_vm/view?usp=drive_link "Figura 1 - Fluxograma de tratamento dos dados")

## 2.1 Unificar Base

Foi criado um único arquivo no formato .parquet unificando todas as informações contidas nos arquivos .json, esse processo foi executado no script python a_unificar_base.py que trata registros inconsistentes nos arquivos json. O tamanho total do arquivo unificado corresponde a 553 MB e contém 8M de registros. Abaixo o diretório do arquivo
- `Cdata/PNAE/CORRECTED_SPENDING/`

## 2.2 Limpar Dados A
A limpeza da base consiste de uma série de etapas que visa garantir a consistência dos tipos de informações como conversão de campos numéricos e de texto, a criação de identificadores únicos e extração de informações relevantes contidas nas variadas descrições dos produtos. Abaixo as etapas aplicadas no script.

1. Conversão de campos numéricos - a base de dados apresenta campos numéricos que estão em formato de texto e foram convertidos para o números, como:
	- Preço unitário do produto (valor_unidade)
	- Preço total de todas as quantidade de um determinado produto (valor_unidade_total)
	- Preço total da nota fiscal (valor_nota_fiscal)
	- Quantidade de itens (qt)
2. Limpeza de campos de texto - esses campos foram submetidos a um tratamento básico de textos conforme o apresentado na figura 1.
3. Padronização das colunas - todas as colunas foram convertidas para o formato minúsculo.
4. Extração de unidades e quantidades a partir da descrição do produto - esse procedimento foi realizado por meio de inteligência artificial devido à complexidade de variações de nomes e ausência de padronização de unidade. Foi utilizado o modelo `"gemini-2.5-flash-lite"` da Meta executado via langchain. Este procedimento possibilita o armazenamento de informações essenciais para a conversão de diferentes unidades para quilograma. Todos os produtos foram analisados e armazenados em um arquivo json chamado `item_change.json`. 

```json
  "FARINHA DE QUIBE 500G GRANFINO": {
		"unit": "G",
		"quantity": 500,
		"confidence": 1.0
  },
  "BISCOITO DE POLVILHO 20X50GR": {
		"unit": "G",
		"quantity": 1000,
		"confidence": 0.95
  },
```

5. Gerar identificador (ID) único para notas fiscais - O ID foi gerado a partir da junção do número do documento + data de emissão + valor da nota fiscal + id único do registro da linha. 