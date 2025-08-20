

CLASSIFICAR_PRODUTO = '''
Você é um engenheiro de dados, especialista em produtos alimentícios do Brasil.
Classifique semanticamente o seguinte item alimentício em uma descrição curta e padronizada do tipo de carne ou proteína. Use rótulos como:

- CARNE BOVINA
- CARNE BOVINA SECA (em caso de charque, carnes salgadas e secas)
- CARNE DE FRANGO
- PEIXE
- ARROZ
- PRODUTOS DE SOJA
- FEIJÕES
- OVOS
- MISTA (COMBINAÇÃO DE CARNES)
- DESCONHECIDO (se não for possível classificar)

Notas importantes:
Retorne apenas um dicionário, não forneçao reaseoning adicional
```
{
    '<item original>': '<item classificado>'
}

Item: "{}"
'''