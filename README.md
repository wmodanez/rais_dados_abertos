# Processador de Dados Abertos da RAIS

Este projeto processa os dados abertos da RAIS (Relação Anual de Informações Sociais) disponibilizados pelo Ministério do Trabalho e Emprego.

## Requisitos

- Python 3.8+
- Bibliotecas Python listadas em `requirements.txt`

## Instalação

1. Clone o repositório:
```
git clone https://github.com/seu-usuario/rais_dados_abertos.git
cd rais_dados_abertos
```

2. Crie e ative um ambiente virtual:
```
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
```

3. Instale as dependências:
```
pip install -r requirements.txt
```

## Uso

### Processamento Completo

Para executar o processamento completo (download, extração e conversão):

```
python main.py --modo completo --anos 2015 2016 2017
```

### Otimização de Memória

O sistema utiliza otimização de memória por padrão para todos os arquivos:
- Processa arquivos em pequenos chunks para economizar memória
- Monitora e gerencia o uso de memória durante o processamento
- Usa compressão eficiente para os arquivos Parquet
- Detecta automaticamente o tamanho do arquivo e ajusta os parâmetros de processamento

Esta otimização é sempre ativa e não requer configuração adicional, garantindo que mesmo arquivos muito grandes possam ser processados em máquinas com recursos limitados.

### Exemplos

Processar arquivos do ano 2015:
```
python main.py --modo completo --anos 2015
```

Processar apenas a etapa de conversão:
```
python main.py --modo converter --anos 2015
```

Processar múltiplos anos:
```
python main.py --modo completo --anos 2015 2016 2017
```

## Estrutura de Diretórios

- `dados-abertos/`: Arquivos CSV/TXT baixados
- `dados-abertos-zip/`: Arquivos 7z baixados
- `parquet/`: Arquivos convertidos para formato Parquet
- `logs/`: Arquivos de log do processamento

## Solução de Problemas

### Erros de Memória

Se você ainda encontrar erros de memória (`MemoryError`) apesar da otimização automática, tente:

1. Reduzir o número de workers:
   ```
   python main.py --modo completo --anos 2015 --workers_convert 1
   ```

2. Processar um ano por vez:
   ```
   python main.py --modo completo --anos 2015
   ```

3. Aumentar a memória disponível para o processo Python.

### Arquivos Corrompidos

Para tentar processar novamente arquivos problemáticos:
```
python main.py --modo converter --anos 2015 --otimizar-memoria --sobrescrever
```

## Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.
