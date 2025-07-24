# 📊 Processador de Dados RAIS

Um sistema completo para download, descompactação e conversão de dados da RAIS (Relação Anual de Informações Sociais) do Ministério do Trabalho e Previdência.

## 🎯 Sobre o Projeto

Este projeto automatiza o processamento de dados RAIS, permitindo:

- Download de arquivos do servidor FTP oficial
- Descompactação de arquivos .7z
- Conversão para formato Parquet otimizado
- Consolidação de múltiplos arquivos em um único dataset por ano
- Pipeline paralelo automático para máxima eficiência
- Verificação inteligente de arquivos existentes para evitar downloads desnecessários
- Filtragem eficiente de campos específicos durante a conversão
- Filtros personalizados por CNAE para análises especializadas

## 🚀 Funcionalidades

### ✅ Download de Dados

- Conexão automática com servidor FTP oficial
- Download seletivo por ano ou faixa de anos
- Retry automático em caso de falhas
- Listagem otimizada - acessa apenas diretórios relevantes
- Pipeline paralelo integrado - processamento simultâneo
- Verificação inteligente de arquivos - compara tamanhos e datas para evitar downloads desnecessários
- Sincronização eficiente - baixa apenas arquivos que realmente precisam ser atualizados

### ✅ Descompactação

- Descompactação automática de arquivos .7z
- Pipeline paralelo integrado
- Organização por ano em pastas estruturadas
- Detecção automática de encoding

### ✅ Conversão para Parquet

- Conversão de arquivos TXT para formato Parquet
- Padronização automática de nomes de colunas
- Processamento em chunks para arquivos grandes
- Detecção automática de encoding e separadores
- Adição de coluna ANO automaticamente
- Pipeline paralelo integrado
- Filtragem otimizada de campos - lê apenas colunas necessárias desde o início
- Lógica simplificada - primeiro renomeia colunas, depois filtra as solicitadas

### ✅ Consolidação

- Consolidação de múltiplos arquivos em dataset único
- Remoção automática de chunks após consolidação
- Estrutura organizada por ano
- Pipeline paralelo integrado
- Consolidação de todos os anos - junta todos os arquivos RAIS_ANO.parquet em um único arquivo histórico

### ✅ Filtro de Campos Durante Conversão

- Filtro seletivo de campos durante a conversão para Parquet
- Redução significativa do tamanho dos arquivos finais
- Melhoria na performance de análise
- Identificação automática de campos por nome padronizado
- Processamento eficiente - lê apenas colunas solicitadas, evitando carregar dados desnecessários
- Mapeamento inteligente - converte nomes de campos para padrões padronizados automaticamente

### ✅ Filtro CNAE Personalizado

- Sistema genérico de filtros CNAE baseado em arquivo CSV
- Suporte a diferentes classificações (empregos verdes, indústria, serviços, etc.)
- Arquivo de exemplo incluído: `cnae_classe_emprego_verde.csv`
- Configuração flexível via linha de comando
- Integração automática durante a conversão

### ✅ Pipeline Paralelo Automático

- Processamento simultâneo de download, descompactação e conversão
- Comportamento padrão - ativo automaticamente em todas as operações
- Monitoramento por arquivo - cada arquivo avança independentemente
- Máxima eficiência com workers paralelos
- Comunicação via filas entre etapas
- Não requer configuração - funciona automaticamente

### ✅ Limpeza Automática de Arquivos

- Limpeza inteligente - apaga arquivos TXT descompactados após conversão bem-sucedida
- Economia de espaço - pode economizar até 70% do espaço em disco
- Controle via flag - ativado com `--limpar-descompactados`
- Limpeza de diretórios - remove diretórios vazios automaticamente
- Logs detalhados - registra cada arquivo removido com seu tamanho
- Segurança garantida - só remove após conversão confirmada

### ✅ Monitoramento e Relatórios

- Formatação de tempo legível - exibe tempos em formato H:M:S para melhor compreensão
- Medição detalhada de performance por etapa
- Relatórios de progresso em tempo real
- Logs estruturados com níveis configuráveis

## 📋 Pré-requisitos

- Python 3.8+
- Conexão com internet para download
- Espaço em disco suficiente para os dados

## 🛠️ Instalação

1. **Clone o repositório:**

```bash
git clone https://github.com/wmodanez/rais_dados_abertos.git rais
cd rais
```

2. **Crie um ambiente virtual:**

```bash
python -m venv venv
```

3. **Ative o ambiente virtual:**

```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

4. **Instale as dependências:**

```bash
pip install -r requirements.txt
```

## 📊 Mapeamento de Campos

O sistema padroniza automaticamente os nomes das colunas durante a conversão, removendo acentos, caracteres especiais e convertendo para maiúsculas com underlines. Isso garante consistência nos dados exportados.

### Relação Completa de Colunas Padronizadas

O sistema automaticamente padroniza todos os nomes de colunas dos dados RAIS. Abaixo está a relação completa das principais colunas reconhecidas pelo sistema:

#### Identificação e Localização

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `CPF do Trabalhador` | `CPF_DO_TRABALHADOR` | CPF do trabalhador |
| `PIS/PASEP` | `PIS_PASEP` | Número PIS/PASEP |
| `Código do Município` | `CODIGO_DO_MUNICIPIO` | Código IBGE do município |
| `Município` | `MUNICIPIO` | Nome do município |
| `UF` | `UF` | Unidade da Federação |
| `Região` | `REGIAO` | Região geográfica |
| `CEP do Domicílio do Trabalhador` | `CEP_DO_DOMICILIO_DO_TRABALHADOR` | CEP de residência |

#### Informações Demográficas

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Sexo Trabalhador` | `SEXO_TRABALHADOR` | Sexo do trabalhador |
| `Faixa Etária` | `FAIXA_ETARIA` | Faixa etária do trabalhador |
| `Data de Nascimento` | `DATA_DE_NASCIMENTO` | Data de nascimento |
| `Idade` | `IDADE` | Idade do trabalhador |
| `Raça Cor` | `RACA_COR` | Raça/cor do trabalhador |
| `Nacionalidade` | `NACIONALIDADE` | Nacionalidade do trabalhador |
| `Pessoa com Deficiência` | `PESSOA_COM_DEFICIENCIA` | Indicador de deficiência |

#### Educação

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Escolaridade` | `ESCOLARIDADE` | Nível de escolaridade |
| `Escolaridade após 2005` | `ESCOLARIDADE_APOS_2005` | Escolaridade pela classificação atual |
| `Grau de Instrução` | `GRAU_DE_INSTRUCAO` | Grau de instrução do trabalhador |

#### Informações da Empresa

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `CNPJ da Empresa` | `CNPJ_DA_EMPRESA` | CNPJ do estabelecimento |
| `CNPJ Raiz` | `CNPJ_RAIZ` | CNPJ raiz da empresa |
| `Razão Social` | `RAZAO_SOCIAL` | Razão social da empresa |
| `Nome Fantasia` | `NOME_FANTASIA` | Nome fantasia |
| `Natureza Jurídica` | `NATUREZA_JURIDICA` | Natureza jurídica da empresa |
| `Código da Natureza Jurídica` | `CODIGO_DA_NATUREZA_JURIDICA` | Código da natureza jurídica |
| `Tamanho do Estabelecimento` | `TAMANHO_DO_ESTABELECIMENTO` | Porte da empresa |
| `Indicador de Atividade Ano` | `INDICADOR_DE_ATIVIDADE_ANO` | Indicador de atividade |

#### Classificação Econômica (CNAE)

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `CNAE 2.0 Classe` | `CNAE_2_0_CLASSE` | Classificação CNAE 2.0 |
| `CNAE 2.0 Subclasse` | `CNAE_2_0_SUBCLASSE` | Subclasse CNAE 2.0 |
| `CNAE 95 Classe` | `CNAE_95_CLASSE` | Classificação CNAE 95 |
| `Código CNAE` | `CODIGO_CNAE` | Código CNAE da atividade |
| `Classe CNAE` | `CLASSE_CNAE` | Classe CNAE |
| `Subclasse CNAE` | `SUBCLASSE_CNAE` | Subclasse CNAE |

#### Ocupação (CBO)

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `CBO Ocupação 2002` | `CBO_OCUPACAO_2002` | Código CBO 2002 |
| `Ocupação 2002` | `OCUPACAO_2002` | Descrição da ocupação |
| `CBO 1994` | `CBO_1994` | Código CBO 1994 |
| `Ocupação 1994` | `OCUPACAO_1994` | Ocupação pela CBO 1994 |
| `Função` | `FUNCAO` | Função exercida |

#### Informações do Vínculo

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Vínculo Ativo 31/12` | `VINCULO_ATIVO_31_12` | Status do vínculo em 31/12 |
| `Tipo Vínculo` | `TIPO_VINCULO` | Tipo de vínculo empregatício |
| `Motivo Desligamento` | `MOTIVO_DESLIGAMENTO` | Motivo do desligamento |
| `Data de Admissão` | `DATA_DE_ADMISSAO` | Data de admissão |
| `Data de Desligamento` | `DATA_DE_DESLIGAMENTO` | Data de desligamento |
| `Tipo Admissão` | `TIPO_ADMISSAO` | Tipo de admissão |
| `Causa do Afastamento 1` | `CAUSA_DO_AFASTAMENTO_1` | Primeira causa de afastamento |
| `Causa do Afastamento 2` | `CAUSA_DO_AFASTAMENTO_2` | Segunda causa de afastamento |
| `Causa do Afastamento 3` | `CAUSA_DO_AFASTAMENTO_3` | Terceira causa de afastamento |

#### Tempo e Jornada

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Tempo Emprego` | `TEMPO_EMPREGO` | Tempo de emprego |
| `Horas Contratuais` | `HORAS_CONTRATUAIS` | Horas contratuais |
| `Quantidade de Horas Contratadas` | `QUANTIDADE_DE_HORAS_CONTRATADAS` | Horas semanais contratadas |
| `Meses de Trabalho` | `MESES_DE_TRABALHO` | Meses trabalhados no ano |

#### Remuneração

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Valor da Remuneração (R$)` | `VALOR_DA_REMUNERACAO_R` | Valor da remuneração em reais |
| `Remuneração Dezembro (SM)` | `REMUNERACAO_DEZEMBRO_SM` | Remuneração em salários mínimos |
| `Remuneração Média (SM)` | `REMUNERACAO_MEDIA_SM` | Remuneração média em SM |
| `Remuneração Média Nominal` | `REMUNERACAO_MEDIA_NOMINAL` | Remuneração média nominal |
| `Tipo Salário` | `TIPO_SALARIO` | Tipo de salário |
| `13º Salário` | `DECIMO_TERCEIRO_SALARIO` | Valor do 13º salário |

#### Sindicalização

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Indicador Sindicalizado` | `INDICADOR_SINDICALIZADO` | Indicador de sindicalização |
| `CNPJ Sindicato Trabalhador` | `CNPJ_SINDICATO_TRABALHADOR` | CNPJ do sindicato |

#### Benefícios Sociais

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Optante FGTS` | `OPTANTE_FGTS` | Optante pelo FGTS |
| `Data de Opção pelo FGTS` | `DATA_DE_OPCAO_PELO_FGTS` | Data da opção pelo FGTS |
| `Contribuinte Sindical` | `CONTRIBUINTE_SINDICAL` | Contribuinte sindical |

#### Colunas Automáticas

| Nome | Descrição |
|------|-----------|
| `ANO` | Ano de referência dos dados (adicionada automaticamente) |

### Campos Mais Utilizados

#### Informações Demográficas

- `SEXO_TRABALHADOR` - Sexo do trabalhador
- `FAIXA_ETARIA` - Faixa etária
- `RACA_COR` - Raça/cor
- `ESCOLARIDADE` - Nível de escolaridade

#### Informações Geográficas

- `UF` - Unidade da Federação
- `MUNICIPIO` - Município
- `CODIGO_DO_MUNICIPIO` - Código IBGE do município

#### Informações Econômicas

- `CNAE_2_0_CLASSE` - Classificação CNAE 2.0
- `CNAE_2_0_SUBCLASSE` - Subclasse CNAE 2.0
- `VALOR_DA_REMUNERACAO_R` - Valor da remuneração
- `HORAS_CONTRATUAIS` - Horas contratuais

#### Informações do Vínculo

- `VINCULO_ATIVO_31_12` - Status do vínculo em 31/12
- `TIPO_VINCULO` - Tipo de vínculo empregatício
- `TIPO_ADMISSAO` - Tipo de admissão
- `TEMPO_EMPREGO` - Tempo de emprego

### Uso com Filtro de Campos

Ao usar o parâmetro `--campos`, você deve especificar os nomes padronizados das colunas:

```bash
# Exemplo: converter apenas campos demográficos e geográficos
python main.py --converter --ano 2024 --campos SEXO_TRABALHADOR FAIXA_ETARIA RACA_COR UF MUNICIPIO

# Exemplo: converter apenas campos econômicos
python main.py --converter --ano 2024 --campos CNAE_2_0_CLASSE VALOR_DA_REMUNERACAO_R HORAS_CONTRATUAIS

# Exemplo: converter apenas campos de vínculo
python main.py --converter --ano 2024 --campos VINCULO_ATIVO_31_12 TIPO_VINCULO TIPO_ADMISSAO TEMPO_EMPREGO
```

### Colunas Automáticas

O sistema adiciona automaticamente a coluna `ANO` em todos os arquivos convertidos, independentemente dos campos selecionados.

### Colunas Removidas Automaticamente

O sistema remove automaticamente as seguintes colunas que não são relevantes para análise geral:

- `BAIRROS_SP` - Bairros de São Paulo
- `BAIRROS_FORTALEZA` - Bairros de Fortaleza  
- `BAIRROS_RJ` - Bairros do Rio de Janeiro
- `DISTRITOS_SP` - Distritos de São Paulo
- `REGIOES_ADM_DF` - Regiões Administrativas do DF

## 🔍 Filtro CNAE Personalizado

O sistema inclui suporte para filtros CNAE personalizados, permitindo análises especializadas.

### Arquivo de Exemplo

O projeto inclui o arquivo `db/cnae_classe_emprego_verde.csv` com classificação de empregos verdes.

### Estrutura do Arquivo CSV

```csv
CLASSE_CNAE,SITUACAO
01111,1
01121,1
01130,0
```

- `CLASSE_CNAE`: Código da classe CNAE
- `SITUACAO`: 1 para incluir, 0 para excluir

### Uso do Filtro CNAE

```bash
# Usar o arquivo padrão de empregos verdes
python main.py --converter --ano 2024 --arquivo-filtro-cnae db/cnae_classe_emprego_verde.csv

# Usar arquivo personalizado com nome descritivo
python main.py --converter --ano 2024 --arquivo-filtro-cnae meu_filtro.csv --nome-filtro-cnae "Indústria 4.0" --situacao-filtro-cnae 1
```

## 📖 Como Usar

### 🎯 Comandos Principais

#### Download de Dados

```bash
# Baixar apenas arquivos do ano 2024
python main.py --baixar --ano 2024

# Baixar faixa de anos (2020 a 2024)
python main.py --baixar --faixa-anos 2020 2024

# Baixar do ano 2020 até o último disponível
python main.py --baixar --faixa-anos 2020
```

#### Descompactação

```bash
# Baixar e descompactar
python main.py --descompactar --ano 2024

# Apenas descompactar arquivos já baixados
python main.py --apenas-descompactar --ano 2024
```

#### Conversão

```bash
# Baixar, descompactar e converter
python main.py --converter --ano 2024

# Apenas converter arquivos já descompactados
python main.py --apenas-converter --ano 2024

# Descompactar e converter arquivos já baixados
python main.py --descompactar-converter --ano 2024

# Converter apenas campos específicos
python main.py --converter --ano 2024 --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO
```

#### Consolidação

```bash
# Converter com consolidação automática
python main.py --converter --ano 2024 --consolidacao

# Apenas consolidar arquivos já convertidos
python main.py --apenas-consolidar --ano 2024

# Consolidar todos os anos em um único arquivo
python main.py --consolidar-todos-anos
```

#### Filtro de Campos Durante Conversão

```bash
# Converter apenas campos específicos de um ano
python main.py --converter --ano 2024 --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO

# Converter apenas campos específicos de todos os anos
python main.py --converter --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO

# Apenas converter com filtro de campos
python main.py --apenas-converter --ano 2024 --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO
```

#### Filtro CNAE Personalizado

```bash
# Converter com filtro de empregos verdes
python main.py --converter --ano 2024 --arquivo-filtro-cnae db/cnae_classe_emprego_verde.csv

# Converter com filtro personalizado
python main.py --converter --ano 2024 --arquivo-filtro-cnae meu_filtro.csv --nome-filtro-cnae "Minha Classificação"
```

### 🔧 Opções Avançadas

#### Configuração de Performance

```bash
# Definir número de workers
python main.py --converter --ano 2024 --max-workers 8

# Definir tamanho do chunk
python main.py --converter --ano 2024 --chunk-size 50000
```

#### Configuração de Rede

```bash
# Servidor FTP personalizado
python main.py --baixar --ano 2024 --servidor ftp.exemplo.com

# Diretório remoto personalizado
python main.py --baixar --ano 2024 --diretorio-remoto /caminho/remoto

# Configurar tentativas de download
python main.py --baixar --ano 2024 --max-tentativas 10 --tempo-espera 15
```

#### Logging

```bash
# Log detalhado
python main.py --converter --ano 2024 --nivel-log DEBUG

# Log apenas erros
python main.py --converter --ano 2024 --nivel-log ERROR
```

## 📁 Estrutura de Arquivos

```
rais/
├── main.py                 # Script principal
├── requirements.txt        # Dependências
├── README.md              # Este arquivo
├── db/                    # Arquivos de classificação
│   └── cnae_classe_emprego_verde.csv
├── files-zip/             # Arquivos baixados do FTP
│   └── 2024/
│       ├── RAIS_VINC_PUB_CENTRO_OESTE.7z
│       └── RAIS_VINC_PUB_NORTE.7z
├── files-unzip/           # Arquivos descompactados
│   └── 2024/
│       ├── RAIS_VINC_PUB_CENTRO_OESTE.txt
│       └── RAIS_VINC_PUB_NORTE.txt
├── parquet/               # Arquivos convertidos
│   └── 2024/
│       ├── RAIS_2024.parquet          # Consolidado
│       └── [subpastas com chunks]     # Não consolidado
├── logs/                  # Arquivos de log
│   └── rais_YYYY_MM_DD_HH_MM_SS.log
└── src/
    └── util/
        ├── __init__.py
        ├── gerenciador_ftp.py      # Download de arquivos
        ├── descompactador.py       # Descompactação
        ├── conversor_parquet.py    # Conversão para Parquet
        ├── filtro_cnae.py          # Filtros CNAE personalizados
        ├── pipeline_paralelo.py    # Pipeline paralelo automático
        └── utilitarios.py          # Funções auxiliares
```

## 🎛️ Flags Disponíveis

### Flags de Ação (Exclusivas)

| Flag | Descrição |
|------|-----------|
| `--baixar` | Apenas baixar arquivos do servidor FTP |
| `--descompactar` | Baixar e descompactar arquivos |
| `--apenas-descompactar` | Apenas descompactar arquivos já baixados |
| `--converter` | Baixar, descompactar e converter para formato final |
| `--apenas-converter` | Apenas converter arquivos já descompactados |
| `--descompactar-converter` | Descompactar e converter arquivos já baixados |
| `--apenas-consolidar` | Apenas consolidar arquivos já convertidos |
| `--apenas-consolidar-faixa` | Apenas consolidar arquivos de uma faixa de anos |
| `--consolidar-todos-anos` | Consolidar todos os arquivos RAIS_ANO.parquet em um único arquivo histórico |

### Flags de Configuração

| Flag | Padrão | Descrição |
|------|--------|-----------|
| `--ano` | - | Ano específico para processar |
| `--faixa-anos` | - | Faixa de anos (ex: 2020 2024) |
| `--servidor` | ftp.mtps.gov.br | Servidor FTP |
| `--diretorio-remoto` | /pdet/microdados/RAIS | Diretório remoto |
| `--max-tentativas` | 5 | Máximo de tentativas de download |
| `--tempo-espera` | 10 | Tempo entre tentativas (segundos) |
| `--max-workers` | auto | Número de workers paralelos |
| `--chunk-size` | auto | Tamanho do chunk para conversão |
| `--nivel-log` | INFO | Nível de logging |
| `--consolidacao` | False | Consolidar após conversão |
| `--limpar-descompactados` | False | Apagar arquivos TXT após conversão |
| `--campos` | - | Campos específicos para incluir na conversão |
| `--arquivo-filtro-cnae` | - | Caminho para arquivo CSV com classificação CNAE |
| `--nome-filtro-cnae` | CNAE | Nome descritivo do filtro CNAE |
| `--situacao-filtro-cnae` | 1 | Valor da coluna SITUACAO para filtrar |
| `--nome-arquivo` | RAIS_COMPLETO.parquet | Nome do arquivo consolidado final |

## 📊 Exemplos de Uso Completos

### Processamento Completo de um Ano

```bash
# Download, descompactação, conversão e consolidação
python main.py --converter --ano 2024 --consolidacao --nivel-log INFO

# Processamento com campos específicos
python main.py --converter --ano 2024 --consolidacao --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO

# Processamento com limpeza automática (economia de espaço)
python main.py --converter --ano 2024 --consolidacao --limpar-descompactados
```

### Processamento com Filtro CNAE

```bash
# Processar apenas empregos verdes
python main.py --converter --ano 2024 --arquivo-filtro-cnae db/cnae_classe_emprego_verde.csv --consolidacao

# Processar com filtro personalizado e campos específicos
python main.py --converter --ano 2024 --arquivo-filtro-cnae meu_filtro.csv --campos CNAE_2_0_CLASSE MUNICIPIO VALOR_DA_REMUNERACAO_R
```

### Processamento de Múltiplos Anos

```bash
# Processar anos de 2020 a 2024
python main.py --converter --faixa-anos 2020 2024 --consolidacao

# Processar múltiplos anos com limpeza automática
python main.py --converter --faixa-anos 2020 2024 --consolidacao --limpar-descompactados
```

### Otimização para Arquivos Grandes

```bash
# Usar mais workers e chunks menores
python main.py --converter --ano 2024 --max-workers 8 --chunk-size 25000
```

### Processamento Incremental

```bash
# 1. Baixar dados
python main.py --baixar --ano 2024

# 2. Descompactar
python main.py --apenas-descompactar --ano 2024

# 3. Converter
python main.py --apenas-converter --ano 2024

# 4. Consolidar
python main.py --apenas-consolidar --ano 2024
```

### Economia de Espaço com Limpeza Automática

```bash
# Converter e limpar arquivos descompactados
python main.py --apenas-converter --ano 2024 --limpar-descompactados

# Pipeline completo com limpeza
python main.py --converter --ano 2024 --consolidacao --limpar-descompactados

# Verificar economia de espaço
du -sh files-unzip/ parquet/
```

**Economia típica por ano:**
- Arquivo .7z: ~500 MB
- Arquivo .txt descompactado: ~2.5 GB  
- Arquivo .parquet: ~800 MB
- Economia com limpeza: ~1.7 GB por ano

### Consolidação de Todos os Anos

```bash
# Consolidar todos os anos em um único arquivo
python main.py --consolidar-todos-anos

# Consolidar com nome personalizado
python main.py --consolidar-todos-anos --nome-arquivo RAIS_HISTORICO_2015_2024.parquet

# Consolidar com mais workers para melhor performance
python main.py --consolidar-todos-anos --max-workers 8 --nome-arquivo RAIS_COMPLETO.parquet
```

**Cenário típico com 10 anos (2015-2024):**
- Tempo estimado: 5-15 minutos
- Uso de memória: ~2-4 GB RAM
- Espaço em disco: ~8-12 GB para arquivo final
- Arquivo resultante: `parquet/RAIS_COMPLETO.parquet`

## 🔍 Monitoramento e Logs

### Arquivos de Log
- Localização: `logs/rais_YYYY_MM_DD_HH_MM_SS.log`
- Incluem: comandos executados, progresso, erros, tempo de execução
- Níveis: DEBUG, INFO, WARNING, ERROR, CRITICAL

### Medição de Tempo
O sistema mede automaticamente:
- Tempo total de execução
- Tempo por etapa (Download, Descompactação, Conversão, Consolidação)
- Performance por arquivo
- Formatação legível - tempos exibidos em formato H:M:S para melhor compreensão

### Monitoramento por Arquivo
O pipeline paralelo monitora individualmente:
- Status de download de cada arquivo
- Status de descompactação de cada arquivo  
- Status de conversão de cada arquivo
- Progresso em tempo real por arquivo
- Verificação de sincronização - identifica arquivos que precisam ser baixados

### Exemplo de Saída
```
============================================================
RESUMO DE TEMPO - PROCESSAMENTO RAIS
============================================================
Tempo Total: 14:37
Tempo das Etapas: 14:37 (100.0%)
Tempo Não Medido: 0:00

Detalhamento por Etapa:
Etapa                          Tempo        % do Total
------------------------------ ------------ ----------
Descompactação                 8:31         58.3      %
Conversão                      6:06         41.7      %
============================================================
```

## 🚨 Tratamento de Erros

### Falhas de Rede
- Retry automático configurável
- Log detalhado de tentativas
- Continuação após falhas parciais

### Falhas de Processamento
- Log de erros específicos
- Continuação com outros arquivos
- Relatório de sucessos/falhas

### Problemas Comuns
- **Arquivo não encontrado**: Verificar se o ano existe no servidor
- **Erro de encoding**: Sistema detecta automaticamente
- **Memória insuficiente**: Ajustar `--chunk-size` e `--max-workers`
- Downloads desnecessários: Sistema verifica automaticamente se arquivos já existem

## 🔧 Configuração Avançada

### Otimização de Performance
```bash
# Para sistemas com muita RAM
python main.py --converter --ano 2024 --max-workers 16 --chunk-size 100000

# Para sistemas com pouca RAM
python main.py --converter --ano 2024 --max-workers 2 --chunk-size 25000
```

### Configuração de Rede

```bash
# Para conexões lentas
python main.py --baixar --ano 2024 --max-tentativas 10 --tempo-espera 30

# Para conexões instáveis
python main.py --baixar --ano 2024 --max-tentativas 20 --tempo-espera 5
```

## 📈 Estatísticas de Performance

### Tempos Típicos (ano 2024)

- Download: ~5-10 minutos (depende da conexão)
- Descompactação: ~1-2 minutos
- Conversão: ~2-5 minutos
- Consolidação: ~1-3 minutos

### Benefícios do Pipeline Paralelo

- Processamento simultâneo: Download, descompactação e conversão ocorrem ao mesmo tempo
- Eficiência aumentada: ~40-60% de redução no tempo total
- Monitoramento granular: Controle individual por arquivo
- Listagem otimizada: Acesso seletivo apenas aos diretórios relevantes
- Verificação inteligente: Evita downloads desnecessários comparando arquivos existentes

### Otimizações de Filtragem

- Redução de memória: Lê apenas colunas solicitadas
- Processamento mais rápido: Evita carregar dados desnecessários
- Arquivos menores: Resultado final otimizado para análise específica
- Mapeamento automático: Converte nomes de campos para padrões padronizados

### Tamanhos de Arquivo

- Arquivo consolidado 2024: ~673 MB
- Linhas por ano: ~10.3 milhões
- Compressão Parquet: ~70% menor que CSV
- Com filtro de campos: Redução adicional de 60-80% dependendo dos campos selecionados

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique os logs em `logs/`
2. Use `--nivel-log DEBUG` para mais detalhes
3. Abra uma issue no repositório

---
