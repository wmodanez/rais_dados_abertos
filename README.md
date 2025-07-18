# 📊 Processador de Dados RAIS

Um sistema completo para download, descompactação e conversão de dados da RAIS (Relação Anual de Informações Sociais) do Ministério do Trabalho e Previdência.

## 🎯 Sobre o Projeto

Este projeto automatiza o processamento de dados RAIS, permitindo:

- **Download** de arquivos do servidor FTP oficial
- **Descompactação** de arquivos .7z
- **Conversão** para formato Parquet otimizado
- **Consolidação** de múltiplos arquivos em um único dataset por ano
- **Pipeline paralelo automático** para máxima eficiência
- **Verificação inteligente** de arquivos existentes para evitar downloads desnecessários
- **Filtragem eficiente** de campos específicos durante a conversão

## 🚀 Funcionalidades

### ✅ Download de Dados

- Conexão automática com servidor FTP oficial
- Download seletivo por ano ou faixa de anos
- Retry automático em caso de falhas
- Listagem otimizada - acessa apenas diretórios relevantes
- Pipeline paralelo integrado - processamento simultâneo
- **Verificação inteligente de arquivos** - compara tamanhos e datas para evitar downloads desnecessários
- **Sincronização eficiente** - baixa apenas arquivos que realmente precisam ser atualizados

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
- **Filtragem otimizada de campos** - lê apenas colunas necessárias desde o início
- **Lógica simplificada** - primeiro renomeia colunas, depois filtra as solicitadas

### ✅ Consolidação

- Consolidação de múltiplos arquivos em dataset único
- Remoção automática de chunks após consolidação
- Estrutura organizada por ano
- Pipeline paralelo integrado

### ✅ Filtro de Campos Durante Conversão

- Filtro seletivo de campos durante a conversão para Parquet
- Redução significativa do tamanho dos arquivos finais
- Melhoria na performance de análise
- Identificação automática de campos por nome padronizado
- **Processamento eficiente** - lê apenas colunas solicitadas, evitando carregar dados desnecessários
- **Mapeamento inteligente** - converte nomes de campos para padrões padronizados automaticamente

### ✅ Pipeline Paralelo Automático

- Processamento simultâneo de download, descompactação e conversão
- Comportamento padrão - ativo automaticamente em todas as operações
- Monitoramento por arquivo - cada arquivo avança independentemente
- Máxima eficiência com workers paralelos
- Comunicação via filas entre etapas
- Não requer configuração - funciona automaticamente

### ✅ Monitoramento e Relatórios

- **Formatação de tempo legível** - exibe tempos em formato H:M:S para melhor compreensão
- **Medição detalhada de performance** por etapa
- **Relatórios de progresso** em tempo real
- **Logs estruturados** com níveis configuráveis

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

### **Exemplos de Mapeamento de Colunas**

| Nome Original | Nome Padronizado | Descrição |
|---------------|------------------|-----------|
| `Código do Município` | `CODIGO_DO_MUNICIPIO` | Código IBGE do município |
| `Valor da Remuneração (R$)` | `VALOR_DA_REMUNERACAO_R` | Valor da remuneração em reais |
| `CPF do Trabalhador` | `CPF_DO_TRABALHADOR` | CPF do trabalhador |
| `CNAE 2.0 Classe` | `CNAE_2_0_CLASSE` | Classificação CNAE 2.0 |
| `Vínculo Ativo 31/12` | `VINCULO_ATIVO_31_12` | Status do vínculo em 31/12 |
| `Faixa Etária` | `FAIXA_ETARIA` | Faixa etária do trabalhador |
| `Escolaridade` | `ESCOLARIDADE` | Nível de escolaridade |
| `Sexo Trabalhador` | `SEXO_TRABALHADOR` | Sexo do trabalhador |
| `Raça Cor` | `RACA_COR` | Raça/cor do trabalhador |
| `Tempo Emprego` | `TEMPO_EMPREGO` | Tempo de emprego |
| `Horas Contratuais` | `HORAS_CONTRATUAIS` | Horas contratuais |
| `Tipo Admissão` | `TIPO_ADMISSAO` | Tipo de admissão |
| `Tipo Salário` | `TIPO_SALARIO` | Tipo de salário |
| `Tipo Vínculo` | `TIPO_VINCULO` | Tipo de vínculo empregatício |

### **Campos Mais Utilizados**

#### **Informações Demográficas**

- `SEXO_TRABALHADOR` - Sexo do trabalhador
- `FAIXA_ETARIA` - Faixa etária
- `RACA_COR` - Raça/cor
- `ESCOLARIDADE` - Nível de escolaridade

#### **Informações Geográficas**

- `UF` - Unidade da Federação
- `MUNICIPIO` - Município
- `CODIGO_DO_MUNICIPIO` - Código IBGE do município

#### **Informações Econômicas**

- `CNAE_2_0_CLASSE` - Classificação CNAE 2.0
- `CNAE_2_0_SUBCLASSE` - Subclasse CNAE 2.0
- `VALOR_DA_REMUNERACAO_R` - Valor da remuneração
- `HORAS_CONTRATUAIS` - Horas contratuais

#### **Informações do Vínculo**

- `VINCULO_ATIVO_31_12` - Status do vínculo em 31/12
- `TIPO_VINCULO` - Tipo de vínculo empregatício
- `TIPO_ADMISSAO` - Tipo de admissão
- `TEMPO_EMPREGO` - Tempo de emprego

### **Uso com Filtro de Campos**

Ao usar o parâmetro `--campos`, você deve especificar os nomes **padronizados** das colunas:

```bash
# Exemplo: converter apenas campos demográficos e geográficos
python main.py --converter --ano 2024 --campos SEXO_TRABALHADOR FAIXA_ETARIA RACA_COR UF MUNICIPIO

# Exemplo: converter apenas campos econômicos
python main.py --converter --ano 2024 --campos CNAE_2_0_CLASSE VALOR_DA_REMUNERACAO_R HORAS_CONTRATUAIS

# Exemplo: converter apenas campos de vínculo
python main.py --converter --ano 2024 --campos VINCULO_ATIVO_31_12 TIPO_VINCULO TIPO_ADMISSAO TEMPO_EMPREGO
```

### **Colunas Automáticas**

O sistema adiciona automaticamente a coluna `ANO` em todos os arquivos convertidos, independentemente dos campos selecionados.

### **Colunas Removidas Automaticamente**

O sistema remove automaticamente as seguintes colunas que não são relevantes para análise geral:

- `BAIRROS_SP` - Bairros de São Paulo
- `BAIRROS_FORTALEZA` - Bairros de Fortaleza  
- `BAIRROS_RJ` - Bairros do Rio de Janeiro
- `DISTRITOS_SP` - Distritos de São Paulo
- `REGIOES_ADM_DF` - Regiões Administrativas do DF

## 📖 Como Usar

### 🎯 Comandos Principais

#### **Download de Dados**

```bash
# Baixar apenas arquivos do ano 2024
python main.py --baixar --ano 2024

# Baixar faixa de anos (2020 a 2024)
python main.py --baixar --faixa-anos 2020 2024

# Baixar do ano 2020 até o último disponível
python main.py --baixar --faixa-anos 2020
```

#### **Descompactação**

```bash
# Baixar e descompactar
python main.py --descompactar --ano 2024

# Apenas descompactar arquivos já baixados
python main.py --apenas-descompactar --ano 2024
```

#### **Conversão**

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

#### **Consolidação**

```bash
# Converter com consolidação automática
python main.py --converter --ano 2024 --consolidacao

# Apenas consolidar arquivos já convertidos
python main.py --apenas-consolidar --ano 2024
```

#### **Filtro de Campos Durante Conversão**

```bash
# Converter apenas campos específicos de um ano
python main.py --converter --ano 2024 --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO

# Converter apenas campos específicos de todos os anos
python main.py --converter --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO

# Apenas converter com filtro de campos
python main.py --apenas-converter --ano 2024 --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO
```

### 🔧 Opções Avançadas

#### **Configuração de Performance**

```bash
# Definir número de workers
python main.py --converter --ano 2024 --max-workers 8

# Definir tamanho do chunk
python main.py --converter --ano 2024 --chunk-size 50000
```

#### **Configuração de Rede**

```bash
# Servidor FTP personalizado
python main.py --baixar --ano 2024 --servidor ftp.exemplo.com

# Diretório remoto personalizado
python main.py --baixar --ano 2024 --diretorio-remoto /caminho/remoto

# Configurar tentativas de download
python main.py --baixar --ano 2024 --max-tentativas 10 --tempo-espera 15
```

#### **Logging**

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
        ├── pipeline_paralelo.py    # Pipeline paralelo automático
        └── utilitarios.py          # Funções auxiliares
```

## 🎛️ Flags Disponíveis

### **Flags de Ação (Exclusivas)**

| Flag | Descrição |
|------|-----------|
| `--baixar` | Apenas baixar arquivos do servidor FTP |
| `--descompactar` | Baixar e descompactar arquivos |
| `--apenas-descompactar` | Apenas descompactar arquivos já baixados |
| `--converter` | Baixar, descompactar e converter para formato final |
| `--apenas-converter` | Apenas converter arquivos já descompactados |
| `--descompactar-converter` | Descompactar e converter arquivos já baixados |
| `--apenas-consolidar` | Apenas consolidar arquivos já convertidos |


### **Flags de Configuração**

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
| `--campos` | - | Campos específicos para incluir na conversão |

## 📊 Exemplos de Uso Completos

### **Processamento Completo de um Ano**

```bash
# Download, descompactação, conversão e consolidação
python main.py --converter --ano 2024 --consolidacao --nivel-log INFO

# Processamento com campos específicos
python main.py --converter --ano 2024 --consolidacao --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO
```

### **Processamento de Múltiplos Anos**

```bash
# Processar anos de 2020 a 2024
python main.py --converter --faixa-anos 2020 2024 --consolidacao
```

### **Otimização para Arquivos Grandes**

```bash
# Usar mais workers e chunks menores
python main.py --converter --ano 2024 --max-workers 8 --chunk-size 25000
```

### **Processamento Incremental**

```bash
# 1. Baixar dados
python main.py --baixar --ano 2024

# 2. Descompactar
python main.py --apenas-descompactar --ano 2024

# 3. Converter
python main.py --apenas-converter --ano 2024

# 4. Consolidar
python main.py --apenas-consolidar --ano 2024

## 🔍 Monitoramento e Logs

### **Arquivos de Log**
- Localização: `logs/rais_YYYY_MM_DD_HH_MM_SS.log`
- Incluem: comandos executados, progresso, erros, tempo de execução
- Níveis: DEBUG, INFO, WARNING, ERROR, CRITICAL

### **Medição de Tempo**
O sistema mede automaticamente:
- Tempo total de execução
- Tempo por etapa (Download, Descompactação, Conversão, Consolidação)
- Performance por arquivo
- **Formatação legível** - tempos exibidos em formato H:M:S para melhor compreensão

### **Monitoramento por Arquivo**
O pipeline paralelo monitora individualmente:
- Status de download de cada arquivo
- Status de descompactação de cada arquivo  
- Status de conversão de cada arquivo
- Progresso em tempo real por arquivo
- **Verificação de sincronização** - identifica arquivos que precisam ser baixados

### **Exemplo de Saída**
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

### **Falhas de Rede**
- Retry automático configurável
- Log detalhado de tentativas
- Continuação após falhas parciais

### **Falhas de Processamento**
- Log de erros específicos
- Continuação com outros arquivos
- Relatório de sucessos/falhas

### **Problemas Comuns**
- **Arquivo não encontrado**: Verificar se o ano existe no servidor
- **Erro de encoding**: Sistema detecta automaticamente
- **Memória insuficiente**: Ajustar `--chunk-size` e `--max-workers`
- **Downloads desnecessários**: Sistema verifica automaticamente se arquivos já existem

## 🔧 Configuração Avançada

### **Otimização de Performance**
```bash
# Para sistemas com muita RAM
python main.py --converter --ano 2024 --max-workers 16 --chunk-size 100000

# Para sistemas com pouca RAM
python main.py --converter --ano 2024 --max-workers 2 --chunk-size 25000
```

### **Configuração de Rede**

```bash
# Para conexões lentas
python main.py --baixar --ano 2024 --max-tentativas 10 --tempo-espera 30

# Para conexões instáveis
python main.py --baixar --ano 2024 --max-tentativas 20 --tempo-espera 5
```

## 📈 Estatísticas de Performance

### **Tempos Típicos (ano 2024)**

- **Download**: ~5-10 minutos (depende da conexão)
- **Descompactação**: ~1-2 minutos
- **Conversão**: ~2-5 minutos
- **Consolidação**: ~1-3 minutos

### **Melhorias com Pipeline Paralelo**

- **Processamento simultâneo**: Download, descompactação e conversão ocorrem ao mesmo tempo
- **Eficiência aumentada**: ~40-60% de redução no tempo total
- **Monitoramento granular**: Controle individual por arquivo
- **Listagem otimizada**: Acesso seletivo apenas aos diretórios relevantes
- **Verificação inteligente**: Evita downloads desnecessários comparando arquivos existentes

### **Otimizações de Filtragem**

- **Redução de memória**: Lê apenas colunas solicitadas
- **Processamento mais rápido**: Evita carregar dados desnecessários
- **Arquivos menores**: Resultado final otimizado para análise específica
- **Mapeamento automático**: Converte nomes de campos para padrões padronizados

### **Tamanhos de Arquivo**

- **Arquivo consolidado 2024**: ~673 MB
- **Linhas por ano**: ~10.3 milhões
- **Compressão Parquet**: ~70% menor que CSV
- **Com filtro de campos**: Redução adicional de 60-80% dependendo dos campos selecionados

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
