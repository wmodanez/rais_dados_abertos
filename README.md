# RAIS - Processador de Dados Abertos

Sistema completo para download, processamento e análise de dados da RAIS (Relação Anual de Informações Sociais) do Brasil.

## 📋 Sobre o Projeto

Este projeto automatiza o download e processamento dos microdados da RAIS disponibilizados pelo Ministério do Trabalho e Previdência Social (MTPS). O sistema oferece um pipeline completo que:

- Baixa automaticamente os dados do FTP oficial
- Descompacta arquivos .7z
- Converte arquivos TXT para formato Parquet otimizado
- Consolida dados por ano em arquivos únicos
- Processa dados em paralelo para máxima performance

## 🚀 Funcionalidades

### Modos de Operação
- **baixar**: Download do FTP oficial da RAIS
- **descompactar**: Extração de arquivos .7z
- **converter**: Conversão de TXT para Parquet
- **consolidar**: Consolidação de dados por ano
- **extrair-converter**: Descompactar + converter (sem consolidar)
- **processar**: Descompactar + converter + consolidar (sem baixar)
- **completo**: Pipeline completo (baixar → descompactar → converter → consolidar)

### Características Técnicas
- **Processamento paralelo**: Workers configuráveis para cada etapa
- **Otimização automática**: Detecta recursos da máquina e ajusta workers automaticamente
- **Otimização de memória**: Uso do Dask para arquivos grandes (10GB+)
- **Filtros automáticos**: Exclusão de arquivos EST e NI
- **Limpeza de dados**: Remoção automática de acentos nas colunas
- **Formato otimizado**: Compressão Snappy no Parquet
- **Logging detalhado**: Acompanhamento visual com barras de progresso individuais
- **Monitoramento em tempo real**: Mostra qual arquivo está sendo processado e seu status
- **Logs em arquivo**: Sistema completo de logging em arquivos diários para auditoria
- **Medição de tempo**: Cronômetro automático para cada etapa e tempo total de execução
- **Formato consolidado flexível**: Arquivo único (.parquet) ou múltiplos arquivos (padrão)

## 📦 Instalação

### Compatibilidade
- **Windows**: 10/11 (testado)
- **Linux**: Ubuntu 18.04+, CentOS 7+, Debian 10+ (testado)
- **Python**: 3.8+ (recomendado 3.9+)

### Instalação Windows
```cmd
# PowerShell ou Command Prompt
git clone <repositorio>
cd rais
pip install -r requirements.txt
python main.py --listar
```

### Instalação Linux
```bash
# Terminal
git clone <repositorio>
cd rais
pip install -r requirements.txt
python main.py --listar
```

### Dependências Principais
- pandas
- dask[complete]
- pyarrow
- py7zr
- tqdm
- psutil (detecção de recursos do sistema)

## 🔧 Uso

### Exemplos Básicos

#### Windows (PowerShell/CMD)
```cmd
# Listar anos disponíveis no FTP
python main.py --modo baixar --listar

# Download do ano mais recente
python main.py --modo baixar --baixar-opcao atual

# Download de todos os anos
python main.py --modo baixar --baixar-opcao todos

# Download apenas de anos específicos
python main.py --modo baixar --anos 2020 2021 2022

# Download de uma faixa de anos
python main.py --modo baixar --faixa-anos 2015 2020
```

#### Linux (Terminal)
```bash
# Listar anos disponíveis no FTP
python3 main.py --modo baixar --listar

# Download do ano mais recente
python3 main.py --modo baixar --baixar-opcao atual

# Download de todos os anos  
python3 main.py --modo baixar --baixar-opcao todos

# Download apenas de anos específicos
python3 main.py --modo baixar --anos 2020 2021 2022

# Download de uma faixa de anos
python3 main.py --modo baixar --faixa-anos 2015 2020
```

### Pipeline Completo

```bash
# Processamento completo (baixar + processar tudo)
python main.py --modo completo

# Processamento completo pulando o download
python main.py --modo completo --pular-download

# Processamento de anos específicos
python main.py --modo completo --anos 2020 2021

# Processamento preservando arquivos intermediários
python main.py --modo completo --preservar
```

### Processamento por Etapas

```bash
# Apenas descompactar arquivos já baixados
python main.py --modo descompactar --anos 2020

# Apenas converter TXT para Parquet
python main.py --modo converter --anos 2020

# Apenas consolidar arquivos Parquet
python main.py --modo consolidar --anos 2020

# Descompactar + converter (sem baixar nem consolidar)
python main.py --modo extrair-converter --anos 2020

# Descompactar + converter + consolidar (sem baixar)
python main.py --modo processar --anos 2020

# Processar com arquivo consolidado único
python main.py --modo processar --anos 2020 --consolidado-unico

# Processar preservando arquivos intermediários
python main.py --modo processar --anos 2020 --preservar
```

### Otimização de Performance

```bash
# O sistema detecta automaticamente os recursos e otimiza workers
python main.py --modo completo

# Exemplo de saída da detecção automática:
# Recursos detectados:
#   - CPUs físicos: 8
#   - CPUs lógicos: 16
#   - Memória RAM: 32.0 GB
# Workers otimizados calculados:
#   - Download: 8 (I/O intensivo)
#   - Extração: 8 (CPU + I/O)
#   - Conversão: 4 (memória intensiva)

# Sobrescrever valores automáticos se necessário
python main.py --modo completo \
  --workers-download 6 \
  --workers-extract 4 \
  --workers-convert 2

# Limitar número de arquivos para teste
python main.py --modo completo --max 5

# Configurar partições do Parquet
python main.py --modo converter --npartitions 10
```

### Opções Avançadas

```bash
# Sobrescrever arquivos existentes
python main.py --modo completo --sobrescrever

# Pausar entre arquivos (para sistemas com recursos limitados)
python main.py --modo descompactar --pausar 2

# Configurar tamanho dos chunks
python main.py --modo converter --chunksize 50000

# Controle de verbosidade do console
python main.py --modo completo --log-level DEBUG    # Muito detalhado
python main.py --modo completo --log-level INFO     # Padrão (recomendado)
python main.py --modo completo --log-level WARNING  # Apenas avisos/erros
python main.py --modo completo --log-level ERROR    # Apenas erros

# Arquivo consolidado como arquivo único
python main.py --modo completo --consolidado-unico  # Arquivo .parquet único
python main.py --modo processar --consolidado-unico # Sem download, arquivo único
```

## 📁 Estrutura de Diretórios

```
rais/
├── dados-abertos-zip/          # Arquivos .7z baixados do FTP
│   ├── 2020/
│   ├── 2021/
│   └── 2022/
├── dados-abertos/              # Arquivos TXT extraídos
│   ├── 2020/
│   ├── 2021/
│   └── 2022/
├── parquet/                    # Arquivos Parquet processados
│   ├── 2020/
│   │   ├── arquivo1.parquet
│   │   ├── arquivo2.parquet
│   │   └── RAIS_2020_consolidado/  # Arquivo consolidado
│   ├── 2021/
│   └── 2022/
├── logs/                       # Logs diários do sistema
│   ├── rais_2024_01_15.log
│   ├── rais_2024_01_16.log
│   └── rais_2024_01_17.log
└── main.py                     # Script principal
```

## ⚙️ Parâmetros Disponíveis

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `--anos` | int[] | Anos específicos para processar |
| `--sobrescrever` | flag | Sobrescreve arquivos existentes |
| `--max` | int | Número máximo de arquivos a processar |
| `--pausar` | int | Segundos de pausa entre arquivos |
| `--listar` | flag | Lista anos disponíveis sem processar |
| `--modo` | string | Modo de operação |
| `--chunksize` | int | Tamanho dos chunks (padrão: 100.000) |
| `--npartitions` | int | Número de partições Parquet |
| `--preservar` | flag | Preserva arquivos intermediários |
| `--workers-download` | int | Workers para download (padrão: auto-detectado) |
| `--workers-extract` | int | Workers para descompactação (padrão: auto-detectado) |
| `--workers-convert` | int | Workers para conversão (padrão: auto-detectado) |
| `--auto-workers` | flag | Ativa detecção automática de workers (padrão: ativo) |
| `--baixar-opcao` | string | Opção de download (atual/todos/faltantes) |
| `--faixa-anos` | int int | Faixa de anos para baixar |
| `--pular-download` | flag | Pula etapa de download no modo completo |
| `--log-level` | string | Nível de log do console (DEBUG/INFO/WARNING/ERROR/CRITICAL) |
| `--consolidado-unico` | flag | Salva consolidado como arquivo único (padrão: múltiplos arquivos) |

## 🔍 Filtros Automáticos

O sistema automaticamente exclui arquivos que contenham:
- **_EST**: Dados de estabelecimentos
- **NI**: Dados não identificados

Esta filtragem é aplicada em todas as etapas do processamento.

## 💾 Formato de Saída

Os dados finais são salvos em formato Parquet com:
- **Compressão**: Snappy
- **Colunas limpas**: Sem acentos e caracteres especiais
- **Metadados**: Coluna `fonte_arquivo` para rastreabilidade
- **Estrutura flexível**: Arquivo único ou múltiplos arquivos

#### Formatos de Consolidação

**📁 Múltiplos Arquivos (Padrão)**
```bash
python main.py --modo completo
# Resultado: parquet/2020/RAIS_2020_consolidado/ (diretório com múltiplos .parquet)
```
- **Vantagens**: Paralelização de leitura, flexibilidade
- **Ideal para**: Análises com Dask/Spark, datasets muito grandes

**📄 Arquivo Único**
```bash
python main.py --modo completo --consolidado-unico
# Resultado: parquet/2020/RAIS_2020_consolidado.parquet (arquivo único)
```
- **Vantagens**: Simplicidade, compatibilidade universal
- **Ideal para**: Análises com Pandas, transferência, backup

#### Estrutura de Saída

| Modo | Local | Formato | Tamanho Típico |
|------|-------|---------|----------------|
| **Padrão** | `parquet/2020/RAIS_2020_consolidado/` | Diretório | 2-8 GB |
| **Único** | `parquet/2020/RAIS_2020_consolidado.parquet` | Arquivo | 2-8 GB |

## 📊 Performance

### Recomendações de Hardware

#### Requisitos Mínimos (Windows/Linux)
- **RAM**: 8GB (processamento de 1 ano)
- **CPU**: 4 cores (físicos)
- **Armazenamento**: 100GB livres
- **Rede**: Banda larga para download

#### Configuração Recomendada
- **RAM**: 16GB+ (processamento de múltiplos anos)
- **CPU**: 8+ cores (físicos) com hyperthreading
- **Armazenamento**: SSD com 200GB+ livres
- **Rede**: Conexão estável 10Mbps+

#### Performance por Plataforma
- **Linux**: ~10-15% mais rápido (especialmente I/O)
- **Windows**: Performance similar com SSD e exclusões do antivírus
- **WSL2**: Performance intermediária, compatível com comandos Linux

### Workers e Processamento Paralelo

#### O que são Workers?
Workers são processos ou threads paralelos que executam tarefas simultaneamente, acelerando significativamente o processamento. Cada etapa do pipeline usa um tipo específico de paralelismo:

#### Tipos de Workers por Etapa

**🔽 Download Workers (ThreadPoolExecutor)**
- **Tipo**: Threads (I/O bound)
- **Função**: Baixar múltiplos arquivos simultaneamente do FTP
- **Limitação**: Rede e servidor FTP
- **Compatibilidade**: Windows/Linux

**📦 Extração Workers (ThreadPoolExecutor)**  
- **Tipo**: Threads (CPU + I/O bound)
- **Função**: Descompactar múltiplos arquivos .7z simultaneamente
- **Limitação**: CPU e disco
- **Compatibilidade**: Windows/Linux

**⚡ Conversão Workers (ProcessPoolExecutor)**
- **Tipo**: Processos (Memory bound)
- **Função**: Converter arquivos TXT para Parquet em paralelo
- **Limitação**: RAM disponível (cada processo consome ~2-4GB)
- **Compatibilidade**: Windows/Linux (com proteção de spawn)
- **Monitoramento**: Progresso detalhado por arquivo (detectar separador → analisar colunas → carregar dados → salvar)

#### Configurações Automáticas

O sistema detecta automaticamente os recursos e calcula valores otimizados:

| RAM | Download | Extração | Conversão | Observações |
|-----|----------|----------|-----------|-------------|
| < 8GB | 4-8 | 2-8 | **1** | Conservador para evitar OOM |
| 8-16GB | 4-8 | 2-8 | **2** | Balanceado |
| 16-32GB | 4-8 | 2-8 | **2-4** | Performance otimizada |
| > 32GB | 4-8 | 2-8 | **2-6** | Máxima performance |

**Lógica de cálculo multiplataforma:**
- **Download**: I/O intensivo, usa CPUs lógicos (máx 8)
- **Extração**: CPU + I/O, usa CPUs físicos + 2 (máx 8)  
- **Conversão**: Memória intensiva, limitado pela RAM disponível
- **Compatibilidade**: Testado em Windows 10/11 e Linux (Ubuntu/CentOS)

#### Sistema de Monitoramento

O sistema fornece feedback visual detalhado durante todo o processamento:

**🌐 Download FTP**
```
🌐 Download FTP: 45%|████▌     | 23/51 [01:23<01:47, 1.2arquivo/s] ✅15 ❌1
📥 RAIS_2020_AC.7z: 45%|████▌ | 234MB/521MB [00:23<00:28, 10.2MB/s]
✅ RAIS_2020_AC.7z baixado com sucesso
```

**📦 Extração 7z**
```
📦 Extração 7z: 78%|███████▊  | 35/45 [02:15<00:32, 2.1arquivo/s] ✅32 ❌1
📦 Extraindo RAIS_2020_AC.7z: 100%|██████████| 521MB/521MB [00:45<00:00, 11.5MB/s]
✅ RAIS_2020_AC.7z descompactado com sucesso (1 arquivos extraídos)
```

**🔄 Conversão TXT→Parquet**
```
🔄 TXT→Parquet: 23%|██▎       | 12/52 [03:45<12:30, 1.8s/arquivo] ✅11 ❌0
🔄 Convertendo RAIS_2020_AC.txt (1.2GB): 60%|██████    | 60/100 [01:23<00:55] Salvando Parquet...
✅ RAIS_2020_AC.txt → Parquet (47 colunas, 89.3MB)
```

**📊 Consolidação**
```
📊 Iniciando consolidação de 27 arquivos...
📖 Lendo Parquets 2020: 85%|████████▌ | 23/27 [00:45<00:08, 1.9arquivo/s]
    ✅ RAIS_2020_AC: 123,456 linhas, 47 colunas
🔗 Concatenando 27 DataFrames...
📈 Resultado: 3,456,789 linhas, 48 colunas
💾 Salvando arquivo consolidado...
✅ Arquivo consolidado criado: 2.34 GB
```

#### Sistema de Logging

O sistema mantém logs detalhados em arquivos diários para auditoria e troubleshooting:

**📁 Estrutura de Logs**
```
rais/
├── logs/
│   ├── rais_2024_01_15.log    # Log do dia 15/01/2024
│   ├── rais_2024_01_16.log    # Log do dia 16/01/2024
│   └── rais_2024_01_17.log    # Log atual
└── main.py
```

**📊 Níveis de Log**
- **DEBUG**: Detalhes técnicos completos (separadores, colunas, tempos individuais)
- **INFO**: Etapas principais, início/fim de processos, estatísticas (padrão console)
- **WARNING**: Situações que merecem atenção mas não impedem execução
- **ERROR**: Erros que impedem processamento de arquivos específicos
- **CRITICAL**: Erros críticos que param o sistema

**🎛️ Configuração de Níveis**
```bash
# Nível padrão (INFO) - mostra etapas principais
python main.py --modo completo

# Modo verboso (DEBUG) - mostra todos os detalhes
python main.py --modo completo --log-level DEBUG

# Modo silencioso (WARNING) - apenas avisos e erros
python main.py --modo completo --log-level WARNING

# Apenas erros (ERROR) - console muito limpo
python main.py --modo completo --log-level ERROR
```

**📝 Arquivo vs Console**
- **Arquivo**: Sempre grava TODOS os níveis (DEBUG+) para auditoria completa
- **Console**: Nível configurável conforme parâmetro `--log-level`

**📝 Exemplo de Conteúdo do Log**
```
2024-01-17 14:30:15 | INFO     | main                 | RAIS - Sistema de Processamento Iniciado
2024-01-17 14:30:15 | INFO     | main                 | Arquivo de log: logs/rais_2024_01_17.log
2024-01-17 14:30:16 | INFO     | calcular_workers_oti | Recursos do sistema detectados:
2024-01-17 14:30:16 | INFO     | calcular_workers_oti |   - CPUs físicos: 8
2024-01-17 14:30:16 | INFO     | calcular_workers_oti |   - CPUs lógicos: 16
2024-01-17 14:30:16 | INFO     | calcular_workers_oti |   - Memória RAM: 32.0 GB
2024-01-17 14:30:16 | INFO     | main                 | Argumentos da linha de comando: {'anos': [2020], 'modo': 'completo'}
2024-01-17 14:30:16 | INFO     | baixar_dados_ftp     | INICIANDO ETAPA DE DOWNLOAD DO FTP
2024-01-17 14:30:17 | DEBUG    | baixar_arquivo_ftp   | Iniciando download de RAIS_2020_AC.7z do ano 2020
2024-01-17 14:30:45 | INFO     | baixar_arquivo_ftp   | Download concluído: RAIS_2020_AC.7z (521.3MB em 28.2s, 18.5MB/s)
2024-01-17 14:31:20 | INFO     | descompactar_arquivo | Extração concluída: RAIS_2020_AC.7z (1 arquivos em 35.1s)
2024-01-17 14:31:21 | INFO     | converter_arquivo_wo | Iniciando conversão de RAIS_2020_AC.txt (1234.5MB)
2024-01-17 14:32:45 | DEBUG    | converter_arquivo_wo | Separador detectado para RAIS_2020_AC.txt: ';'
2024-01-17 14:32:46 | DEBUG    | converter_arquivo_wo | Arquivo RAIS_2020_AC.txt possui 47 colunas
2024-01-17 14:34:12 | INFO     | converter_arquivo_wo | Conversão concluída: RAIS_2020_AC.txt → RAIS_2020_AC.parquet (47 colunas, 89.3MB em 171.2s)
```

#### Sistema de Medição de Tempo

O sistema automaticamente mede e reporta o tempo de execução de cada etapa:

**⏱️ Medição Automática**
```bash
# Ao final de qualquer execução, você verá:
============================================================
📊 RESUMO DOS TEMPOS DE EXECUÇÃO
============================================================
⏱️  Download FTP                  : 02m45s
⏱️  Descompactação               : 01m23s
⏱️  Conversão TXT→Parquet        : 08m17s
⏱️  Consolidação Parquet         : 00m42s
------------------------------------------------------------
⏱️  TEMPO TOTAL                  : 13m07s
============================================================
```

**📊 Logs Detalhados**
- Cada etapa registra tempo de início e fim nos logs
- Arquivos individuais mostram tempo de processamento
- Velocidades de download e conversão são calculadas
- Relatório final com breakdown completo

**🎯 Benefícios**
- **Otimização**: Identificar etapas mais lentas
- **Planejamento**: Estimar tempo para grandes volumes
- **Monitoramento**: Acompanhar performance ao longo do tempo
- **Debugging**: Detectar gargalos de performance

## 🛠️ Desenvolvimento

### Estrutura do Código
- `main.py`: Script principal com todas as funcionalidades
- `requirements.txt`: Dependências do projeto
- Pipeline modular com funções independentes

### Contribuição
1. Fork do projeto
2. Criar branch para feature
3. Implementar mudanças
4. Testar funcionamento
5. Submit pull request

## 📜 Licença

Este projeto é distribuído sob licença MIT. Veja LICENSE para mais detalhes.

## 🔧 Troubleshooting

### Análise de Logs

Os logs são essenciais para identificar problemas e otimizar performance:

```bash
# Visualizar log do dia atual
tail -f logs/rais_$(date +%Y_%m_%d).log

# Buscar erros específicos
grep "ERROR" logs/rais_2024_01_17.log

# Analisar performance de downloads
grep "Download concluído" logs/rais_2024_01_17.log

# Verificar tempo de conversão
grep "Conversão concluída" logs/rais_2024_01_17.log

# Buscar arquivos problemáticos
grep "Erro durante" logs/rais_2024_01_17.log

# Analisar tempos de execução
grep "⏱️" logs/rais_2024_01_17.log

# Verificar se arquivos foram limpos
grep "Removendo arquivos" logs/rais_2024_01_17.log
```

### Problemas Comuns Windows
```cmd
# Erro de codificação
set PYTHONIOENCODING=utf-8
python main.py --modo completo

# Erro de multiprocessing
# Certifique-se de executar em prompt administrativo
python main.py --workers-convert 1

# Problema com paths longos
# Ativar suporte a paths longos no Windows 10/11

# Verificar logs para detalhes
type logs\rais_%date:~-4,4%_%date:~-10,2%_%date:~-7,2%.log

# Verificar se consolidado foi criado corretamente
dir parquet\2020\RAIS_2020_consolidado*
```

### Problemas Comuns Linux
```bash
# Permissões de arquivo
chmod +x main.py
sudo chown -R $USER:$USER ./

# Memória insuficiente
# Reduzir workers se OOM (Out of Memory)
python main.py --modo completo --workers-convert 1

# Dependências de sistema (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install python3-dev python3-pip
```

### Otimização por Plataforma

**Windows:**
- Use SSD se possível (melhora significativa na extração)
- Configure Windows Defender para excluir a pasta do projeto
- Use PowerShell 7+ para melhor performance

**Linux:**
- Configure ulimit para mais arquivos abertos: `ulimit -n 4096`
- Use ext4 ou xfs para melhor performance com arquivos grandes
- Configure swappiness baixo: `echo 10 | sudo tee /proc/sys/vm/swappiness`

### Problemas com Novas Funcionalidades

**Arquivos não sendo limpos no modo processar:**
```bash
# Verificar se --preservar está ativo (impede limpeza)
python main.py --modo processar --anos 2020  # Limpa automaticamente

# Forçar preservação
python main.py --modo processar --anos 2020 --preservar
```

**Erro com arquivo consolidado único:**
```bash
# Se erro de memória com --consolidado-unico, use modo padrão
python main.py --modo processar --anos 2020  # Múltiplos arquivos

# Ou reduza workers
python main.py --modo processar --anos 2020 --consolidado-unico --workers-convert 1
```

**Tempos não aparecendo:**
```bash
# Verificar nível de log (INFO+ necessário para ver tempos)
python main.py --modo processar --anos 2020 --log-level INFO
```

## 📞 Suporte

Para questões técnicas ou sugestões:
- Abra uma issue no GitHub
- Consulte a documentação oficial da RAIS
- Verifique os logs de erro para troubleshooting
- Inclua informações do sistema (OS, Python, RAM) ao reportar bugs

## 🔗 Links Úteis

- [RAIS - Ministério do Trabalho](http://www.rais.gov.br/)
- [FTP Oficial](ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/)
- [Layout dos Dados](http://www.rais.gov.br/sitio/download.jsf)