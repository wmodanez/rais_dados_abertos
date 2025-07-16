# RAIS - Processador de Dados Abertos

Sistema completo para download, processamento e análise de dados da RAIS (Relação Anual de Informações Sociais) do Brasil.

## 📋 Sobre o Projeto

Este projeto automatiza o download e processamento dos microdados da RAIS disponibilizados pelo Ministério do Trabalho e Previdência Social (MTPS). O sistema oferece um pipeline completo que:

- Baixa automaticamente os dados do FTP oficial
- Descompacta arquivos .7z
- Converte arquivos TXT para formato Parquet otimizado
- Adiciona coluna ANO_RAIS automaticamente nos arquivos processados
- Consolida dados por ano em arquivos únicos
- Consolida todos os anos em uma única pasta/arquivo
- **Processamento com pipeline otimizado** usando múltiplos processos
- **Remoção automática de colunas desnecessárias** para otimização de espaço
- **Verificação automática de integridade** com re-download imediato
- **Sincronização inteligente** com detecção de arquivos faltantes/corrompidos

## 🚀 Funcionalidades

### Modos de Operação
- **baixar**: Download do FTP oficial da RAIS
- **descompactar**: Extração de arquivos .7z
- **converter**: Conversão de TXT para Parquet
- **consolidar**: Consolidação de dados por ano
- **consolidar-geral**: Consolidação de todos os anos em uma pasta única
- **extrair-converter**: Descompactar + converter (sem consolidar)
- **completo**: **Pipeline otimizado completo** (baixar → processamento por ano → consolidação)
- **verificar-7z**: Verificação preventiva de integridade de arquivos 7z

### Funcionalidades Principais

#### 🔍 Verificação Automática de Integridade
- **Verificação robusta**: Detecta arquivos corrompidos, vazios ou com problemas
- **Re-download imediato**: Baixa novamente arquivos corrompidos automaticamente
- **Validação pós-download**: Confirma integridade após correção
- **Múltiplas verificações**: Tamanho, conteúdo, estrutura 7z e arquivos TXT internos

#### 🔄 Sistema de Tolerância a Erros
- **Múltiplas tentativas**: 3 tentativas por arquivo (configurável via `--max-tentativas-download`)
- **Backoff exponencial**: Tempo de espera crescente entre tentativas (5s → 10s → 15s)
- **Limpeza automática**: Remove arquivos parciais após falhas
- **Validação rigorosa**: Verifica tamanho e detecta arquivos vazios

#### 📊 Sincronização Inteligente
- **Verificação de quantidade**: Compara número de arquivos locais vs remotos
- **Detecção de órfãos**: Identifica arquivos locais que não existem no FTP
- **Comparação byte-a-byte**: Detecta arquivos com tamanhos diferentes
- **Relatórios detalhados**: Mostra exatamente quais arquivos precisam ser baixados

#### Coluna ANO_RAIS Automática
- **Extração inteligente**: O ano é extraído automaticamente do nome do arquivo ou pasta
- **Retrocompatibilidade**: Adiciona a coluna mesmo em arquivos já processados
- **Identificação única**: Facilita análises temporais e filtros por ano

#### 🗂️ Gerenciamento Inteligente de Arquivos
- **--preservar-descompactados**: Mantém apenas arquivos TXT, remove Parquets intermediários
- **--remover-anos-pos-consolidacao**: Remove pastas de anos após consolidação geral
- **--incremental**: Adiciona apenas anos novos à consolidação existente

#### 🚀 Pipeline Paralelo Otimizado
- **--completo**: Modo que processa cada ano completamente antes do próximo usando pipeline assíncrono interno
- **--max-anos-paralelos**: Controla quantos anos são processados simultaneamente (padrão: 2)
- **Processamento multi-processo**: Usa `ProcessPoolExecutor` para paralelismo real
- **Remoção automática de colunas**: Remove automaticamente colunas de bairros desnecessárias
- **Vantagens**: Economia de espaço, maior eficiência, melhor aproveitamento de recursos

#### Consolidação Geral
- **União de todos os anos**: Consolida arquivos de diferentes anos em uma única estrutura
- **Flexibilidade de formato**: Arquivo único (.parquet) ou diretório com múltiplos arquivos
- **Preservação de metadados**: Mantém informações de origem e ano de cada registro

### Características Técnicas
- **Processamento paralelo**: Workers configuráveis para cada etapa
- **Otimização automática**: Detecta recursos da máquina e ajusta workers automaticamente
- **Otimização de memória**: Uso do Dask para arquivos grandes (10GB+)
- **Filtros automáticos**: Exclusão de arquivos EST e NI
- **Limpeza de dados**: Remoção automática de acentos nas colunas
- **Formato otimizado**: Compressão Snappy no Parquet
- Metadados enriquecidos: Coluna `ANO_RAIS` em todos os registros
- **Logging detalhado**: Acompanhamento visual com barras de progresso individuais
- **Monitoramento em tempo real**: Mostra qual arquivo está sendo processado e seu status
- **Logs em arquivo**: Sistema completo de logging em arquivos diários para auditoria
- **Medição de tempo**: Cronômetro automático para cada etapa e tempo total de execução
- **Verificação de integridade**: Sistema robusto de detecção e correção de arquivos corrompidos
- **Controle de erros thread-safe**: Rastreamento de erros por ano e etapa

## 🎯 Exemplos de Uso

### Processamento Básico
```bash
# Processar arquivos já baixados
python main.py --modo completo --sobrescrever

# Processamento completo (download + processamento)
python main.py --modo completo --sobrescrever
```

### 📥 Download Inteligente de Anos Faltantes

O sistema faz **verificação inteligente** comparando arquivos locais vs. remotos:

#### 🔍 Verificação Avançada
- **📏 Comparação de tamanho**: Detecta arquivos corrompidos/incompletos
- **📁 Arquivos faltantes**: Identifica arquivos que não existem localmente
- **📊 Verificação de quantidade**: Compara número de arquivos locais vs remotos
- **🗑️ Detecção de órfãos**: Identifica arquivos locais que não existem no FTP
- **🚫 Filtros automáticos**: Exclui arquivos EST, ESTB, IGN e NI
- **⚡ Performance otimizada**: ~1.2 segundos por ano verificado

#### Opções de Download
```bash
# 🎯 PADRÃO: Verificação inteligente (recomendado)
python main.py --modo baixar --baixar-opcao faltantes

# 📅 Baixar apenas o ano mais atual
python main.py --modo baixar --baixar-opcao atual

# 📚 Baixar todos os anos (mesmo já baixados)
python main.py --modo baixar --baixar-opcao todos --sobrescrever

# 📊 Verificar quais anos estão disponíveis
python main.py --modo baixar --listar
```

#### Como Funciona a Verificação Inteligente
```
📅 Modo: Verificação inteligente de anos faltantes
  🔍 Comparando arquivos locais vs. remotos...
🔍 Verificando anos: 100%|████████████| 40/40 [00:47<00:00]

  ⚠️  Ano 2024: Quantidade de arquivos divergente - Local: 26, Remoto: 27
📁 Ano 2024: 1 arquivo(s) - RAIS_VINC_PUB_SP.7z (não existe localmente)
📁 Ano 2022: 1 arquivo(s) - RAIS_VINC_PUB_NORTE.7z (tamanho diferente)
  🗑️  Ano 2021: 1 arquivos órfãos encontrados (existem localmente mas não no FTP)
      • RAIS_VINC_PUB_ANTIGO.7z

📊 Resumo da verificação:
  • Anos disponíveis no FTP: 40
  • Anos que precisam de download/atualização: 2  
  • Total de arquivos a baixar/atualizar: 2
  • Anos: [2022, 2024]
```

#### Vantagens da Verificação Inteligente
- 🔧 **Detecta corrupção**: Arquivos com tamanho diferente são redownloadados
- 📊 **Verifica completude**: Identifica quando faltam arquivos
- 🗑️ **Detecta órfãos**: Mostra arquivos locais que não existem mais no FTP
- ⚡ **Economia de tempo**: Evita downloads desnecessários
- 💾 **Economia de banda**: Baixa apenas o que precisa ser atualizado
- 🔄 **Retomada automática**: Continua de onde parou em caso de interrupção
- 📊 **Feedback detalhado**: Mostra exatamente o que será baixado e por quê

### 🔍 Verificação Preventiva de Integridade

#### Verificação Manual de Arquivos 7z
```bash
# Verificar integridade de todos os arquivos 7z
python main.py --verificar-7z
```

#### Exemplo de Saída da Verificação
```
🔍 VERIFICAÇÃO PREVENTIVA DE ARQUIVOS 7Z
============================================================
📂 Encontradas 38 pastas de anos: 1985, 1986, ..., 2024

📅 Verificando ano 2024...
   📦 Verificando 27 arquivos...
   ❌ 2 arquivos com ERROS:
      • RAIS_VINC_PUB_AC.7z: Arquivo 7z corrompido ou inválido
      • RAIS_VINC_PUB_SP.7z: Arquivo vazio (0 bytes)
   ⚠️  1 arquivos com AVISOS:
      • RAIS_VINC_PUB_RJ.7z: Arquivo muito pequeno (512 bytes)

📊 RESUMO DA VERIFICAÇÃO:
   • Total de arquivos verificados: 1.234
   • Arquivos íntegros: 1.231 (99.8%)
   • Arquivos com problemas: 3 (0.2%)
   • Anos com problemas: 1
   • Anos afetados: 2024

💾 Relatório detalhado salvo em: logs/verificacao_7z_2025_07_15_143022.txt
```

#### Verificação Automática Durante Processamento
```bash
# Verificação automática habilitada por padrão
python main.py --modo completo --sobrescrever

# Desabilitar verificação automática (não recomendado)
python main.py --modo descompactar --anos 2024 --auto-redownload false
```

### 🔄 Sistema de Tolerância a Erros

#### Configuração de Tolerância
```bash
# Configurar múltiplas tentativas e tempo de espera
python main.py --modo baixar --max-tentativas-download 5 --tempo-espera-download 10

# Download com máxima robustez
python main.py --modo baixar --baixar-opcao faltantes --max-tentativas-download 5
```

#### Exemplo de Re-download Automático
```
🔍 Verificando integridade de RAIS_VINC_PUB_SP.7z
⚠️  Arquivo RAIS_VINC_PUB_SP.7z com problema: Arquivo 7z corrompido ou inválido
🔄 Iniciando re-download IMEDIATO de RAIS_VINC_PUB_SP.7z

📥 Re-baixando RAIS_VINC_PUB_SP.7z: 100%|████████| 45.2MB/45.2MB [00:23<00:00, 1.97MB/s]
✅ Re-download concluído: RAIS_VINC_PUB_SP.7z (45.2MB em 23.1s, 1.97MB/s)
🔍 Verificando integridade após re-download de RAIS_VINC_PUB_SP.7z
✅ Arquivo RAIS_VINC_PUB_SP.7z re-baixado e verificado com sucesso
```

#### Tratamento de Falhas Persistentes
```
🔄 Tentando re-download de RAIS_VINC_PUB_AC.7z
Tentativa 1/3 falhou para re-download de RAIS_VINC_PUB_AC.7z: Connection timeout
Aguardando 5s antes da próxima tentativa...
Tentativa 2/3 falhou para re-download de RAIS_VINC_PUB_AC.7z: Server error 500
Aguardando 10s antes da próxima tentativa...
Tentativa 3/3 falhou para re-download de RAIS_VINC_PUB_AC.7z: Network unreachable
Todas as 3 tentativas falharam para re-download de RAIS_VINC_PUB_AC.7z
❌ RAIS_VINC_PUB_AC.7z: Arquivo corrompido e falha no re-download
```

### 📋 Análise de Logs

#### Como Analisar Problemas
```bash
# Usar nível de log detalhado para diagnóstico
python main.py --log-level DEBUG

# Verificar integridade preventivamente
python main.py --verificar-7z
```

#### Onde Encontrar Informações de Erro
```
📂 logs/
├── rais_2025_07_15.log          # Log principal com todos os detalhes
├── rais_2025_07_16.log          # Logs diários
└── verificacao_7z_*.txt         # Relatórios de verificação

💡 Dica: Use --log-level DEBUG para obter informações mais detalhadas durante a execução
```

#### Exemplo de Uso em Python
```python
import pandas as pd

# Carregar arquivo consolidado geral
df = pd.read_parquet('parquet/RAIS_TODOS_ANOS_consolidado.parquet')

# Filtrar por ano específico
df_2020 = df[df['ANO_RAIS'] == '2020']

# Análise temporal
vinculos_por_ano = df.groupby('ANO_RAIS').size()

# Análise por estado requer filtros baseados em outras colunas
# Exemplo: filtrar por município ou outras características geográficas
```

### 📊 Consolidação de Dados

#### Consolidação Geral (Todos os Anos)
```bash
# Consolidação geral básica
python main.py --modo consolidar-geral --sobrescrever

# Consolidação geral incremental (adiciona apenas anos novos)
python main.py --modo consolidar-geral --incremental

# Consolidação geral com remoção automática de anos individuais
python main.py --modo consolidar-geral --sobrescrever --remover-anos-pos-consolidacao

# Pipeline completo com consolidação geral otimizada
python main.py --modo completo --sobrescrever --preservar-descompactados
python main.py --modo consolidar-geral --incremental --remover-anos-pos-consolidacao
```

#### Controle de Formato de Saída
```bash
# Consolidação por ano preservando arquivos TXT
python main.py --modo completo --sobrescrever --preservar-descompactados

# Consolidação por ano removendo arquivos intermediários (padrão)
python main.py --modo completo --sobrescrever
```

### Processamento Seletivo
```bash
# Anos específicos
python main.py --modo completo --anos 2020 2021 2022 --sobrescrever

# Faixa de anos com consolidação geral
python main.py --modo completo --faixa-anos 2018 2022 --sobrescrever
python main.py --modo consolidar-geral --sobrescrever --remover-anos-pos-consolidacao
```

### Configurações Avançadas
```bash
# Workers personalizados
python main.py --modo completo --sobrescrever --workers-extract 6 --workers-convert 3

# Processamento limitado (teste)
python main.py --modo completo --sobrescrever --max 5

# Preservar todos os arquivos intermediários
python main.py --modo completo --sobrescrever --preservar

# Preservar apenas arquivos descompactados
python main.py --modo completo --sobrescrever --preservar-descompactados
```

## 📁 Estrutura de Diretórios Atualizada

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
│   │   ├── arquivo1.parquet    # Individual (com ANO_RAIS)
│   │   ├── arquivo2.parquet
│   │   └── RAIS_2020_consolidado/     # Consolidado por ano
│   ├── 2021/
│   ├── 2022/
│   └── RAIS_TODOS_ANOS_consolidado/  # Consolidado geral (diretório)
│   └── RAIS_TODOS_ANOS_consolidado.parquet  # Ou arquivo único
├── logs/                       # Logs diários do sistema
│   ├── rais_2025_07_15.log          # Log principal com todos os detalhes
│   ├── rais_2025_07_16.log          # Logs diários separados
│   └── verificacao_7z_2025_07_15_143022.txt  # Relatórios de verificação
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
| `--modo` | string | Modo de operação (incluindo **consolidar-geral**, **verificar-7z**, **relatorio-problemas**) |
| `--chunksize` | int | Tamanho dos chunks (padrão: 100.000) |
| `--npartitions` | int | Número de partições Parquet |
| `--preservar` | flag | Preserva arquivos intermediários |
| `--preservar-descompactados` | flag | **Preserva apenas arquivos TXT, removendo Parquets intermediários** |
| `--incremental` | flag | **Modo incremental: adiciona apenas anos novos ao consolidado geral** |
| `--remover-anos-pos-consolidacao` | flag | **Remove pastas de anos após consolidação geral para economizar espaço** |
| `--workers-download` | int | Workers para download (padrão: auto-detectado) |
| `--workers-extract` | int | Workers para descompactação (padrão: auto-detectado) |
| `--workers-convert` | int | Workers para conversão (padrão: auto-detectado) |
| `--auto-workers` | flag | Ativa detecção automática de workers (padrão: ativo) |
| `--baixar-opcao` | string | Opção de download (atual/todos/faltantes) |
| `--faixa-anos` | int int | Faixa de anos para baixar |
| `--pular-download` | flag | Pula etapa de download no modo completo |
| `--log-level` | string | Nível de log do console (DEBUG/INFO/WARNING/ERROR/CRITICAL) |
| `--max-tentativas-download` | int | **Número máximo de tentativas para download (padrão: 3)** |
| `--tempo-espera-download` | int | **Tempo base de espera entre tentativas em segundos (padrão: 5)** |
| `--auto-redownload` | flag | **Habilita re-download automático de arquivos corrompidos (padrão: ativo)** |
| `--verificar-7z` | flag | **Executa verificação preventiva de integridade de arquivos 7z** |
| `--max-anos-paralelos` | int | **Número máximo de anos processados simultaneamente (padrão: 2)** |

## 🔍 Filtros Automáticos

O sistema automaticamente exclui arquivos que contenham:
- **_EST**: Dados de estabelecimentos
- **ESTB**: Dados de estabelecimentos
- **IGN**: Dados ignorados
- **NI**: Dados não identificados

Esta filtragem é aplicada em todas as etapas do processamento.

## 💾 Formatos de Saída

### 📁 Consolidação por Ano

**📁 Múltiplos Arquivos (Padrão)**
```bash
python main.py --modo completo
# Resultado: parquet/2020/RAIS_2020_consolidado/ (diretório)
```

**💾 Preservando Arquivos Descompactados**
```bash
python main.py --modo completo --preservar-descompactados
# Resultado: parquet/2020/RAIS_2020_consolidado/ + mantém arquivos TXT
```

### 📊 Consolidação Geral (Todos os Anos)

**📁 Consolidação Básica**
```bash
python main.py --modo consolidar-geral --sobrescrever
# Resultado: parquet/RAIS_TODOS_ANOS_consolidado/ (diretório)
```

**🔄 Consolidação Incremental**
```bash
python main.py --modo consolidar-geral --incremental
# Resultado: Adiciona apenas anos novos ao arquivo existente
```

**🗑️ Consolidação com Remoção de Anos**
```bash
python main.py --modo consolidar-geral --sobrescrever --remover-anos-pos-consolidacao
# Resultado: Consolida tudo + remove pastas de anos individuais
```

#### Estrutura de Saída

| Modo | Local | Formato | Colunas Extras | Tamanho Típico |
|------|-------|---------|----------------|----------------|
| **Ano Individual** | `parquet/2020/RAIS_2020_consolidado/` | Diretório | `ANO_RAIS` | 2-8 GB |
| **Geral Básica** | `parquet/RAIS_TODOS_ANOS_consolidado/` | Diretório | `ANO_RAIS` | 20-80 GB |
| **Geral Incremental** | `parquet/RAIS_TODOS_ANOS_consolidado/` | Diretório | `ANO_RAIS` | 20-80 GB |
| **Geral Otimizada** | `parquet/RAIS_TODOS_ANOS_consolidado/` | Diretório | `ANO_RAIS` | 20-80 GB |

## 🔧 Casos de Uso Avançados

### 🚀 Pipeline de Produção com Verificação Robusta
```bash
# 1. Verificação preventiva de integridade
python main.py --verificar-7z

# 2. Download inteligente com tolerância a erros
python main.py --modo baixar --baixar-opcao faltantes --max-tentativas-download 5

# 3. Processamento completo com verificação automática
python main.py --modo completo --sobrescrever --preservar-descompactados

# 4. Consolidação geral otimizada
python main.py --modo consolidar-geral --incremental --remover-anos-pos-consolidacao

# 5. Verificar logs para problemas (se houver)
# Consulte os arquivos de log na pasta 'logs/' para detalhes de erros
```

### 🔧 Recuperação de Erros Automatizada
```bash
# 1. Identificar problemas nos logs
# Consulte os arquivos de log na pasta 'logs/' para identificar problemas

# 2. Verificar integridade dos arquivos
python main.py --verificar-7z

# 3. Reprocessar com máxima tolerância
python main.py --modo completo --sobrescrever --max-tentativas-download 5 --tempo-espera-download 10
```

### Análise Temporal Completa
```bash
# 1. Processar anos específicos
python main.py --modo completo --anos 2018 2019 2020 2021 2022 --sobrescrever

# 2. Consolidar tudo em arquivo único para análise
python main.py --modo consolidar-geral --sobrescrever

# 3. Analisar em Python
```

```python
import pandas as pd
import matplotlib.pyplot as plt

# Carregar base completa
df = pd.read_parquet('parquet/RAIS_TODOS_ANOS_consolidado.parquet')

# Análise temporal de vínculos
vinculos_ano = df.groupby('ANO_RAIS').size()
vinculos_ano.plot(kind='bar', title='Evolução dos Vínculos RAIS')

# Análise por estado requer filtros baseados em outras colunas
# Exemplo: filtrar por município ou outras características geográficas
```

## 🆕 Estrutura de Dados Atualizada

#### Colunas de Metadados Automáticas
Todos os arquivos processados incluem:

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| `ANO_RAIS` | string | Ano extraído automaticamente | `2020` |

**📝 Colunas Otimizadas:**
- `BAIRROS_SP`, `BAIRROS_FORTALEZA`, `BAIRROS_RJ`: Removidas automaticamente para otimização
- `DISTRITOS_SP`, `REGIOES_ADM_DF`: Removidas automaticamente para otimização
- Apenas `ANO_RAIS` é adicionada como coluna de metadados

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

#### 📊 Consolidação Geral
- **RAM**: 32GB+ (recomendado para consolidação geral)
- **Armazenamento**: 100-150GB livres para consolidação de todos os anos
- **CPU**: 8+ cores para processamento eficiente

### 🔍 Monitoramento de Integridade

#### **🔍 Verificação Automática Durante Processamento**
```
📦 Extração 7z: 85%|████████▌ | 23/27 [05:42<01:02, 1.8s/arquivo] ✅21 ❌2
🔍 Verificando integridade de RAIS_VINC_PUB_SP.7z
⚠️  Arquivo RAIS_VINC_PUB_SP.7z com problema: Arquivo 7z corrompido
🔄 Iniciando re-download IMEDIATO de RAIS_VINC_PUB_SP.7z
✅ Arquivo RAIS_VINC_PUB_SP.7z re-baixado e verificado com sucesso
```

#### **🔄 Sistema de Tolerância a Erros**
```
📥 Re-baixando RAIS_VINC_PUB_AC.7z (tentativa 2): 100%|████████| 45.2MB/45.2MB [00:23<00:00, 1.97MB/s]
Tentativa 2/3 falhou para re-download: Connection timeout
Aguardando 10s antes da próxima tentativa...
```

## 🔍 Auditoria e Relatórios

### 📋 Relatório de Problemas
O sistema possui controle robusto de erros que registra automaticamente problemas durante o processamento:

#### **--relatorio-problemas**: Análise de Logs
```bash
# Gerar relatório consolidado dos problemas encontrados
python main.py --relatorio-problemas
```

#### **--verificar-7z**: Verificação Preventiva
```bash
# Verificar integridade de todos os arquivos 7z antes do processamento
python main.py --verificar-7z
```

### 🔄 Re-download Automático de Arquivos Corrompidos

O sistema agora inclui **verificação automática de integridade** durante a descompactação:

#### **--auto-redownload**: Correção Automática (Padrão: Ativo)
- **🔍 Verificação automática**: Testa integridade de cada arquivo 7z antes da descompactação
- **🔄 Re-download inteligente**: Baixa novamente arquivos corrompidos do FTP automaticamente
- **✅ Verificação pós-download**: Confirma que o arquivo re-baixado está íntegro
- **📝 Registro de erros**: Documenta tentativas de correção e falhas persistentes

#### **Tolerância a Erros de Download**
- **🔄 Múltiplas tentativas**: Sistema resiliente com 3 tentativas por arquivo (configurável)
- **⏱️ Backoff exponencial**: Tempo de espera crescente entre tentativas (5s, 10s, 15s)
- **🧹 Limpeza automática**: Remove arquivos parciais após falhas
- **📊 Validação rigorosa**: Verifica tamanho e integridade após cada download

#### Funcionalidades de Verificação:
- **Detecção de corrupção**: Identifica arquivos 7z corrompidos ou vazios
- **Validação de conteúdo**: Verifica se há arquivos TXT válidos no arquivo compactado
- **Teste de integridade**: Executa teste completo de integridade do arquivo 7z
- **Verificação de tamanho**: Detecta arquivos muito pequenos ou vazios
- **Recuperação automática**: Tenta corrigir problemas baixando novamente do FTP
- **Fallback inteligente**: Registra erros se o re-download também falhar

### 🔄 Verificação Automática de Sincronização

#### **--baixar-opcao faltantes**: Sincronização Inteligente (Padrão)
- **📊 Comparação automática**: Verifica todos os arquivos remotos vs locais automaticamente
- **📏 Validação de tamanho**: Compara tamanhos de arquivos byte por byte
- **📊 Verificação de quantidade**: Compara número de arquivos locais vs remotos
- **🗑️ Detecção de órfãos**: Identifica arquivos locais que não existem no FTP
- **🔄 Download inteligente**: Baixa apenas arquivos faltantes ou diferentes
- **📋 Relatório detalhado**: Mostra quais arquivos precisam ser baixados e por quê

## 🔄 Migração de Versões Anteriores

Se você já possui arquivos processados sem a coluna `ANO_RAIS`:

```bash
# Reprocessar apenas a conversão (adiciona ANO_RAIS automaticamente)
python main.py --modo converter --sobrescrever

# Ou reprocessar a consolidação (adiciona ANO_RAIS se não existir)
python main.py --modo consolidar --sobrescrever

# Criar base geral com dados antigos e novos
python main.py --modo consolidar-geral --sobrescrever
```

## 📞 Suporte e Documentação

### Troubleshooting
```bash
# Verificar estrutura de arquivos
python main.py --listar

# Modo debug detalhado
python main.py --modo completo --log-level DEBUG

# Teste com arquivo limitado
python main.py --modo completo --max 1 --sobrescrever

# Verificação preventiva de integridade
python main.py --verificar-7z

# Consultar logs para problemas
# Verifique os arquivos de log na pasta 'logs/' para detalhes de erros
```

### Logs e Auditoria
- **Arquivo**: Todos os detalhes em `logs/rais_YYYY_MM_DD.log`
- **Console**: Configurável via `--log-level`
- **Progressão**: Barras de progresso em tempo real
- **Tempos**: Cronômetro automático por etapa
- **Relatórios de verificação**: Arquivos TXT com análise de integridade
- **Sistema de controle de erros**: Rastreamento thread-safe de erros por ano e etapa

### Erros Comuns e Soluções
- **Memória insuficiente**: Reduza `--workers-convert` ou use `--preservar-descompactados`
- **Espaço em disco**: Monitore espaço livre, especialmente para consolidação geral
- **Arquivos corrompidos**: Sistema detecta e corrige automaticamente via re-download
- **Performance lenta**: Ajuste workers manualmente ou use SSD
- **Falhas de download**: Configure `--max-tentativas-download` e `--tempo-espera-download`
- **Problemas de integridade**: Execute `--verificar-7z` antes do processamento

### Reportar Problemas
- Consulte a documentação oficial da RAIS
- Verifique os logs de erro para troubleshooting
- Consulte os logs na pasta 'logs/' para análise detalhada de erros
- Inclua informações do sistema (OS, Python, RAM) ao reportar bugs

## 🔗 Links Úteis

- [RAIS - Ministério do Trabalho](http://www.rais.gov.br/)
- [FTP Oficial](ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/)
- [Layout dos Dados](http://www.rais.gov.br/sitio/download.jsf)

## 🔧 Funcionalidades Técnicas Avançadas

### 🔧 **Sistema de Controle de Erros Thread-Safe**
- **Classe**: `ControladorErros` - Rastreamento automático de erros por ano e etapa
- **Funcionalidade**: Registra automaticamente erros durante download, descompactação, conversão e consolidação
- **Thread-Safe**: Funciona corretamente em processamento paralelo
- **Logging**: Todos os erros são registrados automaticamente nos logs

### 🚀 **Processamento Pipeline Otimizado**
- **Função**: `processar_sequencial_pipeline_otimizado()` - Usado no modo `--completo`
- **Funcionalidade**: Pipeline assíncrono interno que processa download → descompactar → converter → consolidar
- **Paralelismo**: Usa `ProcessPoolExecutor` para processamento multi-processo real
- **Economia**: Processa um ano por vez, liberando recursos antes do próximo

### ⚙️ **Configuração Automática de Workers**
- **Função**: `calcular_workers_otimizados()` - Detecta recursos da máquina automaticamente
- **Algoritmo**: Baseado em CPU cores, RAM disponível e tipo de operação
- **Otimização**: Ajusta workers para download, descompactação e conversão separadamente

### 🕒 **Sistema de Monitoramento de Tempo**
- **Funções**: `iniciar_tempo()`, `finalizar_tempo()`, `mostrar_resumo_tempos()`
- **Funcionalidade**: Cronômetro automático para cada etapa do processamento
- **Relatório**: Mostra resumo completo de tempos ao final da execução

### 🗑️ **Remoção Automática de Colunas Desnecessárias**
- **Colunas removidas**: `BAIRROS_SP`, `BAIRROS_FORTALEZA`, `BAIRROS_RJ`, `DISTRITOS_SP`, `REGIOES_ADM_DF`
- **Funcionalidade**: Remoção automática antes da conversão para economizar processamento
- **Otimização**: Reduz significativamente o tamanho dos arquivos finais

### 📊 **Configuração Global de Tolerância a Erros**
- **Variável**: `config_tolerancia` - Configuração centralizada para todas as operações
- **Parâmetros**: `max_tentativas`, `tempo_espera_base`
- **Funcionalidade**: Controla comportamento de retry em downloads e re-downloads

## 🚀 Comandos Práticos das Novas Funcionalidades

### 💡 Resposta às Suas Perguntas

#### 1. **Comando para Processar Todos os Arquivos em uma Única Pasta Consolidada**
```bash
python main.py --modo consolidar-geral --sobrescrever
```
**Resultado**: Cria `parquet/RAIS_TODOS_ANOS_consolidado/` com todos os anos unidos.

#### 2. **Sistema Capaz de Remover Anos Individuais Após Consolidação**
```bash
# ✅ SIM! Use o parâmetro --remover-anos-pos-consolidacao
python main.py --modo consolidar-geral --sobrescrever --remover-anos-pos-consolidacao
```
**Resultado**: 
- Consolida todos os anos em uma pasta
- Remove automaticamente as pastas individuais (2015/, 2016/, etc.)
- Economiza ~50-70% do espaço em disco

#### 3. **Sistema Capaz de Preservar Arquivos Descompactados**
```bash
# ✅ SIM! Use o parâmetro --preservar-descompactados
python main.py --modo completo --sobrescrever --preservar-descompactados
```
**Resultado**:
- Mantém arquivos TXT descompactados para testes
- Remove apenas Parquets intermediários 
- Evita retrabalho de descompactação

#### 4. **🆕 Sistema com Verificação Automática de Integridade**
```bash
# ✅ SIM! Verificação automática habilitada por padrão
python main.py --modo completo --sobrescrever
```
**Resultado**:
- Detecta arquivos corrompidos automaticamente
- Re-baixa arquivos corrompidos imediatamente
- Verifica integridade após re-download
- Registra erros persistentes para análise

#### 5. **🆕 Sistema com Sincronização Inteligente**
```bash
# ✅ SIM! Verificação de sincronização automática
python main.py --modo baixar --baixar-opcao faltantes
```
**Resultado**:
- Compara quantidade de arquivos locais vs remotos
- Detecta arquivos órfãos (locais que não existem no FTP)
- Baixa apenas arquivos faltantes ou diferentes
- Mostra relatório detalhado de divergências

### 🔄 Funcionalidades Implementadas

#### **📈 Modo Incremental**
```bash
# Primeira execução: consolida tudo
python main.py --modo consolidar-geral --sobrescrever

# Execuções futuras: adiciona apenas anos novos
python main.py --modo consolidar-geral --incremental
```
**Vantagem**: Processa apenas dados novos, economizando tempo.

#### **💾 Gerenciamento Inteligente de Espaço**
```bash
# Pipeline otimizado para produção
python main.py --modo completo --preservar-descompactados
python main.py --modo consolidar-geral --incremental --remover-anos-pos-consolidacao
```

#### **🔍 Verificação e Correção Automática**
```bash
# Pipeline robusto com verificação completa
python main.py --verificar-7z
python main.py --modo completo --sobrescrever --max-tentativas-download 5
# Consulte os logs na pasta 'logs/' para verificar se houve problemas
```

### 📊 Comparação de Estratégias

| Estratégia | Comando | Espaço em Disco | Tempo | Confiabilidade |
|------------|---------|------------------|-------|----------------|
| **Conservadora** | `--preservar` | 🔴 Alto (3x) | 🟢 Rápido | 🟢 Máxima |
| **Balanceada** | `--preservar-descompactados` | 🟡 Médio (2x) | 🟡 Médio | 🟢 Alta |
| **Otimizada** | `--remover-anos-pos-consolidacao` | 🟢 Baixo (1x) | 🔴 Lento | 🟢 Alta |
| **🆕 Robusta** | `--max-tentativas-download 5` | 🟡 Médio | 🟡 Médio | 🟢 Máxima |

### 🚀 **Modo Recomendado: Pipeline Robusto**

#### **⚡ Processamento Robusto com Verificação (RECOMENDADO)**
```bash
# Modo completo com verificação automática de integridade
python main.py --modo completo --sobrescrever --preservar-descompactados --max-tentativas-download 5

# Com múltiplos anos em paralelo (máquinas potentes)
python main.py --modo completo --sobrescrever --max-anos-paralelos 3 --max-tentativas-download 5
```

**Vantagens do modo robusto:**
- ✅ **Detecção automática**: Identifica arquivos corrompidos durante processamento
- 🔄 **Correção imediata**: Re-baixa arquivos corrompidos automaticamente
- 🛡️ **Tolerância a erros**: Múltiplas tentativas com backoff exponencial
- 📊 **Verificação completa**: Compara arquivos locais vs remotos
- 💾 **Economia de espaço**: Não acumula TODOS os TXT antes de converter
- ⚡ **Mais eficiente**: Paralelismo entre I/O e CPU
- 🔄 **Progressão clara**: Cada ano é finalizado antes do próximo

### 🎯 Casos de Uso Recomendados

#### **💻 Ambiente de Desenvolvimento/Testes**
```bash
python main.py --modo completo --preservar-descompactados --anos 2020 2021 --max-tentativas-download 3
```

#### **🏭 Ambiente de Produção (Máxima Robustez)**
```bash
# Verificação preventiva
python main.py --verificar-7z

# Pipeline robusto por ano
python main.py --modo completo --sobrescrever --preservar-descompactados --max-tentativas-download 5

# Consolidação geral final
python main.py --modo consolidar-geral --incremental --remover-anos-pos-consolidacao

# Relatório final
python main.py --relatorio-problemas
```

#### **📅 Atualização Mensal (Automatizada)**
```bash
# Pipeline completo automatizado
python main.py --modo baixar --baixar-opcao faltantes --max-tentativas-download 5
python main.py --modo completo --preservar-descompactados
python main.py --modo consolidar-geral --incremental
# Consulte os logs na pasta 'logs/' para verificar se houve problemas
```

## 📋 Orientações Técnicas de Consolidação

### 📋 **Conformidade com Manual RAIS 2024**

A funcionalidade de consolidação geral foi desenvolvida seguindo **orientações técnicas rigorosas**:

#### ✅ **Consolidação Aprovada - Justificativas Técnicas**

**1. Compatibilidade de Schema**
- Todos os arquivos de vínculos RAIS possuem estrutura **idêntica**
- 60+ campos padronizados mantidos desde 2006
- Layout oficial garante homogeneidade dos dados

**2. Filtragem Automática Obrigatória**
```bash
# Exclusões automáticas (conforme orientações MTPS):
- Arquivos EST/ESTB: Estabelecimentos (estrutura diferente)
- Arquivos NI: Não identificados (dados incompletos)
- Arquivos IGN: Dados ignorados
```

**3. Rastreabilidade Completa**
- `ANO_RAIS`: Identificação temporal única para cada registro
- `fonte_arquivo`: Origem geográfica (estado) preservada
- Metadados enriquecidos para auditoria completa

**4. Integridade dos Dados**
- ✅ Preservação completa da informação original
- ✅ Zero perda de dados durante consolidação
- ✅ Adição apenas de metadados de controle
- ✅ **Verificação automática de integridade**

**5. Casos de Uso Aprovados**
- 📊 Análises temporais de séries históricas
- 🗺️ Comparações inter-regionais
- 📈 Estudos longitudinais de mercado de trabalho
- ⚡ Processamento analítico otimizado (OLAP)

#### 🚫 **Limitações Respeitadas**

**Exclusões Obrigatórias** (implementadas automaticamente):
- **EST/ESTB**: Arquivos de estabelecimentos têm estrutura diferente
- **NI**: Dados não identificados são incompletos por definição
- **IGN**: Dados ignorados conforme orientações técnicas
- **Layout incompatível**: Apenas vínculos são consolidados

#### 📊 **Resultado da Consolidação**

```python
# Estrutura final consolidada:
RAIS_TODOS_ANOS_consolidado.parquet
├── Todos os campos originais da RAIS (60+ colunas)
├── ANO_RAIS: "2015", "2016", ..., "2024"
├── fonte_arquivo: "RAIS_2020_AC", "RAIS_2021_SP", etc.
└── Índices otimizados para consultas temporais
```

#### ⚡ **Performance e Escalabilidade**

| Aspecto | Especificação | Justificativa |
|---------|---------------|---------------|
| **Tamanho** | 20-80 GB (todos os anos) | Compressão Snappy otimizada |
| **Consultas** | 10-100x mais rápidas | Formato colunar Parquet |
| **Memória** | Lazy loading com Dask | Processa datasets maiores que RAM |
| **Compatibilidade** | Pandas, Spark, R, SQL | Formato padrão analytics |
| **Integridade** | Verificação automática | Detecção e correção de problemas |
