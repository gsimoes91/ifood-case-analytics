## iFood Analytics Case

Este repositório contém a solução técnica e estratégica para o desafio de Data Analytics, focado na análise de um Teste A/B de campanhas de incentivo (cupom).

O projeto utiliza **PySpark** para processamento de dados massivos e **Poetry** para gerenciamento de dependências, garantindo um ambiente reprodutível e isolado.

---

## Estrutura do Projeto
```text
ifood-case-analytics/
├── data/                      # Dados brutos e processados (ignorados no git)
├── logs/                      # Dados de log (ignorados no git)
├── notebooks/                 # Notebooks de análise (Jupyter)
│   ├── 01_exploracao_ab.ipynb # Cálculos de KPI, Lift, ROI e validação estatística
│   └── 02_segmentacao.ipynb   # Análise de perfis (Lealdade, Geo, Device) e conclusão
├── reports/                   # Pasta para armazenar o relatorio final
│   └── Relatorio Final.pdf
├── src/                       # Código fonte (ETL e funções auxiliares)
│   ├── config.py              # Configurações globais e caminhos de arquivos
│   ├── load_data.py           # Módulo de ingestão de dados
│   ├── transform.py           # Módulo de tratamento de dados
│   └── utils.py               # Funções utilitárias (Logging, Setups)
├── .gitignore                 # Arquivos ignorados
├── README.md                  # Documentação do projeto
├── main.py                    # Script principal de orquestração do pipeline
├── pyproject.toml             # Definição de dependências (Poetry)
└── README.md                  # Documentação do projeto
```

## Requisitos de Sistema

Para executar este projeto, você precisará de:
* **Python 3.10+**
* **Poetry** (Gerenciador de dependências)
* **Java 8 ou 11** (Necessário para rodar o Apache Spark localmente)

---

## Instalação e Configuração

Siga os passos abaixo para configurar o ambiente virtual e instalar as bibliotecas necessárias.

### 1. Clonar o Repositório
```bash
git clone https://github.com/gsimoes91/ifood-case-analytics.git
cd ifood-case-analytics
```

### 2. Instalar Dependências (Via Poetry)
Este projeto utiliza o arquivo pyproject.toml para travar as versões das bibliotecas.

```bash
# Instala todas as dependências listadas (PySpark, Pandas, Loguru, etc.)
poetry install
```

## Como Executar o Projeto
A execução deve seguir a ordem lógica abaixo para garantir que os dados sejam processados e as tabelas geradas corretamente.

Passo 1: Processamento de Dados (ETL)  
Execute o script principal. Este passo é obrigatório, pois ele:

* Ingere os dados brutos (CSV/JSON).
* Realiza o tratamento e limpeza.
* Cria as pastas de data/processed necessárias para os notebooks.

```bash
# Certifique-se de estar dentro do ambiente
poetry run python main.py

(Aguarde até visualizar a mensagem de "Pipeline executado com sucesso" nos logs).
```

Passo 2: Análises Exploratórias e KPIs

Abra o Jupyter Notebook ou Lab:

```bash
poetry run jupyter lab

Caso o jupyter lab não inicie no navegador, basta copiar o link do terminal que inicia com localhost, algo como:
http://localhost:8888/lab?token=7aab7f20e4a95010e9079cdff3a2b3529f94e5d9b50dd7af
```

Execute o arquivo: notebooks/01_exploracao_ab.ipynb

Contém o cálculo do Lift, significância estatística, validação de safra e métricas financeiras (ROI).

Passo 3: Segmentação e Recomendação Final  

Execute o arquivo: notebooks/02_segmentacao.ipynb

Contém as análises de perfil (Lealdade, Geografia, Device), comportamento do usuário e a carta final com recomendações estratégicas.

## Resumo dos Resultados (Highlights)
A análise do teste A/B revelou os seguintes insights estratégicos:

Lift de Frequência: A campanha gerou um aumento real de +13,57% na frequência de compra.

Retenção: Houve uma redução de 10 p.p. na base de usuários de "risco" (1 pedido), promovendo migração para perfis fiéis.

Financeiro: O ticket médio manteve-se estagnado, resultando em um ROI marginal negativo de curto prazo.

Recomendação: Implementação de Smart Pricing (Cupons com gatilho de valor mínimo) para alavancar a rentabilidade.

## Autor: Gabriel Simões de Souza
