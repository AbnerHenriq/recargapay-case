CASE - Recarga Pay

## Descrição

Este projeto é um pipeline de ETL, utilizando o Spark para processar os dados. Ele é composto por três etapas:

1. Bronze
2. Silver
    - Diversas correções foram aplicadas nas colunas, como: telefone, estado, genero, etc.
3. Gold
    - Enriquecimento da dim_users com informações de transações (tpv_lifetime, ticket médio)
4. Data Quality

O projeto possui raw, sendo inserida manualmente dentro do Databricks.
Bronze sendo uma camada 1=1 com a raw.
Silver contendo todas as transformações e cleaning da base. 
E gold aplicando modelagem dimensional para criar uma `dim_users` e uma `fct_transactions`. 

O projeto possui testes de data quality utilizando o Great Expectations, com o objetivo de fazer três testes básicos: `not_null`, `unique` e `accepted values`. 

![Arquitetura do Pipeline](Screenshot%202025-08-24%20at%2022.52.55.png)

O pipeline deve funcionar da seguinte forma: rodar sequencialmente todas as layers, e ao final, caso algum dos testes falhem, o job de teste deverá retornar erro e gerar um alerta - os pipelines de transformações devem rodar independentemente de algum teste falhar.


Para esse projeto, quis trabalhar com as seguintes métricas:

| Métrica | Cálculo (Tableau/Pseudo-SQL) | Motivo da Importância |
|---------|------------------------------|----------------------|
| **TPV (Total Payment Volume)** | `SUM([product_amount])` | Mede o volume bruto de transações movimentadas na plataforma (saúde geral do negócio). |
| **Interchange (Transaction Fee)** | `SUM([transaction_fee])` | Receita direta da operação, representa quanto a empresa ganha em taxas. |
| **Cashback** | `SUM([cashback])` | Mede o gasto da empresa em incentivos para atrair/reter clientes. |
| **Loyalty Points** | `SUM([loyalty_points])` | Indica o valor distribuído no programa de fidelidade (engajamento do cliente). |
| **Net Revenue** | `SUM([transaction_fee]) - SUM([cashback])` | Lucro líquido das operações, já considerando o custo dos incentivos. |
| **Cashback/Fee Ratio** | `SUM([cashback]) / SUM([transaction_fee])` | Mede sustentabilidade: quanto do fee está sendo "comido" pelo cashback. Se >100%, prejuízo. |
| **Cashback/TPV Ratio** | `SUM([cashback]) / SUM([product_amount])` | Indica a proporção do volume que foi devolvido em cashback (eficiência do incentivo). |
| **% Transações Negativas** | `COUNT(IF [net_revenue] < 0 THEN 1 END) / COUNT([transaction_id])` | Percentual de transações que deram prejuízo → risco de perda de margem. |
| **Qtd. Usuários Ativos** | `COUNTD([user_id])` com transação > 0 | Mede a base de clientes realmente engajados. |
| **Ticket Médio** | `SUM([product_amount]) / COUNTD([user_id])` | Mostra o gasto médio por usuário → indicador de poder aquisitivo e engajamento. |
| **% Novos Usuários** | `COUNTD([user_id] WHERE [activation_date] = mês atual) / COUNTD([user_id])` | Mede aquisição de clientes e crescimento da base. |
| **Distribuição por Idade** | `DATEDIFF('year', [birth_date], TODAY())` | Identifica o perfil etário predominante (jovens vs. adultos vs. sêniores). |
| **Distribuição por Faixa de Renda** | `COUNT([user_id]) GROUP BY [income_range]` | Mostra qual público econômico é mais representativo. |


#### Storytelling

Utilizando Tableau, o plano era desenvolver uma história em 5 partes:
- Exec Dashboard
    - Visão rápida da performance do negócio
- Financeiro
    - Monitoramento de receita e custos
- Incentivos (Cashback + Loyalty)
    - Entender impacto da efeciência dos incentivos
- Clientes 
    - Analisar o perfil da base de usuários
- Riscos $ Fraude 
    - Identificar possíveis perdas ou distorções 

‼️ Infelizmente, eu apenas consegui trabalhar no case por 3 dias. Existe uma boa parte do que eu queria desenvolver que não foi possível. Decidi entregar com o que já foi construído dado o limite do tempo, mas estou a disposição para continuar o desenvolvimento caso necessário ou conversar sobre futuras melhorias que eu iria implementar.

## Dashboards Tableau

### Exec Dash
![Dashboard Executivo](Screenshot%202025-08-24%20at%2023.03.29.png)

### Incentivos
![Dashboard Financeiro](Screenshot%202025-08-24%20at%2023.03.44.png)

#### Principais insights 
- O modelo de Incentivos hoje está com prejuízo. O cashback é maior que a receita gerada pelas transações, gerando uma receita líquida negativa (Net Revenue)
- As categorias `Farmácia` e `Eletrônicos` são os maiores ofensores do alto % de prejuízo.
- Todos os segmentos de renda são em prejuízo. Não existe nenhum segmento de renda ofensor. Todas as classes estão aproveitando o cashback ofensivo.


#### Melhorias
Se eu tivesse mais tempo, ou o que eu faria para completar todo o desenvolvimento que acredito que ainda falta, seria:
- Implementar de maneira efetiva o `great_expectations` para testes de data quality
- Finalizar a implementação de todos os dashboards que ainda não foram concluídos 