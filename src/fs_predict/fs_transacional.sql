

WITH tb_transacao AS (

    SELECT  *, substr(DtCriacao,0,11) AS dtDia

    FROM transacoes

    WHERE DtCriacao < '2025-10-01'

), 

tb_agg_transacao AS (

    SELECT  idCliente, 

            -- Quantidade de dias que teve alguma ação
            COUNT(DISTINCT dtDia) AS qtdeAtivacaoVida,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-7 day') THEN dtDia END) AS qtdeAtivacaoD7,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-14 day') THEN dtDia END) AS qtdeAtivacaoD14,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-28 day') THEN dtDia END) AS qtdeAtivacaoD28,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-56 day') THEN dtDia END) AS qtdeAtivacaoD56,
            
            -- Quantidade de transações
            COUNT(DISTINCT IdTransacao) AS qtdeTransacaoVida,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-7 day') THEN IdTransacao END) AS qtdeTransacaoD7,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-14 day') THEN IdTransacao END) AS qtdeTransacaoD14,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-28 day') THEN IdTransacao END) AS qtdeTransacaoD28,
            COUNT(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-56 day') THEN IdTransacao END) AS qtdeTransacaoD56,

            -- Valor dos pontos (Saldo)
            SUM(QtdePontos) AS saldoVida,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-7 day') THEN QtdePontos ELSE 0 END) AS saldoD7,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-14 day') THEN QtdePontos ELSE 0 END) AS saldoD14,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-28 day') THEN QtdePontos ELSE 0 END) AS saldoD28,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-56 day') THEN QtdePontos ELSE 0 END) AS saldoD56,
            -- Valor dos pontos (Negativo)
            SUM(CASE WHEN QtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegVida,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-7 day') AND QtdePontos < 0 THEN QtdePontos ELSE 0 END) AS qtdePontosNegD7,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-14 day') AND QtdePontos < 0 THEN QtdePontos ELSE 0 END) AS qtdePontosNegD14,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-28 day') AND QtdePontos < 0 THEN QtdePontos ELSE 0 END) AS qtdePontosNegD28,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-56 day') AND QtdePontos < 0 THEN QtdePontos ELSE 0 END) AS qtdePontosNegD56,
            -- Valor dos pontos (Positivos)
            SUM(CASE WHEN QtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPosVida,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-7 day') AND QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosPosD7,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-14 day') AND QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosPosD14,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-28 day') AND QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosPosD28,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-56 day') AND QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosPosD56

    FROM tb_transacao

    GROUP BY IdCliente

), 

tb_agg_calc AS (

    SELECT  *,

            -- Quantidade de transações por dia
            COALESCE(1.0 * qtdeTransacaoVida / qtdeAtivacaoVida,0) AS QtdeTransacoesDiaVida,
            COALESCE(1.0 * qtdeTransacaoD7 / qtdeAtivacaoD7,0) AS QtdeTransacoesDiaD7,
            COALESCE(1.0 * qtdeTransacaoD14 / qtdeAtivacaoD14,0) AS QtdeTransacoesDiaD14,
            COALESCE(1.0 * qtdeTransacaoD28 / qtdeAtivacaoD28,0) AS QtdeTransacoesDiaD28,
            COALESCE(1.0 * qtdeTransacaoD56 / qtdeAtivacaoD56,0) AS QtdeTransacoesDiaD56,

            -- Percentual de ativação do MAU
            COALESCE(1.0 * qtdeAtivacaoD28 / 28,0) AS pctAtivacaoMAU


    FROM tb_agg_transacao

), 

tb_horas_dias AS (

    SELECT idCliente,
        dtDia,
        24 * (MAX(julianday(dtCriacao)) - MIN(julianday(dtCriacao))) AS duracao


    FROM tb_transacao

    GROUP BY idCliente, dtDia

),

tb_hora_cliente AS (

    SELECT  IdCliente,
            -- Horas assistidas (D7, D14, D28, D56)
            sum(duracao) AS qtdeHorasVida,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-7 day') THEN duracao ELSE 0 END) qtdeHorasD7,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-14 day') THEN duracao ELSE 0 END) qtdeHorasD14,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-28 day') THEN duracao ELSE 0 END) qtdeHorasD28,
            SUM(CASE WHEN dtDia >= date('2025-10-01', '-56 day') THEN duracao ELSE 0 END) qtdeHorasD56

    FROM tb_horas_dias

    GROUP BY idCliente

),

tb_lag_dia AS (

    SELECT  IdCliente,
            dtDia,
            LAG(dtDia, 1) OVER (PARTITION BY idCliente ORDER BY dtDia) as lagDia


    FROM tb_horas_dias

),

tb_intervalo AS (

    SELECT idCliente, 
        -- Média entre os dias de ativação (Tem que ser NULL pois se for 0 significa que o cara volta sempre!)
        -- No modelo iremos colocar o limite maximo que encontrar na base (o cara n voltou até tal dia)
        avg(julianday(dtDia) - julianday(lagDia)) AS avgIntervaloDiasVida,
        avg(CASE WHEN dtDia >= date('2025-10-01', '-28 day') THEN julianday(dtDia) - julianday(lagDia) END) AS avgIntervaloDiasD28
        

    FROM tb_lag_dia
    
    GROUP BY idCliente

)

SELECT  t1.*,
        t2.qtdeHorasVida,
        t2.qtdeHorasD7,
        t2.qtdeHorasD14,
        t2.qtdeHorasD28,
        t2.qtdeHorasD56,
        t3.avgIntervaloDiasVida,
        t3.avgIntervaloDiasD28

FROM tb_agg_calc AS t1
LEFT JOIN tb_hora_cliente AS t2
ON T1.IdCliente = T2.IdCliente
LEFT JOIN tb_intervalo AS T3
ON T1.IdCliente = T3.IdCliente
