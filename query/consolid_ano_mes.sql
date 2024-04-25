CREATE TABLE projectbot-421014.dataset_bot.consolid_ano_mes AS
SELECT 
    EXTRACT(YEAR FROM DATA_VENDA) AS Ano,
    EXTRACT(MONTH FROM DATA_VENDA) AS Mes,
    SUM(QTD_VENDA) AS Total_Vendas
FROM projectbot-421014.dataset_bot.bases-BOT 
GROUP BY Ano, Mes
ORDER BY Mes, Ano;