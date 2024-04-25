CREATE TABLE projectbot-421014.dataset_bot.consolid_Lin_Ano_Mes AS
SELECT
  LINHA,
  EXTRACT(YEAR FROM DATA_VENDA) AS Ano,
  EXTRACT(MONTH FROM DATA_VENDA) AS Mes,
  SUM(QTD_VENDA) AS Total_Vendas
FROM projectbot-421014.dataset_bot.bases-BOT 
GROUP BY LINHA, Ano, Mes;