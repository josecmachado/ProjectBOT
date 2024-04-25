CREATE TABLE projectbot-421014.dataset_bot.consolid_marc_lin AS
SELECT
  MARCA,
  LINHA,
  SUM(QTD_VENDA) AS Total_Vendas
FROM projectbot-421014.dataset_bot.bases-BOT 
GROUP BY MARCA, LINHA;