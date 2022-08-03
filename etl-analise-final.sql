/*Processo de ETL com SQL
Criação de campanha para clientes estimulando novos empréstimos para Empresa Financeira
Quais sãos os 1000 melhores clientes que podemos oferecer empréstimo para a ação da próxima semana*/

/*1 - EXTRACT: IMPORTAÇÃO DE DADOS*/

/*Análise prévia de como estão a padronização dos dados*/

SELECT * from pessoas LIMIT 5;
SELECT * from historico_emp LIMIT 5;

/*Juntar as planilhas com LEFT JOIN*/
SELECT *
FROM pessoas P
LEFT JOIN historico_emp H
ON P.ID = H.ID;

/*Criar uma Tabela Temporária.
Não pode ter o mesmo nome com colunas diferentes. 
É mais simples criar uma tabela temporária somente com os dados necessários.
Não fazer alterações diretamente na fonte*/

CREATE TEMPORARY TABLE analise_temp as
SELECT P.*, H.motivo_emp, H.valor_emp, H.tx_emp, H.emp_ativo
FROM pessoas P
LEFT JOIN historico_emp H
ON P.ID = H.ID;

/*2 - TRANSFORM: VALIDAÇÃO DOS DADOOS*/

SELECT * FROM analise_temp LIMIT 5;

/*Analisar colunas para verificar se tem algum outlier: colunas idade e anos_trabalho_atual
menor_idade e menor_anos_trab OK
maior_idade e maior_anos_trab tem iformações com idade acima de 100 anos. NÃO CONFIAR NESSE DADO!*/

SELECT MIN(idade) menor_idade
, MAX(IDADE) maior_idade
, MIN(anos_trabalho_atual) menor_anos_trab
, MAX(anos_trabalho_atual) maior_anos_trab
FROM analise_temp;

/*Buscar informação de quais clientes eu não tenho nenhum histório de empréstimos
238 clientes sem informação de histórico de empréstimos*/

SELECT count(*)
FROM analise_temp
WHERE motivo_emp is null and valor_emp is null and tx_emp is null and emp_ativo is null;

/*Quantos clientes tem sem dado de contatos?
25805 clientes não tem dado de contato*/

select count(*) /*conta linha a linha, quantidade de linhas*/
from analise
where dados_contato = 0;

/*Idades acima de 60 não é o ideal para a campanha já que a Financeira orientou a não trabalhar com aposentados
INFORMAÇÃO DE DADOS QUENTE: 5520 clientes com pelo menos um contato e com idade entre 18 e 60*/

select count(*) 
from analise
where dados_contato >= 1
and idade between 18 and 60;

select idade, case when idade > 60 then 'NÃO IDEAL PARA ESSA AÇÃO' ELSE 'IDEAL' END status_cliente
from analise
where dados_contato = 1;

/*Ver quais clientes não está com a renda impactada, ou seja, que tenha a renda comprometia (emp_ativo) em relação a renda_ano
Tratar informação NULL como se tivesse um empréstimo ativo, pois, já que não tem informação, melhor considerar que há empréstimo 
30766 clientes tem menos de 40% da renda comprometida e sem empréstimo ou menos de 20% da renda comprometida */

select count(*) 
from analise_temp
where 
(
	(valor_emp / renda_ano) <= 0.4 
    and emp_ativo = 0
) 
or
(
	(valor_emp / renda_ano) <= 0.2 
    and (emp_ativo = 1 or emp_ativo is null)
);

/*3 - TRANSFORM: RECONSTRUÇÃO E PADRONIZAÇÃO DOS DADOS*/

/*Troca de Valores com UPDATE e SUBQUERY
Clientes com idade vazia trocar pela média de idade dentro do range de 18 a 60 anos 
*/

UPDATE analise_temp
SET idade = (SELECT AVG(idade) FROM analise_temp WHERE idade BETWEEN 18 AND 60)
WHERE idade IS NULL;

UPDATE analise_temp
SET anos_trabalho_atual = (SELECT AVG(anos_trabalho_atual) 
                           FROM analise_temp 
                           WHERE anos_trabalho_atual <=30)
WHERE anos_trabalho_atual IS NULL;

/*Verificando se ainda tem NULL*/

SELECT COUNT(idade) AS I, COUNT(anos_trabalho_atual) AS A
FROM analise_temp 
WHERE idade IS NULL OR anos_trabalho_atual IS NULL;

/*DELETE: apagar os dados que realmente não faz sentido mantê-los
Deletar dados não confiáveis: clientes que tem idade acima de 99 anos 
e os que tem mais de 40 anos de trabalho*/

DELETE FROM analise_temp
WHERE anos_trabalho_atual > 40 OR idade > 99

/*4 - LOAD: REMOÇÃO DE DUPLICADOS E ENTREGA DIFERENCIADA
Utilizar CASE WHEN para selecionar/fazer marcação dos clientes ideais para a campanha*/

SELECT COUNT(*)
FROM 
(SELECT *
, CASE WHEN dados_contato = 1 AND anos_hist_credito >= 2 THEN 1 ELSE 0 END contato_hist_credito
, CASE WHEN ((valor_emp / renda_ano) <= 0.4 and emp_ativo = 0)
			or ((valor_emp / renda_ano) <= 0.2 and (emp_ativo = 1 or emp_ativo is null))
  THEN 1 ELSE 0 END impacto_fin_ok
, CASE WHEN idade BETWEEN 18 AND 60 AND anos_trabalho_atual >= 3 THEN 1 ELSE 0 END idade_tempo_trab
FROM analise_temp)
WHERE contato_hist_credito = 1 AND impacto_fin_ok = 1  AND idade_tempo_trab = 1;

/*Garantir que na saída não tem informação duplicada pela coluna ID*/

SELECT id, COUNT(*)
FROM 
(SELECT *
, CASE WHEN dados_contato = 1 AND anos_hist_credito >= 2 THEN 1 ELSE 0 END contato_hist_credito
, CASE WHEN ((valor_emp / renda_ano) <= 0.4 and emp_ativo = 0)
			or ((valor_emp / renda_ano) <= 0.2 and (emp_ativo = 1 or emp_ativo is null))
  THEN 1 ELSE 0 END impacto_fin_ok
, CASE WHEN idade BETWEEN 18 AND 60 AND anos_trabalho_atual >= 3 THEN 1 ELSE 0 END idade_tempo_trab
FROM analise_temp)
-- WHERE contato_hist_credito = 1 AND impacto_fin_ok = 1  AND idade_tempo_trab = 1
-- Comentário acima feito para ver a tabela inteira e não só o WHERE
GROUP BY id
ORDER BY 2 DESC;

/*Criar tabela final*/
CREATE TABLE analise_final as
SELECT *
FROM 
(SELECT *
, CASE WHEN dados_contato = 1 AND anos_hist_credito >= 2 THEN 1 ELSE 0 END contato_hist_credito
, CASE WHEN ((valor_emp / renda_ano) <= 0.4 and emp_ativo = 0)
			or ((valor_emp / renda_ano) <= 0.2 and (emp_ativo = 1 or emp_ativo is null))
  THEN 1 ELSE 0 END impacto_fin_ok
, CASE WHEN idade BETWEEN 18 AND 60 AND anos_trabalho_atual >= 3 THEN 1 ELSE 0 END idade_tempo_trab
FROM analise_temp)
-- WHERE contato_hist_credito = 1 AND impacto_fin_ok = 1  AND idade_tempo_trab = 1
-- Comentário acima feito para ver a tabela inteira e não só o WHERE
-- GROUP BY id
-- ORDER BY 2 DESC;




