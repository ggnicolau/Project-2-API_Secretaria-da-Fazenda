SELECT * FROM as_social_2020

DELETE FROM saude_2019 WHERE "codOrgao" = 93

DELETE FROM saude_2019 WHERE "anoEmpenho" = 2020

SELECT * FROM saude_2019

SELECT "anoEmpenho" FROM as_social_2020

SELECT * FROM (
SELECT "codEmpenho", MAX ("mesEmpenho") as n
FROM as_social_2020 
GROUP BY "codEmpenho") as bla WHERE n < 12

