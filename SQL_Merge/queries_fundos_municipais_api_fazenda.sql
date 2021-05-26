CREATE TABLE saude
AS
  SELECT * FROM saude_2020
    UNION
  SELECT * FROM saude_2019
    UNION
  SELECT * FROM saude_2018;

SELECT * FROM as_social
  
SELECT "txtRazaoSocial" FROM saude
  
SELECT CASE "txtRazaoSocial"            
          WHEN 'SPDM - ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA' THEN 'SPDM - ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA'
          WHEN 'SPDM ASSOCIAÇÃO  PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA' THEN 'spdm - associação paulista para o desenvolvimento da medicina'
          ELSE "txtRazaoSocial"
       END AS "txtRazaoSocial"          
FROM   saude;

SELECT "txtRazaoSocial"            
          WHEN 'SPDM - ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA'
		   END AS "txtRazaoSocial"
		  FROM saude;
