SELECT dtRef, descLifeCycle,count(*) AS qtdeClientes



FROM life_cycle

WHERE descLifeCycle <> '05-ZUMBI'
AND dtRef = (SELECT MAX(dtRef) from life_cycle)

GROUP BY dtRef, descLifeCycle

