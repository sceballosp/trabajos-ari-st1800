select temperatura.fecha, temperatura.temperatura, humedad.humedad, temperatura.departamento, temperatura.municipio
from "clima-colombia".temperatura
join "clima-colombia".humedad
on temperatura.fecha = humedad.fecha
and temperatura.codigo_estacion = humedad.codigo_estacion
limit 10;


select count(*)
from "clima-colombia".temperatura
join "clima-colombia".humedad
on temperatura.fecha = humedad.fecha
and temperatura.codigo_estacion = humedad.codigo_estacion
where temperatura.temperatura > 23
and humedad.humedad > 60;