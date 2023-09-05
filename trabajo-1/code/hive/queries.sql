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


create external table temperatura(
codigo_estacion bigint,
fecha string,
temperatura float,
nombre_estacion string,
departamento string,
municipio string,
zona string)
row format delimited
fields terminated by '\t'
stored as parquet
location 's3://clima-colombia/trusted-data/temperatura';


select temperatura.fecha, temperatura.temperatura, humedad.humedad, temperatura.departamento, temperatura.municipio
from clima_colombia_hive.temperatura
join clima_colombia_hive.humedad
on temperatura.fecha = humedad.fecha
and temperatura.codigo_estacion = humedad.codigo_estacion
limit 10;


select count(*)
from clima_colombia_hive.temperatura
join clima_colombia_hive.humedad
on temperatura.fecha = humedad.fecha
and temperatura.codigo_estacion = humedad.codigo_estacion
where temperatura.temperatura > 23
and humedad.humedad > 60;