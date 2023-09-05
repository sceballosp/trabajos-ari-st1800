create external schema myspectrum_schema
from data catalog
database 'clima-colombia'
iam_role 'arn:aws:iam::322853454703:role/LabRole'
create external database if not exists;


create external table myspectrum_schema.temperatura(
codigo_estacion bigint,
fecha varchar,
temperatura decimal,
nombre_estacion varchar,
departamento varchar,
municipio varchar,
zona varchar)
row format delimited
fields terminated by '\t'
stored as parquet
location 's3://clima-colombia/trusted-data/temperatura/'
table properties ('numRows'='586069');


select count(*) from myspectrum_schema.temperatura;


create table humedad(
codigo_estacion bigint,
fecha varchar,
humedad DOUBLE PRECISION,
nombre_estacion varchar,
departamento varchar,
municipio varchar,
zona varchar);


copy humedad
from 's3://clima-colombia/trusted-data/humedad/'
IAM_ROLE 'arn:aws:iam::322853454703:role/LabRole'
FORMAT as parquet;


select COUNT(*) from humedad;


select humedad.fecha, myspectrum_schema.temperatura.temperatura, humedad.humedad, humedad.departamento, humedad.municipio
from myspectrum_schema.temperatura
join humedad
on myspectrum_schema.temperatura.fecha = humedad.fecha
and myspectrum_schema.temperatura.codigo_estacion = humedad.codigo_estacion
limit 10;