import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://clima-colombia/raw-data/temperatura/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Change Schema
ChangeSchema_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("codigoestacion", "string", "codigo_estacion", "bigint"),
        ("fechaobservacion", "string", "fecha", "string"),
        ("valorobservado", "string", "temperatura", "float"),
        ("nombreestacion", "string", "nombre_estacion", "string"),
        ("departamento", "string", "departamento", "string"),
        ("municipio", "string", "municipio", "string"),
        ("zonahidrografica", "string", "zona", "string"),
    ],
    transformation_ctx="ChangeSchema_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://clima-colombia/trusted-data/temperatura/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="clima-colombia", catalogTableName="temperatura"
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(ChangeSchema_node2)
job.commit()
