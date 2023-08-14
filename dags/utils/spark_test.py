from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create a Spark session
spark = SparkSession.builder.appName("EmpresaSchema").getOrCreate()

metadata = {
    "CNPJ_BASICO": "CNPJ BÁSICO NÚMERO BASE DE INSCRIÇÃO NO CNPJ",
    "RAZAO_SOCIAL_NOME_EMPRESARIAL": "NOME EMPRESARIAL DA PESSOA JURÍDICA",
    "NATUREZA_JURIDICA": "CÓDIGO DA NATUREZA JURÍDICA",
    "QUALIFICACAO_RESPONSAVEL": "QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA",
    "CAPITAL_SOCIAL": "CAPITAL SOCIAL DA EMPRESA",
    "PORTE_EMPRESA": "CÓDIGO DO PORTE DA EMPRESA",
    "ENTE_RESPONSAVEL": "ENTE FEDERATIVO RESPONSÁVEL"
}

# Define the schema for the EMPRESAS dataset 
empresa_schema = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("RAZAO_SOCIAL_NOME_EMPRESARIAL", StringType(), True),
    StructField("NATUREZA_JURIDICA", StringType(), True),
    StructField("QUALIFICACAO_RESPONSAVEL", StringType(), True),
    StructField("CAPITAL_SOCIAL", DoubleType(), True),
    StructField("PORTE_EMPRESA", StringType(), True),
    StructField("ENTE_RESPONSAVEL", StringType(), True)
])
# Load the CSV file into a DataFrame
csv_file_path = "./tmp/raw/K3241.K03200Y0.D30708.EMPRECSV"
df = spark.read.csv(csv_file_path, header=False, schema=empresa_schema, sep=';')
for col_name, desc in metadata.items():
    df = df.withColumn(col_name, col(col_name).alias(col_name, metadata={"description": desc}))


# Show the first few rows of the DataFrame
df.limit(10).show()

# Stop the Spark session
spark.stop()