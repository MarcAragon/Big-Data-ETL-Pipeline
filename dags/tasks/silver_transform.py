import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_date, regexp_replace, trim
from pyspark.sql.types import IntegerType


CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3a://proyecto2/silver"
S3_ENDPOINT = "http://minio:9000"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password"

BRONZE_BASE = "s3a://proyecto2/bronze"

USERS_PATH = f"{BRONZE_BASE}/users/*.parquet"
COMMENTS_2023_PATH = f"{BRONZE_BASE}/comments_2023/*.parquet"
COMMENTS_2024_PATH = f"{BRONZE_BASE}/comments_2024/*.parquet"
POSTS_2023_PATH = f"{BRONZE_BASE}/posts_2023/*.parquet"
POSTS_2024_PATH = f"{BRONZE_BASE}/posts_2024/*.parquet"

SILVER_NAMESPACE = "nessie.silver"
USERS_TABLE = f"{SILVER_NAMESPACE}.users_hist"
COMMENTS_TABLE = f"{SILVER_NAMESPACE}.comments_hist"
POSTS_TABLE = f"{SILVER_NAMESPACE}.posts_hist"

# límites pequeños para prueba
USERS_LIMIT = 1000
COMMENTS_LIMIT = 1000
POSTS_LIMIT = 200


def build_spark_session() -> SparkSession:
    conf = (
        pyspark.SparkConf()
        .setAppName("combined_spark_app")
        .set("spark.driver.memory", "8g")
        .set("spark.executor.memory", "16g")
        .set("spark.sql.shuffle.partitions", "400")
        .set("spark.driver.maxResultSize", "4g")
        .set("spark.network.timeout", "800s")
        .set("spark.executor.heartbeatInterval", "60s")
        .set("spark.sql.broadcastTimeout", "1200")
        .set("spark.sql.autoBroadcastJoinThreshold", "104857600")
        .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "268435456")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .set(
            "spark.jars.packages",
            ",".join(
                [
                    "org.postgresql:postgresql:42.7.3",
                    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0",
                    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.77.1",
                    "software.amazon.awssdk:bundle:2.24.8",
                    "software.amazon.awssdk:url-connection-client:2.24.8",
                    "org.apache.hadoop:hadoop-aws:3.2.0",
                ]
            ),
        )
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", CATALOG_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.nessie.s3.endpoint", S3_ENDPOINT)
        .set("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .set("spark.sql.catalog.nessie.s3.access-key-id", AWS_ACCESS_KEY)
        .set("spark.sql.catalog.nessie.s3.secret-access-key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_namespace_if_needed(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")


def spark_table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def merge_into_iceberg(df_new: DataFrame, table_name: str, key_col: str = "id") -> None:
    """
    MERGE manual usando PySpark puro (sin SQL MERGE).
    
    Estrategia:
      1. Si la tabla no existe → crearla directamente.
      2. Si existe → leer la tabla actual, hacer anti-join para quedarse
         con los registros que NO están en el nuevo batch (los que no cambian),
         luego hacer union con el nuevo batch (que contiene tanto inserts como updates)
         y sobreescribir la tabla completa.
    
    Esto es equivalente semánticamente a:
      MERGE INTO target USING source ON key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    spark = df_new.sparkSession

    if not spark_table_exists(spark, table_name):
        print(f"La tabla {table_name} no existe. Creando con carga inicial...")
        (
            df_new.writeTo(table_name)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        print(f"Tabla {table_name} creada correctamente.")
        return

    print(f"Leyendo tabla existente {table_name}...")
    df_existing = spark.read.format("iceberg").load(table_name)

    # Registros del existing que NO están en el nuevo batch → se conservan sin cambios
    df_unchanged = df_existing.join(
        df_new.select(key_col),
        on=key_col,
        how="left_anti"
    )

    # Union: registros sin cambios + todos los del nuevo batch (updates + inserts)
    df_merged = df_unchanged.unionByName(df_new, allowMissingColumns=True)

    print(f"Sobreescribiendo {table_name} con datos mergeados...")
    (
        df_merged.writeTo(table_name)
        .using("iceberg")
        .overwritePartitions()
    )

    print(f"MERGE completado en {table_name}.")


def transform_users(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("display_name", col("display_name").cast("string"))
        .withColumn("location", col("location").cast("string"))
        .withColumn("about_me", col("about_me").cast("string"))
        .withColumn("website_url", col("website_url").cast("string"))
        .withColumn("reputation", col("reputation").cast("string").cast(IntegerType()))
        .withColumn("fecha_cargue", current_date())
    )


def transform_comments(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("text", col("text").cast("string"))
        .withColumn("user_display_name", col("user_display_name").cast("string"))
        .withColumn("fecha_cargue", current_date())
    )


def transform_posts(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("body", col("body").cast("string"))
        .withColumn("title", col("title").cast("string"))
        .withColumn("tags", col("tags").cast("string"))
        .withColumn("content_license", col("content_license").cast("string"))
        .withColumn("parent_id", col("parent_id").cast("string").cast(IntegerType()))
    )

    df = df.withColumn("body", regexp_replace(col("body"), "<[^>]*>", ""))
    df = df.withColumn("tags", regexp_replace(col("tags"), r"\|", ","))
    df = df.withColumn("tags", regexp_replace(trim(col("tags")), r"^,|,$", ""))
    df = df.withColumn("fecha_cargue", current_date())

    return df


def run_silver_users() -> None:
    spark = build_spark_session()
    create_namespace_if_needed(spark)

    print("Leyendo users desde Bronze...")
    df_users = spark.read.parquet(USERS_PATH).limit(USERS_LIMIT)
    df_users = transform_users(df_users)

    print(f"Haciendo MERGE sobre {USERS_TABLE}...")
    merge_into_iceberg(df_users, USERS_TABLE, key_col="id")

    print(f"Tabla actualizada con merge: {USERS_TABLE}")
    print("Silver users completado correctamente.")


def run_silver_comments() -> None:
    spark = build_spark_session()
    create_namespace_if_needed(spark)

    print("Leyendo comments desde Bronze...")
    df_comments_2023 = spark.read.parquet(COMMENTS_2023_PATH).limit(COMMENTS_LIMIT)
    df_comments_2024 = spark.read.parquet(COMMENTS_2024_PATH).limit(COMMENTS_LIMIT)

    df_comments_2023 = transform_comments(df_comments_2023)
    df_comments_2024 = transform_comments(df_comments_2024)

    df_comments = df_comments_2023.unionByName(df_comments_2024, allowMissingColumns=True)

    print(f"Haciendo MERGE sobre {COMMENTS_TABLE}...")
    merge_into_iceberg(df_comments, COMMENTS_TABLE, key_col="id")

    print(f"Tabla actualizada con merge: {COMMENTS_TABLE}")
    print("Silver comments completado correctamente.")


def run_silver_posts() -> None:
    spark = build_spark_session()
    create_namespace_if_needed(spark)

    print("Leyendo posts desde Bronze...")
    df_posts_2023 = spark.read.parquet(POSTS_2023_PATH).limit(POSTS_LIMIT)
    df_posts_2024 = spark.read.parquet(POSTS_2024_PATH).limit(POSTS_LIMIT)

    df_posts_2023 = transform_posts(df_posts_2023)
    df_posts_2024 = transform_posts(df_posts_2024)

    df_posts = df_posts_2023.unionByName(df_posts_2024, allowMissingColumns=True)

    print(f"Haciendo MERGE sobre {POSTS_TABLE}...")
    merge_into_iceberg(df_posts, POSTS_TABLE, key_col="id")

    print(f"Tabla actualizada con merge: {POSTS_TABLE}")
    print("Silver posts completado correctamente.")


if __name__ == "__main__":
    run_silver_users()
