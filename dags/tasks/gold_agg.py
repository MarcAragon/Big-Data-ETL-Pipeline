import os
import sys
import logging

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_date,
    count,
    avg,
    sum as spark_sum,
    when,
    lit,
    round as spark_round,
    coalesce,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("gold_agg")

# ---------------------------------------------------------------------------
# Variables de entorno requeridas por el SDK de AWS (MinIO no usa región real)
# ---------------------------------------------------------------------------
os.environ["AWS_REGION"]            = "us-east-1"
os.environ["AWS_DEFAULT_REGION"]    = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"]     = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

# ---------------------------------------------------------------------------
# Parámetros de conexión
# ---------------------------------------------------------------------------
CATALOG_URI    = "http://nessie:19120/api/v1"
WAREHOUSE      = "s3a://proyecto2/gold"
S3_ENDPOINT    = "http://minio:9000"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password"

# Tablas fuente (Silver)
SILVER_NAMESPACE = "nessie.silver"
USERS_TABLE      = f"{SILVER_NAMESPACE}.users_hist"
POSTS_TABLE      = f"{SILVER_NAMESPACE}.posts_hist"

# Tabla destino (Gold)
GOLD_NAMESPACE     = "nessie.gold"
GOLD_METRICS_TABLE = f"{GOLD_NAMESPACE}.cant_post_x_user_hist"


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    conf = (
        pyspark.SparkConf()
        .setAppName("gold_agg")
        .set("spark.driver.memory", "8g")
        .set("spark.executor.memory", "16g")
        .set("spark.sql.shuffle.partitions", "200")
        .set("spark.driver.maxResultSize", "4g")
        .set("spark.network.timeout", "800s")
        .set("spark.executor.heartbeatInterval", "60s")
        .set("spark.sql.broadcastTimeout", "1200")
        .set("spark.sql.autoBroadcastJoinThreshold", "104857600")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .set(
            "spark.jars.packages",
            ",".join([
                "org.postgresql:postgresql:42.7.3",
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0",
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.77.1",
                "software.amazon.awssdk:bundle:2.24.8",
                "software.amazon.awssdk:url-connection-client:2.24.8",
                "org.apache.hadoop:hadoop-aws:3.2.0",
            ]),
        )
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # Catálogo Nessie
        .set("spark.sql.catalog.nessie",                        "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri",                    CATALOG_URI)
        .set("spark.sql.catalog.nessie.ref",                    "main")
        .set("spark.sql.catalog.nessie.authentication.type",    "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl",           "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse",              WAREHOUSE)
        .set("spark.sql.catalog.nessie.io-impl",                "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.nessie.s3.endpoint",            S3_ENDPOINT)
        .set("spark.sql.catalog.nessie.s3.path-style-access",  "true")
        .set("spark.sql.catalog.nessie.s3.access-key-id",      AWS_ACCESS_KEY)
        .set("spark.sql.catalog.nessie.s3.secret-access-key",  AWS_SECRET_KEY)
        .set("spark.sql.catalog.nessie.s3.region",             "us-east-1")
        # Hadoop S3A
        .set("spark.hadoop.fs.s3a.endpoint",                    S3_ENDPOINT)
        .set("spark.hadoop.fs.s3a.access.key",                  AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key",                  AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.path.style.access",           "true")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider",   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .set("spark.hadoop.fs.s3a.impl",                       "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled",     "false")
        .set("spark.hadoop.fs.s3a.endpoint.region",            "us-east-1")
        .set(
            "spark.driver.extraJavaOptions",
            "-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password",
        )
        .set(
            "spark.executor.extraJavaOptions",
            "-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password",
        )
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession iniciada correctamente.")
    return spark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def create_namespace_if_needed(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    log.info("Namespace %s listo.", GOLD_NAMESPACE)


def spark_table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def merge_into_iceberg(
    spark: SparkSession,
    df_new: DataFrame,
    table_name: str,
    key_col: str = "user_id",
) -> None:
    """
    MERGE manual con PySpark puro — mismo patrón que Silver.
    - Si la tabla no existe: crea con carga inicial (Iceberg v2).
    - Si existe: conserva registros no presentes en el nuevo batch
      y sobreescribe los que sí vienen (update + insert).
    """
    if not spark_table_exists(spark, table_name):
        log.info("Tabla %s no existe. Creando con carga inicial...", table_name)
        (
            df_new.writeTo(table_name)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        log.info("Tabla %s creada correctamente.", table_name)
        return

    log.info("Leyendo tabla existente %s...", table_name)
    df_existing = spark.read.format("iceberg").load(table_name)

    # Registros que NO vienen en el nuevo batch → se conservan tal cual
    df_unchanged = df_existing.join(
        df_new.select(key_col),
        on=key_col,
        how="left_anti",
    )

    # Union: sin cambios + nuevo batch (cubre updates e inserts)
    df_merged = df_unchanged.unionByName(df_new, allowMissingColumns=True)

    log.info("Sobreescribiendo %s con datos mergeados...", table_name)
    (
        df_merged.writeTo(table_name)
        .using("iceberg")
        .overwritePartitions()
    )
    log.info("MERGE completado en %s.", table_name)


# ---------------------------------------------------------------------------
# Lógica principal Gold
# ---------------------------------------------------------------------------
def run(spark: SparkSession) -> None:

    # ── 1. Namespace ────────────────────────────────────────────────────────
    create_namespace_if_needed(spark)

    # ── 2. Leer Silver ──────────────────────────────────────────────────────
    log.info("Leyendo tablas Silver...")
    df_users = spark.read.format("iceberg").load(USERS_TABLE)
    df_posts = spark.read.format("iceberg").load(POSTS_TABLE)
    log.info("  users_hist : %d registros", df_users.count())
    log.info("  posts_hist : %d registros", df_posts.count())

    # ── 3. Agregar métricas de posts por usuario ─────────────────────────────
    log.info("Agregando metricas de posts por usuario...")

    df_posts_agg = (
        df_posts
        .filter(col("owner_user_id").isNotNull())
        .groupBy(col("owner_user_id").alias("user_id"))
        .agg(
            # Volumetría general
            count("id").alias("total_posts"),
            count(when(col("post_type_id") == 1, True)).alias("total_preguntas"),
            count(when(col("post_type_id") == 2, True)).alias("total_respuestas"),

            # Calidad / puntuación
            spark_sum("score").alias("score_total"),
            spark_round(avg("score"), 2).alias("score_promedio"),

            # Alcance e interacción
            spark_sum("view_count").alias("total_vistas"),
            spark_sum("comment_count").alias("total_comentarios_recibidos"),

            # Efectividad
            spark_sum(
                when(col("accepted_answer_id") > 0, 1).otherwise(0)
            ).alias("respuestas_aceptadas"),
        )
    )

    log.info("Usuarios con al menos un post: %d", df_posts_agg.count())

    # ── 4. JOIN con users_hist + reglas de negocio ──────────────────────────
    log.info("Cruzando con users_hist y aplicando reglas de negocio...")

    df_gold = (
        df_users
        # LEFT JOIN: conservamos todos los usuarios aunque no tengan posts
        .join(df_posts_agg, df_users["id"] == df_posts_agg["user_id"], "left")
        .drop(df_posts_agg["user_id"])          # eliminar columna duplicada

        # Rellenar nulos para usuarios sin posts
        .withColumn("total_posts",                 coalesce(col("total_posts"),                 lit(0)))
        .withColumn("total_preguntas",             coalesce(col("total_preguntas"),             lit(0)))
        .withColumn("total_respuestas",            coalesce(col("total_respuestas"),            lit(0)))
        .withColumn("score_total",                 coalesce(col("score_total"),                 lit(0)))
        .withColumn("score_promedio",              coalesce(col("score_promedio"),              lit(0.0)))
        .withColumn("total_vistas",                coalesce(col("total_vistas"),                lit(0)))
        .withColumn("total_comentarios_recibidos", coalesce(col("total_comentarios_recibidos"), lit(0)))
        .withColumn("respuestas_aceptadas",        coalesce(col("respuestas_aceptadas"),        lit(0)))

        # Regla 1: tasa de aceptación (evitar división por cero)
        .withColumn(
            "tasa_aceptacion_pct",
            spark_round(
                when(
                    col("total_preguntas") > 0,
                    col("respuestas_aceptadas") / col("total_preguntas") * 100,
                ).otherwise(lit(0.0)),
                2,
            ),
        )

        # Regla 2: segmento según reputación de Stack Overflow
        .withColumn(
            "segmento_reputacion",
            when(col("reputation") >= 10000, lit("Experto"))
            .when(col("reputation") >= 1000,  lit("Avanzado"))
            .when(col("reputation") >= 100,   lit("Intermedio"))
            .otherwise(lit("Principiante")),
        )

        # Regla 3: ratio votos positivos / negativos (evitar división por cero)
        .withColumn(
            "ratio_votos",
            spark_round(
                when(
                    col("down_votes") > 0,
                    col("up_votes") / col("down_votes"),
                ).otherwise(col("up_votes").cast("double")),
                2,
            ),
        )

        # Renombrar clave principal
        .withColumnRenamed("id", "user_id")

        # Columna de auditoría obligatoria en Gold
        .withColumn("fecha_cargue", current_date())

        # Orden de columnas final
        .select(
            "user_id",
            "display_name",
            "reputation",
            "segmento_reputacion",
            "location",
            "creation_date",
            "last_access_date",
            "views",
            "up_votes",
            "down_votes",
            "ratio_votos",
            "total_posts",
            "total_preguntas",
            "total_respuestas",
            "respuestas_aceptadas",
            "tasa_aceptacion_pct",
            "score_total",
            "score_promedio",
            "total_vistas",
            "total_comentarios_recibidos",
            "fecha_cargue",
        )
    )

    log.info("Registros en tabla Gold: %d", df_gold.count())

    # ── 5. Escribir con merge ────────────────────────────────────────────────
    merge_into_iceberg(spark, df_gold, GOLD_METRICS_TABLE, key_col="user_id")
    log.info("Gold cant_post_x_user_hist completado.")

    # ── 6. Resumen final ─────────────────────────────────────────────────────
    df_result = spark.read.format("iceberg").load(GOLD_METRICS_TABLE)
    total = df_result.count()

    log.info("=" * 55)
    log.info("RESUMEN GOLD")
    log.info("=" * 55)
    log.info("  Tabla : %s", GOLD_METRICS_TABLE)
    log.info("  Filas : %d", total)
    log.info("=" * 55)

    # Distribución por segmento (validación rápida de reglas de negocio)
    log.info("Distribucion por segmento_reputacion:")
    df_result.groupBy("segmento_reputacion").count().orderBy("count").show()

    log.info("Gold aggregation completado correctamente.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    spark = build_spark_session()
    try:
        run(spark)
    except Exception as exc:
        log.exception("Error en gold_agg: %s", exc)
        sys.exit(1)
    finally:
        spark.stop()
        log.info("SparkSession cerrada.")