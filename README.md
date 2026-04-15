# Big Data ETL Pipeline — Arquitectura Medallion

Pipeline de datos completo sobre Stack Overflow implementando la arquitectura *Bronze → Silver → Gold* con MinIO, Apache Iceberg, Apache Spark, Nessie, Dremio, Trino, Airflow y DLT.

---

## Arquitectura

Fuentes (S3 externo)

│  
▼  

**BRONZE (MinIO)**  
Parquet crudo · override  
DLT via Airflow  

│  
▼  

**SILVER (Iceberg · Nessie)**  
Tablas curadas · merge  
PySpark · spark-submit  

│  
▼  

**GOLD (Iceberg · Nessie)**  
Métricas y KPIs · merge  
PySpark · spark-submit  

│  
▼  

**Consulta SQL**  
Trino · Dremio · ClickHouse


### Capas

| Capa | Formato | Escritura | Contenido |
|---|---|---|---|
| Bronze | Parquet | Override | Datos crudos de Stack Overflow (posts, comments, users) para 2023 y 2024 |
| Silver | Iceberg v2 | Merge/upsert | Tablas curadas, normalizadas e históricas con fecha_cargue |
| Gold | Iceberg v2 | Merge/upsert | Tabla cant_post_x_user_hist con métricas por usuario y reglas de negocio |

### Tablas Silver

| Tabla | Clave | Descripción |
|---|---|---|
| nessie.silver.users_hist | id | Usuarios históricos de Stack Overflow |
| nessie.silver.comments_hist | id | Comentarios históricos (2023 + 2024) |
| nessie.silver.posts_hist | id | Posts históricos (2023 + 2024) |

### Tabla Gold

| Tabla | Clave | Descripción |
|---|---|---|
| nessie.gold.cant_post_x_user_hist | user_id | Métricas por usuario: total posts, score, tasa de aceptación, segmento de reputación, ratio de votos |

---

## Stack tecnológico

| Servicio | Versión | Puerto | Función |
|---|---|---|---|
| MinIO | latest | 9000 / 9001 | Object storage (Bronze, Silver, Gold) |
| Nessie | latest | 19120 | Catálogo de metadatos Iceberg |
| Apache Spark | 3.5.5 | 9090 (UI) / 7077 | Motor de procesamiento (Silver + Gold) |
| Apache Airflow | 2.9.0 | 8080 | Orquestación del pipeline |
| Jupyter | latest | 8888 | Ejecución manual de notebooks |
| Trino | latest | 8085 | Motor SQL sobre Iceberg |
| Dremio OSS | latest | 9047 | Motor SQL alternativo |
| ClickHouse | 24.1 | 8123 | Base de datos analítica |
| PostgreSQL | 13 | — | Backend de Airflow |
| Redis | 7.2 | — | Broker de Celery para Airflow |

---

## Estructura del proyecto
```
Big-Data-ETL-Pipeline/
├── dags/
│   ├── dag.py                    # DAG principal de Airflow
│   └── tasks/
│       ├── __init__.py
│       ├── bronze_ingest.py      # Tareas Bronze (DLT)
│       └── silver_transform.py   # Referencia (no usado por Airflow directamente)
├── jobs/
│   ├── silver_transform.py       # Job PySpark Silver (spark-submit)
│   └── gold_agg.py               # Job PySpark Gold (spark-submit)
├── notebooks/
│   ├── Bronze/
│   │   └── bronze_ingest.ipynb   # Ingesta manual Bronze
│   ├── Silver/
│   │   └── silver_transform.ipynb # Transformación manual Silver
│   └── Gold/
│       └── gold_agg.ipynb        # Agregación manual Gold
├── trino/
│   └── catalog/
│       └── nessie.properties     # Configuración catálogo Nessie para Trino
├── data/                         # Volumen de MinIO (generado automáticamente)
├── logs/                         # Logs de Airflow (generado automáticamente)
├── Dockerfile                    # Imagen Airflow personalizada
├── Dockerfile.spark              # Imagen Spark con JARs de Iceberg/Nessie
├── dockerfile.jupyter            # Imagen Jupyter con PySpark
├── docker-compose.yml
├── requirements.txt              # Dependencias Python para Airflow
├── spark-defaults.conf           # Configuración por defecto de Spark
└── README.md
```

---

## Despliegue

### Prerrequisitos

- Docker Desktop instalado y corriendo
- 16 GB RAM recomendados
- Puertos libres: 8080, 8085, 8888, 9000, 9001, 9047, 9090, 19120

### 1. Clonar el repositorio
```bash
git clone <https://github.com/MarcAragon/Big-Data-ETL-Pipeline.git> 
cd Big-Data-ETL-Pipeline
```

### 2. Levantar los contenedores
```bash
docker compose up -d --build
```

El primer build tarda ~5-10 minutos porque descarga los JARs de Iceberg, Nessie y AWS SDK para Spark.

Verificar que todos estén healthy:

```bash
docker compose ps
```



Esperar a que todos los servicios de Airflow muestren healthy.

### 3. Crear el bucket en MinIO

1. Abrir http://localhost:9001
2. Usuario: admin / Contraseña: password
3. Crear bucket con nombre exacto: *proyecto2*

### 4. Configurar la conexión Spark en Airflow

1. Abrir http://localhost:8080 (usuario: admin / contraseña: admin)
2. Ir a *Admin → Connections*
3. Editar o crear spark_default:
   - *Connection Type:* Spark
   - *Host:* spark://spark_master
   - *Port:* 7077

### 5. Configurar Trino para consultar Iceberg

# Linux/Mac
```bash
docker cp trino/catalog/nessie.properties trino:/etc/trino/catalog/nessie.properties
docker restart trino
```


# Windows PowerShell
```bash
docker cp trino/catalog/nessie.properties trino:/etc/trino/catalog/nessie.properties
docker restart trino
```

---

## Ejecutar el pipeline

### Opción A — Airflow (orquestación completa)

1. Ir a http://localhost:8080
2. Buscar el DAG proyecto2
3. Activar el toggle (ON)
4. Hacer clic en ▶ *Trigger DAG*

El DAG ejecuta automáticamente en este orden:


```
bronze_comments ──┐
bronze_posts    ──┼──► silver_users    ──┐
bronze_users    ──┘    silver_comments ──┼──► gold_cant_post_x_user_hist
                        silver_posts    ──┘
```


Bronze tarda 20-45 minutos (descarga ~25M filas desde internet). Silver y Gold tardan ~5-10 minutos cada uno.

> Para pruebas rápidas: en dags/tasks/bronze_ingest.py cambiar MAX_BATCHES = 2 — reduce Bronze a ~2 minutos.

### Opción B — Notebooks manuales (fallback)

Abrir http://localhost:8888 y ejecutar en orden:

1. notebooks/Bronze/bronze_ingest.ipynb → Kernel → Restart & Run All
2. notebooks/Silver/silver_transform.ipynb → Kernel → Restart & Run All
3. notebooks/Gold/gold_agg.ipynb → Kernel → Restart & Run All

Esperar que cada notebook muestre el mensaje de completado antes de pasar al siguiente.

---

## Consultar los datos

### Trino

```bash
docker exec trino trino --execute "SHOW SCHEMAS FROM nessie"
```

```sql
-- Ver tablas disponibles
SHOW TABLES FROM nessie.silver;
SHOW TABLES FROM nessie.gold;

-- Consultar Gold
SELECT user_id, display_name, reputation, segmento_reputacion,
       total_posts, tasa_aceptacion_pct, score_promedio
FROM nessie.gold.cant_post_x_user_hist
ORDER BY reputation DESC
LIMIT 20;

-- Distribución por segmento
SELECT segmento_reputacion, COUNT(*) as total
FROM nessie.gold.cant_post_x_user_hist
GROUP BY segmento_reputacion
ORDER BY total DESC;
```

### Dremio

1. Abrir http://localhost:9047
2. Agregar fuente *Nessie*:
   - Endpoint: http://nessie:19120/api/v2
   - Authentication: None
3. En *Advanced Options → Connection Properties*:
   - fs.s3a.endpoint = http://minio:9000
   - fs.s3a.path.style.access = true
   - fs.s3a.connection.ssl.enabled = false
   - fs.s3a.access.key = admin
   - fs.s3a.secret.key = password

### MinIO (verificar datos Bronze)

Abrir http://localhost:9001 → bucket proyecto2 → carpeta bronze/

### Spark UI

Abrir http://localhost:9090 para monitorear jobs en ejecución.

---

## Accesos rápidos

| Servicio | URL | Usuario | Contraseña |
|---|---|---|---|
| Airflow | http://localhost:8080 | admin | admin |
| MinIO | http://localhost:9001 | admin | password |
| Jupyter | http://localhost:8888 | — | — |
| Spark Master UI | http://localhost:9090 | — | — |
| Nessie API | http://localhost:19120 | — | — |
| Trino | http://localhost:8085 | — | — |
| Dremio | http://localhost:9047 | — | — |
| ClickHouse | http://localhost:8123 | admin | password |

---

## Solución de problemas

### Bronze falla con bucket_url not found
DLT no encuentra las credenciales. Verificar que en dags/tasks/bronze_ingest.py las variables _BUCKET_URL, _ENDPOINT_URL, _AWS_ACCESS_KEY y _AWS_SECRET_KEY estén correctamente definidas.

### Silver/Gold falla con JAVA_GATEWAY_EXITED
Spark no está disponible en el worker de Airflow. Verificar que el DAG use SparkSubmitOperator y que la conexión spark_default apunte a spark://spark_master:7077.

### Silver falla con NoClassDefFoundError: S3Exception
Los JARs de AWS SDK no están disponibles. Verificar que jobs/silver_transform.py use SparkSession.builder.getOrCreate() sin configurar spark.jars.packages (los JARs ya están en /opt/spark/jars del cluster).

### Contenedor de Airflow pierde conectividad a internet
Reiniciar Docker Desktop y volver a levantar los contenedores:
bash
docker compose down
docker compose up -d


### Trino no encuentra el catálogo nessie
El archivo nessie.properties no está montado. Copiarlo manualmente:
bash
docker cp trino/catalog/nessie.properties trino:/etc/trino/catalog/nessie.properties
docker restart trino


---

## Detener el proyecto

```bash
docker compose down
```
Para eliminar también los volúmenes (datos de MinIO, Nessie, etc.):

```bash
docker compose down -v
```
