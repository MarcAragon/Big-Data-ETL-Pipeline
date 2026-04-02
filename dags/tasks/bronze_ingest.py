import os
import time
import tempfile

import pyarrow.parquet as pq
import requests
import toml


def read_parquet_in_batches(
    parquet_url,
    max_retries=5,
    delay=5,
    batch_size=50000,
    convert_types=False,
):
    attempt = 0

    while attempt < max_retries:
        tmp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
                print(f"Descargando {parquet_url} ...")

                with requests.get(parquet_url, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            tmp_file.write(chunk)

                tmp_file_path = tmp_file.name

            parquet_file = pq.ParquetFile(tmp_file_path)

            print(f"Total filas: {parquet_file.metadata.num_rows}")
            print(f"Row groups: {parquet_file.num_row_groups}")

            for i, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
                print(f"Procesando batch {i + 1}")
                df = batch.to_pandas()

                if convert_types:
                    df = df.convert_dtypes()

                yield df

            break

        except Exception as e:
            attempt += 1
            print(f"Error intento {attempt}: {e}")

            if attempt < max_retries:
                time.sleep(delay)
            else:
                raise

        finally:
            if tmp_file_path and os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)


def run_comments_pipeline():
    os.environ["DLT_HOME"] = "/opt/airflow/dags/tasks"
    secrets_path = "/opt/airflow/dags/tasks/.dlt/secrets.toml"
    toml.load(secrets_path)

    import dlt

    @dlt.resource(table_name="Comments_2023")
    def comments_2023():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/comments/2023.parquet"
        yield from read_parquet_in_batches(
            url,
            batch_size=20000,
            convert_types=False,
        )

    @dlt.resource(table_name="Comments_2024")
    def comments_2024():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/comments/2024.parquet"
        yield from read_parquet_in_batches(
            url,
            batch_size=20000,
            convert_types=False,
        )

    pipeline = dlt.pipeline(
        pipeline_name="Bronze_MinIO_1",
        destination="filesystem",
        dataset_name="Bronze",
    )

    load_info = pipeline.run(
        [comments_2023, comments_2024],
        loader_file_format="parquet",
        write_disposition="replace",
    )

    print(load_info)


def run_posts_pipeline():
    os.environ["DLT_HOME"] = "/opt/airflow/dags/tasks"
    secrets_path = "/opt/airflow/dags/tasks/.dlt/secrets.toml"
    toml.load(secrets_path)

    import dlt

    @dlt.resource(table_name="Posts_2023")
    def posts_2023():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2023.parquet"
        yield from read_parquet_in_batches(
            url,
            batch_size=5000,
            convert_types=False,
        )

    @dlt.resource(table_name="Posts_2024")
    def posts_2024():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2024.parquet"
        yield from read_parquet_in_batches(
            url,
            batch_size=5000,
            convert_types=False,
        )

    pipeline = dlt.pipeline(
        pipeline_name="Bronze_MinIO_2",
        destination="filesystem",
        dataset_name="Bronze",
    )

    load_info = pipeline.run(
        [posts_2023, posts_2024],
        loader_file_format="parquet",
        write_disposition="replace",
    )

    print(load_info)


def run_users_pipeline():
    os.environ["DLT_HOME"] = "/opt/airflow/dags/tasks"
    secrets_path = "/opt/airflow/dags/tasks/.dlt/secrets.toml"
    toml.load(secrets_path)

    import dlt

    @dlt.resource(table_name="Users")
    def users():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet"
        yield from read_parquet_in_batches(
            url,
            batch_size=10000,
            convert_types=False,
        )

    pipeline = dlt.pipeline(
        pipeline_name="Bronze_MinIO_3",
        destination="filesystem",
        dataset_name="Bronze",
    )

    load_info = pipeline.run(
        [users],
        loader_file_format="parquet",
        write_disposition="replace",
    )

    print(load_info)