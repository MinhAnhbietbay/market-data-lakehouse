# Spark Infra

This directory defines the reproducible Spark runtime used by `pipelines/raw`.

## Runtime Composition

- Spark distribution: `apache/spark:3.5.8-python3` copied from the official image in a build stage.
- Runtime base: Python `3.11` slim with Java `17`.
- Python metadata dependency: PyYAML installed in the image.
- Pinned runtime JARs: Iceberg Spark runtime `1.9.2`, Hadoop AWS `3.3.4`, AWS SDK bundle `1.12.262`.
- Spark defaults: `conf/spark-defaults.conf` configures Hive Metastore, Iceberg, and MinIO/S3A interoperability.

Using the official Spark image as a distribution source avoids the slow direct Apache archive download while preserving the Python version required by pipeline code.

## Build And Verify

```bash
docker compose --progress plain --env-file .env -f infra/docker-compose.yml build spark-master
docker compose --env-file .env -f infra/docker-compose.yml up -d --force-recreate spark-master spark-worker-1 spark-worker-2
docker compose --env-file .env -f infra/docker-compose.yml exec -T spark-master \
  python3 -c 'import yaml; print(yaml.__version__)'
docker compose --env-file .env -f infra/docker-compose.yml exec -T spark-master spark-submit --version
```

A runtime change is complete only after a real Raw Iceberg load succeeds. Building the image alone is insufficient.

## Container Contract

- `spark-master`, `spark-worker-1`, and `spark-worker-2` all run the same image, built once from this Dockerfile via `spark-master`'s `build:` block.
- `spark-master` is both the cluster master daemon and the Raw job submission point: Raw jobs run as `spark-submit --master spark://spark-master:7077 ...` executed inside this container, so the driver and the master process are two JVMs in the same container talking over `localhost:7077`.
- Only `spark-master` mounts repository code/config/contracts at `/workspace`, sets `PYTHONPATH=/workspace` for pipeline package imports, and sets `RUN_MANIFEST_ROOT=/workspace/artifacts/run-manifests` for the shared host manifest directory — the driver is the only process that needs any of this.
- `spark-worker-1`/`spark-worker-2` mount none of the above: Raw jobs are pure PySpark DataFrame/SQL with no Python UDFs, and landing data is read via `s3a://` (MinIO), not the host filesystem, so executors need nothing beyond the image's JARs and `spark-defaults.conf`.

## Boundaries

Business mapping and table behavior belong in pipeline code/contracts/table configs, not in this Dockerfile or Spark defaults.
