from pyspark.sql import DataFrame, SparkSession


def build_landing_path(source: str, endpoint: str, run_date: str) -> str:
    """Directory holding one endpoint's landed objects for one run_date.

    Deliberately NOT a `.../dt={run_date}/*.json` glob string: some endpoints'
    `partition_key` templates (e.g. `ohlcv_non_split_adjusted`'s `{symbol}/{from}`,
    see configs/sources/data_sources.yaml) produce objects nested one or more path
    segments below `dt={run_date}/` (e.g. `dt=2026-08-12/AAPL/2016-08-12.json`). A
    bare `*` in a Hadoop/Spark glob does not cross a `/` boundary, so a `*.json`
    suffix here would silently match zero files for those endpoints. Callers use
    `recursiveFileLookup` (Spark) / `listFiles(..., recursive=True)` (Hadoop FS)
    against this exact directory instead, which finds both flat (`page-0.json`,
    `{symbol}.json`) and nested (`{symbol}/{from}.json`) layouts through the same
    code path, with no per-endpoint special-casing. The path stays scoped to
    exactly this one source/endpoint/run_date directory, so recursion never
    reaches another run_date, another endpoint, or -- importantly -- a
    `dt={run_date}__non_2xx/` quarantine sibling (see
    `pipelines/landing/ingest.py::ingest_one_with_payload`'s `quarantine_non_2xx`),
    since that lives outside this directory, not nested inside it.
    """
    return f"s3a://lakehouse/landing/{source}/{endpoint}/dt={run_date}"


def landing_path_exists(spark: SparkSession, source: str, endpoint: str, run_date: str) -> bool:
    path = build_landing_path(source=source, endpoint=endpoint, run_date=run_date)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(hadoop_conf)
    if not fs.exists(fs_path):
        return False
    # A plain `fs.exists` only proves the `dt={run_date}` directory itself is
    # there -- it says nothing about whether any `.json` object actually lives
    # inside it (recursively, to also catch nested partition_key layouts like
    # `{symbol}/{from}`). `listFiles(path, recursive=True)` is the Hadoop-FS-level
    # equivalent of Spark's `recursiveFileLookup` read option used below, so
    # existence and read-ability agree on what "there's data here" means.
    file_iterator = fs.listFiles(fs_path, True)
    while file_iterator.hasNext():
        if file_iterator.next().getPath().getName().endswith(".json"):
            return True
    return False


def read_landing_envelopes(
    spark: SparkSession,
    source: str,
    endpoint: str,
    run_date: str,
) -> DataFrame:
    path = build_landing_path(source=source, endpoint=endpoint, run_date=run_date)
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .json(path)
    )
