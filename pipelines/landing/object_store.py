import json
import os
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError


def write_local_json(root: str | Path, object_key: str, data: dict[str, Any]) -> str:
    output_path = Path(root) / object_key
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    return str(output_path)


def make_s3_client():
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION", "us-east-1")

    if not endpoint_url:
        raise RuntimeError("Missing required environment variable: AWS_ENDPOINT_URL")
    if not access_key:
        raise RuntimeError("Missing required environment variable: AWS_ACCESS_KEY_ID")
    if not secret_key:
        raise RuntimeError("Missing required environment variable: AWS_SECRET_ACCESS_KEY")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def write_minio_json(bucket: str, object_key: str, data: dict[str, Any]) -> str:
    client = make_s3_client()
    body = json.dumps(data, indent=2).encode("utf-8")

    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body,
        ContentType="application/json",
    )

    return f"s3://{bucket}/{object_key}"


def _minio_object_exists(bucket: str, object_key: str) -> bool:
    try:
        make_s3_client().head_object(Bucket=bucket, Key=object_key)
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def object_exists(write_target: str, landing_root: str | Path, bucket: str, object_key: str) -> bool:
    """Object landing đã có chưa -- nền của --skip-existing.

    `both` chỉ tính là có khi CẢ bản gốc cục bộ lẫn bản MinIO đều có: đứt giữa hai lần ghi thì
    phải kéo lại, nếu không bản MinIO thiếu vĩnh viễn mà không ai biết.
    """
    local = (Path(landing_root) / object_key).is_file()
    if write_target == "local":
        return local
    if write_target == "minio":
        return _minio_object_exists(bucket, object_key)
    if write_target == "both":
        return local and _minio_object_exists(bucket, object_key)
    raise ValueError(f"Unsupported write target: {write_target}")
