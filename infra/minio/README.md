# MinIO Infra

## Purpose
Object storage configuration for landing files, lakehouse data, and artifacts.

## What belongs here
- bucket bootstrap config
- MinIO policies
- local storage config

## What does NOT belong here
- data pipeline code
- business schemas

## Inputs
Object storage needs from pipelines/dbt/services.

## Outputs
S3-compatible storage for the platform.

## Ownership
Infrastructure owner

## Examples
- `bucket-init.sh`
