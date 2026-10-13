"""Pure, Airflow-independent logic backing market_data_modeling's
`write_run_manifest` task (Task 13).

Kept out of `infra/airflow/dags/market_data_modeling.py` — which imports
`airflow` at module top and is therefore only importable inside the
`airflow` Docker service — so the actual classification/self-exclusion
logic is directly unit-testable from the plain `alpha-dsl` conda env, with
plain fake objects, no real Airflow `TaskInstance`/`DagRun` required. This
mirrors this module's sibling `stamp_build_id.py`'s / `curated_columns.py`'s
pattern: pure business logic lives here, the DAG's `@task` body lazily
imports it and supplies the real Airflow objects.

`fetch_task_instances` below is the one IO-touching exception (same
"real infra, not unit-tested against fakes" carve-out `curated_columns.py`
documents for its own live-Trino dependency): Airflow 3's Task SDK blocks
task code from touching the metadata DB directly at all
(`RuntimeError: Direct database access via the ORM is not allowed in
Airflow 3.0`, raised in `airflow.sdk.execution_time.supervisor` — verified
live, not from docs) — the public REST API is the supported replacement
for a task that needs its own DAG run's sibling task-instance states.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Protocol

import requests


# Airflow's TaskInstanceState is a `str, Enum` — its members compare equal
# to their plain string value (e.g. `TaskInstanceState.FAILED == "failed"`
# is True), so comparing against plain strings here works whether the
# caller passes real Airflow enum members or bare strings (as the tests
# do), with no `airflow` import needed in this module.
FAILURE_STATES = {"failed", "upstream_failed"}


class TaskInstanceLike(Protocol):
    task_id: str
    state: Any


def summarize_task_instances(
    task_instances: list[TaskInstanceLike],
    current_task_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Classify a DAG run's task instances into (outputs, failures).

    `current_task_id` (the manifest-writer task itself) is excluded — its
    own task instance hasn't reached a terminal state while it's the one
    doing this recording, so including it would be misleading.

    Every other task instance becomes one `outputs` entry
    (`{"task_id": ..., "state": ...}`), regardless of outcome. Task
    instances whose state is in FAILURE_STATES (`failed`, `upstream_failed`)
    additionally become a `failures` entry with the same shape.
    """
    outputs: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for task_instance in task_instances:
        if task_instance.task_id == current_task_id:
            continue
        entry = {"task_id": task_instance.task_id, "state": task_instance.state}
        outputs.append(entry)
        if task_instance.state in FAILURE_STATES:
            failures.append(dict(entry))
    return outputs, failures


def fetch_task_instances(
    base_url: str,
    dag_id: str,
    run_id: str,
    username: str,
    password: str,
) -> list[TaskInstanceLike]:
    """Fetch a DAG run's task instances via Airflow's own public REST API.

    Two requests: log in against FabAuthManager's `/auth/token` for a JWT
    (the same flow the Airflow UI itself uses), then list task instances
    for the run. Raises on any non-2xx response — a manifest run that can't
    see its own DAG run's states should fail loudly, not write a partial
    manifest.
    """
    token_response = requests.post(
        f"{base_url}/auth/token",
        json={"username": username, "password": password},
        timeout=30,
    )
    token_response.raise_for_status()
    token = token_response.json()["access_token"]

    ti_response = requests.get(
        f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    ti_response.raise_for_status()
    return [
        SimpleNamespace(task_id=ti["task_id"], state=ti["state"])
        for ti in ti_response.json()["task_instances"]
    ]
