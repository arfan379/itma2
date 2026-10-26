"""
Unit tests for ITMA Airflow Daily DAG (tests/test_airflow_dag.py)
Validates DAG import, task structure, acyclic nature, and schedule.
"""

from dags.itma_daily import dag


def test_dag_loaded():
    """Validates DAG is loaded with correct ID and configuration."""
    assert dag is not None
    assert dag.dag_id == "itma_daily"
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False


def test_dag_task_count_and_ids():
    """Validates all required tasks exist in DAG."""
    expected_tasks = {
        "ingest_gtfs",
        "run_delay_simulator",
        "run_telemetry_producer",
        "run_kafka_consumer",
        "dbt_deps",
        "dbt_seed",
        "dbt_run",
        "dbt_test",
        "dbt_docs_generate"
    }
    task_ids = {t.task_id for t in dag.tasks}
    assert expected_tasks.issubset(task_ids)


def test_dag_has_no_cycles():
    """Validates that DAG is strictly a Directed Acyclic Graph."""
    # cycle detection using DFS
    visited = set()
    rec_stack = set()

    def has_cycle(task):
        visited.add(task.task_id)
        rec_stack.add(task.task_id)
        for downstream in task.downstream_list:
            if downstream.task_id not in visited:
                if has_cycle(downstream):
                    return True
            elif downstream.task_id in rec_stack:
                return True
        rec_stack.remove(task.task_id)
        return False

    for task in dag.tasks:
        if task.task_id not in visited:
            assert not has_cycle(task)


def test_dag_task_dependencies_order():
    """Validates that ingestion precedes simulation and simulation precedes dbt."""
    ingest = dag.get_task("ingest_gtfs")
    delay = dag.get_task("run_delay_simulator")
    telemetry = dag.get_task("run_telemetry_producer")
    consumer = dag.get_task("run_kafka_consumer")
    dbt_run = dag.get_task("dbt_run")
    dbt_test = dag.get_task("dbt_test")

    assert delay in ingest.downstream_list
    assert telemetry in delay.downstream_list
    assert consumer in telemetry.downstream_list
    assert dbt_test in dbt_run.downstream_list
    # dbt_utils harus terpasang (dbt deps) sebelum seed/run/test
    dbt_deps = dag.get_task("dbt_deps")
    dbt_seed = dag.get_task("dbt_seed")
    assert dbt_deps in consumer.downstream_list
    assert dbt_seed in dbt_deps.downstream_list


def test_dbt_tasks_use_dbt_bin_variable():
    """Semua task dbt memanggil biner lewat ${DBT_BIN:-dbt} agar jalan di image Airflow ITMA."""
    for task_id in ("dbt_deps", "dbt_seed", "dbt_run", "dbt_test", "dbt_docs_generate"):
        assert "${DBT_BIN:-dbt}" in dag.get_task(task_id).bash_command
