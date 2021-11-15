import importlib
import os
from pathlib import Path

import pytest

from airflow.models.dag import DAG
from airflow.utils import dag_cycle_tester

from app import dags as dagdir


@pytest.fixture(scope="session")
def dags():
    result = []
    dag_path = Path(dagdir.__file__).parent
    dag_files = list(dag_path.rglob("*.py"))
    for dag_file in dag_files:
        module_name, _ = os.path.splitext(dag_file)
        module_path = os.path.join(dag_path, dag_file)
        mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(module)
        dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
        result.extend(dag_objects)
    if not result:
        raise ValueError(f"No DAGs found in {dag_path}")
    return result


def test_dag_correctness(dags):
    for dag in dags:
        assert dag is not None
        assert len(dag.tasks) > 1


def test_dag_cycles(dags):
    for dag in dags:
        dag_cycle_tester.check_cycle(dag)