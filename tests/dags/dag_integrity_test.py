import glob
import importlib
import os

from airflow.models.dag import DAG
from airflow.utils import dag_cycle_tester


def test_dag_integrity():
    for dags in get_dags():
        assert dags
        for dag in dags:
            dag_cycle_tester.test_cycle(dag)


def get_dags():
    dag_path = os.path.join(os.path.dirname(__file__), "…", "…", "dags/**/*.py")
    dag_files = glob.glob(dag_path, recursive=True)
    for dag_file in dag_files:
        module_name, _ = os.path.splitext(dag_file)
        module_path = os.path.join(dag_path, dag_file)
        mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(module)
        dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
        yield dag_objects
