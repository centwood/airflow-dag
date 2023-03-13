from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator


def print_variables() -> str:
    var_user_email = Variable.get("gitHubToken")
    #var_sample_json = Variable.get("sample_json", deserialize_json=True)
    #var_env_test = Variable.get("test")
    #var_env_test_json = Variable.get("test_json", deserialize_json=True)

    return f"""
        var_user_email = {var_user_email},
        
        
        
    """


with DAG(...) as dag:

    task_print_variables = PythonOperator(
        task_id="print_variables",
        python_callable=print_variables
    )