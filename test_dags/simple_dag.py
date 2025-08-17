from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    dag_id="unique_name",
    start_date=datetime(2022,12,10),
    schedule="* * * * *",
    catchup=False
)
def my_dag():

    @task
    def say_hi():
        return "hi"

    say_hi()

# without this line the DAG will not show up in the UI
my_dag()

