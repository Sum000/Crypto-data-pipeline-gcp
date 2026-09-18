from airflow.sdk import dag, task


@dag(
    dag_id="crypto_airflow_test",
    schedule=None,
    catchup=False,
    tags=["crypto", "test"],
)
def crypto_airflow_test():

    @task
    def hello_airflow():
        print("Airflow is working successfully!")

        return "hello from task 1"

    @task
    def confirm_result(message):
        print(f"Received from previous task: {message}")
        print("Task dependency and XCom are working!")

    message = hello_airflow()

    confirm_result(message)


crypto_airflow_test()