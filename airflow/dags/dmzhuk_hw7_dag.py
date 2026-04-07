import time
import random
from datetime import datetime, timezone
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.task.trigger_rule import TriggerRule as tr

# Налаштування підключення та схеми
CONNECTION_ID = "goit_mysql_db_dmzhuk"
SCHEMA_NAME = "olympic_dataset"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4, tzinfo=timezone.utc),
}


# Функція вибору задачі
def _pick_medal():
    chosen = random.choice(["Bronze", "Silver", "Gold"])
    return f"calc_{chosen}"


# Функція для затримки
def _generate_delay():
    # Для демонстрації SUCCESS: 5-10''
    # Для демонстрації FAIL: change to 35''
    delay = 10
    print(f"Sleeping for {delay} seconds...")
    time.sleep(delay)


with DAG(
    dag_id="medal_processing_workflow",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["hw7", "mysql"],
) as dag:
    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id="create_table",
        conn_id=CONNECTION_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.dmzhuk_medal_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # 2. Завдання вибору - випадково обирає одне із трьох значень
    pick_medal = PythonOperator(
        task_id="pick_medal", python_callable=lambda: print("Picking a medal type...")
    )

    # 3. Розгалуження
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task", python_callable=_pick_medal
    )

    # 4. Обчислення
    def create_calc_task(medal):
        return MySqlOperator(
            task_id=f"calc_{medal}",
            conn_id=CONNECTION_ID,
            sql=f"""
            INSERT INTO {SCHEMA_NAME}.dmzhuk_medal_results (medal_type, count, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = '{medal}';
            """,
        )

    calc_bronze = create_calc_task("Bronze")
    calc_silver = create_calc_task("Silver")
    calc_gold = create_calc_task("Gold")

    # 5. Затримка
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=_generate_delay,
        trigger_rule=tr.ONE_SUCCESS,
    )

    # 6. Сенсор для перевірки результатів
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONNECTION_ID,
        sql=f"""
        SELECT COUNT(*) FROM {SCHEMA_NAME}.dmzhuk_medal_results
        WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        mode="poke",
        poke_interval=5,
        timeout=10,
    )

    # Побудова графа
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]
    [calc_bronze, calc_silver, calc_gold] >> generate_delay >> check_for_correctness
