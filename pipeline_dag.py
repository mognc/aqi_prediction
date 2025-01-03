from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'Ali',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'feature_and_training_pipeline',
    default_args=default_args,
    description='Pipeline for feature engineering and model training',
    schedule_interval=None,  # Set to your desired schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def preprocess_data():
        print("Collecting and preprocessing data...")
        # Add your API data fetching and preprocessing code here

    def train_model():
        print("Training the model...")
        # Add your training code here

    task1 = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    task2 = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    task1 >> task2  # Define task dependencies
