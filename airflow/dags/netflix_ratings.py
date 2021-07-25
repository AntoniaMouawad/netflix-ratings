from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'antoniam',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'catchup': False
}

dag = DAG('netflix-ratings',
          default_args=default_args,
          description='Load and transform imdb, rotten tomatoes and netflix data in Redshift with Airflow'
          )

start_operator = DummyOperator(task_id='start_execution', dag=dag)
create_tables = DummyOperator(task_id='create_tables', dag=dag)

# Stage the tables
stage_imdb_titles = DummyOperator(task_id='stage_imdb_titles_to_redshift', dag=dag)
stage_imdb_principals = DummyOperator(task_id='stage_imdb_principals', dag=dag)
stage_imdb_crew = DummyOperator(task_id='stage_imdb_crew', dag=dag)
stage_imdb_ratings = DummyOperator(task_id='stage_imdb_ratings', dag=dag)
stage_imdb_names = DummyOperator(task_id='stage_imdb_names', dag=dag)
stage_rt_titles = DummyOperator(task_id='stage_rt_titles', dag=dag)
stage_netflix_titles = DummyOperator(task_id='stage_netflix_titles', dag=dag)

# Load tables
load_roles = DummyOperator(task_id='load_roles', dag=dag)
load_genres = DummyOperator(task_id='load_genres', dag=dag)
load_persons = DummyOperator(task_id='load_persons', dag=dag)
load_titles = DummyOperator(task_id='load_titles', dag=dag)

# Run quality checks
run_checks = DummyOperator(task_id='run_quality_checks', dag=dag)
end_operator = DummyOperator(task_id='end_execution', dag=dag)

start_operator >> create_tables
create_tables >> stage_imdb_titles
create_tables >> stage_imdb_names
create_tables >> stage_imdb_principals
create_tables >> stage_imdb_crew
create_tables >> stage_rt_titles
create_tables >> stage_netflix_titles
create_tables >> stage_imdb_ratings

stage_imdb_titles >> load_genres
stage_imdb_names >> load_persons

stage_imdb_titles >> load_roles
stage_imdb_principals >> load_roles
stage_imdb_crew >> load_roles

stage_netflix_titles >> load_titles
stage_imdb_ratings >> load_titles
stage_rt_titles >> load_titles
load_roles >> load_titles
load_persons >> load_titles
