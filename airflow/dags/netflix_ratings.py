from airflow import DAG
import datetime
from datetime import timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# TODO: fix this
import sys

sys.path.append('/Users/antoniam/Desktop/personal/netflix-ratings/airflow')

from plugins.operators.stage_redshift import StageToRedshiftOperator

default_args = {
    'owner': 'antoniam',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime.datetime(2015, 10, 10),
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'catchup': False
}

dag = DAG('netflix-ratings',
          default_args=default_args,
          description='Load and transform imdb, rotten tomatoes and netflix data in Redshift with Airflow'
          )

start_operator = DummyOperator(task_id='start_execution', dag=dag)
create_tables = PostgresOperator(task_id='create_tables',
                                 dag=dag,
                                 postgres_conn_id='redshift',
                                 sql='/sql/create_tables.sql')

# Stage the tables
stage_imdb_titles = StageToRedshiftOperator(
    task_id='Stage_titles',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="antoniam",
    s3_key="imdb/title.basics.tsv",
    table="staging_imdb_titles",
    delimiter='\t'
)
stage_imdb_principals = StageToRedshiftOperator(
    task_id='Stage_imdb_principals',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="antoniam",
    s3_key="imdb/title.principals.tsv",
    table="staging_imdb_principals",
    delimiter='\t'
)
stage_imdb_crew = StageToRedshiftOperator(
    task_id='Stage_imdb_crew',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="antoniam",
    s3_key="imdb/title.crew.tsv",
    table="staging_imdb_crew",
    delimiter='\t'
)

stage_imdb_ratings = StageToRedshiftOperator(
    task_id='Stage_imdb_ratings',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="antoniam",
    s3_key="imdb/title.ratings.tsv",
    table="staging_imdb_ratings",
    delimiter='\t'
)

stage_imdb_names = StageToRedshiftOperator(
    task_id='Stage_imdb_names',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="antoniam",
    s3_key="imdb/name.basics.tsv",
    table="staging_imdb_names",
    delimiter='\t'
)
stage_rt_titles = StageToRedshiftOperator(
    task_id='Stage_rt_titles',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="antoniam",
    s3_key="rotten_tomatoes/rotten_tomatoes_movies.csv",
    table="staging_rotten_tomatoes_titles",
    delimiter=','
)
stage_netflix_titles = StageToRedshiftOperator(
    task_id='Stage_netflix_titles',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="antoniam",
    s3_key="netflix/netflix_titles.csv",
    table="staging_netflix_titles",
    delimiter=','
)

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

load_titles >> run_checks
load_persons >> run_checks
load_roles >> run_checks
load_genres >> run_checks

run_checks >> end_operator
