from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'susannah',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': False
}

dag_name = 'sparkify_project_dag' 

dag = DAG(dag_name,
          default_args=default_args,
          description='Extract Load and Transform data from S3 to Redshift',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    table="staging_events",
    json_option="s3://udacity-dend/log_json_path.json",
    provide_context=True,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    table="staging_songs",
    json_option="auto",
    provide_context=True,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplays_table_insert,
    dag=dag
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.users_table_insert,
    mode='truncate',
    dag=dag
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.songs_table_insert,
    mode='truncate',
    dag=dag
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artists_table_insert,
    mode='truncate',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,
    mode='truncate',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=["songplays","artists","songs","time","users"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift,
                   stage_songs_to_redshift] >> load_songplays_table 
                   
load_songplays_table >> [load_users_dimension_table, load_songs_dimension_table,
                        load_artists_dimension_table, load_time_dimension_table] >> run_quality_checks
 
run_quality_checks >> end_operator
