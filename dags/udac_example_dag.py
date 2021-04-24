from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity'   
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.now(),
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)




stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',    
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    table='staging_events',
    aws_credentials_id='aws_conn_id',
    redshift_conn_id="redshift",
    copy_options="JSON 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='song_data/A/A/',
    table='staging_songs',
    aws_credentials_id='aws_conn_id',
    redshift_conn_id="redshift",
    copy_options="FORMAT AS JSON 'auto'"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    load_sql_stmt=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    load_sql_stmt=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    load_sql_stmt=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    load_sql_stmt=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    load_sql_stmt=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',    
    redshift_conn_id="redshift",
    tables=["songplays", "songs", "artists",  "time", "users"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator>>stage_events_to_redshift>>stage_songs_to_redshift
#start_operator>>stage_songs_to_redshift
stage_songs_to_redshift>>load_user_dimension_table
stage_events_to_redshift>>load_user_dimension_table

stage_songs_to_redshift>>load_songplays_table
stage_events_to_redshift>>load_songplays_table

stage_songs_to_redshift>>load_artist_dimension_table
stage_events_to_redshift>>load_artist_dimension_table

stage_songs_to_redshift>>load_time_dimension_table
stage_events_to_redshift>>load_time_dimension_table

stage_songs_to_redshift>>load_song_dimension_table
stage_events_to_redshift>>load_song_dimension_table

load_song_dimension_table>>run_quality_checks


run_quality_checks>>end_operator
