from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

import logging

log = logging.getLogger(__name__)

# =============================================================================
# 1. Set up the main configurations for DAG
# =============================================================================

default_args = {
    'start_date': datetime(2022, 4, 3),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('test_spark', 
          schedule_interval='@once', 
          catchup=False,
          default_args=default_args)


cmd = f'airflow connections delete \'ssh_spark_conn\'; \
        airflow connections add \'ssh_spark_conn\' \
         --conn-type \'ssh\' \
        --conn-login \'master\' \
        --conn-password  \'master\' \
        --conn-host \'spark-server\' '
'''
This task programatically adds a airflow connectio for ssh server
'''
housekeeper_task = BashOperator(
    task_id='create_ssh_conn',
    bash_command=cmd,
    dag=dag
)


'''
Next task makes use of the abv ssh connection
and executes a PI calculation example via spark
on spark server
'''
#sparkcmd = '/usr/spark-2.4.0/bin/spark-submit  $SPARK_HOME/examples/src/main/python/pi.py '

sparkcmd = '/usr/spark-2.4.0/bin/spark-submit  /home/master/scripts/mllib_spark.py '


spark_test= SSHOperator(
    ssh_conn_id='ssh_spark_conn',
    task_id='spark_run_pi_example',
    command= sparkcmd,
    dag=dag) 

housekeeper_task >>  spark_test