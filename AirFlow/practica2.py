from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from utils import *
from shutil import rmtree

# Airflow variable
path_workflow = Variable.get ("path_workflow")

# Default Arguments that we can use when creating tasks
# These args will get passed on to each operator
default_args = {
    'owner': 'Mar',
    'depends_on_past': False,
    'start_date': days_ago(1), # days_ago.now()
    'email': ['maralguacil@correo.ugr.es'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Instantiate the DAG
dag = DAG( 'CCSA-Forecast',
            default_args=default_args,
            description='CC - Practical assignment 2',
            schedule_interval=None,#timedelta(days=1),
         )


#######################################################################
#                                                                     #
# PREPARE ENVIROMENT                                                  #
#   1) Create the new folder from the environment variable            #
#   'path_workflow'.                                                  #
#                                                                     #
#######################################################################
PrepareEnviroment = BashOperator(task_id='PrepareEnviroment',
                                 depends_on_past=False,
                                 bash_command='mkdir -p {{var.value.path_workflow}}',
                                 dag=dag
                                )

#######################################################################
#                                                                     #
# EXTRACT DATA                                                        #
#   1) Download the data from the Manu Parra's repository.            #
#   2) Unzip data.                                                    #
#                                                                     #
#######################################################################
GetDataA = BashOperator( task_id='GetDataHumidity',
                         depends_on_past=False,
                         bash_command='wget --output-document={{var.value.path_workflow}}/humidity.csv.zip \
                         https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip && \
                         unzip {{var.value.path_workflow}}/humidity.csv.zip -d {{var.value.path_workflow}}',
                         dag=dag
                       )

GetDataB = BashOperator( task_id='GettDataTemperature',
                          depends_on_past=False,
                          bash_command= 'wget --output-document={{var.value.path_workflow}}/temperature.csv.zip \
                          https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip && \
                          unzip {{var.value.path_workflow}}/temperature.csv.zip -d {{var.value.path_workflow}}',
                          dag=dag
                        )

#######################################################################
#                                                                     #
# PROCESS DATA                                                        #
#   1) Extract DATETIME and SAN FRANCISCO columns from humidity.csv   #
#   and temperature.csv.                                              #
#   2) Create a new dataset with the following columns:               #
#       - DATE - intersection of the DATETIME columns from two both   #
#   datasets.                                                         #
#       - TEMP - SAN FRANCISCO column from temperature.csv.           #
#       - HUM  - SAN FRANCISCO column from humidity.csv.              #
#   3) Store the new dataset in MongoDB.                              #
#                                                                     #
#######################################################################
ProcessData = PythonOperator( task_id='ProcessData',
                              provide_context=True,
                              python_callable=mergeDataSets,
                              op_kwargs={
                                  'hum_file'   : '{{var.value.path_workflow}}/humidity.csv',
                                  'temp_file'  : '{{var.value.path_workflow}}/temperature.csv'
                              },
                              dag=dag
                            )

#######################################################################
#                                                                     #
# CREATE ARIMA MODEL                                                  #
#   1) Extract dataset from MongoDB.                                  #
#   2) Train with the humidity and temperature sets.                  #
#   3) Stores the ARIMA models in picke files.                        #
#                                                                     #
#######################################################################
TrainARIMA = PythonOperator( task_id='TrainARIMA',
                             provide_context=True,
                             python_callable=trainARIMA,
                             op_kwargs={
                                 'path' : str(Path.home())+'/.models/'
                             },
                             dag=dag
                           )

#######################################################################
#                                                                     #
# CREATE RANDOM FOREST MODEL                                          #
#   1) Extract dataset from MongoDB.                                  #
#   2) Train with the humidity and temperature sets.                  #
#   3) Stores the Random Forest models with pickle.                   #
#                                                                     #
#######################################################################
TrainRF = PythonOperator( task_id='TrainRF',
                          provide_context=True,
                          python_callable=trainRandomForest,
                          op_kwargs={
                              'path' : str(Path.home())+'/.models/'
                          },
                          dag=dag
                        )

#######################################################################
#                                                                     #
# CLONE REPOSITORY                                                    #
#   1) Clone CCSA-AirFlow-Forecast repository into                    #
#   'path_workflow'/services folder.                                  #
#                                                                     #
#######################################################################
CloneRepo = BashOperator( task_id='CloneRepo',
                          depends_on_past=False,
                          bash_command= 'git clone https://github.com/MarAl15/CCSA-AirFlow-Forecast.git \
                          {{var.value.path_workflow}}/services',
                          dag=dag
                        )

#######################################################################
#                                                                     #
# RUN UNIT TESTS                                                      #
#   1) Run all unit tests.                                            #
#                                                                     #
#######################################################################
RunUnitTestsV1 = BashOperator( task_id='RunUnitTestsV1',
                          depends_on_past=False,
                          bash_command= 'python3 {{var.value.path_workflow}}/services/test.py v1',
                          dag=dag
                        )

RunUnitTestsV2 = BashOperator( task_id='RunUnitTestsV2',
                          depends_on_past=False,
                          bash_command= 'python3 {{var.value.path_workflow}}/services/test.py v2',
                          dag=dag
                        )

#######################################################################
#                                                                     #
# DEPLOY RESTFUL API APPS                                             #
#   1) Build a Docker image from the Dockerfile.                      #
#   2) Deploy the microservice on a specific port.                    #
#                                                                     #
#######################################################################
DeployAPIv1 = BashOperator( task_id='DeployAPIv1',
                            depends_on_past=False,
                            bash_command= 'docker build --rm --build-arg VERSION=v1 -t api_v1 \
                            {{var.value.path_workflow}}/services && \
                            docker run -d --rm --name apiV1 -e PORT=5000 \
                            -v ~/.models/:/root/.models/ -p 5000:5000 api_v1',
                            dag=dag
                          )

DeployAPIv2 = BashOperator( task_id='DeployAPIv2',
                            depends_on_past=False,
                            bash_command= 'docker build --rm  --build-arg VERSION=v2 -t api_v2 \
                            {{var.value.path_workflow}}/services && \
                            docker run -d --rm --name apiV2 -e PORT=8000 \
                            -v ~/.models/:/root/.models/ -p 8000:8000 api_v2',
                            dag=dag
                          )

#######################################################################
#                                                                     #
# CLEAN UP                                                            #
#   1) Remove 'path_workflow' folder.                                 #
#                                                                     #
#######################################################################
CleanUp = PythonOperator( task_id='CleanUp',
                          python_callable=rmtree,
                          op_kwargs={
                            'path' : '{{var.value.path_workflow}}'
                          },
                          dag=dag
                        )


# Setting up Dependencies
PrepareEnviroment >> [GetDataA, GetDataB] >> ProcessData >> [TrainARIMA,TrainRF] >> CloneRepo >> [RunUnitTestsV1, RunUnitTestsV2]

RunUnitTestsV1.set_downstream(DeployAPIv1)
RunUnitTestsV2.set_downstream(DeployAPIv2)

DeployAPIv1 >> CleanUp << DeployAPIv2
