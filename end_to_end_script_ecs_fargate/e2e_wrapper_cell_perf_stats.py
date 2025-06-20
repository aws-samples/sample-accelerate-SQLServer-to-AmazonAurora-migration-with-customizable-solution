"""End to end wrapper for big table ETL"""

import argparse
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
import logging
import boto3
from enum import Enum

#configure the logging settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

#create a logger for this script
logger = logging.getLogger(__name__)

# Initialize SSM client
ssm = boto3.client('ssm')

# Read from Parameter Store
def get_parameter(parameter_name):
    try:
        response = ssm.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"Error getting parameter {parameter_name}: {str(e)}")
        raise

    
#Get all environment and parmater values
target_region = os.environ.get('AWS_TARGET_REGION', '')
target_account_id = os.environ.get('TARGET_ACCOUNT_ID', '')
log_level = os.environ.get('LOG_LEVEL', 'WARNING')
migration_parameter = os.environ.get('MIGRATION_PARAMETER', '')
server_mappings_parameter = os.environ.get('SERVER_MAPPINGS_PARAMETER', ' ')

stage = os.environ.get('ENVIRONMENT_NAME', "development")
transformed_bucket = os.environ.get(
    'TRANSFORMED_BUCKET', "<your-transformed-data-bucket>"
)
raw_bucket = os.environ.get('RAW_BUCKET', "<your-raw-data-bucket>")
glue_bucket = os.environ.get('GLUE_BUCKET', "<your-glue-asset-bucket>")
stats_bucket = os.environ.get('STATS_BUCKET', "<your-migration-stats-bucket>")
rds_endpoint = os.environ.get(
    'RDS_ENDPOINT', "aurora-pg-cluster.cluster-mydb.eu-west-1.rds.amazonaws.com"
)
rds_port = os.environ.get('RDS_PORT', "5432")
rds_user = os.environ.get('RDS_USER', "mydb_user")
platform_name = os.environ.get('PLATFORM_NAME', '')


migration_patameter_values = json.loads(migration_parameter)
print(f"migration parameter, {migration_patameter_values}")

server_mappings_values = json.loads(migration_parameter)
print(f"server parameter, {server_mappings_values}")


job_mode = migration_patameter_values["job_mode"]
input_partition = migration_patameter_values["input_partition"]
source_filter_cell_performance_data = migration_patameter_values["source_filter_cell_performance_data"]
source_filter_ue_statistics_data = migration_patameter_values["source_filter_ue_statistics_data"]

print(f"job mode is, {job_mode}")
print(f"input partition is, {input_partition}")
print(f"source filter cell_performance_data, {source_filter_cell_performance_data}")
print(f"source filter ue_statistics_data, {source_filter_ue_statistics_data}")

json_data = json.loads(server_mappings_values)
server_ip_mapping = json_data.get('server_mappings', {})

print(f"json data is, {json_data}")
print(f"server mapping is, {server_ip_mapping}")

class Job_Type(Enum):

    EXPORTMSSSQL = "ExportMssqlToRaw"
    RAWTOTRANSFORMED = "rawToTransformed"
    TRUNCATEAURORA = "truncate_stageTableAurora"
    TRANSFORMEDTOAURORA = "transformedToAurora"


def start_glue_job(glue, task, job_type):
    print(f"Job type is {job_type}")

    partition_col_date = ""
    source_filter = ""

    table = ""
    ip_address = ""
    database = ""
    server = ""
    database_lowercase = ""

    if not job_type == Job_Type.TRUNCATEAURORA.value:
        table = task["table"]
        ip_address = task["ip_address"]
        database = task["database"]
        server = task["server_num"]

        database_lowercase = database[0].lower() + database[1:]

    table_mapping = {
        "ue_statistics_data": {
            "source_table": "ue_statistics_data"
        },
        "cell_performance_data": {
            "source_table": "cell_performance_data"
        }
    }
    source_table = ""

    if job_type == Job_Type.EXPORTMSSSQL.value:
        job_name = f'{table}_{job_type}'
        source_table = table_mapping[table]["source_table"]
    elif job_type == Job_Type.RAWTOTRANSFORMED.value:
        job_name = f'cellPerfStats_{job_type}'
    elif job_type == Job_Type.TRUNCATEAURORA.value:
        job_name = "truncate_stageTableAurora"
    else:
        job_name = f'cellPerfStats_{job_type}'

    secret_name = f"{stage}/secret/reveal/aurora-migration/db"

    if job_name.startswith(f"ue_statistics_data"):
        source_filter = source_filter_ue_statistics_data
        partition_col_date = "data_timestamp"
    elif job_name.startswith(f"cell_performance_data"):
        source_filter = source_filter_ue_statistics_data
        partition_col_date = "data_timestamp"

    logger.info("job name is: %s", job_name)

    base_args = {
        Job_Type.EXPORTMSSSQL.value:{
            "--aws_region": f"{target_region}",
            "--driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "--flag_file_path_prefix": "export_flag/",
            "--input_partition_yyyymmdd": f"{input_partition}",
            "--job_mode": f"{job_mode}",
            "--log_level": f"{log_level}",
            "--output_format": "parquet",
            "--output_num_files": "0",
            "--partition_col_date": f"{partition_col_date}",
            "--secret_name": f"{secret_name}",
            "--source_filter": f"{source_filter}",
            "--target_data_bucket": f"{raw_bucket}",
            '--jdbc_url': f"jdbc:sqlserver://{ip_address}:1433;database={database}",
            "--source_table": source_table,
            "--stats_data_bucket": stats_bucket,
            "--target_path_prefix": f"{server}/{database_lowercase}/dbo/{source_table}/"
        },
        Job_Type.RAWTOTRANSFORMED.value: {
            "--flag_file_path_prefix": "transformed_flag/",
            "--input_format": "parquet",
            "--input_partition_yyyymmdd": f"{input_partition}",
            "--job_mode": f"{job_mode}",
            "--log_level": f"{log_level}",
            "--output_format": "parquet",
            "--output_num_files": "10",
            "--output_path": f"s3://{transformed_bucket}/",
            "--src_data_bucket": f"{raw_bucket}",
            "--source_table": source_table,
            "--src_data_prefix": f"{server}/{database_lowercase}/dbo/cell_performance_data/"
        },
        Job_Type.TRUNCATEAURORA.value: {
            "--aws_region": f"{target_region}",
            "--input_partition_yyyymmdd": f"{input_partition}",
            "--job_mode": f"{job_mode}",
            "--log_level": f"{log_level}",
            "--partition_col_date" : "data_timestamp",
            "--rds_database_name": "database",
            "--rds_endpoint": f"{rds_endpoint}",
            "--rds_tcp_port": f"{rds_port}",
            "--rds_user": f"{rds_user}",
            "--schema_name": "stage",
            "--table_name": "CELL_PERF_STATS"
        },
        Job_Type.TRANSFORMEDTOAURORA.value: {
            "--aws_region": f"{target_region}",
            "--flag_file_path_prefix": "import_flag/",
            "--input_format": "parquet",
            "--input_partition_yyyymmdd": f"{input_partition}",
            "--job_mode": f"{job_mode}",
            "--log_level": f"{log_level}",
            "--rds_database_name": "database",
            "--rds_endpoint": f"{rds_endpoint}",
            "--rds_tcp_port": f"{rds_port}",
            "--rds_user": f"{rds_user}",
            "--src_data_bucket": f"{transformed_bucket}",
            "--src_data_prefix": f"{server}/{database_lowercase}/stage/CELL_PERF_STATS/"
        }
    }

    job_args = base_args[job_type]
    
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=job_args
        )
        run_id = response['JobRunId']

        print(f"Started {job_name} for {database} with run ID {run_id}")

        while True:
            response_status = glue.get_job_run(JobName=job_name, RunId=run_id)
            status = response_status['JobRun']['JobRunState']
            if status == 'SUCCEEDED':
                print(f"Job {job_name} completed successfully")
                return True
            elif status in ('FAILED', 'STOPPED'):
                print(f"Job {job_name} failed")
                return False
            print(f"Job {job_name} is {status}")
            time.sleep(30)

    except Exception as err:
        logger.error(f"glue job failed due to: {err}")
    
    

def main(extra_args):
    job_type = Job_Type.EXPORTMSSSQL.value

    all_export_jobs_successful = False
    all_raw_to_transformed_successful = False
    all_truncate_jobs_successful = False
    all_transformed_to_aurora_jobs_successful = False

    with ThreadPoolExecutor() as executor:
        session = boto3.session.Session()

        glue = session.client('glue', region_name='<region-name>')
        
        for server_num, ip_address in server_ip_mapping.items():

            for tables in json_data["categories"]['CELL_PERF_STATS']:
                tasks = []

                for database in json_data["databases"]:

                    tasks.extend([
                        {
                            'server_num': server_num,
                            'database': database,
                            'table': tables,
                            'ip_address': ip_address
                        }
                    ])

                print(tasks)
                results = executor.map(lambda task: start_glue_job(glue, task, job_type), tasks)
                
                if all(results):
                    all_export_jobs_successful = True
                else: 
                    print("Some export glue jobs failed")

    if all_export_jobs_successful:
        job_type = Job_Type.RAWTOTRANSFORMED.value

        with ThreadPoolExecutor() as executor:
            session = boto3.session.Session()

            glue = session.client('glue', region_name='<region-name>')
            
            for server_num, ip_address in server_ip_mapping.items():

                tasks = []
            
                for database in json_data["databases"]:

                    tasks.extend([
                        {
                            'server_num': server_num,
                            'database': database,
                            'table': 'cell_performance_data',
                            'ip_address': ip_address
                        }
                    ])

                print(tasks)
                results = executor.map(lambda task: start_glue_job(glue, task, job_type), tasks)
                    
                if all(results):
                    all_raw_to_transformed_successful = True
                else:
                    print("Some Transform Glue Jobs failed")

    if all_raw_to_transformed_successful:
        job_type = Job_Type.TRUNCATEAURORA.value

    
        session = boto3.session.Session()

        glue = session.client('glue', region_name='<region-name>')
        all_truncate_jobs_successful = True

        task = {}
        
        results = start_glue_job(glue, task, job_type)
            
        if results:
            all_truncate_jobs_successful = True
        else:
             print("Some Truncate Glue Jobs failed")

    if all_truncate_jobs_successful:
        job_type = Job_Type.TRANSFORMEDTOAURORA.value

        with ThreadPoolExecutor() as executor:
            session = boto3.session.Session()

            glue = session.client('glue', region_name='<region-name>')
            for server_num, ip_address in server_ip_mapping.items():

                tasks = []
            
                for database in json_data["databases"]:

                    tasks.extend([
                        {
                            'server_num': server_num,
                            'database': database,
                            'table': 'cell_performance_data',
                            'ip_address': ip_address
                        }
                    ])

                print(tasks)
                results = executor.map(lambda task: start_glue_job(glue, task, job_type), tasks)
                    
                if all(results):
                    all_transformed_to_aurora_jobs_successful = True
                else: 
                    print("Some transform to aurora glue jobs failed")

    if all_transformed_to_aurora_jobs_successful:
        print("CELL_PERF_STATS tables data loaded to aurora")
    else:
        print("some glue jobs failed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--extra_args', type=str, nargs='*', help='Additional key-value pair arguments', required=False)

    args = parser.parse_args()
    extra_args_dict = dict(arg.split('=') for arg in args.extra_args) if args.extra_args else {}

    main(extra_args_dict)