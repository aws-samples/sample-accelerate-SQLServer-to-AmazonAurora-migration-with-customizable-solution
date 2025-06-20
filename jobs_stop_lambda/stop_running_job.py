import boto3
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ecs_client = boto3.client('ecs')
glue_client = boto3.client('glue')

# Configuration
ECS_CONFIG = {
    'cluster': 'data-mig-app-cluster',
    'service': 'data-mig-app-srv'
}

GLUE_JOBS_TO_ABORT = [
    'cell_performance_data_ExportMssqlToRaw',
    'cellPerfStats_rawToTransformed',
    'cellPerfStats_transformedToAurora',
    'ue_statistics_data_ExportMssqlToRaw'
]

def get_running_ecs_tasks():
    """Get list of running ECS tasks"""
    try:
        response = ecs_client.list_tasks(
            cluster=ECS_CONFIG['cluster'],
            serviceName=ECS_CONFIG['service'],
            desiredStatus='RUNNING'
        )
        return response.get('taskArns', [])
    except Exception as e:
        logger.error(f"Error getting running ECS tasks: {str(e)}")
        return []

def get_running_glue_jobs():
    """Get list of running Glue jobs"""
    running_jobs = []
    try:
        for job_name in GLUE_JOBS_TO_ABORT:
            response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
            for job_run in response.get('JobRuns', []):
                if job_run['JobRunState'] in ['RUNNING', 'STARTING', 'STOPPING']:
                    running_jobs.append((job_name, job_run['Id']))
        return running_jobs
    except Exception as e:
        logger.error(f"Error getting running Glue jobs: {str(e)}")
        return []

def stop_ecs_task(task_arn):
    """Stop a running ECS task"""
    try:
        ecs_client.stop_task(cluster=ECS_CONFIG['cluster'], task=task_arn, reason='Stopped by Lambda function')
        logger.info(f"Stopped ECS task: {task_arn}")
    except Exception as e:
        logger.error(f"Error stopping ECS task {task_arn}: {str(e)}")

def abort_glue_job(job_name, run_id):
    """Abort a running Glue job"""
    try:
        glue_client.batch_stop_job_run(JobName=job_name, JobRunIds=[run_id])
        logger.info(f"Stopped Glue job: {job_name}, run ID: {run_id}")
    except Exception as e:
        logger.error(f"Error stopping Glue job {job_name}, run ID {run_id}: {str(e)}")

def lambda_handler(event, context):
    """Main Lambda handler"""
    logger.info("Starting process to stop ECS tasks and Glue jobs")
    
    results = {
        'ecs_tasks_stopped': 0,
        'glue_jobs_stopped': 0,
        'timestamp': datetime.utcnow().isoformat()
    }

    # Stop ECS tasks
    for task_arn in get_running_ecs_tasks():
        stop_ecs_task(task_arn)
        results['ecs_tasks_stopped'] += 1

    # Stop Glue jobs
    for job_name, run_id in get_running_glue_jobs():
        abort_glue_job(job_name, run_id)
        results['glue_jobs_stopped'] += 1

    logger.info(f"Process complete. Summary: {results}")

    return {
        'statusCode': 200,
        'body': results
    }
