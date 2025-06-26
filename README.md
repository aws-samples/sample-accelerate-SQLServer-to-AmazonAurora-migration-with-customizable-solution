# SQLServer-To-Aurora-Miration-Accelerator

Modernizing your data infrastructure has become not just a competitive edge, but a necessity for businesses. You may want to reduce the high licensing costs of your SQL Server databases or switch to a more cloud-native database solution such as Amazon Aurora, but complex schema transformations during data migration can seem daunting. To accelerate your migration journey, we have developed a migration framework that offers ease and flexibility for customers seeking to migrate multi-terabyte SQL Server databases to Aurora PostgreSQL. This framework, which achieves fast data migration and minimum downtime, can be customized to meet specific business requirements. 
In this post, we showcase the core features of the migration framework, demonstrated through a complex use case of consolidating 32 clusters into a single instance with near-zero downtime, while addressing technical debt through refactoring. The framework’s capabilities include re-entrant points, decoupled and parallel executions, and configurability, enabling seamless database application migrations through multi-threaded processing, chunking imports, and checksum validation. We walk you through the step-by-step solution implementation that you can use to reinvent your data infrastructure. Finally, we discuss the solution benefits, including flexibility, scalability, observability, reusability, and resilience.
Solution overview 
Our solution provides a robust framework for migrating large-scale SQL Server databases to Amazon Aurora PostgreSQL-Compatible Edition, with built-in support for handling distributed data sources, complex transformations, and schema conversions. The framework is designed with several key features that provide reliable, efficient, and configurable migrations while maintaining data consistency in live environments.
The solution implements three critical capabilities: 
•	Re-entrant points allow jobs to resume from where they left off after interruptions, making the migration process resilient to failures
•	The decoupled execution model separates export, transform, and import operations into independent components that can run in parallel, significantly improving throughput for large-scale migrations
•	The framework offers extensive configurability through configurable parameters and mapping files, enabling you to define custom transformations and control execution parameters without modifying the core migration logic

## Documentation

Data migration between heterogeneous database systems is a common challenge in enterprise environments. This solution offers a flexible approach that can handle both straightforward 1:1 source-to-target table mappings and more complex scenarios involving data transformations and table joins.
For simple migrations where the source and target table structures are identical or very similar, this solution can directly map and transfer data with minimal transformation. This is particularly useful for large-scale migrations where maintaining data integrity and structure is paramount.
However, the real power of this solution lies in its ability to handle more complex migration scenarios. It can perform data transformations, combine data from multiple source tables, and create new consolidated target tables. This is especially valuable when migrating from legacy systems to modern databases, where data restructuring is often necessary to optimize for new business requirements.

To demonstrate this capability, let's consider a more complex example. Let's say we have two tables (cell_performance_data and ue_statistics_data) in the source SQL Server database, and we want to perform a heterogeneous migration of these tables to the target PostgreSQL database. Instead of simply copying these tables as-is, we want to create a new, consolidated target table called cell_perf_stats by joining the two source tables.

This join operation allows us to:
1.	Combine related data from multiple sources
2.	Potentially reduce data redundancy
3.	Create a more comprehensive view of the data for analysis in the target system

Here's how we might approach this join operation:

SELECT 
    c.cell_location,
    c.cell_id,
    c.bandwidth_usage_total,
    c.signal_strength_indicator_1,
    c.dropped_calls_total,
    u.device_id,
    u.data_usage_total,
    u.network_quality_indicator_1,
    u.handover_attempts,
    c.data_timestamp
FROM 	cell_performance_data c
LEFT JOIN 
        ue_statistics_data u
ON 	c.cell_id        = u.device_id
AND 	c.data_timestamp = u.data_timestamp
;

One of the ways to achieve this is to perform the join in the source database and then move the joined data to the target database, but this approach has some major drawbacks:
•	With a table containing billions of records, joining on the source database, which is serving live queries, is going to have impact on the database performance
•	You would need a considerable amount of database space, which could be costly
•	If you have these tables distributed between several source servers or databases, it will be lot of manual and time-consuming work to perform this join across all the databases and then move all those to the target database
The solution described in the following sections helps mitigate these issues.

## Prerequisites

For this walkthrough, you should have the following prerequisites: 
•	AWS account
•	Git
•	Python 3.7 or higher
•	SQL Server database (source)
•	Aurora PostgreSQL database (target)

## Set up source and target databases

Complete the following steps to set up your source and target databases:
1.	Create tables in the source SQL Server database:

    -- Table 1: cell_performance_data
    CREATE TABLE cell_performance_data (
        cell_location STRING,
        cell_id STRING,
        power_consumption_idle DECIMAL(11,3),
        power_consumption_peak DECIMAL(11,3),
        bandwidth_usage_total DECIMAL(11,3),
        measurement_time TIMESTAMP,
        signal_strength_indicator_1 INT,
        signal_strength_indicator_2 INT,
        dropped_calls_total INT,
        antenna_tilt SMALLINT,
        channel_utilization SMALLINT,
        data_timestamp TIMESTAMP,
        PRIMARY KEY (cell_id, data_timestamp)
    );

    -- Table 2: ue_statistics_data
    CREATE TABLE ue_statistics_data (
        device_coordinate STRING,
        device_id STRING,
        data_usage_download DECIMAL(11,3),
        data_usage_upload DECIMAL(11,3),
        data_usage_total DECIMAL(11,3),
        connection_time TIMESTAMP,
        total_connected_time INT,
        total_idle_time INT,
        total_active_time INT,
        network_quality_indicator_1 STRING,
        handover_attempts INT,
        data_timestamp TIMESTAMP,
        PRIMARY KEY (device_id, data_timestamp),
        FOREIGN KEY (device_id) REFERENCES cell_performance_data(cell_id)
    );

2.	Create the target table in Aurora PostgreSQL-Compatible:

    CREATE TABLE cell_perf_stats (
        cell_location STRING,
        cell_id STRING,
        bandwidth_usage_total DECIMAL(11,3),
        signal_strength_indicator_1 INT,
        dropped_calls_total INT,
        device_id STRING,
        data_usage_total DECIMAL(11,3),
        network_quality_indicator_1 STRING,
        handover_attempts INT,
        data_timestamp TIMESTAMP
    );

## Set up AWS Glue ETL

Complete the following steps to set up AWS Glue extract, transform, and load (ETL) jobs:
1.	Create the following S3 buckets:
    a.	raw-mssql-dev-us-west-1: For exported data from SQL Server
    b.	transformed-dev-us-west-1: For transformed data
    c.	stats-dev-us-west-1: For job run statistics
2.	Create a secret in Secrets Manager for SQL Server authentication:

    {
  "username": "",
  "password": "",
  "engine": "sqlserver",
  "host": "*",
  "port": "1433",
  "dbname": "DatabaseA"
    }

3.	Create an IAM role for AWS Glue jobs with access to S3 buckets and Secrets Manager, and attach the AWSGlueServiceRole policy.
4.	Import the following AWS Glue jobs:
    a.	ue_statistics_data_ExportMssqlToRaw.json
    b.	cell_performance_data_ExportMssqlToRaw.json
    c.	cellPerfStats_rawToTransformed.json
    d.	truncate_stageTableAurora.json
    e.	cellPerfStats_transformedToAurora.json
5.	Update job parameters on each job’s Job details tab with your specific S3 bucket, Secrets Manager secret, and other details.

## Set up the orchestration script and Fargate

In the following code examples, replace <ACC-ID> with the actual AWS account number where you are deploying this solution:
1.	Create two Parameter Store entries:
    a.	Migration control parameter:

    Name: /migration-parameter/cell-perf-stats
    Value:
    {
        "job_mode": "resume",
        "log_level": "INFO",
        "input_partition": "20250101",
        "partition_col_date": "data_timestamp",
        "source_filter_cell_performance_data": "data_timestamp BETWEEN '2025-01-01 00:00:00:000' AND '2025-03-31 23:59:59' ",
        "source_filter_ue_statistics_data": "data_timestamp BETWEEN '2022-10-01 00:00:00:000' AND '2025-03-31 23:59:59' "
    }

    b.	Server mappings:

    Name: /migration-parameter/server-mappings
    Value:
    {
        "server_mappings": {
            "server01": "sql-server-host-name:1433"
        },
        "databases": ["DatabaseA", "DatabaseB"],
        "categories": {
            "cell_perf_stats": [
                "cell_performance_data",
                "ue_statistics_data"
            ]
        }
    }

Now you’re ready to set up Amazon ECR and Amazon ECS.

2.	Create a docker file “Dockerfile” with below dependencies and commands:

    # Install all necessary system packages:
    RUN apt-get update && \
        apt-get install -y \
        build-essential \
        zlib1g-dev \
        libncurses5-dev \
        libgdbm-dev \
        libnss3-dev \
        libssl-dev \
        libreadline-dev \
        libffi-dev \ 
        curl \
        wget \
        git \
        jq \
        nodejs \
        npm \
        python3 \
        python3-pip \
        python3-venv \
        unzip
    
    # Install AWS CLI:
    RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip
    RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
    # Add local bin to PATH
    ENV PATH="/root/.local/bin:${PATH}"
    
    # Create and activate virtual environment
    RUN python3 -m venv /opt/venv
    ENV PATH="/opt/venv/bin:$PATH"

    COPY dev-requirements.txt .
    RUN pip install -r dev-requirements.txt -q
    COPY e2e_wrapper_cell_perf_stats.py .
    # Set the default command
    CMD ["python3", "-u", "e2e_wrapper_cell_perf_stats.py"]

3.	Build the Docker image:

    docker build -t data-mig-app:latest .

4.	Create the ECR repository:

    aws ecr create-repository --repository-name data-mig-app

5.	Authenticate Docker to Amazon ECR:

    aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin <ACC-ID>.dkr.ecr.us-west-1.amazonaws.com

6.	Tag and push the image to Amazon ECR:

    docker tag data-mig-app:latest <ACC-ID>.dkr.ecr.us-west-1.amazonaws.com/data-mig-app
    docker push <ACC-ID>.dkr.ecr.us-west-1.amazonaws.com/data-mig-app

7.	Create an ECS task execution role:

    aws iam create-role --role-name ecsTaskExecutionRole --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
        "Effect": "Allow",
        "Principal": {
            "Service": "ecs-tasks.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
        }
    ]
    }'

    aws iam attach-role-policy --role-name ecsTaskExecutionRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

8.	Create a task definition (task-definition.json). Update the environment variables in the following task according to your environment:

    {
    "family": "data-mig-app",
    "networkMode": "awsvpc",
    "executionRoleArn": "arn:aws:iam::<ACC-ID>:role/ecsTaskExecutionRole",
    "containerDefinitions": [
        {
        "name": "data-mig-app",
        "image": "<ACC-ID>.dkr.ecr.us-west-1.amazonaws.com/data-mig-app:latest",
        "portMappings": [
            {
            "containerPort": 80,
            "hostPort": 80,
            "protocol": "tcp"
            }
        ],
        "environment": [
            {
            "name": "MIGRATION_PARAMETER",
            "value": "/migration-parameter/cell-perf-stats"
            },
            {
            "name": "SERVER_MAPPINGS_PARAMETER",
            "value": "/migration-parameter/server-mappings"
            },
            {
            "name": "RDS_ENDPOINT",
            "value": "aurora-pg-cluster.cluster-mydb.eu-west-1.rds.amazonaws.com"
            },
            {
            "name": "RDS_USER",
            "value": "mydb_user"
            }
        ],
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
            "awslogs-group": "/ecs/data-mig-app",
            "awslogs-region": "us-west-1",
            "awslogs-stream-prefix": "ecs"
            }
        },
        "essential": true
        }
    ],
    "requiresCompatibilities": [
        "FARGATE"
    ],
    "cpu": "256",
    "memory": "512"
    }

9.	Register the task definition:

    aws ecs register-task-definition --cli-input-json file://task-definition.json

10.	Create an ECS cluster:

    aws ecs create-cluster --cluster-name data-mig-app-cluster

11.	Create the Fargate service. Replace the subnets and security groups in the following command according to your account setup:

    aws ecs create-service \
  --cluster data-mig-app-cluster \
  --service-name data-mig-app-srv \
  --task-definition data-mig-app \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet1- xxxxx,subnet2- xxxxx],securityGroups=[sg- xxxxx],assignPublicIp=ENABLED}"

## Launch the ECS migration task
Execute the ECS task with the following code. Replace the subnets and security groups according to your account setup:

    aws ecs run-task \
  --cluster your-cluster-name \
  --task-definition data-mig-app \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxxxx],securityGroups=[sg-xxxxx]}" \
  --launch-type FARGATE \
  --region us-west-1

## Complete the data migration using a stored procedure

Create the stored procedure in Aurora PostgreSQL-Compatible using the script stage.stage_ctas_null_upd_cell_perf_stats.sql, then run the procedure with the following command:
    CALL stage.stage_ctas_null_upd_cell_perf_stats('database', 'cell_perf_stats');
