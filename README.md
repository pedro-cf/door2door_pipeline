# Door2Door Data Pipeline

## Introduction

Door2Door collects live vehicle positions from its fleet via GPS sensors in each vehicle. The vehicles operate during certain periods managed by Door2Door's operators. This data is collected via an API and stored on an S3 bucket in raw format. The goal of this pipeline is to provide a solution to automate the build of a simple, scalable data lake and data warehouse to enable the BI team to answer questions about the vehicle fleet.

This data pipeline automates the process of fetching, processing, and storing vehicle data for Door2Door. By using scalable technologies such as Apache Airflow, MinIO Buckets, and PSQL, this pipeline can handle large volumes of data and provide Door2Door's BI team with valuable insights into their vehicle fleet.

## Architecture

The data pipeline is built using the following technologies:

- Apache Airflow: a platform to programmatically author, schedule, and monitor workflows.
- MinIO Buckets: an object storage server that implements Amazon S3 APIs, which serves as a replacement for S3 buckets.
- PSQL: a powerful, open-source SQL database system.

The architecture of the pipeline is shown below:

![Door2Door Data Pipeline Architecture](https://i.imgur.com/lNibu5o.png)

## Installation & Usage

To run this pipeline, you need to have the following tools installed on your system:

- Docker
- Docker Compose

You can follow these steps to install and run the pipeline:

1. Clone the repository:

```bash
git clone https://github.com/pedro-cf/door2door_pipeline.git
cd door2door_pipeline
```

2. Set the environment variables in the `.env` file according to your needs.

3. Run locally with:

```
./run.sh
```

4. Access the Airflow web UI by visiting `http://localhost:8080` in your web browser.

5. Access the PSQL database with pgAdmin, f.e.

## Deployment

To deploy this solution with separate entities for MinIO, Apache Airflow, and PSQL Database on AWS cloud, you can follow these steps:

# Launch MinIO on a separate Amazon EC2 instance
1. Launch a EC2 instance for MinIO
2. Install and configure MinIO according to the official documentation
3. Configure the security group to allow incoming traffic from the Airflow instance

# Launch PostgreSQL on a separate Amazon RDS instance
1. Go to the Amazon RDS console and click "Create database"
2. Select "PostgreSQL" (or "PostGIS" preferebly) as the database engine and choose the version you want to use
3. Choose the instance class and the amount of storage you need
4. Configure the database settings, such as the database name, username, and password
5. Choose the VPC and subnet where you want to launch the database instance
6. Configure the security group to allow incoming traffic from the Airflow instance
7. Launch the RDS instance

# Launch Apache Airflow on an Amazon EC2 instance
1. Go to the AWS Management Console and navigate to the EC2 service
2. Click "Launch Instance" to create a new EC2 instance
3. Select an Amazon Machine Image (AMI) that supports Apache Airflow, such as the official Airflow AMI from the AWS Marketplace
4. Choose an instance type that meets your resource requirements
5. Configure the instance settings, such as the VPC and subnet where you want to launch the instance
6. Choose a key pair to securely connect to the instance
8. Launch the EC2 instance by clicking the "Launch" button
9. Connect to the EC2 instance using SSH
10. Install Apache Airflow by following the installation guide for the specific distribution you are using
11. Install the necessary dependencies for the DAG