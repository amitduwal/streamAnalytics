# Real-Time Streaming Analytics Pipeline for Health Monitoring

This project implements a real-time streaming analytics pipeline in AWS for health monitoring using AWS CDK. The system processes real-time health data streams, detects anomalies, stores data, and provides visualizations through OpenSearch dashboards.

## Architecture Overview

The pipeline includes:
- Data ingestion through Amazon Kinesis
- Cold storage in Amazon S3 via Kinesis Firehose
- Real-time processing with Kinesis Data Analytics
- Anomaly detection and alerts using Lambda and SNS
- Data storage in DynamoDB
- Visualization using Amazon OpenSearch
- Infrastructure as Code using AWS CDK

## Prerequisites

- Node.js (v14.x or later)
- AWS CLI installed and configured
- AWS CDK CLI installed
- An AWS account with appropriate permissions
- Git

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd health-monitoring-pipeline
```

2. Install dependencies:
```bash
npm install
```

3. Configure AWS CLI with your credentials:
```bash
aws configure
```

4. Bootstrap AWS CDK (required only once per account/region):
```bash
cdk bootstrap
```

5. Review the changes that will be deployed:
```bash
cdk diff
```

6. Deploy the stack:
```bash
cdk deploy
```

## Project Structure

```
health-monitoring-pipeline/
├── lib/                     # CDK stack definitions
├── bin/                     # CDK app entry point
├── Scripts/                 # Scripts for producer
├── test/                    # Unit tests
└── cdk.json                # CDK configuration
```

## Features

- Real-time health data ingestion
- Anomaly detection for health events (e.g., seizures)
- Automated alerts via SNS
- Long-term data storage in S3
- Real-time dashboards in OpenSearch
- Scalable and secure infrastructure

## Configuration

The following environment variables can be set in your `.env` file:

```
AWS_REGION=<your-aws-region>
STACK_NAME=<your-stack-name>
KINESIS_STREAM_NAME=<stream-name>
```

## Cleanup

To avoid incurring charges, destroy the stack when not in use:

```bash
cdk destroy
```

## Security

This project handles sensitive health data and implements several security measures:
- Encrypted data storage
- Secure API endpoints
- IAM roles with least privilege

## License

This project is licensed under the MIT License - see the LICENSE file for details.

