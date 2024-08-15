import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesisfirehose from 'aws-cdk-lib/aws-kinesisfirehose';
import * as Sns from 'aws-cdk-lib/aws-sns';
import * as snssub from 'aws-cdk-lib/aws-sns-subscriptions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as kinesisanalytics from 'aws-cdk-lib/aws-kinesisanalytics';
import * as fs from 'fs';
import * as path from 'path';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { TableViewer } from 'cdk-dynamo-table-viewer';
import { Domain, EngineVersion } from 'aws-cdk-lib/aws-opensearchservice';

import * as dotenv from 'dotenv'
dotenv.config()


export class StreamAnalyticsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    // rootStream is a raw kinesis stream in which we build other modules on top of.
    const rootStream = new kinesis.Stream(this, 'RootStream', {
      streamMode: kinesis.StreamMode.PROVISIONED,
      shardCount: 1,
    }
    );

    // Output the stream name
    new cdk.CfnOutput(this, 'RootStreamName', {
      value: rootStream.streamName,
    });

    // =============================================================================
    // Cold Module:
    
    // The Cold Module reads raw data from the stream, compresses it, and sends it
    //  to S3 for later analysis
    
    // +---------+      +----------+      +-----------+
    // |         |      |          |      |           |
    // | Stream  +----->+ Firehose +----->+ S3 Bucket |
    // |         |      |          |      |           |
    // +---------+      +----------+      +-----------+

    //S3 BUCKET
    const rawDataBucket = new s3.Bucket(this, 'RawDataBucket', {
      bucketName: 'raw-data-bucket-234',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,// Empty bucket before deleting
    });

    //Output the bucket name
    new cdk.CfnOutput(this, 'RawDataBucketName', {
      value: rawDataBucket.bucketName,
    });

    //Firehose Role
    const firehoseRole = new iam.Role(this, 'FirehoseRole', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    rootStream.grantRead(firehoseRole);
    rootStream.grant(firehoseRole, 'kinesis:DescribeStream');

    //Allow firehose to write to the bucket
    rawDataBucket.grantReadWrite(firehoseRole);

    const firehoseStreamToS3 = new kinesisfirehose.CfnDeliveryStream(this, 'FirehoseStreamToS3', {
      deliveryStreamName: "StreamRawToS3",
      deliveryStreamType: "KinesisStreamAsSource",
      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: rootStream.streamArn,
        roleArn: firehoseRole.roleArn
      },
      s3DestinationConfiguration: {
        bucketArn: rawDataBucket.bucketArn,
        bufferingHints: {
          sizeInMBs: 64,
          intervalInSeconds: 60
        },
        compressionFormat: "GZIP",
        encryptionConfiguration: {
          noEncryptionConfig: "NoEncryption"
        },
    
        prefix: "raw/",
        roleArn: firehoseRole.roleArn
      },
    })

    // Ensures our role is created before we try to create a Kinesis Firehose
    firehoseStreamToS3.node.addDependency(firehoseRole)


    // =============================================================================
    // OpenSearch Module:

    const opensearchDomain = new Domain(this, 'Domain', {
      version: EngineVersion.OPENSEARCH_2_13,
      domainName: 'opensearch-domain',
      capacity: {
        dataNodes: 1,
        dataNodeInstanceType: 't3.medium.search',
        multiAzWithStandbyEnabled: false,
      },
      ebs: {
        volumeSize: 20,
      },
      enforceHttps: true,
        nodeToNodeEncryption: true,
        encryptionAtRest: {
          enabled: true,
        },
      fineGrainedAccessControl: {
        masterUserName: process.env.OPENSEARCH_USER,
        masterUserPassword: cdk.SecretValue.unsafePlainText(process.env.OPENSEARCH_PASSWORD as string),
      },
      accessPolicies: [
        new iam.PolicyStatement({
          actions: ['es:*'],
          resources: ['*'],
          effect: iam.Effect.ALLOW,
          principals: [new iam.AnyPrincipal()],
        })],
    });

    // // Output the domain dashboard
    new cdk.CfnOutput(this, 'DomainDashboard', {
      value: opensearchDomain.domainEndpoint,
    });

    //analusis lambda role
    const analysisLambdaRole = new iam.Role(this, 'AnalysisLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      // lambda execution role
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
    });

    // // Allow the lambda function to read from the raw data bucket
    rawDataBucket.grantReadWrite(analysisLambdaRole);


    // docker lambda function
    const dockerLambda = new lambda.DockerImageFunction(this, 'DockerLambda', {
      code: lambda.DockerImageCode.fromImageAsset('lib/lambda_docker'),
      role: analysisLambdaRole,
      environment: {
        BUCKET_NAME: rawDataBucket.bucketName,
        OPENSEARCH_ENDPOINT: opensearchDomain.domainEndpoint,
        OPENSEARCH_USER: process.env.OPENSEARCH_USER as string,
        OPENSEARCH_PASSWORD: process.env.OPENSEARCH_PASSWORD as string,
      },

    });

    // =============================================================================
    // Hot Module:
    //
    // The Hot modules reads off a kinesis stream searching for a value to surpass
    //  a certain threshold. Once this threshold is breached, it triggers a lambda
    //  function that enriches the data and fans it out to SNS and DynamoDB.
    //  SNS notifies all subscribers about the breach and DynamoDB powers a simple
    //  Table viewer web application.
    //
    //                                                       +-----------+    +-----------+
    //                                                       |           |    |           |
    //                                                       |    SNS    |    |  Emails   |
    //                                                    +->+           +--->+           |
    //                                                    |  |           |    |           |
    // +-----------+     +-----------+     +-----------+  |  +-----------+    +-----------+
    // |           |     |           |     |           |  |
    // |Raw Stream +---->+Kinesis App+---->+  Lambda   +--+
    // |           |     |           |     |           |  |  +-----------+    +-----------+
    // |           |     |           |     |           |  |  |           |    |           |
    // +-----------+     +-----------+     +-----------+  |  |  DynamoDB |    |  Table    |
    //                                                    +->+           +--->+    Viewer |
    //                                                       |           |    |           |
    //                                                       +-----------+    +-----------+

    // The DynamoDB table that stores anomalies detected by our kinesis analytic app
    const seizuresTable = new dynamodb.Table(this, 'SeizuresTable', {
      partitionKey: { name: 'patientId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'eventTimestamp', type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY // REMOVE FOR PRODUCTION
    })

    // TableViewer is a high level demo construct of a web app that will read and display values from DynamoDB
    const tableViewer = new TableViewer(this, 'TableViewer', {
      title: "Real Time Seizures Table",
      table: seizuresTable,
      sortBy: "-eventTimestamp"
    })

    const seizureNotificationTopic = new Sns.Topic(this, 'SeizureNotificationTopic', {
      displayName: 'Seizure Notification',
    }
    )

    seizureNotificationTopic.addSubscription(new snssub.EmailSubscription('duwalamit@gmail.com'))

    // Lambda function to  sns
    const fanoutLambda = new lambda.Function(this, 'LambdaFanoutFunction',{
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'fanout.handler',
      code: lambda.Code.fromAsset('lib/src'),
      environment: {
        TABLE_NAME: seizuresTable.tableName,
        TOPIC_ARN: seizureNotificationTopic.topicArn
      }
    })

    // grant the lambda function permissions to write to the sns topic
    seizureNotificationTopic.grantPublish(fanoutLambda)
    seizuresTable.grantReadWriteData(fanoutLambda)

    const streamToAnalyticsRole = new iam.Role(this, 'streamToAnalyticsRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com')
    });

    streamToAnalyticsRole.addToPolicy(new iam.PolicyStatement({
      resources: [
        fanoutLambda.functionArn,
        rootStream.streamArn,
      ],
      actions: ['kinesis:*', 'lambda:*'] 
    }));

    const thresholdDetector = new kinesisanalytics.CfnApplication(this, "KinesisAnalyticsApplication", {
      applicationName: 'seizure-detector',
      applicationCode: fs.readFileSync(path.join(__dirname, 'src/app.sql')).toString(),
      inputs: [
          {
              namePrefix: "SOURCE_SQL_STREAM",
              kinesisStreamsInput: {
                  resourceArn: rootStream.streamArn,
                  roleArn: streamToAnalyticsRole.roleArn
              },
              inputParallelism: { count: 1 },
              inputSchema: {
                  recordFormat: {
                      recordFormatType: "JSON",
                      mappingParameters: { jsonMappingParameters: { recordRowPath: "$" } }
                  },
                  recordEncoding: "UTF-8",
                  recordColumns: [
                      {
                          name: "patientId",
                          mapping: "$.patientId",
                          sqlType: "VARCHAR(64)"
                      },
                      {
                          name: "name",
                          mapping: "$.name",
                          sqlType: "VARCHAR(64)"
                      },
                      {
                          name: "age",
                          mapping: "$.age",
                          sqlType: "INTEGER"
                      },
                      {
                          name: "heartRate",
                          mapping: "$.heartRate",
                          sqlType: "INTEGER"
                      },
                      {
                          name: "respiratoryRate",
                          mapping: "$.respiratoryRate",
                          sqlType: "INTEGER"
                      },
                      {
                          name: "oxygenSaturation",
                          mapping: "$.oxygenSaturation",
                          sqlType: "INTEGER"
                      },
                      {
                          name: "seizureDetected",
                          mapping: "$.seizureDetected",
                          sqlType: "BOOLEAN"
                      },
                      {
                          name: "seizureDuration",
                          mapping: "$.seizureDuration",
                          sqlType: "INTEGER"
                      },
                      {
                          name: "seizureSeverity",
                          mapping: "$.seizureSeverity",
                          sqlType: "VARCHAR(32)"
                      },
                      {
                          name: "currentLocation",
                          mapping: "$.currentLocation",
                          sqlType: "VARCHAR(256)"
                      },
                      {
                          name: "hospitalId",
                          mapping: "$.hospitalId",
                          sqlType: "VARCHAR(32)"
                      },
                      {
                          name: "eventTimestamp",
                          mapping: "$.eventTimestamp",
                          sqlType: "VARCHAR(32)"
                      }
                  ]
              }
          }
      ]
  });
  thresholdDetector.node.addDependency(streamToAnalyticsRole);
  
  const thresholdDetectorOutput = new kinesisanalytics.CfnApplicationOutput(this, 'AnalyticsAppOutput', {
      applicationName: 'seizure-detector',
      output: {
          name: "SEIZURE_DETECTION_STREAM",
          lambdaOutput: {
              resourceArn: fanoutLambda.functionArn,
              roleArn: streamToAnalyticsRole.roleArn
          },
          destinationSchema: {
              recordFormatType: "JSON"
          }
      }
  });
  thresholdDetectorOutput.node.addDependency(thresholdDetector);


  }
}
