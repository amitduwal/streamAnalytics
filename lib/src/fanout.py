"""fanout.py reads from kinesis analytic output and fans out to SNS"""

import base64
import json
import os
from datetime import datetime

import boto3

ddb = boto3.resource("dynamodb")
sns = boto3.client("sns")


def handler(event, context):
    payload = event["records"][0]["data"]
    data_dump = base64.b64decode(payload).decode("utf-8")
    data = json.loads(data_dump)
    # print(data, flush=True)
    print("A new seizure event has been detected", flush=True)

    table = ddb.Table(os.environ["TABLE_NAME"])

    item = {
    "patientId": data["patientId"],
    "name": data["name"],
    "age": data["age"],
    "heartRate": data["heartRate"],
    "respiratoryRate": data["respiratoryRate"],
    "oxygenSaturation": data["oxygenSaturation"],
    "seizureDetected": data["seizureDetected"],
    "seizureDuration": data["seizureDuration"],
    "seizureSeverity": data["seizureSeverity"],
    "currentLocation": data["currentLocation"], 
    "hospitalId": data["hospitalId"], 
    "eventTimestamp": data["eventTimestamp"],
    "inspectedAt": str(datetime.now())
    }


    # Best effort, Kinesis Analytics Output is "at least once" delivery, meaning this lambda function can be invoked multiple times with the same item
    # We can ensure idempotency with a condition expression
    try:
        result = table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(inspectedAt)"
        )
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        print("Item already processed", flush=True)
        return {"statusCode": 409, "body": "Item already processed"} 
    
    sns.publish(
        TopicArn=os.environ["TOPIC_ARN"],
        Message= f"""🚨 Seizure Alert 🚨

        Patient with seizure detected!

        Patient Name: {data['name']}
        Patient ID: {data['patientId']}
        Age: {data['age']}
        Heart Rate: {data['heartRate']} BPM
        Respiratory Rate: {data['respiratoryRate']} breaths/min
        Oxygen Saturation: {data['oxygenSaturation']}%
        Seizure Severity: {data['seizureSeverity']}
        Seizure Duration: {data['seizureDuration']} seconds
        Location: {data['currentLocation']}
        Event Timestamp: {data['eventTimestamp']}
        
        Please take immediate action.
        """,

    )

    return {"statusCode": 200, "body": json.dumps(item)}