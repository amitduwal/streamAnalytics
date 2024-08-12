import json
import time
import uuid
import random
from datetime import datetime
from pprint import pprint

import boto3

from faker import Faker

# This boots up the kinesis analytic application so you don't have to click "run" on the kinesis analytics console
try:
    kinesisanalytics = boto3.client("kinesisanalyticsv2")
    kinesisanalytics.start_application(
        ApplicationName="seizure-detector",
        RunConfiguration={
            'SqlRunConfigurations': [
                {
                    'InputId': '1.1',
                    'InputStartingPositionConfiguration': {
                        'InputStartingPosition': 'NOW'
                    }
                },
            ]
        }
    )
    print("Giving 30 seconds for the kinesis analytics application to boot")
    time.sleep(30)
except kinesisanalytics.exceptions.ResourceInUseException:
    print("Application already running, skipping start up step")

rootSteamName = input("Please enter the stream name that was outputted from cdk deploy - (StreamingSolutionWithCdkStack.RootStreamName): ")
kinesis = boto3.client("kinesis")
fake = Faker()


while True:
    # Simulating vital signs and seizure detection
    seizure_event = random.random() < 0.20  # Randomly decide if a seizure occurs
    payload = {
        "patientId": str(uuid.uuid4()),
        "name": fake.name(),
        "age": fake.random_int(min=18, max=85, step=1),
        "heartRate": random.randint(60, 180),  # BPM, elevated if seizure occurs
        "respiratoryRate": random.randint(12, 40),  # Breaths per minute, elevated if seizure occurs
        "oxygenSaturation": random.randint(80, 100),  # SpO2 percentage, could drop during a seizure
        "seizureDetected": seizure_event,
        "seizureDuration": random.randint(30, 300) if seizure_event else 0,  # Duration in seconds if seizure occurs
        "seizureSeverity": random.choice(["Mild", "Moderate", "Severe"]) if seizure_event else "None",
        "emergencyContact": {
            "name": fake.name(),
            "relationship": random.choice(["Spouse", "Parent", "Child", "Guardian"]),
            "phone": fake.phone_number(),
            "email": fake.email()
        },
        "location": {
            "currentLocation": fake.address(),
            "hospitalId": fake.swift()
        },
        "createdAt": str(datetime.now()),
        "eventTimestamp": str(datetime.now())
    }

    # Print the generated data (for testing)
    # pprint(payload)

    if seizure_event:
        print("Patient with Seizure Generated")
    else:
        print("Patient Generated")
    
    # Send the data to the Kinesis stream
    response = kinesis.put_record(
        StreamName= rootSteamName, Data=json.dumps(payload), PartitionKey="abc"
    )
    # pprint(response)
    
    # Wait 1 second before generating the next record
    time.sleep(0.5)