import boto3
import gzip
from io import BytesIO
import json
from opensearchpy import OpenSearch,RequestsHttpConnection, helpers
from datetime import datetime
import os

s3 = boto3.client("s3")

host = os.environ["OPENSEARCH_ENDPOINT"] # cluster endpoint, for example: my-test-domain.us-east-1.es.amazonaws.com
port = 443
region = 'us-west-2' # e.g. us-west-1
credentials = (os.environ["OPENSEARCH_USER"],os.environ["OPENSEARCH_PASSWORD"])

client = OpenSearch(
    hosts = [f'{host}:{port}'],
    http_auth = credentials,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

def create_index(index_name):
    index_name = index_name
    if client.indices.exists(index=index_name):
        # # delete the index
        # client.indices.delete(index=index_name)
        print(f"Index already exists.")
        return
    
    
    # Define the index settings and mappings
    index_body = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 1
        },
        'mappings': {
            'properties': {
                'patientId': {'type': 'keyword'},
                'name': {'type': 'text'},
                'age': {'type': 'integer'},
                'heartRate': {'type': 'integer'},
                'respiratoryRate': {'type': 'integer'},
                'oxygenSaturation': {'type': 'integer'},
                'seizureDetected': {'type': 'boolean'},
                'seizureDuration': {'type': 'integer'},
                'seizureSeverity': {'type': 'keyword'},
                'emergencyContact': {
                    'properties': {
                        'name': {'type': 'text'},
                        'relationship': {'type': 'keyword'},
                        'phone': {'type': 'keyword'},
                        'email': {'type': 'keyword'}
                    }
                },
                'location': {
                    'properties': {
                        'currentLocation': {'type': 'text'},
                        'hospitalId': {'type': 'keyword'}
                    }
                },
                'createdAt': {
                    'type': 'date'
                },
                'eventTimestamp': {
                    'type': 'date'
                }
            }
        }
    }

    # Create the index
    client.indices.create(index=index_name, body=index_body)
    # index to store file names
    index_body = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 1
        },
        'mappings': {
            'properties': {
                'filename': {'type': 'keyword'},
            }
        }
    }
    client.indices.create(index='file-names', body=index_body)

    print(f"Index {index_name} created successfully.")
    return

def format_date(date_str):
    # Parse the date string and remove microseconds
    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    return date_obj.strftime("%Y-%m-%dT%H:%M:%S")  # ISO format without microseconds

def prepare_data_for_indexing(docs, index):
    for i in range(len(docs)):
        docs[i]["_index"] = index
        docs[i]['createdAt'] = format_date(docs[i]['createdAt'])
        docs[i]['eventTimestamp'] = format_date(docs[i]['eventTimestamp'])
    return docs

def process_gz_file(bucket_name, key):
    
    # Get the GZ file from S3
    response = s3.get_object(Bucket=bucket_name, Key=key)
    
    # Read and extract the GZ file
    with gzip.GzipFile(fileobj=response["Body"]) as gzipfile:
        content = gzipfile.read()

    json_objects = str(content).split("}{")
     
    processed_jsons = []  
    for json_object in json_objects:
            try:
                json_object = '{' + json_object + '}'
                data = json.loads(json_object)
                processed_jsons.append(data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue  # Skip invalid JSON entries
        
    return processed_jsons

def check_file_processed(filename):
    # Check if the file has already been indexed in the file-names index
    search_body = {
        "query": {
            "term": {
                "filename": filename
            }
        }
    }
    
    response = client.search(index="file-names", body=search_body)
    return len(response['hits']['hits']) > 0

def index_filename(filename):
    # Index the filename into the file-names index
    doc = {
        "filename": filename
    }
    client.index(index="file-names", body=doc)
    

def lambda_handler(event, context):
    # get bucket name from environment variable
    bucket_name = os.environ['BUCKET_NAME']
    prefix = 'raw/'  
    
    result = s3.list_objects(Bucket=bucket_name, Prefix = prefix)
    processed_jsons = []
    index_name = 'patient-data'
    create_index(index_name)
    # Process each file in the result
    if 'Contents' in result:
        for obj in result['Contents']:
            key = obj['Key']
            if key.endswith('.gz'):  # Check if the file is a TGZ file
                print(f"Processing file: {key}")
                

                 # Check if the file has been processed already
                if not check_file_processed(key):
                    processed_jsons = process_gz_file(bucket_name, key)
                    # Index the filename to track that it has been processed
                    post_processed_jsons = prepare_data_for_indexing(processed_jsons, index_name)
                    response = helpers.bulk(client, post_processed_jsons, max_retries=3)
                    print(response)
                    index_filename(key)
                    print(f"File {key} processed and indexedsuccessfully.")
                else:
                    print(f"File {key} has already been processed.")
               
    return {"statusCode": 200, "body":"Files Successfully Processed"}
    
                
    
