import json
import urllib3
from boto3.dynamodb.types import TypeDeserializer

URL = "<ES-ENDPOINT>/posts/_doc/{0}"
headers = {'Content-Type': 'application/json'}

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}

def lambda_handler(event, context):
    print(event)
    output = {}
    for x in event["Records"]:
        event_type = x['eventName']
        input_id = x["dynamodb"]["Keys"]["<UniqueIdKey>"]["S"]
    
        if event_type == "REMOVE":
            print("Removing id: " + str(input_id))
            http = urllib3.PoolManager()
            r = http.request('DELETE', URL.format(input_id), headers=headers)
            output = json.loads(r.data.decode('utf-8'))
            print(output)
    
        elif event_type == "INSERT":
            parsed_event_data = from_dynamodb_to_json(x["dynamodb"]["NewImage"])
            print("inserting into index, id: " + str(input_id))
            http = urllib3.PoolManager()
            encoded_data = json.dumps(parsed_event_data).encode('utf-8')
            r = http.request('POST', URL.format(input_id), headers=headers, body=encoded_data)
            output = json.loads(r.data.decode('utf-8'))
            print(output)
        else:
            print("Invalid event type: "+str(event_type))
    
    return {
        'statusCode': 200,
        'body': json.dumps(output)
