# Built-in
import json
# Third party
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('dollar')
response = {
    'body': '',
    'statusCode': 404
}


def get_item(key: dict) -> dict:
    return table.get_item(Key=key).get('Item', {})
    

def lambda_handler(event, context):
    
    if not 'queryStringParameters' in event:
        response['body'] = 'Missing query string'
    
    else:

        date = event['queryStringParameters'].get('date')
    
        if (item := get_item({'date': date})):
            response['body'] = item
            response['statusCode'] = 200
    
        else:
            response['body'] = {
                "error": "There isn't any value associate with that date"
            }
        
        response['body'] = json.dumps(response['body'])

    return response
