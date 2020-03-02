# Built-in
import json
from datetime import datetime
from decimal import Decimal
# Third party
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('dollar')
response = {
    'body': '',
    'statusCode': 404
}
valid_modifiers = ['eq', 'gte', 'lte', "between"]
from_epoch = ['from_date', 'until_date', 'date']
    

def query_items(
    index_value: int=0, sort_value:str='', *, fe=[], **kwargs) -> list:

    # get the key so we don't have to pass it
    primary_index = table.key_schema[0]['AttributeName']
    secondary_index = table.key_schema[1]['AttributeName']

    if not (index_value or sort_value or fe):
        raise Exception("Expected index_value or sort_value or fe")
    # Allow to query only by the index key
    if index_value:
        query = Key(primary_index).eq(index_value)
    
        if sort_value:
            query = query & Key(secondary_index).eq(sort_value)
    if fe:
        _fe = fe[0] 
        query =  (
            Key(_fe['key']).__getattribute__(_fe['comparision'])(*_fe['values'])
        )

    for arg in fe[1:]:
        query = (
            query
            & Key(arg['key'])
            .__getattribute__(arg['comparision'])
            (*arg['values'])
        )

    return table.scan(
        FilterExpression=query
    ).get('Items', [])


def split_time_into_objects(date):
    
    date_object = datetime.strptime(date, '%Y-%m-%d')
    return (
        date_object.year,
        date_object.month,
        str((date_object - datetime(1970,1,1)).total_seconds()) # Epoch time
    )


def lambda_handler(event, context):
    
    if not event.get('queryStringParameters'):
        response['body'] = 'Missing query string'
    
    else:
        parameters = event['queryStringParameters']

        if (date := parameters.get('date')):
            
            year, _, epoch_date = split_time_into_objects(date)

            if (
                item := query_items(
                    year, 
                    fe=[{
                        'key': 'date',
                        'values': [epoch_date],
                        'comparision': 'eq'
                    }]
                )
            ):
                response['body'] = item
                response['statusCode'] = 200
        
            else:
                response['body'] = {
                    "error": "There isn't any value associate with that date"
                }

        elif ((from_date := parameters.get('from_date')) and
            (until_date := parameters.get('until_date'))):


            _, _, from_epoch_date = split_time_into_objects(from_date)
            _, _, until_epoch_date = split_time_into_objects(until_date)
            
            # In between query across two different fields
            fe=[
                {
                    'key': 'from_date',
                    'values': [from_epoch_date],
                    'comparision': 'gte'
                },
                {
                    'key': 'until_date',
                    'values': [until_epoch_date],
                    'comparision': 'lte'
                },
            ]
            
            if (items := query_items(fe=fe)):
                response['body'] = items
                response['statusCode'] = 200

            else:
                response['body'] = {
                    "error": "There isn't any value associate with that date"
                }
            
        elif (from_date := parameters.get('from_date')):

            year, _, epoch_date = split_time_into_objects(from_date)
            if (items := query_items(
                    year,
                    fe=[{
                        'key': 'from_date',
                        'values': [epoch_date],
                        'comparision': 'gte'
                    }]
                )
            ):
                response['body'] = items
                response['statusCode'] = 200

            else:
                response['body'] = {
                    "error": "There isn't any value associate with that date"
                }

        elif (until := parameters.get('until_date')):

            year, _, epoch_date = split_time_into_objects(until)

            if (items := query_items(
                    year,
                    fe=[{
                        'key': 'until_date',
                        'values': [epoch_date],
                        'comparision': 'lte'
                    }]
                )
            ):
                response['body'] = items
                response['statusCode'] = 200

            else:
                response['body'] = {
                    "error": "There isn't any value associate with that date"
                }
        else:

            response['body'] = (
                'Specify one of the following '
                f'parameters {from_epoch}'
            )

        if isinstance(response['body'], list):
            for index, item in enumerate(response['body']):
                for key, value in item.items():
                    if isinstance(value, Decimal):
                        value = int(value)

                    response['body'][index][key] = value

    response['body'] = json.dumps(response['body'])

    return response
