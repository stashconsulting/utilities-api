import json
from datetime import datetime
import boto3

dynamo = boto3.resource('dynamodb')
table = dynamo.Table('dollar')

response = {
    'statusCode': 404,
    'body': ''
}
expected_keys = ["sell", "date", "until", "buy", "from"]


def convert_to_epoch(date: str) -> str:
    return str(
        (
            datetime.strptime(date, "%Y-%m-%d")
            - datetime(1970, 1, 1)
        ).total_seconds()
    )


def lambda_handler(event, context):

    if(data := event.get('body')):

        data = json.loads(data)
        submited_keys = list(data.keys())

        if submited_keys == expected_keys:
            date = datetime.strptime(data['date'], "%Y-%m-%d")

            data['year'] = int(date.year)
            data['month'] = int(date.month)

            data['date'] = convert_to_epoch(data['date'])
            data['from'] = convert_to_epoch(data['from'])
            data['until'] = convert_to_epoch(data['until'])

            table.put_item(Item=data)
            response['body'] = "Inserted!"
            response['statusCode'] = 200

        else:
            response['body'] = (
                f"The expected parameters are {expected_keys} "
                f"but you sent {submited_keys} "
            )
    else:
        response['body'] = (
            "Missing Body"
            f"The expected parameters are {expected_keys}"
        )

    return response
