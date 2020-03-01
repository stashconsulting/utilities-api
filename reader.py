# Built-in
import re
import json
# Third party
import boto3

s3 = boto3.resource('s3')


def read_document(doc: str):
    with open(doc, "r") as fi:
        return ''.join([line for line in fi])


def extract_values(text: str):

    data = re.findall(
        r"RD\$(.+?)\/",
        text
    )

    if (_from := re.search(r"cierre\sdel\s(.*?),", text)):
        data.append(_from.group())

    if (until := re.search(r"hasta\sel\s.*?(\d.+?)\.", text)):
        data.append(until.group())

    return data


def lambda_handler(event, context):
    s3_data = event['Records'][0]['s3']
    bucket = s3_data['bucket']
    _object = s3_data['object']

    obj = s3.Object(bucket['name'], _object['key'])
    data = obj.get()['Body'].read().decode('utf-8')
    data = extract_values(data)

    return json.dumps({
        'statusCode': 200,
        'body': data
    })


def main():
    return extract_values(read_document('tasaus_mc.txt'))


if __name__ == "__main__":
    main()
