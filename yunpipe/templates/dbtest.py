import boto3

db = boto3.resource('dynamodb')

table = db.create_table(
    TableName='demo',
    KeySchema=[{'AttributeName': 'jobid', 'KeyType': 'HASH'}],
    AttributeDefinitions=[{'AttributeName': 'jobid', 'AttributeType': 'S'}],
    ProvisionedThroughput={'ReadCapacityUnits': 3, 'WriteCapacityUnits': 1}
)

table = db.Table('demo')

table.put_item(Item={'jobid': '1', 'input1': {'key': 'aaa', 'bucket': '123'}})

response = table.get_item(Key={'jobid': '1'})
print(response['Item'])

table.update_item(
    Key={'jobid': '1'},
    UpdateExpression='SET input2 = :val',
    ExpressionAttributeValues={':val': {'key': 'bbb', 'bucket': '456'}}
)

