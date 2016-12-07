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


table.put_item(Item={
    'jobid': '07',
    'intermediate_s3': 'container-clouds-intermediate',
    'db_table': 'demo1'
})

GLOBAL = {'inp': {'key': 'nums.txt', 'bucket': 'container-clouds-input'}}

table.update_item(
    Key={'jobid': '07'},
    UpdateExpression='SET inputs = :val',
    ExpressionAttributeValues={':val': {'inp': {'key': 'nums.txt', 'bucket': 'container-clouds-input'}}}
)
