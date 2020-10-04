import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id='~~~~~~~~~~~~~', aws_secret_access_key='~~~~~~~~~~~~~')

try:
	s3.create_bucket(Bucket='cs1660project-jpm160', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}) 
except:
	print("this may already exist")

bucket = s3.Bucket("cs1660project-jpm160")
bucket.Acl().put(ACL='public-read')

body = open('ccDataStorage.py', 'rb')

o = s3.Object('cs1660project-jpm160', 'test').put(Body=body )
s3.Object('cs1660project-jpm160', 'test').Acl().put(ACL='public-read')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dyndb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='~~~~~~~~~~~~~', aws_secret_access_key='~~~~~~~~~~~~~')

try:
	table = dyndb.create_table(
		TableName='csDataSciTable', KeySchema=[
			{'AttributeName': 'PartitionKey', 'KeyType': 'HASH'}, 
			{'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
	], 
		AttributeDefinitions=[
		{'AttributeName': 'PartitionKey', 'AttributeType': 'S'}, 
		{'AttributeName': 'RowKey','AttributeType': 'S'},
	],
 		ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
)
except:
	table = dyndb.Table("csDataSciTable")

table.meta.client.get_waiter('table_exists').wait(TableName='csDataSciTable')
print(table.item_count)	

#removed <, 'rb'> from open statement
with open('experiments.csv') as csvfile: 
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	for item in csvf:
		print(item)
		#body = open('~/Desktop/cs1660DataStorage'+item[3], 'rb') 
		s3.Object('cs1660project-jpm160', item[3]).put(Body=body )
		md = s3.Object('cs1660project-jpm160', item[3]).Acl().put(ACL='public-read')
		
		url = " https://s3-us-west-2.amazonaws.com/cs1660project-jpm160/"+item[3] 
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description' : item[4], 'date' : item[2], 'url':url}

		try: 
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")

#response = table.get_item( Key={'PartitionKey': 'experiment2','RowKey': '3' })
#item = response['Item'] 
#print(item)			










